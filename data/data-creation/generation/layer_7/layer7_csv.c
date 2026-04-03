/*
 * layer7_csv.c - Layer 7: CSV I/O, database loading, statistics
 *
 * Loads reference databases, reads the enriched input CSV, dispatches
 * each record to layer7_calculate_record(), writes the output CSV.
 */
#include "layer7_calculation.h"
#include <ctype.h>
#include "raw_materials/raw_materials.h"
#include "processing/material_processing.h"
#include "packaging/packaging.h"
#include "aware/aware.h"

/* CSV field extraction -- handles quoted fields containing commas */
static const char *next_csv_field(const char *p, char *buf, size_t bsz)
{
    size_t wi = 0;
    int in_q = 0;
    if (!p || !buf) return NULL;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p == '"') { in_q = 1; p++; }
    while (*p) {
        if (in_q) {
            if (*p == '"' && *(p + 1) == '"') {
                if (wi < bsz - 1) buf[wi++] = '"';
                p += 2;
            } else if (*p == '"') { in_q = 0; p++; }
            else { if (wi < bsz - 1) buf[wi++] = *p; p++; }
        } else {
            if (*p == ',') { p++; break; }
            if (*p == '\n' || *p == '\r') { p++; break; }
            if (wi < bsz - 1) buf[wi++] = *p;
            p++;
        }
    }
    buf[wi] = '\0';
    return (*p || wi > 0) ? p : NULL;
}

/* Update running min/max/sum statistics */
static void update_stats(Layer7Stats *s, const Layer7ProductRecord *r,
                         int first)
{
    s->wf_raw_sum   += r->wf_raw_materials_m3_world_eq;
    s->wf_proc_sum  += r->wf_processing_m3_world_eq;
    s->wf_pack_sum  += r->wf_packaging_m3_world_eq;
    s->wf_total_sum += r->wf_total_m3_world_eq;
    if (first) {
        s->wf_raw_min = s->wf_raw_max = r->wf_raw_materials_m3_world_eq;
        s->wf_proc_min = s->wf_proc_max = r->wf_processing_m3_world_eq;
        s->wf_pack_min = s->wf_pack_max = r->wf_packaging_m3_world_eq;
        s->wf_total_min = s->wf_total_max = r->wf_total_m3_world_eq;
    } else {
        if (r->wf_raw_materials_m3_world_eq < s->wf_raw_min)
            s->wf_raw_min = r->wf_raw_materials_m3_world_eq;
        if (r->wf_raw_materials_m3_world_eq > s->wf_raw_max)
            s->wf_raw_max = r->wf_raw_materials_m3_world_eq;
        if (r->wf_processing_m3_world_eq < s->wf_proc_min)
            s->wf_proc_min = r->wf_processing_m3_world_eq;
        if (r->wf_processing_m3_world_eq > s->wf_proc_max)
            s->wf_proc_max = r->wf_processing_m3_world_eq;
        if (r->wf_packaging_m3_world_eq < s->wf_pack_min)
            s->wf_pack_min = r->wf_packaging_m3_world_eq;
        if (r->wf_packaging_m3_world_eq > s->wf_pack_max)
            s->wf_pack_max = r->wf_packaging_m3_world_eq;
        if (r->wf_total_m3_world_eq < s->wf_total_min)
            s->wf_total_min = r->wf_total_m3_world_eq;
        if (r->wf_total_m3_world_eq > s->wf_total_max)
            s->wf_total_max = r->wf_total_m3_world_eq;
    }
}

/* Write statistics summary as JSON */
int layer7_write_statistics(const Layer7Stats *s, const char *path)
{
    double n;
    FILE *f = fopen(path, "w");
    if (!f) {
        layer7_log_error("write_statistics", "Cannot open file", -1);
        return -1;
    }
    n = (double)(s->total_records > 0 ? s->total_records : 1);
    fprintf(f, "{\n  \"metadata\": {\n");
    fprintf(f, "    \"generated_at\": \"%s\",\n", s->calculation_timestamp);
    fprintf(f, "    \"layer\": 7,\n");
    fprintf(f, "    \"description\": \"Water footprint calculation statistics\"\n");
    fprintf(f, "  },\n  \"processing_summary\": {\n");
    fprintf(f, "    \"total_records_processed\": %d\n", s->total_records);
    fprintf(f, "  },\n  \"water_footprint_statistics\": {\n");
    fprintf(f, "    \"wf_raw_materials_m3_world_eq\": "
               "{\"mean\": %.6f, \"min\": %.6f, \"max\": %.6f},\n",
            s->wf_raw_sum / n, s->wf_raw_min, s->wf_raw_max);
    fprintf(f, "    \"wf_processing_m3_world_eq\": "
               "{\"mean\": %.6f, \"min\": %.6f, \"max\": %.6f},\n",
            s->wf_proc_sum / n, s->wf_proc_min, s->wf_proc_max);
    fprintf(f, "    \"wf_packaging_m3_world_eq\": "
               "{\"mean\": %.6f, \"min\": %.6f, \"max\": %.6f},\n",
            s->wf_pack_sum / n, s->wf_pack_min, s->wf_pack_max);
    fprintf(f, "    \"wf_total_m3_world_eq\": "
               "{\"mean\": %.6f, \"min\": %.6f, \"max\": %.6f}\n",
            s->wf_total_sum / n, s->wf_total_min, s->wf_total_max);
    fprintf(f, "  }\n}\n");
    fclose(f);
    return 0;
}

/* Load all reference databases */
static int load_databases(const Layer7Config *cfg,
                          MaterialDatabase *mat_db,
                          MaterialProcessDatabase *combo_db,
                          AwareDatabase *a_agri, AwareDatabase *a_non)
{
    printf("Loading reference databases...\n");
    raw_materials_init_database(mat_db);
    if (raw_materials_load_csv(mat_db, cfg->materials_path) < 0) {
        layer7_log_error("load_databases", "materials DB failed", -1);
        return -1;
    }
    printf("  Materials: %zu entries\n", mat_db->count);

    processing_init_combo_database(combo_db);
    if (processing_load_combinations_csv(
            combo_db, cfg->processing_combinations_path) < 0) {
        layer7_log_error("load_databases", "processing combos failed", -1);
        return -1;
    }
    printf("  Processing combos: %zu entries\n", combo_db->count);

    aware_init_database(a_agri, AWARE_GLOBAL_FALLBACK_AGRI);
    if (aware_load_csv(a_agri, cfg->aware_agri_path) < 0) {
        layer7_log_error("load_databases", "AWARE agri failed", -1);
        return -1;
    }
    aware_load_aliases(a_agri);
    printf("  AWARE agri: %zu entries\n", a_agri->count);

    aware_init_database(a_non, AWARE_GLOBAL_FALLBACK_NONAGRI);
    if (aware_load_csv(a_non, cfg->aware_nonagri_path) < 0) {
        layer7_log_error("load_databases", "AWARE nonagri failed", -1);
        return -1;
    }
    aware_load_aliases(a_non);
    printf("  AWARE nonagri: %zu entries\n", a_non->count);
    return 0;
}

/* Parse one input CSV row into a Layer7ProductRecord */
static const char *parse_input_row(const char *line, Layer7ProductRecord *rec)
{
    const char *p = line;
    char field[L7_MAX_FIELD_LENGTH];
    p = next_csv_field(p, rec->record_id, sizeof(rec->record_id));
    if (p) p = next_csv_field(p, rec->materials_json,
                              sizeof(rec->materials_json));
    if (p) p = next_csv_field(p, rec->material_weights_kg_json,
                              sizeof(rec->material_weights_kg_json));
    if (p) {
        p = next_csv_field(p, field, sizeof(field));
        rec->total_weight_kg = atof(field);
    }
    if (p) p = next_csv_field(p, rec->preprocessing_steps_json,
                              sizeof(rec->preprocessing_steps_json));
    if (p) p = next_csv_field(p, rec->transport_legs_json,
                              sizeof(rec->transport_legs_json));
    if (p) p = next_csv_field(p, rec->packaging_categories_json,
                              sizeof(rec->packaging_categories_json));
    if (p) p = next_csv_field(p, rec->packaging_masses_kg_json,
                              sizeof(rec->packaging_masses_kg_json));
    return p;
}

/* Main CSV processing loop */
int layer7_process_csv(const Layer7Config *config, Layer7Stats *stats)
{
    MaterialDatabase        mat_db;
    MaterialProcessDatabase combo_db;
    AwareDatabase           aware_agri, aware_nonagri;
    FILE *fin = NULL, *fout = NULL;
    char line[L7_MAX_FIELD_LENGTH * 4];
    int rec_count = 0, err_count = 0;

    memset(stats, 0, sizeof(Layer7Stats));
    if (load_databases(config, &mat_db, &combo_db,
                       &aware_agri, &aware_nonagri) != 0)
        return -1;

    fin = fopen(config->input_path, "r");
    if (!fin) {
        layer7_log_error("process_csv", "Cannot open input CSV", -1);
        return -1;
    }
    fout = fopen(config->output_path, "w");
    if (!fout) {
        layer7_log_error("process_csv", "Cannot open output CSV", -1);
        fclose(fin); return -1;
    }
    if (!fgets(line, (int)sizeof(line), fin)) {
        layer7_log_error("process_csv", "Cannot read input header", -1);
        fclose(fin); fclose(fout); return -1;
    }
    fprintf(fout, "record_id,wf_raw_materials_m3_world_eq,"
                  "wf_processing_m3_world_eq,wf_packaging_m3_world_eq,"
                  "wf_total_m3_world_eq,calculation_timestamp,"
                  "calculation_version\n");
    printf("Processing records...\n");

    while (fgets(line, (int)sizeof(line), fin)) {
        Layer7ProductRecord rec;
        memset(&rec, 0, sizeof(rec));
        parse_input_row(line, &rec);
        if (strlen(rec.record_id) == 0) { err_count++; continue; }

        if (layer7_calculate_record(&rec, &mat_db, &combo_db,
                                    &aware_agri, &aware_nonagri,
                                    config->verbose) != 0) {
            layer7_log_error("process_csv", "Calc failed", rec_count);
            err_count++; continue;
        }
        if (config->enable_validation &&
            (rec.wf_raw_materials_m3_world_eq < 0.0 ||
             rec.wf_processing_m3_world_eq < 0.0 ||
             rec.wf_packaging_m3_world_eq < 0.0))
            layer7_log_warning("process_csv", "Negative WF component",
                               rec_count);
        if (config->enable_statistics)
            update_stats(stats, &rec, rec_count == 0);

        fprintf(fout, "%s,%.6f,%.6f,%.6f,%.6f,%s,%s\n",
                rec.record_id, rec.wf_raw_materials_m3_world_eq,
                rec.wf_processing_m3_world_eq,
                rec.wf_packaging_m3_world_eq,
                rec.wf_total_m3_world_eq,
                rec.calculation_timestamp, rec.calculation_version);
        rec_count++;
        if (config->verbose) layer7_print_progress(rec_count, 50000, 10);
    }

    fclose(fin); fclose(fout);
    stats->total_records = rec_count;
    if (rec_count > 0) {
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        strftime(stats->calculation_timestamp,
                 sizeof(stats->calculation_timestamp),
                 "%Y-%m-%dT%H:%M:%SZ", t);
    }
    printf("\nCalculation complete. Records: %d, Errors: %d\n",
           rec_count, err_count);

    if (config->enable_statistics && rec_count > 0) {
        double n = (double)rec_count;
        printf("\nWater Footprint Summary (m3 world-eq):\n");
        printf("  Raw materials: mean=%.4f min=%.4f max=%.4f\n",
               stats->wf_raw_sum / n, stats->wf_raw_min, stats->wf_raw_max);
        printf("  Processing:    mean=%.4f min=%.4f max=%.4f\n",
               stats->wf_proc_sum / n, stats->wf_proc_min, stats->wf_proc_max);
        printf("  Packaging:     mean=%.4f min=%.4f max=%.4f\n",
               stats->wf_pack_sum / n, stats->wf_pack_min, stats->wf_pack_max);
        printf("  Total:         mean=%.4f min=%.4f max=%.4f\n",
               stats->wf_total_sum / n, stats->wf_total_min,
               stats->wf_total_max);
    }

    raw_materials_free_database(&mat_db);
    processing_free_combo_database(&combo_db);
    aware_free_database(&aware_agri);
    aware_free_database(&aware_nonagri);
    return (err_count > 0 && rec_count == 0) ? -1 : 0;
}
