/*
 * layer7_calculation.c - Layer 7: Per-record water footprint calculation
 *
 * Implements the core per-record water footprint formula:
 *   WF_raw  = sum(weight_i * WU_material_i * AWARE_agri_i)
 *   WF_proc = sum(weight_m * WU_process_p  * AWARE_nonagri_p)
 *   WF_pack = sum(mass_j   * WU_packaging_j)  [no AWARE]
 *   WF_total = WF_raw + WF_proc + WF_pack
 *
 * No transport, no 1.02 adjustment. Unit: m3 world-eq.
 */

#include "layer7_calculation.h"
#include <ctype.h>

/* Water footprint C modules */
#include "raw_materials/raw_materials.h"
#include "processing/material_processing.h"
#include "packaging/packaging.h"
#include "aware/aware.h"

/*
 * layer7_calculate_record - Calculate water footprint for one record.
 *
 * Database pointers are void* in the header; cast to concrete types here.
 */
int layer7_calculate_record(Layer7ProductRecord *rec,
                            const void *mat_db_v,
                            const void *combo_db_v,
                            const void *aware_agri_v,
                            const void *aware_nonagri_v,
                            int verbose)
{
    const MaterialDatabase *mat_db =
        (const MaterialDatabase *)mat_db_v;
    const MaterialProcessDatabase *combo_db =
        (const MaterialProcessDatabase *)combo_db_v;
    const AwareDatabase *aware_agri =
        (const AwareDatabase *)aware_agri_v;
    const AwareDatabase *aware_nonagri =
        (const AwareDatabase *)aware_nonagri_v;

    double wf_raw  = 0.0;
    double wf_proc = 0.0;
    double wf_pack = 0.0;

    /* -- 1. Raw materials ------------------------------------------ */
    {
        char mat_names[L7_MAX_MATERIALS][L7_MAX_ITEM_LEN];
        double mat_weights[L7_MAX_MATERIALS];

        int n_names = layer7_parse_string_array(
            rec->materials_json, mat_names, L7_MAX_MATERIALS);
        int n_weights = layer7_parse_double_array(
            rec->material_weights_kg_json, mat_weights, L7_MAX_MATERIALS);
        int n = n_names < n_weights ? n_names : n_weights;

        for (int i = 0; i < n; i++) {
            int idx = raw_materials_find_by_name(mat_db, mat_names[i]);
            if (idx < 0) {
                if (verbose)
                    layer7_log_warning("calculate_record",
                        "material not found in water DB", -1);
                continue;
            }
            double wu = raw_materials_get_water_consumption(
                mat_db, (size_t)idx);
            if (wu < 0.0) continue;

            /* Extract origin country from transport_legs */
            char country[L7_MAX_ITEM_LEN];
            double aware_cf = AWARE_GLOBAL_FALLBACK_AGRI;
            if (layer7_extract_transport_country(
                    rec->transport_legs_json, i,
                    country, sizeof(country)) == 0) {
                char *resolved = aware_extract_country(
                    aware_agri, country);
                if (resolved) {
                    aware_cf = aware_get_factor(aware_agri, resolved);
                    free(resolved);
                }
            }
            wf_raw += mat_weights[i] * wu * aware_cf;
        }
    }

    /* -- 2. Processing --------------------------------------------- */
    {
        char mat_names[L7_MAX_MATERIALS][L7_MAX_ITEM_LEN];
        double mat_weights[L7_MAX_MATERIALS];
        char step_names[L7_MAX_PROCESSING_STEPS][L7_MAX_ITEM_LEN];

        int n_mats = layer7_parse_string_array(
            rec->materials_json, mat_names, L7_MAX_MATERIALS);
        int n_weights = layer7_parse_double_array(
            rec->material_weights_kg_json, mat_weights, L7_MAX_MATERIALS);
        int n_steps = layer7_parse_string_array(
            rec->preprocessing_steps_json, step_names,
            L7_MAX_PROCESSING_STEPS);
        int n_m = n_mats < n_weights ? n_mats : n_weights;

        for (int m = 0; m < n_m; m++) {
            char country[L7_MAX_ITEM_LEN];
            double aware_cf = AWARE_GLOBAL_FALLBACK_NONAGRI;
            if (layer7_extract_transport_country(
                    rec->transport_legs_json, m,
                    country, sizeof(country)) == 0) {
                char *resolved = aware_extract_country(
                    aware_nonagri, country);
                if (resolved) {
                    aware_cf = aware_get_factor(
                        aware_nonagri, resolved);
                    free(resolved);
                }
            }
            for (int s = 0; s < n_steps; s++) {
                double wu = processing_get_water_consumption(
                    combo_db, mat_names[m], step_names[s]);
                if (wu < 0.0) continue;
                wf_proc += mat_weights[m] * wu * aware_cf;
            }
        }
    }

    /* -- 3. Packaging ---------------------------------------------- */
    {
        char pkg_cats[L7_MAX_PACKAGING_ITEMS][L7_MAX_ITEM_LEN];
        double pkg_masses[L7_MAX_PACKAGING_ITEMS];

        int n_cats = layer7_parse_string_array(
            rec->packaging_categories_json, pkg_cats,
            L7_MAX_PACKAGING_ITEMS);
        int n_mass = layer7_parse_double_array(
            rec->packaging_masses_kg_json, pkg_masses,
            L7_MAX_PACKAGING_ITEMS);
        int n = n_cats < n_mass ? n_cats : n_mass;

        for (int j = 0; j < n; j++) {
            double fp = packaging_calculate_single_by_name(
                pkg_cats[j], pkg_masses[j]);
            if (fp >= 0.0) wf_pack += fp;
        }
    }

    /* -- 4. Total (no transport, no adjustment) -------------------- */
    rec->wf_raw_materials_m3_world_eq = wf_raw;
    rec->wf_processing_m3_world_eq    = wf_proc;
    rec->wf_packaging_m3_world_eq     = wf_pack;
    rec->wf_total_m3_world_eq         = wf_raw + wf_proc + wf_pack;

    /* Set metadata */
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(rec->calculation_timestamp,
             sizeof(rec->calculation_timestamp),
             "%Y-%m-%dT%H:%M:%SZ", t);
    strncpy(rec->calculation_version, "v1.0",
            sizeof(rec->calculation_version) - 1);

    return 0;
}
