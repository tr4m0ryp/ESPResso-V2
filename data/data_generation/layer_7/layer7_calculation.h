/*
 * layer7_calculation.h - Layer 7: Water Footprint Calculation Layer
 *
 * Calculates water footprint values for validated records using
 * deterministic formulas with AWARE-weighted water consumption.
 *
 * Calculation Flow:
 * 1. Raw Materials: WF_raw = sum(weight_i * WU_material_i * AWARE_agri_i)
 * 2. Processing:    WF_proc = sum(weight_m * WU_process_p * AWARE_nonagri_p)
 * 3. Packaging:     WF_pack = sum(mass_j * WU_packaging_j)  [no AWARE]
 * 4. Transport:     WF_transport = 0                         [not applicable]
 * 5. Total:         WF_total = WF_raw + WF_proc + WF_pack
 *
 * No 1.02 adjustment. Unit: m3 world-eq.
 */

#ifndef LAYER7_CALCULATION_H
#define LAYER7_CALCULATION_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <errno.h>

/* Maximum field sizes */
#define L7_MAX_FIELD_LENGTH     2048
#define L7_MAX_MATERIALS        50
#define L7_MAX_PROCESSING_STEPS 20
#define L7_MAX_PACKAGING_ITEMS  20
#define L7_MAX_STRING_ITEMS     50
#define L7_MAX_ITEM_LEN         256

/*
 * Layer7ProductRecord - Single product record with water footprint outputs.
 *
 * Input fields come from Layer 5 + Layer 4 join CSV.
 * Output fields are the calculated water footprint components.
 */
typedef struct {
    /* Input fields */
    char record_id[64];
    char materials_json[L7_MAX_FIELD_LENGTH];
    char material_weights_kg_json[L7_MAX_FIELD_LENGTH];
    char preprocessing_steps_json[L7_MAX_FIELD_LENGTH];
    char transport_legs_json[L7_MAX_FIELD_LENGTH];
    char packaging_categories_json[L7_MAX_FIELD_LENGTH];
    char packaging_masses_kg_json[L7_MAX_FIELD_LENGTH];
    double total_weight_kg;

    /* Output fields (water footprint) */
    double wf_raw_materials_m3_world_eq;
    double wf_processing_m3_world_eq;
    double wf_packaging_m3_world_eq;
    double wf_total_m3_world_eq;
    char   calculation_timestamp[32];
    char   calculation_version[16];
} Layer7ProductRecord;

/*
 * Layer7Stats - Aggregate statistics for validation.
 */
typedef struct {
    int    total_records;
    double wf_raw_sum;
    double wf_raw_min;
    double wf_raw_max;
    double wf_proc_sum;
    double wf_proc_min;
    double wf_proc_max;
    double wf_pack_sum;
    double wf_pack_min;
    double wf_pack_max;
    double wf_total_sum;
    double wf_total_min;
    double wf_total_max;
    char   calculation_timestamp[64];
} Layer7Stats;

/*
 * Layer7Config - Configuration for Layer 7 calculation.
 */
typedef struct {
    char input_path[L7_MAX_FIELD_LENGTH];
    char output_path[L7_MAX_FIELD_LENGTH];
    char summary_path[L7_MAX_FIELD_LENGTH];
    char materials_path[L7_MAX_FIELD_LENGTH];
    char processing_steps_path[L7_MAX_FIELD_LENGTH];
    char processing_combinations_path[L7_MAX_FIELD_LENGTH];
    char aware_agri_path[L7_MAX_FIELD_LENGTH];
    char aware_nonagri_path[L7_MAX_FIELD_LENGTH];
    char calculation_version[32];
    int  enable_validation;
    int  enable_statistics;
    int  verbose;
} Layer7Config;

/* --- Configuration ---------------------------------------------------- */

void layer7_init_config(Layer7Config *config);
int  layer7_parse_args(int argc, char *argv[], Layer7Config *config);
int  layer7_validate_config(const Layer7Config *config);

/* --- JSON parsing helpers --------------------------------------------- */

int layer7_parse_double_array(const char *json, double *out,
                              int max_values);

int layer7_parse_string_array(const char *json,
                              char out[][L7_MAX_ITEM_LEN],
                              int max_items);

int layer7_extract_transport_country(const char *transport_legs_json,
                                     int leg_index,
                                     char *country_buf,
                                     size_t buf_size);

/* --- Core calculation ------------------------------------------------- */

/*
 * layer7_calculate_record - Calculate water footprint for a single record.
 *
 * Database pointers are: MaterialDatabase*, MaterialProcessDatabase*,
 * AwareDatabase* (agri), AwareDatabase* (nonagri).
 */
int layer7_calculate_record(Layer7ProductRecord *rec,
                            const void *mat_db,
                            const void *combo_db,
                            const void *aware_agri,
                            const void *aware_nonagri,
                            int verbose);

int layer7_process_csv(const Layer7Config *config, Layer7Stats *stats);

/* --- Logging ---------------------------------------------------------- */

void layer7_log_error(const char *func, const char *msg, int rec);
void layer7_log_warning(const char *func, const char *msg, int rec);
void layer7_print_progress(int current, int total, int step);

/* --- Statistics ------------------------------------------------------- */

int layer7_write_statistics(const Layer7Stats *stats,
                            const char *output_path);

#endif /* LAYER7_CALCULATION_H */
