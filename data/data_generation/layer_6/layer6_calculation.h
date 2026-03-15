/*
 * layer6_calculation.h - Layer 6: Carbon Footprint Calculation Layer
 *
 * This module implements the final layer of the synthetic data generation pipeline.
 * It calculates carbon footprint values for validated records using deterministic
 * formulas from the research paper, integrating all existing C modules in data_calculation/.
 *
 * Calculation Flow:
 * 1. Raw Materials: CF_raw = Σ(weight × EF_material)
 * 2. Transport: CF_transport = (w/1000) × D × (EF_weighted/1000)
 * 3. Processing: CF_processing = Σ(w_m × EF_m,p)
 * 4. Packaging: CF_packaging = Σ(m_i × EF_i)
 * 5. Adjustments: CF_total = CF_modelled × 1.02
 *
 * Reference: research_paper.tex Section 3 (Methodology)
 */

#ifndef LAYER6_CALCULATION_H
#define LAYER6_CALCULATION_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <errno.h>

/* Maximum field sizes */
#define MAX_FIELD_LENGTH 1024
#define MAX_MATERIALS 50
#define MAX_PROCESSING_STEPS 20
#define MAX_TRANSPORT_LEGS 10
#define MAX_PACKAGING_ITEMS 20
#define MAX_CATEGORIES 200

/*
 * Layer6ProductRecord - Complete product record with all calculated footprints
 *
 * This structure represents a single product record from Layer 5 with all
 * calculated carbon footprint components added.
 */
typedef struct {
    /* Layer 1-5 fields (input) */
    char category_id[64];
    char category_name[128];
    char subcategory_id[64];
    char subcategory_name[128];
    char materials_json[MAX_FIELD_LENGTH];
    char material_weights_kg_json[MAX_FIELD_LENGTH];
    char material_percentages_json[MAX_FIELD_LENGTH];
    double total_weight_kg;
    char preprocessing_path_id[64];
    char preprocessing_steps_json[MAX_FIELD_LENGTH];
    char transport_scenario_id[64];
    double total_transport_distance_km;
    char supply_chain_type[64];
    char transport_items_json[MAX_FIELD_LENGTH];
    char transport_modes_json[MAX_FIELD_LENGTH];
    char transport_distances_kg_json[MAX_FIELD_LENGTH];
    char transport_emissions_kg_co2e_json[MAX_FIELD_LENGTH];
    char packaging_config_id[64];
    char packaging_items_json[MAX_FIELD_LENGTH];
    char packaging_categories_json[MAX_FIELD_LENGTH];
    char packaging_masses_kg_json[MAX_FIELD_LENGTH];
    double total_packaging_mass_kg;
    char validation_status[32];
    double plausibility_score;
    double reward_score;
    char deterministic_flags_json[MAX_FIELD_LENGTH];
    char semantic_issues_json[MAX_FIELD_LENGTH];
    char pipeline_version[32];
    char validation_timestamp[64];
    char record_hash[128];
    char final_decision[32];
    double final_score;
    char decision_reasoning[MAX_FIELD_LENGTH];
    
    /* Layer 6 calculated fields (output) */
    double cf_raw_materials_kg_co2eq;
    double cf_transport_kg_co2eq;
    double cf_processing_kg_co2eq;
    double cf_packaging_kg_co2eq;
    double cf_modelled_kg_co2eq;
    double cf_adjustment_kg_co2eq;
    double cf_total_kg_co2eq;
    char transport_mode_probabilities_json[MAX_FIELD_LENGTH];
    
    /* Processing metadata */
    char calculation_timestamp[64];
    char calculation_version[32];
    
} Layer6ProductRecord;

/*
 * CalculationStatistics - Statistics for validation of calculations
 */
typedef struct {
    int total_records;
    double cf_raw_materials_mean;
    double cf_raw_materials_std;
    double cf_raw_materials_min;
    double cf_raw_materials_max;
    double cf_transport_mean;
    double cf_transport_std;
    double cf_transport_min;
    double cf_transport_max;
    double cf_processing_mean;
    double cf_processing_std;
    double cf_processing_min;
    double cf_processing_max;
    double cf_packaging_mean;
    double cf_packaging_std;
    double cf_packaging_min;
    double cf_packaging_max;
    double cf_total_mean;
    double cf_total_std;
    double cf_total_min;
    double cf_total_max;
    char calculation_timestamp[64];
} CalculationStatistics;

/*
 * Layer6Config - Configuration for Layer 6 calculation
 */
typedef struct {
    char input_path[MAX_FIELD_LENGTH];
    char output_path[MAX_FIELD_LENGTH];
    char summary_path[MAX_FIELD_LENGTH];
    char materials_path[MAX_FIELD_LENGTH];
    char processing_steps_path[MAX_FIELD_LENGTH];
    char processing_combinations_path[MAX_FIELD_LENGTH];
    char calculation_version[32];
    int enable_validation_checks;
    int enable_statistics;
    int verbose_logging;
} Layer6Config;

/* Function prototypes */

/*
 * layer6_init_config - Initialize Layer 6 configuration with defaults
 */
void layer6_init_config(Layer6Config *config);

/*
 * layer6_parse_arguments - Parse command line arguments
 */
int layer6_parse_arguments(int argc, char *argv[], Layer6Config *config);

/*
 * layer6_validate_config - Validate configuration parameters
 */
int layer6_validate_config(const Layer6Config *config);

/*
 * layer6_calculate_single_record - Calculate footprints for a single record
 */
int layer6_calculate_single_record(Layer6ProductRecord *record,
                                   const void *material_db,
                                   const void *processing_db,
                                   const void *processing_combos_db);

/*
 * layer6_process_csv - Process entire CSV file and calculate footprints
 */
int layer6_process_csv(const Layer6Config *config,
                      CalculationStatistics *stats);

/*
 * layer6_write_statistics - Write calculation statistics to JSON file
 */
int layer6_write_statistics(const CalculationStatistics *stats,
                           const char *output_path);

/*
 * layer6_validate_calculations - Sanity check calculated values
 */
int layer6_validate_calculations(const Layer6ProductRecord *record);

/*
 * layer6_print_progress - Print progress information
 */
void layer6_print_progress(int current, int total, int step);

/*
 * layer6_log_error - Log error with context
 */
void layer6_log_error(const char *function, const char *message, int record_id);

/*
 * layer6_log_warning - Log warning with context
 */
void layer6_log_warning(const char *function, const char *message, int record_id);

#endif /* LAYER6_CALCULATION_H */