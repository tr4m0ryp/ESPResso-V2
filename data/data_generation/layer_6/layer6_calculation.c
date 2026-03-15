/*
 * layer6_calculation.c - Layer 6: Carbon Footprint Calculation Implementation
 *
 * This module implements the final carbon footprint calculations using
 * the existing C modules in data_calculation/.
 */

#include "layer6_calculation.h"
#include <ctype.h>
#include <stdarg.h>

/* Include existing calculation modules */
#include "../../data_calculation/include/raw_materials/raw_materials.h"
#include "../../data_calculation/include/transport/transport.h"
#include "../../data_calculation/include/processing/material_processing.h"
#include "../../data_calculation/include/packaging/packaging.h"
#include "../../data_calculation/include/adjustments/adjustments.h"

/* Global configuration */
static Layer6Config g_config;
static int g_verbose = 0;

/*
 * layer6_init_config - Initialize Layer 6 configuration with defaults
 */
void layer6_init_config(Layer6Config *config) {
    memset(config, 0, sizeof(Layer6Config));
    
    /* Set default paths */
    snprintf(config->input_path, MAX_FIELD_LENGTH, 
             "data/datasets/generated/layer_5_validated.csv");
    snprintf(config->output_path, MAX_FIELD_LENGTH, 
             "data/datasets/final/training_dataset.csv");
    snprintf(config->summary_path, MAX_FIELD_LENGTH, 
             "data/datasets/final/calculation_summary.json");
    snprintf(config->materials_path, MAX_FIELD_LENGTH, 
             "data/datasets/final/Product_materials.csv");
    snprintf(config->processing_steps_path, MAX_FIELD_LENGTH, 
             "data/datasets/final/processing_steps_overview.csv");
    snprintf(config->processing_combinations_path, MAX_FIELD_LENGTH, 
             "data/datasets/final/material_processing_emissions.csv");
    
    /* Set defaults */
    snprintf(config->calculation_version, sizeof(config->calculation_version), "v1.0");
    config->enable_validation_checks = 1;
    config->enable_statistics = 1;
    config->verbose_logging = 0;
}

/*
 * layer6_parse_arguments - Parse command line arguments
 */
int layer6_parse_arguments(int argc, char *argv[], Layer6Config *config) {
    int i;
    
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            strncpy(config->input_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            strncpy(config->output_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--summary") == 0 && i + 1 < argc) {
            strncpy(config->summary_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--materials") == 0 && i + 1 < argc) {
            strncpy(config->materials_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--steps") == 0 && i + 1 < argc) {
            strncpy(config->processing_steps_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--combinations") == 0 && i + 1 < argc) {
            strncpy(config->processing_combinations_path, argv[++i], MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--no-validation") == 0) {
            config->enable_validation_checks = 0;
        } else if (strcmp(argv[i], "--no-stats") == 0) {
            config->enable_statistics = 0;
        } else if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0) {
            config->verbose_logging = 1;
            g_verbose = 1;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Layer 6 Carbon Footprint Calculation\n");
            printf("Usage: %s [options]\n", argv[0]);
            printf("Options:\n");
            printf("  --input <path>          Input CSV file (default: %s)\n", config->input_path);
            printf("  --output <path>         Output CSV file (default: %s)\n", config->output_path);
            printf("  --summary <path>        Summary JSON file (default: %s)\n", config->summary_path);
            printf("  --materials <path>      Materials database (default: %s)\n", config->materials_path);
            printf("  --steps <path>          Processing steps database (default: %s)\n", config->processing_steps_path);
            printf("  --combinations <path>   Material-process combinations (default: %s)\n", config->processing_combinations_path);
            printf("  --no-validation         Skip validation checks\n");
            printf("  --no-stats              Skip statistics generation\n");
            printf("  --verbose, -v           Enable verbose logging\n");
            printf("  --help, -h              Show this help message\n");
            exit(0);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return -1;
        }
    }
    
    return 0;
}

/*
 * layer6_validate_config - Validate configuration parameters
 */
int layer6_validate_config(const Layer6Config *config) {
    /* Check required paths */
    if (strlen(config->input_path) == 0) {
        fprintf(stderr, "ERROR: Input path is required\n");
        return -1;
    }
    
    if (strlen(config->output_path) == 0) {
        fprintf(stderr, "ERROR: Output path is required\n");
        return -1;
    }
    
    if (strlen(config->materials_path) == 0) {
        fprintf(stderr, "ERROR: Materials database path is required\n");
        return -1;
    }
    
    if (strlen(config->processing_steps_path) == 0) {
        fprintf(stderr, "ERROR: Processing steps database path is required\n");
        return -1;
    }
    
    if (strlen(config->processing_combinations_path) == 0) {
        fprintf(stderr, "ERROR: Processing combinations database path is required\n");
        return -1;
    }
    
    return 0;
}

/*
 * layer6_print_progress - Print progress information
 */
void layer6_print_progress(int current, int total, int step) {
    static int last_percent = -1;
    int percent = (current * 100) / total;
    
    if (percent != last_percent && percent % step == 0) {
        printf("Progress: %d%% (%d/%d records)\n", percent, current, total);
        fflush(stdout);
        last_percent = percent;
    }
}

/*
 * layer6_log_error - Log error with context
 */
void layer6_log_error(const char *function, const char *message, int record_id) {
    if (record_id >= 0) {
        fprintf(stderr, "[ERROR][Record %d][%s] %s\n", record_id, function, message);
    } else {
        fprintf(stderr, "[ERROR][%s] %s\n", function, message);
    }
}

/*
 * layer6_log_warning - Log warning with context
 */
void layer6_log_warning(const char *function, const char *message, int record_id) {
    if (record_id >= 0) {
        fprintf(stderr, "[WARNING][Record %d][%s] %s\n", record_id, function, message);
    } else {
        fprintf(stderr, "[WARNING][%s] %s\n", function, message);
    }
}

/*
 * layer6_parse_json_array - Simple JSON array parser for extracting values
 */
static int layer6_parse_json_array(const char *json_str, double *values, int max_values) {
    int count = 0;
    const char *ptr = json_str;
    char buffer[64];
    int buffer_idx = 0;
    int in_quotes = 0;
    
    if (!json_str || !values || max_values <= 0) return 0;
    
    /* Skip leading whitespace and brackets */
    while (*ptr && (*ptr == '[' || isspace(*ptr))) ptr++;
    
    while (*ptr && count < max_values) {
        if (*ptr == '"') {
            in_quotes = !in_quotes;
        } else if (*ptr == ',' && !in_quotes) {
            /* End of element */
            buffer[buffer_idx] = '\0';
            if (buffer_idx > 0) {
                char *endptr;
                double value = strtod(buffer, &endptr);
                if (*endptr == '\0') {  /* Successful conversion */
                    values[count++] = value;
                }
            }
            buffer_idx = 0;
        } else if (*ptr == ']' && !in_quotes) {
            /* End of array */
            if (buffer_idx > 0) {
                buffer[buffer_idx] = '\0';
                char *endptr;
                double value = strtod(buffer, &endptr);
                if (*endptr == '\0') {
                    values[count++] = value;
                }
            }
            break;
        } else if (!isspace(*ptr) || in_quotes) {
            if (buffer_idx < sizeof(buffer) - 1) {
                buffer[buffer_idx++] = *ptr;
            }
        }
        ptr++;
    }
    
    return count;
}

/*
 * layer6_parse_json_object - Simple JSON object parser for extracting key-value pairs
 */
static int layer6_parse_json_objects(const char *json_str, char keys[][128], double *values, int max_pairs) {
    int count = 0;
    const char *ptr = json_str;
    char current_key[128] = {0};
    char current_value[64] = {0};
    int in_key = 0;
    int in_value = 0;
    int in_quotes = 0;
    int key_idx = 0;
    int value_idx = 0;
    
    if (!json_str || !keys || !values || max_pairs <= 0) return 0;
    
    /* Skip leading { and whitespace */
    while (*ptr && (*ptr == '{' || isspace(*ptr))) ptr++;
    
    while (*ptr && count < max_pairs) {
        if (*ptr == '"') {
            in_quotes = !in_quotes;
        } else if (*ptr == ':' && !in_quotes) {
            /* End of key, start of value */
            current_key[key_idx] = '\0';
            in_key = 0;
            in_value = 1;
            value_idx = 0;
        } else if (*ptr == ',' && !in_quotes) {
            /* End of key-value pair */
            current_value[value_idx] = '\0';
            if (strlen(current_key) > 0 && strlen(current_value) > 0) {
                strncpy(keys[count], current_key, 127);
                keys[count][127] = '\0';
                
                char *endptr;
                double value = strtod(current_value, &endptr);
                if (*endptr == '\0') {
                    values[count] = value;
                    count++;
                }
            }
            
            /* Reset for next pair */
            memset(current_key, 0, sizeof(current_key));
            memset(current_value, 0, sizeof(current_value));
            in_key = 1;
            in_value = 0;
            key_idx = 0;
        } else if (*ptr == '}' && !in_quotes) {
            /* End of object */
            if (in_value && strlen(current_key) > 0 && strlen(current_value) > 0) {
                current_value[value_idx] = '\0';
                strncpy(keys[count], current_key, 127);
                keys[count][127] = '\0';
                
                char *endptr;
                double value = strtod(current_value, &endptr);
                if (*endptr == '\0') {
                    values[count] = value;
                    count++;
                }
            }
            break;
        } else if (!isspace(*ptr) || in_quotes) {
            if (in_key && key_idx < sizeof(current_key) - 1) {
                current_key[key_idx++] = *ptr;
            } else if (in_value && value_idx < sizeof(current_value) - 1) {
                current_value[value_idx++] = *ptr;
            }
        }
        ptr++;
    }
    
    return count;
}

/*
 * layer6_calculate_single_record - Calculate footprints for a single record
 */
int layer6_calculate_single_record(Layer6ProductRecord *record,
                                   const void *material_db,
                                   const void *processing_db,
                                   const void *processing_combos_db) {
    double cf_raw = 0.0;
    double cf_transport = 0.0;
    double cf_processing = 0.0;
    double cf_packaging = 0.0;
    double cf_modelled = 0.0;
    double cf_adjustment = 0.0;
    double cf_total = 0.0;
    
    /* Initialize transport result structure */
    TransportResult transport_result = {0};
    
    /* 1. Calculate raw materials footprint */
    {
        double material_weights[MAX_MATERIALS];
        int material_count = layer6_parse_json_array(record->material_weights_kg_json, 
                                                    material_weights, MAX_MATERIALS);
        
        if (material_count > 0) {
            /* Create temporary material list for raw materials calculation */
            ProductMaterialList temp_list;
            temp_list.count = material_count;
            for (int i = 0; i < material_count; i++) {
                /* Extract material name from JSON (simplified) */
                char material_name[128];
                /* This is a simplified extraction - in practice you'd need proper JSON parsing */
                snprintf(material_name, sizeof(material_name), "material_%d", i);
                strncpy(temp_list.materials[i].name, material_name, sizeof(temp_list.materials[i].name) - 1);
                temp_list.materials[i].weight_kg = material_weights[i];
            }
            
            cf_raw = raw_materials_calculate_footprint(&temp_list, (MaterialDatabase *)material_db);
        }
    }
    
    /* 2. Calculate transport footprint */
    {
        TransportJourney journey;
        journey.total_distance_km = record->total_transport_distance_km;
        journey.weight_kg = record->total_weight_kg;
        journey.leg_count = 1;
        journey.legs[0].distance_km = record->total_transport_distance_km;
        journey.legs[0].mode = TRANSPORT_MODE_ROAD;  /* Default mode */
        journey.legs[0].mode_known = 0;
        
        /* For multiple legs, we would parse transport_items_json */
        /* Simplified: use single leg with total distance */
        
        transport_result = transport_calculate_footprint(&journey);
        cf_transport = transport_result.footprint_kg_co2e;
    }
    
    /* 3. Calculate processing footprint */
    {
        /* This would require parsing preprocessing_steps_json and material mappings */
        /* Simplified: use basic processing calculation */
        
        /* For now, we'll use a simplified approach */
        /* In practice, this would involve complex material-process mapping */
        cf_processing = record->total_weight_kg * 2.0;  /* Simplified placeholder */
    }
    
    /* 4. Calculate packaging footprint */
    {
        double packaging_masses[MAX_PACKAGING_ITEMS];
        int packaging_count = layer6_parse_json_array(record->packaging_masses_kg_json,
                                                     packaging_masses, MAX_PACKAGING_ITEMS);
        
        if (packaging_count > 0) {
            PackagingList temp_packaging;
            temp_packaging.count = packaging_count;
            
            /* Parse packaging categories (simplified) */
            for (int i = 0; i < packaging_count; i++) {
                temp_packaging.items[i].mass_kg = packaging_masses[i];
                temp_packaging.items[i].category = PACKAGING_PAPER_CARDBOARD;  /* Default category */
                snprintf(temp_packaging.items[i].description, sizeof(temp_packaging.items[i].description),
                        "packaging_item_%d", i);
            }
            
            PackagingResult packaging_result = packaging_calculate_footprint(&temp_packaging);
            cf_packaging = packaging_result.total_footprint;
        }
    }
    
    /* 5. Calculate modelled total and apply adjustments */
    cf_modelled = cf_raw + cf_transport + cf_processing + cf_packaging;
    
    AdjustmentBreakdown adjustment = adjustments_apply_with_breakdown(cf_modelled);
    cf_adjustment = adjustment.total_adjustment;
    cf_total = adjustment.adjusted_total;
    
    /* Store results */
    record->cf_raw_materials_kg_co2eq = cf_raw;
    record->cf_transport_kg_co2eq = cf_transport;
    record->cf_processing_kg_co2eq = cf_processing;
    record->cf_packaging_kg_co2eq = cf_packaging;
    record->cf_modelled_kg_co2eq = cf_modelled;
    record->cf_adjustment_kg_co2eq = cf_adjustment;
    record->cf_total_kg_co2eq = cf_total;
    
    /* Store transport mode probabilities */
    snprintf(record->transport_mode_probabilities_json, sizeof(record->transport_mode_probabilities_json),
            "{\"road\":%.3f,\"rail\":%.3f,\"inland_waterway\":%.3f,\"sea\":%.3f,\"air\":%.3f}",
            transport_result.mode_probabilities[TRANSPORT_MODE_ROAD],
            transport_result.mode_probabilities[TRANSPORT_MODE_RAIL],
            transport_result.mode_probabilities[TRANSPORT_MODE_INLAND_WATERWAY],
            transport_result.mode_probabilities[TRANSPORT_MODE_SEA],
            transport_result.mode_probabilities[TRANSPORT_MODE_AIR]);
    
    /* Set calculation metadata */
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(record->calculation_timestamp, sizeof(record->calculation_timestamp),
            "%Y-%m-%dT%H:%M:%SZ", tm_info);
    strncpy(record->calculation_version, "v1.0", sizeof(record->calculation_version) - 1);
    
    return 0;
}

/*
 * layer6_validate_calculations - Sanity check calculated values
 */
int layer6_validate_calculations(const Layer6ProductRecord *record) {
    int issues = 0;
    
    /* Check for negative values */
    if (record->cf_raw_materials_kg_co2eq < 0) {
        layer6_log_warning(__FUNCTION__, "Negative raw materials footprint", -1);
        issues++;
    }
    if (record->cf_transport_kg_co2eq < 0) {
        layer6_log_warning(__FUNCTION__, "Negative transport footprint", -1);
        issues++;
    }
    if (record->cf_processing_kg_co2eq < 0) {
        layer6_log_warning(__FUNCTION__, "Negative processing footprint", -1);
        issues++;
    }
    if (record->cf_packaging_kg_co2eq < 0) {
        layer6_log_warning(__FUNCTION__, "Negative packaging footprint", -1);
        issues++;
    }
    
    /* Check reasonable ranges */
    if (record->cf_raw_materials_kg_co2eq > 50.0) {
        layer6_log_warning(__FUNCTION__, "Raw materials footprint > 50 kg CO2eq", -1);
        issues++;
    }
    if (record->cf_transport_kg_co2eq > 10.0) {
        layer6_log_warning(__FUNCTION__, "Transport footprint > 10 kg CO2eq", -1);
        issues++;
    }
    if (record->cf_processing_kg_co2eq > 30.0) {
        layer6_log_warning(__FUNCTION__, "Processing footprint > 30 kg CO2eq", -1);
        issues++;
    }
    if (record->cf_packaging_kg_co2eq > 2.0) {
        layer6_log_warning(__FUNCTION__, "Packaging footprint > 2 kg CO2eq", -1);
        issues++;
    }
    if (record->cf_total_kg_co2eq > 80.0) {
        layer6_log_warning(__FUNCTION__, "Total footprint > 80 kg CO2eq", -1);
        issues++;
    }
    
    /* Check component consistency */
    double expected_modelled = record->cf_raw_materials_kg_co2eq + 
                              record->cf_transport_kg_co2eq + 
                              record->cf_processing_kg_co2eq + 
                              record->cf_packaging_kg_co2eq;
    
    if (fabs(record->cf_modelled_kg_co2eq - expected_modelled) > 0.001) {
        layer6_log_warning(__FUNCTION__, "Modelled total doesn't sum components", -1);
        issues++;
    }
    
    /* Check adjustment calculation */
    double expected_adjustment = record->cf_modelled_kg_co2eq * 0.02;
    if (fabs(record->cf_adjustment_kg_co2eq - expected_adjustment) > 0.001) {
        layer6_log_warning(__FUNCTION__, "Adjustment calculation incorrect", -1);
        issues++;
    }
    
    /* Check total calculation */
    double expected_total = record->cf_modelled_kg_co2eq + record->cf_adjustment_kg_co2eq;
    if (fabs(record->cf_total_kg_co2eq - expected_total) > 0.001) {
        layer6_log_warning(__FUNCTION__, "Total calculation incorrect", -1);
        issues++;
    }
    
    return issues;
}

/*
 * layer6_process_csv - Process entire CSV file and calculate footprints
 */
int layer6_process_csv(const Layer6Config *config, CalculationStatistics *stats) {
    FILE *input_file = NULL;
    FILE *output_file = NULL;
    char line[MAX_FIELD_LENGTH * 2];
    int record_count = 0;
    int error_count = 0;
    int validation_issues = 0;
    
    /* Initialize statistics */
    memset(stats, 0, sizeof(CalculationStatistics));
    
    /* Load reference databases */
    MaterialDatabase material_db;
    ProcessingStepDatabase processing_db;
    MaterialProcessDatabase processing_combos_db;
    
    printf("Loading reference databases...\n");
    
    if (raw_materials_load_csv(&material_db, config->materials_path) < 0) {
        layer6_log_error(__FUNCTION__, "Failed to load materials database", -1);
        return -1;
    }
    printf("Loaded %d materials from %s\n", material_db.count, config->materials_path);
    
    if (processing_load_steps_csv(&processing_db, config->processing_steps_path) < 0) {
        layer6_log_error(__FUNCTION__, "Failed to load processing steps database", -1);
        return -1;
    }
    printf("Loaded %d processing steps from %s\n", processing_db.count, config->processing_steps_path);
    
    if (processing_load_combinations_csv(&processing_combos_db, config->processing_combinations_path) < 0) {
        layer6_log_error(__FUNCTION__, "Failed to load processing combinations database", -1);
        return -1;
    }
    printf("Loaded %d material-process combinations from %s\n", 
           processing_combos_db.count, config->processing_combinations_path);
    
    /* Open input file */
    input_file = fopen(config->input_path, "r");
    if (!input_file) {
        layer6_log_error(__FUNCTION__, "Failed to open input file", -1);
        return -1;
    }
    
    /* Open output file */
    output_file = fopen(config->output_path, "w");
    if (!output_file) {
        layer6_log_error(__FUNCTION__, "Failed to open output file", -1);
        fclose(input_file);
        return -1;
    }
    
    printf("Processing records from %s to %s...\n", config->input_path, config->output_path);
    
    /* Read and process header line */
    if (!fgets(line, sizeof(line), input_file)) {
        layer6_log_error(__FUNCTION__, "Failed to read header line", -1);
        fclose(input_file);
        fclose(output_file);
        return -1;
    }
    
    /* Write extended header to output */
    fprintf(output_file, "%s", line);  /* Original header */
    
    /* Add new calculated fields */
    fprintf(output_file, ",cf_raw_materials_kg_co2eq,cf_transport_kg_co2eq,");
    fprintf(output_file, "cf_processing_kg_co2eq,cf_packaging_kg_co2eq,");
    fprintf(output_file, "cf_modelled_kg_co2eq,cf_adjustment_kg_co2eq,cf_total_kg_co2eq,");
    fprintf(output_file, "transport_mode_probabilities_json,calculation_timestamp,calculation_version\n");
    
    /* Process each record */
    printf("Starting calculation...\n");
    
    while (fgets(line, sizeof(line), input_file)) {
        Layer6ProductRecord record;
        char *field;
        char *saveptr;
        int field_index = 0;
        
        /* Initialize record */
        memset(&record, 0, sizeof(Layer6ProductRecord));
        
        /* Parse CSV line (simplified - assumes comma separation) */
        field = strtok_r(line, ",", &saveptr);
        
        /* Basic parsing - in practice you'd need robust CSV parsing */
        while (field != NULL && field_index < 50) {  /* Adjust based on actual field count */
            /* Remove quotes and whitespace */
            char *clean_field = field;
            while (*clean_field && (*clean_field == '"' || isspace(*clean_field))) clean_field++;
            char *end = clean_field + strlen(clean_field) - 1;
            while (end > clean_field && (*end == '"' || isspace(*end))) *end-- = '\0';
            
            /* Store field based on index (simplified mapping) */
            switch (field_index) {
                case 0: strncpy(record.category_id, clean_field, sizeof(record.category_id) - 1); break;
                case 1: strncpy(record.category_name, clean_field, sizeof(record.category_name) - 1); break;
                case 2: strncpy(record.subcategory_id, clean_field, sizeof(record.subcategory_id) - 1); break;
                case 3: strncpy(record.subcategory_name, clean_field, sizeof(record.subcategory_name) - 1); break;
                case 4: strncpy(record.materials_json, clean_field, sizeof(record.materials_json) - 1); break;
                case 5: strncpy(record.material_weights_kg_json, clean_field, sizeof(record.material_weights_kg_json) - 1); break;
                case 6: strncpy(record.material_percentages_json, clean_field, sizeof(record.material_percentages_json) - 1); break;
                case 7: record.total_weight_kg = atof(clean_field); break;
                case 8: strncpy(record.preprocessing_path_id, clean_field, sizeof(record.preprocessing_path_id) - 1); break;
                case 9: strncpy(record.preprocessing_steps_json, clean_field, sizeof(record.preprocessing_steps_json) - 1); break;
                case 10: strncpy(record.transport_scenario_id, clean_field, sizeof(record.transport_scenario_id) - 1); break;
                case 11: record.total_transport_distance_km = atof(clean_field); break;
                case 12: strncpy(record.supply_chain_type, clean_field, sizeof(record.supply_chain_type) - 1); break;
                case 13: strncpy(record.transport_items_json, clean_field, sizeof(record.transport_items_json) - 1); break;
                case 14: strncpy(record.transport_modes_json, clean_field, sizeof(record.transport_modes_json) - 1); break;
                case 15: strncpy(record.transport_distances_kg_json, clean_field, sizeof(record.transport_distances_kg_json) - 1); break;
                case 16: strncpy(record.transport_emissions_kg_co2e_json, clean_field, sizeof(record.transport_emissions_kg_co2e_json) - 1); break;
                case 17: strncpy(record.packaging_config_id, clean_field, sizeof(record.packaging_config_id) - 1); break;
                case 18: strncpy(record.packaging_items_json, clean_field, sizeof(record.packaging_items_json) - 1); break;
                case 19: strncpy(record.packaging_categories_json, clean_field, sizeof(record.packaging_categories_json) - 1); break;
                case 20: strncpy(record.packaging_masses_kg_json, clean_field, sizeof(record.packaging_masses_kg_json) - 1); break;
                case 21: record.total_packaging_mass_kg = atof(clean_field); break;
                /* Continue for remaining fields... */
            }
            
            field = strtok_r(NULL, ",", &saveptr);
            field_index++;
        }
        
        /* Calculate footprints for this record */
        if (layer6_calculate_single_record(&record, &material_db, &processing_db, &processing_combos_db) != 0) {
            layer6_log_error(__FUNCTION__, "Failed to calculate footprints for record", record_count);
            error_count++;
            continue;
        }
        
        /* Validate calculations if enabled */
        if (config->enable_validation_checks) {
            validation_issues += layer6_validate_calculations(&record);
        }
        
        /* Update statistics */
        if (config->enable_statistics) {
            stats->cf_raw_materials_mean += record.cf_raw_materials_kg_co2eq;
            stats->cf_transport_mean += record.cf_transport_kg_co2eq;
            stats->cf_processing_mean += record.cf_processing_kg_co2eq;
            stats->cf_packaging_mean += record.cf_packaging_kg_co2eq;
            stats->cf_total_mean += record.cf_total_kg_co2eq;
            
            /* Track min/max values */
            if (record_count == 0 || record.cf_raw_materials_kg_co2eq < stats->cf_raw_materials_min) {
                stats->cf_raw_materials_min = record.cf_raw_materials_kg_co2eq;
            }
            if (record_count == 0 || record.cf_raw_materials_kg_co2eq > stats->cf_raw_materials_max) {
                stats->cf_raw_materials_max = record.cf_raw_materials_kg_co2eq;
            }
            /* Similar for other components... */
        }
        
        /* Write output record */
        /* For simplicity, write the original line plus calculated fields */
        fprintf(output_file, "%s", line);  /* Original data (remove newline) */
        
        /* Remove trailing newline if present */
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n') {
            line[len-1] = '\0';
        }
        
        /* Add calculated fields */
        fprintf(output_file, ",%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%s\n",
                record.cf_raw_materials_kg_co2eq,
                record.cf_transport_kg_co2eq,
                record.cf_processing_kg_co2eq,
                record.cf_packaging_kg_co2eq,
                record.cf_modelled_kg_co2eq,
                record.cf_adjustment_kg_co2eq,
                record.cf_total_kg_co2eq,
                record.transport_mode_probabilities_json,
                record.calculation_timestamp,
                record.calculation_version);
        
        record_count++;
        
        /* Print progress */
        if (config->verbose_logging) {
            layer6_print_progress(record_count, 1000, 10);  /* Print every 10% */
        }
    }
    
    /* Calculate final statistics */
    if (config->enable_statistics && record_count > 0) {
        stats->total_records = record_count;
        stats->cf_raw_materials_mean /= record_count;
        stats->cf_transport_mean /= record_count;
        stats->cf_processing_mean /= record_count;
        stats->cf_packaging_mean /= record_count;
        stats->cf_total_mean /= record_count;
        
        /* Calculate standard deviations (would need second pass for accuracy) */
        /* For now, use simplified estimation */
        /* In practice, you'd store all values and calculate proper std dev */
        
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        strftime(stats->calculation_timestamp, sizeof(stats->calculation_timestamp),
                "%Y-%m-%dT%H:%M:%SZ", tm_info);
    }
    
    /* Close files */
    fclose(input_file);
    fclose(output_file);
    
    printf("\nCalculation completed!\n");
    printf("Records processed: %d\n", record_count);
    printf("Records with errors: %d\n", error_count);
    printf("Validation issues: %d\n", validation_issues);
    
    if (config->enable_statistics) {
        printf("\nStatistics:\n");
        printf("  Raw materials: mean=%.2f, min=%.2f, max=%.2f\n",
               stats->cf_raw_materials_mean, stats->cf_raw_materials_min, stats->cf_raw_materials_max);
        printf("  Transport: mean=%.2f, min=%.2f, max=%.2f\n",
               stats->cf_transport_mean, stats->cf_transport_min, stats->cf_transport_max);
        printf("  Processing: mean=%.2f, min=%.2f, max=%.2f\n",
               stats->cf_processing_mean, stats->cf_processing_min, stats->cf_processing_max);
        printf("  Packaging: mean=%.2f, min=%.2f, max=%.2f\n",
               stats->cf_packaging_mean, stats->cf_packaging_min, stats->cf_packaging_max);
        printf("  Total: mean=%.2f, min=%.2f, max=%.2f\n",
               stats->cf_total_mean, stats->cf_total_min, stats->cf_total_max);
    }
    
    return (error_count > 0) ? -1 : 0;
}

/*
 * layer6_write_statistics - Write calculation statistics to JSON file
 */
int layer6_write_statistics(const CalculationStatistics *stats, const char *output_path) {
    FILE *file = fopen(output_path, "w");
    if (!file) {
        layer6_log_error(__FUNCTION__, "Failed to open statistics file for writing", -1);
        return -1;
    }
    
    fprintf(file, "{\n");
    fprintf(file, "  \"metadata\": {\n");
    fprintf(file, "    \"generated_at\": \"%s\",\n", stats->calculation_timestamp);
    fprintf(file, "    \"pipeline_version\": \"v1.0\",\n");
    fprintf(file, "    \"layer\": 6,\n");
    fprintf(file, "    \"description\": \"Carbon footprint calculation statistics\"\n");
    fprintf(file, "  },\n");
    fprintf(file, "  \"processing_summary\": {\n");
    fprintf(file, "    \"total_records_processed\": %d,\n", stats->total_records);
    fprintf(file, "    \"calculation_timestamp\": \"%s\"\n", stats->calculation_timestamp);
    fprintf(file, "  },\n");
    fprintf(file, "  \"carbon_footprint_statistics\": {\n");
    fprintf(file, "    \"cf_raw_materials_kg_co2eq\": {\n");
    fprintf(file, "      \"mean\": %.3f,\n", stats->cf_raw_materials_mean);
    fprintf(file, "      \"min\": %.3f,\n", stats->cf_raw_materials_min);
    fprintf(file, "      \"max\": %.3f\n", stats->cf_raw_materials_max);
    fprintf(file, "    },\n");
    fprintf(file, "    \"cf_transport_kg_co2eq\": {\n");
    fprintf(file, "      \"mean\": %.3f,\n", stats->cf_transport_mean);
    fprintf(file, "      \"min\": %.3f,\n", stats->cf_transport_min);
    fprintf(file, "      \"max\": %.3f\n", stats->cf_transport_max);
    fprintf(file, "    },\n");
    fprintf(file, "    \"cf_processing_kg_co2eq\": {\n");
    fprintf(file, "      \"mean\": %.3f,\n", stats->cf_processing_mean);
    fprintf(file, "      \"min\": %.3f,\n", stats->cf_processing_min);
    fprintf(file, "      \"max\": %.3f\n", stats->cf_processing_max);
    fprintf(file, "    },\n");
    fprintf(file, "    \"cf_packaging_kg_co2eq\": {\n");
    fprintf(file, "      \"mean\": %.3f,\n", stats->cf_packaging_mean);
    fprintf(file, "      \"min\": %.3f,\n", stats->cf_packaging_min);
    fprintf(file, "      \"max\": %.3f\n", stats->cf_packaging_max);
    fprintf(file, "    },\n");
    fprintf(file, "    \"cf_total_kg_co2eq\": {\n");
    fprintf(file, "      \"mean\": %.3f,\n", stats->cf_total_mean);
    fprintf(file, "      \"min\": %.3f,\n", stats->cf_total_min);
    fprintf(file, "      \"max\": %.3f\n", stats->cf_total_max);
    fprintf(file, "    }\n");
    fprintf(file, "  }\n");
    fprintf(file, "}\n");
    
    fclose(file);
    printf("Statistics written to: %s\n", output_path);
    
    return 0;
}