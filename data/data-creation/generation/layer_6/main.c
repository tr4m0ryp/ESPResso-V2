/*
 * main.c - Layer 6: Carbon Footprint Calculation Main Entry Point
 *
 * This is the main entry point for Layer 6 calculation, which processes
 * validated records from Layer 5 and calculates complete carbon footprints
 * using the existing C modules in data_calculation/.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "layer6_calculation.h"

/*
 * print_usage - Display program usage information
 */
static void print_usage(const char *program_name) {
    printf("Layer 6: Carbon Footprint Calculation\n");
    printf("Usage: %s [options]\n", program_name);
    printf("\n");
    printf("Options:\n");
    printf("  --input <path>          Input CSV file from Layer 5 (default: datasets/generated/layer_5_validated.csv)\n");
    printf("  --output <path>         Output CSV file (default: datasets/final/training_dataset.csv)\n");
    printf("  --summary <path>        Summary statistics JSON (default: datasets/final/calculation_summary.json)\n");
    printf("  --materials <path>      Materials database (default: datasets/final/Product_materials.csv)\n");
    printf("  --steps <path>          Processing steps database (default: datasets/final/processing_steps_overview.csv)\n");
    printf("  --combinations <path>   Material-process combinations (default: datasets/final/material_processing_emissions.csv)\n");
    printf("  --no-validation         Skip validation checks\n");
    printf("  --no-stats              Skip statistics generation\n");
    printf("  --verbose, -v           Enable verbose logging\n");
    printf("  --help, -h              Show this help message\n");
    printf("\n");
    printf("Description:\n");
    printf("  Layer 6 calculates carbon footprints for validated synthetic data using\n");
    printf("  deterministic formulas from the research paper. It integrates all existing\n");
    printf("  C modules (raw materials, transport, processing, packaging, adjustments).\n");
    printf("\n");
    printf("Output:\n");
    printf("  - Complete training dataset with carbon footprint calculations\n");
    printf("  - Summary statistics in JSON format\n");
    printf("  - ~850,000 records ready for ML model training\n");
}

/*
 * main - Main entry point
 */
int main(int argc, char *argv[]) {
    Layer6Config config;
    CalculationStatistics stats;
    time_t start_time, end_time;
    double duration_seconds;
    int result;
    
    /* Print banner */
    printf("========================================\n");
    printf("Layer 6: Carbon Footprint Calculation\n");
    printf("========================================\n\n");
    
    /* Record start time */
    start_time = time(NULL);
    
    /* Initialize configuration */
    layer6_init_config(&config);
    
    /* Parse command line arguments */
    if (layer6_parse_arguments(argc, argv, &config) != 0) {
        fprintf(stderr, "ERROR: Failed to parse arguments\n");
        return 1;
    }
    
    /* Validate configuration */
    if (layer6_validate_config(&config) != 0) {
        fprintf(stderr, "ERROR: Invalid configuration\n");
        return 1;
    }
    
    /* Print configuration summary */
    if (config.verbose_logging) {
        printf("Configuration:\n");
        printf("  Input: %s\n", config.input_path);
        printf("  Output: %s\n", config.output_path);
        printf("  Summary: %s\n", config.summary_path);
        printf("  Materials DB: %s\n", config.materials_path);
        printf("  Processing Steps DB: %s\n", config.processing_steps_path);
        printf("  Combinations DB: %s\n", config.processing_combinations_path);
        printf("  Validation: %s\n", config.enable_validation_checks ? "enabled" : "disabled");
        printf("  Statistics: %s\n", config.enable_statistics ? "enabled" : "disabled");
        printf("\n");
    }
    
    /* Process the CSV file */
    printf("Starting carbon footprint calculations...\n");
    result = layer6_process_csv(&config, &stats);
    
    if (result != 0) {
        fprintf(stderr, "ERROR: Calculation failed with code %d\n", result);
        return 1;
    }
    
    /* Write statistics if enabled */
    if (config.enable_statistics) {
        printf("\nWriting calculation statistics...\n");
        if (layer6_write_statistics(&stats, config.summary_path) != 0) {
            fprintf(stderr, "WARNING: Failed to write statistics\n");
        }
    }
    
    /* Record end time and calculate duration */
    end_time = time(NULL);
    duration_seconds = difftime(end_time, start_time);
    
    /* Print completion summary */
    printf("\n========================================\n");
    printf("Layer 6 Calculation Completed Successfully\n");
    printf("========================================\n");
    printf("Records processed: %d\n", stats.total_records);
    printf("Processing time: %.2f seconds\n", duration_seconds);
    printf("Average time per record: %.4f seconds\n", 
           stats.total_records > 0 ? duration_seconds / stats.total_records : 0.0);
    
    if (config.enable_statistics) {
        printf("\nCarbon Footprint Summary:\n");
        printf("  Raw Materials: %.2f ± %.2f kg CO2eq (range: %.2f - %.2f)\n",
               stats.cf_raw_materials_mean, stats.cf_raw_materials_std,
               stats.cf_raw_materials_min, stats.cf_raw_materials_max);
        printf("  Transport: %.2f ± %.2f kg CO2eq (range: %.2f - %.2f)\n",
               stats.cf_transport_mean, stats.cf_transport_std,
               stats.cf_transport_min, stats.cf_transport_max);
        printf("  Processing: %.2f ± %.2f kg CO2eq (range: %.2f - %.2f)\n",
               stats.cf_processing_mean, stats.cf_processing_std,
               stats.cf_processing_min, stats.cf_processing_max);
        printf("  Packaging: %.2f ± %.2f kg CO2eq (range: %.2f - %.2f)\n",
               stats.cf_packaging_mean, stats.cf_packaging_std,
               stats.cf_packaging_min, stats.cf_packaging_max);
        printf("  Total: %.2f ± %.2f kg CO2eq (range: %.2f - %.2f)\n",
               stats.cf_total_mean, stats.cf_total_std,
               stats.cf_total_min, stats.cf_total_max);
    }
    
    printf("\nOutput files:\n");
    printf("  Training Dataset: %s\n", config.output_path);
    if (config.enable_statistics) {
        printf("  Summary Statistics: %s\n", config.summary_path);
    }
    printf("\nDataset ready for ML model training! 🎉\n");
    
    return 0;
}