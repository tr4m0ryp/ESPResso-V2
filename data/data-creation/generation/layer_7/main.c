/*
 * main.c - Layer 7: Water Footprint Calculation Main Entry Point
 *
 * Processes validated records from Layer 5 and calculates water
 * footprint values using the water_footprint C modules with
 * AWARE country-specific weighting.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "layer7_calculation.h"

int main(int argc, char *argv[])
{
    Layer7Config config;
    Layer7Stats  stats;
    time_t start, end;
    int result;

    printf("========================================\n");
    printf("Layer 7: Water Footprint Calculation\n");
    printf("========================================\n\n");

    start = time(NULL);

    layer7_init_config(&config);

    if (layer7_parse_args(argc, argv, &config) != 0) {
        fprintf(stderr, "ERROR: Failed to parse arguments\n");
        return 1;
    }

    if (layer7_validate_config(&config) != 0) {
        fprintf(stderr, "ERROR: Invalid configuration\n");
        return 1;
    }

    if (config.verbose) {
        printf("Configuration:\n");
        printf("  Input:            %s\n", config.input_path);
        printf("  Output:           %s\n", config.output_path);
        printf("  Summary:          %s\n", config.summary_path);
        printf("  Materials DB:     %s\n", config.materials_path);
        printf("  Processing DB:    %s\n", config.processing_combinations_path);
        printf("  AWARE agri:       %s\n", config.aware_agri_path);
        printf("  AWARE nonagri:    %s\n", config.aware_nonagri_path);
        printf("  Validation:       %s\n",
               config.enable_validation ? "enabled" : "disabled");
        printf("  Statistics:       %s\n",
               config.enable_statistics ? "enabled" : "disabled");
        printf("\n");
    }

    printf("Starting water footprint calculations...\n");
    result = layer7_process_csv(&config, &stats);

    if (result != 0) {
        fprintf(stderr, "ERROR: Calculation failed with code %d\n", result);
        return 1;
    }

    if (config.enable_statistics) {
        printf("\nWriting statistics to %s ...\n", config.summary_path);
        if (layer7_write_statistics(&stats, config.summary_path) != 0) {
            fprintf(stderr, "WARNING: Failed to write statistics\n");
        }
    }

    end = time(NULL);

    printf("\n========================================\n");
    printf("Layer 7 Calculation Completed\n");
    printf("========================================\n");
    printf("Records processed: %d\n", stats.total_records);
    printf("Processing time:   %.0f seconds\n", difftime(end, start));
    if (stats.total_records > 0) {
        printf("Avg time/record:   %.4f seconds\n",
               difftime(end, start) / stats.total_records);
    }

    printf("\nOutput: %s\n", config.output_path);
    if (config.enable_statistics)
        printf("Summary: %s\n", config.summary_path);

    return 0;
}
