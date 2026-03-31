/**
 * @file test_processing.c
 * @ingroup Tests
 * @brief Tests for material processing water footprint module.
 */

#include <stdio.h>
#include <string.h>
#include "processing/material_processing.h"
#include "aware/aware.h"
#include "tests/test_runner.h"

int test_processing(void)
{
    printf("\n=== Material Processing Module Test ===\n\n");

    ProcessingStepDatabase step_db;
    MaterialProcessDatabase combo_db;

    /* Test 1: Load processing steps from CSV */
    printf("Test 1: Loading Processing Steps Database\n");
    if (processing_init_step_database(&step_db) != 0) {
        printf("ERROR: Failed to initialize step database\n");
        return -1;
    }

    int steps_loaded = processing_load_steps_csv(
        &step_db, PROCESSING_STEPS_PATH);
    if (steps_loaded < 0) {
        printf("CSV not found: %s\n", PROCESSING_STEPS_PATH);
        printf("Skipping CSV-dependent tests.\n\n");
    } else {
        printf("Loaded %d processing steps.\n\n", steps_loaded);

        printf("Sample processing steps (first 5):\n");
        printf("%-25s | %-25s | %s\n",
               "Name", "Category", "WU (m3/kg)");
        printf("-------------------------+"
               "---------------------------+"
               "-----------------\n");
        for (size_t i = 0; i < 5 && i < step_db.count; i++) {
            printf("%-25.25s | %-25s | %.6f\n",
                   step_db.steps[i].name,
                   processing_get_category_name(
                       step_db.steps[i].category),
                   step_db.steps[i]
                       .water_consumption_m3_per_kg);
        }
        printf("\n");
    }

    /* Test 2: Load combinations from CSV */
    printf("Test 2: Loading Material-Process "
           "Combinations Database\n");
    if (processing_init_combo_database(&combo_db) != 0) {
        printf("ERROR: Failed to initialize combo db\n");
        return -1;
    }

    int combos_loaded = processing_load_combinations_csv(
        &combo_db, PROCESSING_COMBOS_PATH);
    if (combos_loaded < 0) {
        printf("CSV not found: %s\n", PROCESSING_COMBOS_PATH);
        printf("Skipping CSV-dependent tests.\n\n");
    } else {
        printf("Loaded %d combinations.\n\n", combos_loaded);
    }

    /* Test 3: Synthetic calculation with AWARE */
    printf("Test 3: Water Footprint Calculation "
           "(synthetic data)\n");

    /* Create synthetic combo database */
    MaterialProcessDatabase synth_combo;
    processing_init_combo_database(&synth_combo);

    /* cotton + Spinning: WU = 0.005 m3/kg */
    strncpy(synth_combo.combinations[0].material_name,
            "cotton", MAX_COMBO_MATERIAL_NAME_LEN);
    strncpy(synth_combo.combinations[0].process_name,
            "Spinning", MAX_PROCESS_NAME_LEN);
    synth_combo.combinations[0].water_consumption_m3_per_kg
        = 0.005;
    synth_combo.count = 1;

    /* cotton + Weaving: WU = 0.008 m3/kg */
    strncpy(synth_combo.combinations[1].material_name,
            "cotton", MAX_COMBO_MATERIAL_NAME_LEN);
    strncpy(synth_combo.combinations[1].process_name,
            "Weaving", MAX_PROCESS_NAME_LEN);
    synth_combo.combinations[1].water_consumption_m3_per_kg
        = 0.008;
    synth_combo.count = 2;

    /* Setup AWARE nonagri database */
    AwareDatabase aware_nonagri;
    aware_init_database(&aware_nonagri,
                        AWARE_GLOBAL_FALLBACK_NONAGRI);
    aware_load_aliases(&aware_nonagri);

    /* Add China entry */
    strncpy(aware_nonagri.entries[0].country_name,
            "China", AWARE_MAX_COUNTRY_LEN);
    aware_nonagri.entries[0].aware_cf_annual = 25.0;
    aware_nonagri.count = 1;

    ProductProcessingList product_list;
    ProcessingResult proc_result;

    if (processing_init_product_list(&product_list) != 0) {
        printf("ERROR: Failed to init product list\n");
        return -1;
    }

    /* 0.2 kg cotton processed in China */
    int mat_idx = processing_add_material(
        &product_list, "cotton", 0.2, "China");
    if (mat_idx < 0) {
        printf("ERROR: Failed to add material\n");
        return -1;
    }

    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Spinning");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Weaving");

    if (processing_calculate_footprint(
            &synth_combo, &product_list,
            &aware_nonagri, &proc_result) != 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    double expect_spin = 0.2 * 0.005 * 25.0;
    double expect_weave = 0.2 * 0.008 * 25.0;
    double expect_total = expect_spin + expect_weave;

    printf("Scenario: 0.2 kg cotton, factory in China "
           "(AWARE=25.0)\n");
    printf("  Spinning: 0.2 * 0.005 * 25.0 "
           "= %.6f m3 world-eq\n", expect_spin);
    printf("  Weaving:  0.2 * 0.008 * 25.0 "
           "= %.6f m3 world-eq\n", expect_weave);
    printf("  -----------------------------------------\n");
    printf("  Total: %.6f m3 world-eq\n",
           proc_result.total_footprint_m3_world_eq);
    printf("  Expected: %.6f m3 world-eq\n\n",
           expect_total);

    /* Test 4: Single calculation */
    printf("Test 4: Single Calculation\n");
    double single_wf = processing_calculate_single(
        &synth_combo, "cotton", "Spinning", 0.2, 25.0);
    printf("  cotton + Spinning: %.6f m3 world-eq\n",
           single_wf);
    printf("  Expected: %.6f m3 world-eq\n\n",
           expect_spin);

    /* Cleanup */
    processing_free_step_database(&step_db);
    processing_free_combo_database(&combo_db);
    processing_free_combo_database(&synth_combo);
    aware_free_database(&aware_nonagri);

    printf("=== Processing Test Complete ===\n");

    return 0;
}
