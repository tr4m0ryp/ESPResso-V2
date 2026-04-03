/**
 * @file test_processing.c
 * @ingroup Tests
 * @brief Tests for material processing carbon footprint module.
 */

#include <stdio.h>
#include "processing/material_processing.h"
#include "tests/test_runner.h"

int test_processing(void)
{
    printf("\n=== Material Processing Module Test ===\n\n");

    ProcessingStepDatabase step_db;
    MaterialProcessDatabase combo_db;

    /* Test 1: Load processing steps */
    printf("Test 1: Loading Processing Steps Database\n");
    if (processing_init_step_database(&step_db) != 0) {
        printf("ERROR: Failed to initialize step database\n");
        return -1;
    }

    int steps_loaded = processing_load_steps_csv(
        &step_db, PROCESSING_STEPS_PATH);
    if (steps_loaded < 0) {
        printf("ERROR: Failed to load processing steps "
               "from: %s\n", PROCESSING_STEPS_PATH);
        return -1;
    }
    printf("Loaded %d processing steps.\n\n", steps_loaded);

    /* Display sample processing steps */
    printf("Sample processing steps (first 10):\n");
    printf("%-25s | %-25s | %s\n",
           "Name", "Category", "EF (kg CO2e/kg)");
    printf("-------------------------+"
           "---------------------------+"
           "-----------------\n");
    for (size_t i = 0; i < 10 && i < step_db.count; i++) {
        printf("%-25.25s | %-25s | %.2f\n",
               step_db.steps[i].name,
               processing_get_category_name(
                   step_db.steps[i].category),
               step_db.steps[i]
                   .emission_factor_kg_CO2e_per_kg);
    }
    printf("\n");

    /* Test 2: Load material-process combinations */
    printf("Test 2: Loading Material-Process "
           "Combinations Database\n");
    if (processing_init_combo_database(&combo_db) != 0) {
        printf("ERROR: Failed to initialize "
               "combo database\n");
        return -1;
    }

    int combos_loaded = processing_load_combinations_csv(
        &combo_db, PROCESSING_COMBOS_PATH);
    if (combos_loaded < 0) {
        printf("ERROR: Failed to load combinations "
               "from: %s\n", PROCESSING_COMBOS_PATH);
        return -1;
    }
    printf("Loaded %d material-process combinations.\n\n",
           combos_loaded);

    /* Test 3: Find processing step by name */
    printf("Test 3: Processing Step Lookup\n");
    const char *test_steps[] = {
        "Spinning", "Weaving",
        "Batch Dyeing", "Finishing"
    };
    int num_test_steps =
        sizeof(test_steps) / sizeof(test_steps[0]);

    for (int i = 0; i < num_test_steps; i++) {
        int idx = processing_find_step_by_name(
            &step_db, test_steps[i]);
        if (idx >= 0) {
            printf("  '%s' -> EF: %.2f kg CO2e/kg (%s)\n",
                   test_steps[i],
                   step_db.steps[idx]
                       .emission_factor_kg_CO2e_per_kg,
                   processing_get_category_name(
                       step_db.steps[idx].category));
        } else {
            printf("  '%s' -> NOT FOUND\n", test_steps[i]);
        }
    }
    printf("\n");

    /* Test 4: Emission factor for material-process combo */
    printf("Test 4: Material-Process "
           "Emission Factor Lookup\n");
    struct {
        const char *material;
        const char *process;
    } test_combos[] = {
        {"cotton", "Spinning"},
        {"cotton", "Weaving"},
        {"cotton", "Batch Dyeing"},
        {"polyester", "Extrusion"},
        {"silk", "Degumming"},
        {"wool", "Scouring"}
    };
    int num_combos =
        sizeof(test_combos) / sizeof(test_combos[0]);

    for (int i = 0; i < num_combos; i++) {
        double ef = processing_get_emission_factor(
            &combo_db,
            test_combos[i].material,
            test_combos[i].process);
        if (ef >= 0) {
            printf("  %s + %s -> %.2f kg CO2e/kg\n",
                   test_combos[i].material,
                   test_combos[i].process, ef);
        } else {
            printf("  %s + %s -> NOT FOUND\n",
                   test_combos[i].material,
                   test_combos[i].process);
        }
    }
    printf("\n");

    /* Test 5: Single material-process calculation */
    printf("Test 5: Single Material-Process "
           "Calculation\n");
    printf("Scenario: 0.2 kg cotton "
           "undergoing Spinning\n");
    double single_cf = processing_calculate_single(
        &combo_db, "cotton", "Spinning", 0.2);
    if (single_cf >= 0) {
        printf("Carbon footprint: %.4f kg CO2e\n",
               single_cf);
        double ef = processing_get_emission_factor(
            &combo_db, "cotton", "Spinning");
        printf("Verification: 0.2 kg * %.2f kg CO2e/kg "
               "= %.4f kg CO2e\n\n", ef, 0.2 * ef);
    } else {
        printf("ERROR: Calculation failed\n\n");
    }

    /* Test 6: Complete product processing calculation */
    printf("Test 6: Complete Product Processing "
           "Calculation\n");
    printf("Scenario: Cotton T-shirt (0.2 kg cotton)\n");
    printf("Processing steps: Ginning -> Carding -> "
           "Spinning -> Weaving -> "
           "Batch Dyeing -> Finishing\n\n");

    ProductProcessingList product_list;
    ProcessingResult proc_result;

    if (processing_init_product_list(&product_list) != 0) {
        printf("ERROR: Failed to initialize "
               "product list\n");
        return -1;
    }

    /* Add cotton material */
    int mat_idx = processing_add_material(
        &product_list, "cotton", 0.2);
    if (mat_idx < 0) {
        printf("ERROR: Failed to add material\n");
        return -1;
    }

    /* Add processing steps */
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Ginning");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Carding");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Spinning");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Weaving");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Batch Dyeing");
    processing_add_step_to_material(
        &product_list, (size_t)mat_idx, "Finishing");

    if (processing_calculate_footprint(
            &combo_db, &product_list,
            &proc_result) != 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    printf("Processing step breakdown:\n");
    const char *steps[] = {
        "Ginning", "Carding", "Spinning",
        "Weaving", "Batch Dyeing", "Finishing"
    };
    double total_manual = 0.0;
    for (int i = 0; i < 6; i++) {
        double ef = processing_get_emission_factor(
            &combo_db, "cotton", steps[i]);
        if (ef >= 0) {
            double contrib = 0.2 * ef;
            total_manual += contrib;
            printf("  %-15s: 0.2 kg * %.2f "
                   "= %.4f kg CO2e\n",
                   steps[i], ef, contrib);
        }
    }
    printf("  ----------------------------------------"
           "------\n");
    printf("  Total processing footprint: "
           "%.4f kg CO2e\n",
           proc_result.total_footprint_kg_CO2e);
    printf("  Manual verification:        "
           "%.4f kg CO2e\n\n", total_manual);

    /* Cleanup */
    processing_free_step_database(&step_db);
    processing_free_combo_database(&combo_db);

    printf("=== Processing Test Complete ===\n");

    return 0;
}
