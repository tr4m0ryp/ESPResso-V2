/**
 * @file test_packaging.c
 * @ingroup Tests
 * @brief Tests for packaging carbon footprint module.
 */

#include <stdio.h>
#include "packaging/packaging.h"
#include "tests/test_runner.h"

int test_packaging(void)
{
    printf("\n=== Packaging Module Test ===\n\n");

    /* Test 1: Emission factors by category */
    printf("Test 1: Packaging Category "
           "Emission Factors\n");
    printf("%-20s | %s\n",
           "Category", "EF (kg CO2e/kg)");
    printf("--------------------+-----------------\n");
    for (int c = 0; c < PACKAGING_CATEGORY_COUNT; c++) {
        printf("%-20s | %.1f\n",
               packaging_get_category_name(
                   (PackagingCategory)c),
               packaging_get_emission_factor(
                   (PackagingCategory)c));
    }
    printf("\n");

    /* Test 2: Category string parsing */
    printf("Test 2: Category String Parsing\n");
    const char *test_strings[] = {
        "paper", "cardboard", "carton", "plastic",
        "PE", "polybag", "glass", "bottle",
        "metal", "wood", "unknown"
    };
    int num_strings =
        sizeof(test_strings) / sizeof(test_strings[0]);

    for (int i = 0; i < num_strings; i++) {
        PackagingCategory cat =
            packaging_category_from_string(
                test_strings[i]);
        printf("  \"%s\" -> %s\n", test_strings[i],
               packaging_get_category_name(cat));
    }
    printf("\n");

    /* Test 3: Single item calculation */
    printf("Test 3: Single Item Calculation\n");
    printf("Scenario: 0.05 kg plastic bag\n");
    double single_cf = packaging_calculate_single(
        PACKAGING_PLASTIC, 0.05);
    printf("Carbon footprint: %.4f kg CO2e\n", single_cf);
    printf("Verification: 0.05 kg * 3.5 kg CO2e/kg "
           "= %.4f kg CO2e\n\n", 0.05 * 3.5);

    /* Test 4: Single item by name */
    printf("Test 4: Single Item Calculation by Name\n");
    printf("Scenario: 0.1 kg cardboard box\n");
    double single_cf_name =
        packaging_calculate_single_by_name(
            "cardboard", 0.1);
    printf("Carbon footprint: %.4f kg CO2e\n",
           single_cf_name);
    printf("Verification: 0.1 kg * 1.3 kg CO2e/kg "
           "= %.4f kg CO2e\n\n", 0.1 * 1.3);

    /* Test 5: Complete packaging list calculation */
    printf("Test 5: Complete Packaging List "
           "Calculation\n");
    printf("Scenario: T-shirt packaging\n");
    printf("  - Cardboard box: 0.15 kg\n");
    printf("  - Plastic polybag: 0.02 kg\n");
    printf("  - Tissue paper: 0.01 kg\n\n");

    PackagingList pkg_list;
    PackagingResult pkg_result;

    if (packaging_init_list(&pkg_list) != 0) {
        printf("ERROR: Failed to initialize "
               "packaging list\n");
        return -1;
    }

    packaging_add_item(&pkg_list,
        PACKAGING_PAPER_CARDBOARD, 0.15,
        "Cardboard box");
    packaging_add_item(&pkg_list,
        PACKAGING_PLASTIC, 0.02,
        "Plastic polybag");
    packaging_add_item(&pkg_list,
        PACKAGING_PAPER_CARDBOARD, 0.01,
        "Tissue paper");

    if (packaging_calculate_footprint(
            &pkg_list, &pkg_result) != 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    printf("Calculation breakdown:\n");
    printf("  Cardboard box:    0.15 kg * 1.3 "
           "= %.4f kg CO2e\n", 0.15 * 1.3);
    printf("  Plastic polybag:  0.02 kg * 3.5 "
           "= %.4f kg CO2e\n", 0.02 * 3.5);
    printf("  Tissue paper:     0.01 kg * 1.3 "
           "= %.4f kg CO2e\n", 0.01 * 1.3);
    printf("  ----------------------------------------"
           "------\n");
    printf("  Total packaging footprint: "
           "%.4f kg CO2e\n",
           pkg_result.total_footprint_kg_CO2e);
    printf("  Total packaging mass:      "
           "%.4f kg\n\n",
           pkg_result.total_mass_kg);

    /* Verify calculation */
    double expected =
        (0.15 * 1.3) + (0.02 * 3.5) + (0.01 * 1.3);
    printf("  Manual verification:       "
           "%.4f kg CO2e\n\n", expected);

    /* Test 6: Category breakdown */
    printf("Test 6: Category Breakdown\n");
    for (int c = 0; c < PACKAGING_CATEGORY_COUNT; c++) {
        if (pkg_result.category_masses[c] > 0) {
            printf("  %-20s: %.4f kg -> "
                   "%.4f kg CO2e\n",
                   packaging_get_category_name(
                       (PackagingCategory)c),
                   pkg_result.category_masses[c],
                   pkg_result
                       .category_contributions[c]);
        }
    }
    printf("\n");

    /* Test 7: Add items by name */
    printf("Test 7: Add Items by Name\n");
    packaging_clear_list(&pkg_list);

    packaging_add_item_by_name(&pkg_list,
        "corrugated", 0.2, "Shipping box");
    packaging_add_item_by_name(&pkg_list,
        "HDPE", 0.03, "Plastic wrap");
    /* Zero weight - should not affect total */
    packaging_add_item_by_name(&pkg_list,
        "glass", 0.0, "N/A");

    packaging_calculate_footprint(
        &pkg_list, &pkg_result);

    printf("  Shipping box (corrugated): "
           "0.20 kg -> %.4f kg CO2e\n", 0.2 * 1.3);
    printf("  Plastic wrap (HDPE):       "
           "0.03 kg -> %.4f kg CO2e\n", 0.03 * 3.5);
    printf("  ----------------------------------------"
           "------\n");
    printf("  Total:                     "
           "%.4f kg CO2e\n\n",
           pkg_result.total_footprint_kg_CO2e);

    printf("=== Packaging Test Complete ===\n");

    return 0;
}
