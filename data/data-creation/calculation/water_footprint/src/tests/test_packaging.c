/**
 * @file test_packaging.c
 * @ingroup Tests
 * @brief Tests for packaging water footprint module.
 */

#include <stdio.h>
#include "packaging/packaging.h"
#include "tests/test_runner.h"

int test_packaging(void)
{
    printf("\n=== Packaging Module Test ===\n\n");

    /* Test 1: Water consumption factors by category */
    printf("Test 1: Packaging Category "
           "Water Consumption Factors\n");
    printf("%-20s | %s\n",
           "Category", "WU (m3/kg)");
    printf("--------------------+-----------------\n");
    for (int c = 0; c < PACKAGING_CATEGORY_COUNT; c++) {
        printf("%-20s | %.4f\n",
               packaging_get_category_name(
                   (PackagingCategory)c),
               packaging_get_water_consumption(
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
        (int)(sizeof(test_strings) / sizeof(test_strings[0]));

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
    double single_wf = packaging_calculate_single(
        PACKAGING_PLASTIC, 0.05);
    printf("Water footprint: %.6f m3\n", single_wf);
    printf("Verification: 0.05 kg * %.4f m3/kg "
           "= %.6f m3\n\n",
           WU_PACKAGING_PLASTIC_m3_kg,
           0.05 * WU_PACKAGING_PLASTIC_m3_kg);

    /* Test 4: Single item by name */
    printf("Test 4: Single Item by Name\n");
    printf("Scenario: 0.1 kg cardboard box\n");
    double single_wf_name =
        packaging_calculate_single_by_name(
            "cardboard", 0.1);
    printf("Water footprint: %.6f m3\n", single_wf_name);
    printf("Verification: 0.1 kg * %.4f m3/kg "
           "= %.6f m3\n\n",
           WU_PACKAGING_PAPER_CARDBOARD_m3_kg,
           0.1 * WU_PACKAGING_PAPER_CARDBOARD_m3_kg);

    /* Test 5: Complete packaging list */
    printf("Test 5: Complete Packaging List\n");
    printf("Scenario: T-shirt packaging\n");
    printf("  - Cardboard box: 0.15 kg\n");
    printf("  - Plastic polybag: 0.02 kg\n");
    printf("  - Tissue paper: 0.01 kg\n\n");

    PackagingList pkg_list;
    PackagingResult pkg_result;

    if (packaging_init_list(&pkg_list) != 0) {
        printf("ERROR: Failed to initialize list\n");
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

    double wu_paper = WU_PACKAGING_PAPER_CARDBOARD_m3_kg;
    double wu_plastic = WU_PACKAGING_PLASTIC_m3_kg;

    printf("Calculation breakdown:\n");
    printf("  Cardboard box:    0.15 kg * %.4f "
           "= %.6f m3\n", wu_paper, 0.15 * wu_paper);
    printf("  Plastic polybag:  0.02 kg * %.4f "
           "= %.6f m3\n", wu_plastic, 0.02 * wu_plastic);
    printf("  Tissue paper:     0.01 kg * %.4f "
           "= %.6f m3\n", wu_paper, 0.01 * wu_paper);
    printf("  ------------------------------------------\n");
    printf("  Total water footprint: %.6f m3\n",
           pkg_result.total_footprint_m3);
    printf("  Total packaging mass:  %.4f kg\n\n",
           pkg_result.total_mass_kg);

    double expected =
        (0.15 * wu_paper) + (0.02 * wu_plastic)
        + (0.01 * wu_paper);
    printf("  Manual verification: %.6f m3\n\n", expected);

    /* Test 6: Category breakdown */
    printf("Test 6: Category Breakdown\n");
    for (int c = 0; c < PACKAGING_CATEGORY_COUNT; c++) {
        if (pkg_result.category_masses[c] > 0) {
            printf("  %-20s: %.4f kg -> "
                   "%.6f m3\n",
                   packaging_get_category_name(
                       (PackagingCategory)c),
                   pkg_result.category_masses[c],
                   pkg_result
                       .category_contributions[c]);
        }
    }
    printf("\n");

    printf("=== Packaging Test Complete ===\n");

    return 0;
}
