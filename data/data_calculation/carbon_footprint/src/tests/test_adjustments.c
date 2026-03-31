/**
 * @file test_adjustments.c
 * @ingroup Tests
 * @brief Tests for additional adjustments module.
 */

#include <stdio.h>
#include "adjustments/adjustments.h"
#include "tests/test_runner.h"

int test_adjustments(void)
{
    printf("\n=== Additional Adjustments "
           "Module Test ===\n\n");

    /* Test 1: Basic adjustment calculation */
    printf("Test 1: Basic Adjustment Calculation\n");
    double modelled_footprint = 10.0;
    double adjusted =
        adjustments_apply(modelled_footprint);
    printf("Modelled footprint: %.2f kg CO2e\n",
           modelled_footprint);
    printf("Adjusted footprint: %.2f kg CO2e\n", adjusted);
    printf("Verification: %.2f * 1.02 = %.2f kg CO2e\n\n",
           modelled_footprint,
           modelled_footprint * 1.02);

    /* Test 2: Detailed breakdown */
    printf("Test 2: Detailed Adjustment Breakdown\n");
    AdjustmentBreakdown breakdown;
    if (adjustments_apply_with_breakdown(
            modelled_footprint, &breakdown) != 0) {
        printf("ERROR: Failed to calculate breakdown\n");
        return -1;
    }

    printf("Adjustment breakdown:\n");
    printf("  Modelled footprint:     "
           "%.4f kg CO2e\n",
           breakdown.modelled_footprint_kg_CO2e);
    printf("  Internal transport:     "
           "%.4f kg CO2e (%.1f%%)\n",
           breakdown.internal_transport_kg_CO2e,
           ADJUSTMENT_INTERNAL_TRANSPORT_PERCENT);
    printf("  Waste management:       "
           "%.4f kg CO2e (%.1f%%)\n",
           breakdown.waste_management_kg_CO2e,
           ADJUSTMENT_WASTE_MANAGEMENT_PERCENT);
    printf("  Total adjustment:       "
           "%.4f kg CO2e\n",
           breakdown.total_adjustment_kg_CO2e);
    printf("  Adjusted footprint:     "
           "%.4f kg CO2e\n\n",
           breakdown.adjusted_footprint_kg_CO2e);

    /* Test 3: Component calculations */
    printf("Test 3: Individual Component "
           "Calculations\n");
    double internal_transport =
        adjustments_calculate_component(
            modelled_footprint,
            ADJUSTMENT_TYPE_INTERNAL_TRANSPORT);
    double waste_management =
        adjustments_calculate_component(
            modelled_footprint,
            ADJUSTMENT_TYPE_WASTE_MANAGEMENT);
    double combined =
        adjustments_calculate_component(
            modelled_footprint,
            ADJUSTMENT_TYPE_COMBINED);

    printf("Components for %.2f kg CO2e "
           "modelled footprint:\n",
           modelled_footprint);
    printf("  Internal transport: "
           "%.4f kg CO2e\n", internal_transport);
    printf("  Waste management:   "
           "%.4f kg CO2e\n", waste_management);
    printf("  Combined:           "
           "%.4f kg CO2e\n\n", combined);

    /* Test 4: Adjustment parameters */
    printf("Test 4: Adjustment Parameters\n");
    printf("Adjustment percentages:\n");
    for (int type = 0; type < ADJUSTMENT_TYPE_COUNT;
         type++) {
        printf("  %-35s: %.1f%%\n",
               adjustments_get_type_name(type),
               adjustments_get_percentage(type));
    }
    printf("\nAdjustment multiplier: %.2f\n\n",
           adjustments_get_multiplier());

    /* Test 5: Reverse calculation */
    printf("Test 5: Reverse Calculation\n");
    double test_adjusted = 12.24;
    double original =
        adjustments_reverse(test_adjusted);
    printf("Adjusted footprint: %.2f kg CO2e\n",
           test_adjusted);
    printf("Original modelled:  %.2f kg CO2e\n", original);
    printf("Verification: %.2f / 1.02 "
           "= %.2f kg CO2e\n\n",
           test_adjusted, test_adjusted / 1.02);

    /* Test 6: Complete calculation from components */
    printf("Test 6: Complete Calculation "
           "from Components\n");
    double raw_materials_cf = 5.0;
    double transport_cf = 1.5;
    double processing_cf = 2.8;
    double packaging_cf = 0.7;

    printf("Component footprints:\n");
    printf("  Raw materials:  %.2f kg CO2e\n",
           raw_materials_cf);
    printf("  Transport:      %.2f kg CO2e\n",
           transport_cf);
    printf("  Processing:     %.2f kg CO2e\n",
           processing_cf);
    printf("  Packaging:      %.2f kg CO2e\n",
           packaging_cf);

    AdjustmentBreakdown full_breakdown;
    double total_adjusted =
        adjustments_calculate_from_components(
            raw_materials_cf, transport_cf,
            processing_cf, packaging_cf,
            &full_breakdown);

    printf("\nTotal calculation:\n");
    printf("  Modelled total:   %.2f kg CO2e\n",
           raw_materials_cf + transport_cf
           + processing_cf + packaging_cf);
    printf("  Adjusted total:   %.2f kg CO2e\n",
           total_adjusted);
    printf("  Adjustment total: %.2f kg CO2e (%.1f%%)\n",
           full_breakdown.total_adjustment_kg_CO2e,
           ADJUSTMENT_TOTAL_PERCENT);

    /* Test 7: Edge cases */
    printf("\nTest 7: Edge Cases\n");
    printf("Zero footprint: %.4f kg CO2e "
           "-> %.4f kg CO2e\n",
           0.0, adjustments_apply(0.0));
    printf("Negative input: %.4f kg CO2e "
           "-> %.4f kg CO2e\n",
           -1.0, adjustments_apply(-1.0));

    printf("=== Adjustments Test Complete ===\n");

    return 0;
}
