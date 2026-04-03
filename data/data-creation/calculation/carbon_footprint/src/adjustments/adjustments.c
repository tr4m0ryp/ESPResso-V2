/**
 * @file adjustments.c
 * @ingroup Adjustments
 * @brief Additional adjustment factors for unmodelled life cycle stages.
 *
 * @see research_paper.tex Section 3.6
 */

#include "adjustments/adjustments.h"
#include <stddef.h>

/* Type name strings */
static const char *TYPE_NAMES[ADJUSTMENT_TYPE_COUNT] = {
    "Internal transport and logistics",
    "Emissions, waste, and by-product management",
    "Combined adjustment"
};

/* Percentage values indexed by AdjustmentType */
static const double PERCENTAGES[ADJUSTMENT_TYPE_COUNT] = {
    ADJUSTMENT_INTERNAL_TRANSPORT_PERCENT,
    ADJUSTMENT_WASTE_MANAGEMENT_PERCENT,
    ADJUSTMENT_TOTAL_PERCENT
};

double adjustments_apply(double modelled_footprint_kg_CO2e)
{
    if (modelled_footprint_kg_CO2e < 0.0) {
        return -1.0;
    }

    /*
     * Implementation of formula from research_paper.tex Section 3.6:
     * CF_cradle_to_gate = CF_modelled * 1.02
     */
    return modelled_footprint_kg_CO2e * ADJUSTMENT_MULTIPLIER;
}

int adjustments_apply_with_breakdown(double modelled_footprint_kg_CO2e,
                                      AdjustmentBreakdown *breakdown)
{
    if (breakdown == NULL || modelled_footprint_kg_CO2e < 0.0) {
        return -1;
    }

    breakdown->modelled_footprint_kg_CO2e = modelled_footprint_kg_CO2e;

    /*
     * Calculate individual adjustment components
     * Each component is calculated as: CF_modelled * (percentage / 100)
     */
    breakdown->internal_transport_kg_CO2e =
        modelled_footprint_kg_CO2e * (ADJUSTMENT_INTERNAL_TRANSPORT_PERCENT / 100.0);

    breakdown->waste_management_kg_CO2e =
        modelled_footprint_kg_CO2e * (ADJUSTMENT_WASTE_MANAGEMENT_PERCENT / 100.0);

    breakdown->total_adjustment_kg_CO2e =
        breakdown->internal_transport_kg_CO2e + breakdown->waste_management_kg_CO2e;

    breakdown->adjusted_footprint_kg_CO2e =
        modelled_footprint_kg_CO2e + breakdown->total_adjustment_kg_CO2e;

    return 0;
}

double adjustments_calculate_component(double modelled_footprint_kg_CO2e,
                                        AdjustmentType type)
{
    if (modelled_footprint_kg_CO2e < 0.0) {
        return -1.0;
    }

    if (type < 0 || type >= ADJUSTMENT_TYPE_COUNT) {
        return -1.0;
    }

    return modelled_footprint_kg_CO2e * (PERCENTAGES[type] / 100.0);
}

double adjustments_get_multiplier(void)
{
    return ADJUSTMENT_MULTIPLIER;
}

double adjustments_get_percentage(AdjustmentType type)
{
    if (type < 0 || type >= ADJUSTMENT_TYPE_COUNT) {
        return -1.0;
    }

    return PERCENTAGES[type];
}

const char *adjustments_get_type_name(AdjustmentType type)
{
    if (type < 0 || type >= ADJUSTMENT_TYPE_COUNT) {
        return "Unknown";
    }

    return TYPE_NAMES[type];
}

double adjustments_reverse(double adjusted_footprint_kg_CO2e)
{
    if (adjusted_footprint_kg_CO2e < 0.0) {
        return -1.0;
    }

    /*
     * Reverse calculation:
     * CF_modelled = CF_adjusted / 1.02
     */
    return adjusted_footprint_kg_CO2e / ADJUSTMENT_MULTIPLIER;
}

double adjustments_calculate_from_components(double raw_materials_kg_CO2e,
                                              double transport_kg_CO2e,
                                              double processing_kg_CO2e,
                                              double packaging_kg_CO2e,
                                              AdjustmentBreakdown *breakdown)
{
    /* Validate inputs - negative values indicate error */
    if (raw_materials_kg_CO2e < 0.0 || transport_kg_CO2e < 0.0 ||
        processing_kg_CO2e < 0.0 || packaging_kg_CO2e < 0.0) {
        return -1.0;
    }

    /* Sum all modelled phases */
    double modelled_total = raw_materials_kg_CO2e +
                            transport_kg_CO2e +
                            processing_kg_CO2e +
                            packaging_kg_CO2e;

    /* Apply adjustment and optionally fill breakdown */
    if (breakdown != NULL) {
        if (adjustments_apply_with_breakdown(modelled_total, breakdown) != 0) {
            return -1.0;
        }
        return breakdown->adjusted_footprint_kg_CO2e;
    }

    return adjustments_apply(modelled_total);
}
