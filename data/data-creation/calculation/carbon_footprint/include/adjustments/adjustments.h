/**
 * @file adjustments.h
 * @ingroup Adjustments
 * @brief Additional adjustments for cradle-to-gate carbon footprint.
 *
 * Implements adjustment factors for life cycle stages not modelled
 * explicitly, as defined in ISO 14040/14044 and PEFCR for Apparel
 * and Footwear v3.1.
 *
 * Formula: CF_cradle_to_gate = CF_modelled * 1.02
 *
 * The 2% adjustment accounts for:
 * - 1% internal transport and logistics
 * - 1% emissions, waste, and by-product management
 *
 * @see research_paper.tex Section 3.6
 */

#ifndef ADJUSTMENTS_H_
#define ADJUSTMENTS_H_

/**
 * @name Adjustment factors (percentages).
 * Conservative estimates based on sector-specific LCAs.
 * @see research_paper.tex Section 3.6
 * @{
 */
#define ADJUSTMENT_INTERNAL_TRANSPORT_PERCENT   1.0  /**< Internal transport (1%). */
#define ADJUSTMENT_WASTE_MANAGEMENT_PERCENT     1.0  /**< Waste management (1%). */
#define ADJUSTMENT_TOTAL_PERCENT                2.0  /**< Combined (2%). */
#define ADJUSTMENT_MULTIPLIER                   1.02 /**< Multiplier form. */
/** @} */

/**
 * @brief Classification of adjustment types.
 */
typedef enum {
    ADJUSTMENT_TYPE_INTERNAL_TRANSPORT, /**< Internal transport component. */
    ADJUSTMENT_TYPE_WASTE_MANAGEMENT,   /**< Waste management component. */
    ADJUSTMENT_TYPE_COMBINED,           /**< Combined adjustment. */
    ADJUSTMENT_TYPE_COUNT               /**< Sentinel count value. */
} AdjustmentType;

/**
 * @brief Detailed breakdown of adjustment calculation.
 */
typedef struct {
    double modelled_footprint_kg_CO2e;   /**< Input modelled footprint. */
    double internal_transport_kg_CO2e;   /**< Internal transport contribution. */
    double waste_management_kg_CO2e;     /**< Waste management contribution. */
    double total_adjustment_kg_CO2e;     /**< Total adjustment amount. */
    double adjusted_footprint_kg_CO2e;   /**< Final adjusted footprint. */
} AdjustmentBreakdown;

/**
 * @brief Apply adjustment factor to modelled footprint.
 *
 * Implements: CF_cradle_to_gate = CF_modelled * 1.02
 *
 * @param[in] modelled_footprint_kg_CO2e  Sum of all modelled phases.
 * @return Adjusted footprint (kg CO2e), or -1.0 on error.
 */
double adjustments_apply(double modelled_footprint_kg_CO2e);

/**
 * @brief Apply adjustment with detailed breakdown.
 *
 * @param[in]  modelled_footprint_kg_CO2e  Sum of all modelled phases.
 * @param[out] breakdown                   Pointer to AdjustmentBreakdown.
 * @return 0 on success, -1 on error.
 */
int adjustments_apply_with_breakdown(
    double modelled_footprint_kg_CO2e,
    AdjustmentBreakdown *breakdown);

/**
 * @brief Calculate individual adjustment component.
 *
 * @param[in] modelled_footprint_kg_CO2e  Sum of modelled phases.
 * @param[in] type                        Adjustment type.
 * @return Adjustment in kg CO2e, or -1.0 on error.
 */
double adjustments_calculate_component(
    double modelled_footprint_kg_CO2e,
    AdjustmentType type);

/**
 * @brief Get the adjustment multiplier.
 *
 * @return 1.02 by default.
 */
double adjustments_get_multiplier(void);

/**
 * @brief Get the adjustment percentage.
 *
 * @param[in] type  Adjustment type.
 * @return Percentage value (e.g., 1.0 for 1%).
 */
double adjustments_get_percentage(AdjustmentType type);

/**
 * @brief Get string name for adjustment type.
 *
 * @param[in] type  AdjustmentType enum value.
 * @return Static string with type name.
 */
const char *adjustments_get_type_name(AdjustmentType type);

/**
 * @brief Reverse-calculate modelled footprint from adjusted value.
 *
 * CF_modelled = CF_adjusted / 1.02
 *
 * @param[in] adjusted_footprint_kg_CO2e  Adjusted footprint.
 * @return Original modelled footprint, or -1.0 on error.
 */
double adjustments_reverse(double adjusted_footprint_kg_CO2e);

/**
 * @brief Calculate adjusted footprint from individual phase components.
 *
 * @param[in]  raw_materials_kg_CO2e  Raw materials phase.
 * @param[in]  transport_kg_CO2e      Transport phase.
 * @param[in]  processing_kg_CO2e     Processing phase.
 * @param[in]  packaging_kg_CO2e      Packaging phase.
 * @param[out] breakdown              Optional breakdown (can be NULL).
 * @return Total adjusted footprint (kg CO2e), or -1.0 on error.
 */
double adjustments_calculate_from_components(
    double raw_materials_kg_CO2e,
    double transport_kg_CO2e,
    double processing_kg_CO2e,
    double packaging_kg_CO2e,
    AdjustmentBreakdown *breakdown);

#endif  /* ADJUSTMENTS_H_ */
