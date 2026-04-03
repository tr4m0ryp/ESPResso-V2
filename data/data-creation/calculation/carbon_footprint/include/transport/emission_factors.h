/**
 * @file emission_factors.h
 * @ingroup Transport
 * @brief Transport mode emission factor definitions.
 *
 * Defines weighted-average emission factors per transport mode
 * category. Values derived from research_paper.tex Table 2.
 * These represent Well-to-Wheel (WtW) emission intensities,
 * aggregated from mode subtypes using weighted averages.
 *
 * @see research_paper.tex Section 3.2.1
 */

#ifndef EMISSION_FACTORS_H_
#define EMISSION_FACTORS_H_

#include "transport.h"

/**
 * @name Weighted-average emission factors (gCO2e/tkm)
 * Source: research_paper.tex Table 2.
 * Formula: EF_m = sum(EF_m,k * u_m,k)
 * @{
 */
#define EF_ROAD_gCO2e_tkm       72.9  /**< Road: Diesel articulated HGV. */
#define EF_RAIL_gCO2e_tkm       22.0  /**< Rail: Freight train generic. */
#define EF_IWW_gCO2e_tkm        31.0  /**< Inland waterway: Barge generic. */
#define EF_SEA_gCO2e_tkm        10.3  /**< Sea: Container ship weighted avg. */
#define EF_AIR_gCO2e_tkm        782.0 /**< Air: Long-haul freight weighted avg. */
/** @} */

/**
 * @name Subtype emission factors (gCO2e/tkm)
 * Reference values from research_paper.tex Table 1.
 * @{
 */
#define EF_ROAD_HGV_GT33T_LADEN_gCO2e_tkm       74.0   /**< HGV >33t laden. */
#define EF_ROAD_HGV_3_5_33T_FULL_gCO2e_tkm      67.63  /**< HGV 3.5-33t full. */
#define EF_ROAD_GENERIC_ACADEMIC_gCO2e_tkm      78.0   /**< Generic academic. */
#define EF_SEA_DEEP_gCO2e_tkm                   8.4    /**< Deep-sea container. */
#define EF_SEA_SHORT_gCO2e_tkm                  16.0   /**< Short-sea container. */
#define EF_AIR_LONGHAUL_FREIGHTER_gCO2e_tkm     560.0  /**< Freighter aircraft. */
#define EF_AIR_LONGHAUL_BELLY_gCO2e_tkm         990.0  /**< Belly-hold aircraft. */
/** @} */

/**
 * @brief Get emission factor for transport mode.
 *
 * @param[in] mode  Transport mode enum value.
 * @return Emission factor in gCO2e/tkm, or -1.0 if invalid.
 */
double emission_factor_get(TransportMode mode);

/**
 * @brief Get array of all emission factors.
 *
 * @param[out] factors  Array of size TRANSPORT_MODE_COUNT.
 * @return 0 on success, -1 if factors is NULL.
 */
int emission_factor_get_all(
    double factors[TRANSPORT_MODE_COUNT]);

/**
 * @brief Calculate weighted emission factor from probabilities.
 *
 * Formula: EF_weighted = sum(P_m * EF_m)
 *
 * @param[in] probabilities  Array of mode probabilities (sum to 1.0).
 * @return Weighted EF in gCO2e/tkm, or -1.0 on error.
 */
double emission_factor_calculate_weighted(
    const double probabilities[TRANSPORT_MODE_COUNT]);

#endif  /* EMISSION_FACTORS_H_ */
