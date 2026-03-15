/**
 * @file mode_probability.h
 * @ingroup Transport
 * @brief Multinomial logit model for transport mode estimation.
 *
 * Implements a distance-dependent multinomial logit model to
 * estimate the probability distribution of transport modes when
 * specific mode information is unavailable.
 *
 * Model: P_m(D) = exp(U_m(D)) / sum_k(exp(U_k(D)))
 * Utility: U_m(D) = beta_0,m + beta_1,m * ln(D)
 *
 * Road is the reference mode with U_road(D) = 0.
 *
 * @see research_paper.tex Section 3.2.2
 */

#ifndef MODE_PROBABILITY_H_
#define MODE_PROBABILITY_H_

#include "transport.h"

/**
 * @name Beta coefficients for multinomial logit model.
 * Derived from TU Delft freight transport research with sea
 * and air modes reverse-engineered for global textile supply chains.
 * @see research_paper.tex Section 3.2.2
 * @{
 */
#define BETA_0_ROAD     0.0   /**< Road intercept (reference). */
#define BETA_1_ROAD     0.0   /**< Road distance sensitivity. */
#define BETA_0_RAIL     -2.5  /**< Rail intercept. */
#define BETA_1_RAIL     0.35  /**< Rail distance sensitivity. */
#define BETA_0_IWW      -3.5  /**< IWW intercept. */
#define BETA_1_IWW      0.25  /**< IWW distance sensitivity. */
#define BETA_0_SEA      -5.5  /**< Sea intercept. */
#define BETA_1_SEA      0.75  /**< Sea distance sensitivity. */
#define BETA_0_AIR      -6.0  /**< Air intercept. */
#define BETA_1_AIR      0.55  /**< Air distance sensitivity. */
/** @} */

/**
 * @brief Beta coefficients for a transport mode.
 */
typedef struct {
    double beta_0;  /**< Mode-specific intercept. */
    double beta_1;  /**< Log-distance coefficient. */
} ModeCoefficients;

/**
 * @brief Calculate utility for a transport mode at given distance.
 *
 * Implements: U_m(D) = beta_0,m + beta_1,m * ln(D)
 *
 * @param[in] mode         Transport mode.
 * @param[in] distance_km  Travel distance (km). Clamped to min 1 km.
 * @return Utility value.
 */
double mode_probability_calculate_utility(TransportMode mode,
                                          double distance_km);

/**
 * @brief Calculate utilities for all modes at given distance.
 *
 * @param[in]  distance_km  Travel distance (km).
 * @param[out] utilities    Array of size TRANSPORT_MODE_COUNT.
 * @return 0 on success, -1 on error.
 */
int mode_probability_calculate_all_utilities(
    double distance_km,
    double utilities[TRANSPORT_MODE_COUNT]);

/**
 * @brief Calculate probability for a transport mode.
 *
 * Implements: P_m(D) = exp(U_m(D)) / sum_k(exp(U_k(D)))
 *
 * @param[in] mode         Transport mode.
 * @param[in] distance_km  Travel distance (km).
 * @return Probability (0.0 to 1.0).
 */
double mode_probability_calculate(TransportMode mode,
                                  double distance_km);

/**
 * @brief Calculate probabilities for all modes.
 *
 * @param[in]  distance_km    Travel distance (km).
 * @param[out] probabilities  Array of size TRANSPORT_MODE_COUNT.
 * @return 0 on success, -1 on error.
 *
 * @note Output probabilities sum to 1.0 within floating-point precision.
 */
int mode_probability_calculate_all(
    double distance_km,
    double probabilities[TRANSPORT_MODE_COUNT]);

/**
 * @brief Get beta coefficients for a mode.
 *
 * @param[in]  mode    Transport mode.
 * @param[out] coeffs  Pointer to ModeCoefficients to populate.
 * @return 0 on success, -1 if invalid.
 */
int mode_probability_get_coefficients(TransportMode mode,
                                      ModeCoefficients *coeffs);

/**
 * @brief Get most likely mode for a given distance.
 *
 * @param[in] distance_km  Travel distance (km).
 * @return TransportMode with highest probability.
 */
TransportMode mode_probability_get_dominant_mode(
    double distance_km);

#endif  /* MODE_PROBABILITY_H_ */
