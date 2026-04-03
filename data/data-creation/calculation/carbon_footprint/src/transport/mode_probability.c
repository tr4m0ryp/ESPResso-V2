/**
 * @file mode_probability.c
 * @ingroup Transport
 * @brief Multinomial logit model for mode choice estimation.
 *
 * @see research_paper.tex Section 3.2.2
 */

#include "transport/mode_probability.h"
#include <math.h>
#include <stddef.h>

/* Minimum distance to avoid log(0) issues */
#define MIN_DISTANCE_KM 1.0

/*
 * Static array of beta coefficients indexed by TransportMode enum.
 * Road is reference mode with zero coefficients.
 */
static const ModeCoefficients BETA_COEFFICIENTS[TRANSPORT_MODE_COUNT] = {
    { BETA_0_ROAD, BETA_1_ROAD },    /* TRANSPORT_MODE_ROAD */
    { BETA_0_RAIL, BETA_1_RAIL },    /* TRANSPORT_MODE_RAIL */
    { BETA_0_IWW,  BETA_1_IWW  },    /* TRANSPORT_MODE_INLAND_WATERWAY */
    { BETA_0_SEA,  BETA_1_SEA  },    /* TRANSPORT_MODE_SEA */
    { BETA_0_AIR,  BETA_1_AIR  }     /* TRANSPORT_MODE_AIR */
};

double mode_probability_calculate_utility(TransportMode mode, double distance_km)
{
    if (mode < 0 || mode >= TRANSPORT_MODE_COUNT) {
        return 0.0;
    }

    /* Clamp distance to minimum to avoid log issues */
    if (distance_km < MIN_DISTANCE_KM) {
        distance_km = MIN_DISTANCE_KM;
    }

    /*
     * Utility function: U_m(D) = beta_0,m + beta_1,m * ln(D)
     *
     * As defined in research_paper.tex Section 3.2.2
     */
    const ModeCoefficients *coeffs = &BETA_COEFFICIENTS[mode];

    return coeffs->beta_0 + coeffs->beta_1 * log(distance_km);
}

int mode_probability_calculate_all_utilities(double distance_km,
                                              double utilities[TRANSPORT_MODE_COUNT])
{
    if (utilities == NULL) {
        return -1;
    }

    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        utilities[i] = mode_probability_calculate_utility((TransportMode)i, distance_km);
    }

    return 0;
}

double mode_probability_calculate(TransportMode mode, double distance_km)
{
    if (mode < 0 || mode >= TRANSPORT_MODE_COUNT) {
        return 0.0;
    }

    /* Calculate all utilities */
    double utilities[TRANSPORT_MODE_COUNT];
    mode_probability_calculate_all_utilities(distance_km, utilities);

    /*
     * Multinomial logit probability:
     * P_m(D) = exp(U_m(D)) / sum_k(exp(U_k(D)))
     *
     * To avoid numerical overflow, subtract max utility from all values.
     */
    double max_utility = utilities[0];
    for (int i = 1; i < TRANSPORT_MODE_COUNT; i++) {
        if (utilities[i] > max_utility) {
            max_utility = utilities[i];
        }
    }

    double exp_sum = 0.0;
    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        exp_sum += exp(utilities[i] - max_utility);
    }

    return exp(utilities[mode] - max_utility) / exp_sum;
}

int mode_probability_calculate_all(double distance_km,
                                    double probabilities[TRANSPORT_MODE_COUNT])
{
    if (probabilities == NULL) {
        return -1;
    }

    /* Calculate all utilities */
    double utilities[TRANSPORT_MODE_COUNT];
    if (mode_probability_calculate_all_utilities(distance_km, utilities) != 0) {
        return -1;
    }

    /*
     * Calculate probabilities using softmax formula.
     * Subtract max utility for numerical stability.
     */
    double max_utility = utilities[0];
    for (int i = 1; i < TRANSPORT_MODE_COUNT; i++) {
        if (utilities[i] > max_utility) {
            max_utility = utilities[i];
        }
    }

    double exp_values[TRANSPORT_MODE_COUNT];
    double exp_sum = 0.0;

    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        exp_values[i] = exp(utilities[i] - max_utility);
        exp_sum += exp_values[i];
    }

    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        probabilities[i] = exp_values[i] / exp_sum;
    }

    return 0;
}

int mode_probability_get_coefficients(TransportMode mode, ModeCoefficients *coeffs)
{
    if (coeffs == NULL || mode < 0 || mode >= TRANSPORT_MODE_COUNT) {
        return -1;
    }

    coeffs->beta_0 = BETA_COEFFICIENTS[mode].beta_0;
    coeffs->beta_1 = BETA_COEFFICIENTS[mode].beta_1;

    return 0;
}

TransportMode mode_probability_get_dominant_mode(double distance_km)
{
    double probabilities[TRANSPORT_MODE_COUNT];
    mode_probability_calculate_all(distance_km, probabilities);

    TransportMode dominant = TRANSPORT_MODE_ROAD;
    double max_prob = probabilities[0];

    for (int i = 1; i < TRANSPORT_MODE_COUNT; i++) {
        if (probabilities[i] > max_prob) {
            max_prob = probabilities[i];
            dominant = (TransportMode)i;
        }
    }

    return dominant;
}
