/**
 * @file emission_factors.c
 * @ingroup Transport
 * @brief Transport mode emission factor retrieval and weighting.
 *
 * @see research_paper.tex Section 3.2.1
 */

#include "transport/emission_factors.h"
#include <stddef.h>

/*
 * Static array of emission factors indexed by TransportMode enum.
 * Values from research_paper.tex Table 2.
 */
static const double EMISSION_FACTORS[TRANSPORT_MODE_COUNT] = {
    EF_ROAD_gCO2e_tkm,      /* TRANSPORT_MODE_ROAD */
    EF_RAIL_gCO2e_tkm,      /* TRANSPORT_MODE_RAIL */
    EF_IWW_gCO2e_tkm,       /* TRANSPORT_MODE_INLAND_WATERWAY */
    EF_SEA_gCO2e_tkm,       /* TRANSPORT_MODE_SEA */
    EF_AIR_gCO2e_tkm        /* TRANSPORT_MODE_AIR */
};

double emission_factor_get(TransportMode mode)
{
    if (mode < 0 || mode >= TRANSPORT_MODE_COUNT) {
        return -1.0;
    }

    return EMISSION_FACTORS[mode];
}

int emission_factor_get_all(double factors[TRANSPORT_MODE_COUNT])
{
    if (factors == NULL) {
        return -1;
    }

    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        factors[i] = EMISSION_FACTORS[i];
    }

    return 0;
}

double emission_factor_calculate_weighted(const double probabilities[TRANSPORT_MODE_COUNT])
{
    if (probabilities == NULL) {
        return -1.0;
    }

    /*
     * Calculate weighted emission factor:
     * EF_weighted = sum(P_m * EF_m)
     *
     * As defined in research_paper.tex Section 3.2.2
     */
    double weighted_ef = 0.0;

    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        weighted_ef += probabilities[i] * EMISSION_FACTORS[i];
    }

    return weighted_ef;
}
