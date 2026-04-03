/**
 * @file transport.c
 * @ingroup Transport
 * @brief Upstream transport carbon footprint calculation.
 *
 * @see research_paper.tex Section 3.2
 */

#include "transport/transport.h"
#include "transport/emission_factors.h"
#include "transport/mode_probability.h"
#include <string.h>
#include <strings.h>
#include <stddef.h>

/* Mode name strings for string conversion */
static const char *MODE_NAMES[TRANSPORT_MODE_COUNT] = {
    "road",
    "rail",
    "inland_waterway",
    "sea",
    "air"
};

int transport_init_journey(TransportJourney *journey, double shipment_weight_kg)
{
    if (journey == NULL || shipment_weight_kg <= 0.0) {
        return -1;
    }

    memset(journey->legs, 0, sizeof(journey->legs));
    journey->leg_count = 0;
    journey->shipment_weight_kg = shipment_weight_kg;

    return 0;
}

int transport_add_leg(TransportJourney *journey, double distance_km,
                      TransportMode mode, int mode_known)
{
    if (journey == NULL || distance_km <= 0.0) {
        return -1;
    }

    if (journey->leg_count >= MAX_TRANSPORT_LEGS) {
        return -1;
    }

    if (mode_known && (mode < 0 || mode >= TRANSPORT_MODE_COUNT)) {
        return -1;
    }

    TransportLeg *leg = &journey->legs[journey->leg_count];
    leg->distance_km = distance_km;
    leg->mode = mode_known ? mode : TRANSPORT_MODE_ROAD;
    leg->mode_known = mode_known;

    journey->leg_count++;

    return 0;
}

int transport_add_leg_unknown_mode(TransportJourney *journey, double distance_km)
{
    return transport_add_leg(journey, distance_km, TRANSPORT_MODE_ROAD, 0);
}

double transport_calculate_single_leg(double distance_km, double weight_kg,
                                       TransportMode mode, int mode_known)
{
    if (distance_km <= 0.0 || weight_kg <= 0.0) {
        return -1.0;
    }

    double ef_gCO2e_tkm;

    if (mode_known) {
        /* Use mode-specific emission factor directly */
        ef_gCO2e_tkm = emission_factor_get(mode);
        if (ef_gCO2e_tkm < 0) {
            return -1.0;
        }
    } else {
        /* Calculate weighted emission factor using multinomial logit model */
        double probabilities[TRANSPORT_MODE_COUNT];
        if (mode_probability_calculate_all(distance_km, probabilities) != 0) {
            return -1.0;
        }

        ef_gCO2e_tkm = emission_factor_calculate_weighted(probabilities);
        if (ef_gCO2e_tkm < 0) {
            return -1.0;
        }
    }

    /*
     * Transport emission formula from research_paper.tex Section 3.2:
     * E(D) = (w/1000) * D * (EF/1000)
     *
     * Where:
     *   w = weight in kg
     *   D = distance in km
     *   EF = emission factor in gCO2e/tkm
     *
     * Result is in kg CO2-eq
     */
    double weight_tonnes = weight_kg / 1000.0;
    double ef_kgCO2e_tkm = ef_gCO2e_tkm / 1000.0;

    return weight_tonnes * distance_km * ef_kgCO2e_tkm;
}

int transport_calculate_footprint(const TransportJourney *journey,
                                   TransportResult *result)
{
    if (journey == NULL || result == NULL) {
        return -1;
    }

    if (journey->leg_count == 0) {
        result->carbon_footprint_kg_CO2eq = 0.0;
        result->total_distance_km = 0.0;
        result->weighted_ef_gCO2e_tkm = 0.0;
        memset(result->mode_probabilities, 0, sizeof(result->mode_probabilities));
        return 0;
    }

    double total_footprint = 0.0;
    double total_distance = 0.0;
    double aggregate_probabilities[TRANSPORT_MODE_COUNT] = {0};
    double total_tkm = 0.0;

    for (size_t i = 0; i < journey->leg_count; i++) {
        const TransportLeg *leg = &journey->legs[i];

        /* Calculate footprint for this leg */
        double leg_footprint = transport_calculate_single_leg(
            leg->distance_km,
            journey->shipment_weight_kg,
            leg->mode,
            leg->mode_known
        );

        if (leg_footprint < 0) {
            return -1;
        }

        total_footprint += leg_footprint;
        total_distance += leg->distance_km;

        /* Calculate tonne-km for this leg for weighting */
        double leg_tkm = (journey->shipment_weight_kg / 1000.0) * leg->distance_km;
        total_tkm += leg_tkm;

        /* Aggregate mode probabilities weighted by tonne-km */
        double leg_probs[TRANSPORT_MODE_COUNT];
        if (leg->mode_known) {
            /* Known mode: 100% probability for that mode */
            memset(leg_probs, 0, sizeof(leg_probs));
            leg_probs[leg->mode] = 1.0;
        } else {
            /* Unknown mode: use multinomial logit probabilities */
            mode_probability_calculate_all(leg->distance_km, leg_probs);
        }

        for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
            aggregate_probabilities[m] += leg_probs[m] * leg_tkm;
        }
    }

    /* Normalize aggregate probabilities by total tonne-km */
    if (total_tkm > 0) {
        for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
            aggregate_probabilities[m] /= total_tkm;
        }
    }

    /* Calculate weighted average emission factor */
    double weighted_ef = emission_factor_calculate_weighted(aggregate_probabilities);

    /* Populate result structure */
    result->carbon_footprint_kg_CO2eq = total_footprint;
    result->total_distance_km = total_distance;
    result->weighted_ef_gCO2e_tkm = weighted_ef;
    memcpy(result->mode_probabilities, aggregate_probabilities,
           sizeof(result->mode_probabilities));

    return 0;
}

const char *transport_get_mode_name(TransportMode mode)
{
    if (mode < 0 || mode >= TRANSPORT_MODE_COUNT) {
        return "unknown";
    }

    return MODE_NAMES[mode];
}

TransportMode transport_mode_from_string(const char *mode_str)
{
    if (mode_str == NULL) {
        return TRANSPORT_MODE_ROAD;
    }

    /* Case-insensitive comparison */
    for (int i = 0; i < TRANSPORT_MODE_COUNT; i++) {
        if (strcasecmp(mode_str, MODE_NAMES[i]) == 0) {
            return (TransportMode)i;
        }
    }

    /* Check common aliases */
    if (strcasecmp(mode_str, "iww") == 0 ||
        strcasecmp(mode_str, "barge") == 0 ||
        strcasecmp(mode_str, "waterway") == 0) {
        return TRANSPORT_MODE_INLAND_WATERWAY;
    }

    if (strcasecmp(mode_str, "ship") == 0 ||
        strcasecmp(mode_str, "ocean") == 0 ||
        strcasecmp(mode_str, "maritime") == 0) {
        return TRANSPORT_MODE_SEA;
    }

    if (strcasecmp(mode_str, "truck") == 0 ||
        strcasecmp(mode_str, "lorry") == 0 ||
        strcasecmp(mode_str, "hgv") == 0) {
        return TRANSPORT_MODE_ROAD;
    }

    if (strcasecmp(mode_str, "train") == 0 ||
        strcasecmp(mode_str, "railway") == 0) {
        return TRANSPORT_MODE_RAIL;
    }

    if (strcasecmp(mode_str, "plane") == 0 ||
        strcasecmp(mode_str, "flight") == 0 ||
        strcasecmp(mode_str, "aircraft") == 0) {
        return TRANSPORT_MODE_AIR;
    }

    /* Default to road if not recognized */
    return TRANSPORT_MODE_ROAD;
}
