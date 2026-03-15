/**
 * @file transport.h
 * @ingroup Transport
 * @brief Upstream transport carbon footprint calculation module.
 *
 * Implements the upstream transport phase of the cradle-to-gate
 * carbon footprint calculation as defined in ISO 14040/14044 and
 * PEFCR for Apparel and Footwear v3.1.
 *
 * Formula: E(D) = (w/1000) * D * (EF_weighted/1000)
 *
 * @see research_paper.tex Section 3.2
 */

#ifndef TRANSPORT_H_
#define TRANSPORT_H_

#include <stddef.h>

/** @brief Maximum number of transport legs in a single shipment. */
#define MAX_TRANSPORT_LEGS 10

/**
 * @brief Classification of transport mode categories.
 *
 * As defined in research_paper.tex Table 1 and Section 3.2.1.
 * Road is used as the reference mode with utility normalized to zero.
 */
typedef enum {
    TRANSPORT_MODE_ROAD,            /**< Road (HGV). */
    TRANSPORT_MODE_RAIL,            /**< Rail (freight train). */
    TRANSPORT_MODE_INLAND_WATERWAY, /**< Inland waterway (barge). */
    TRANSPORT_MODE_SEA,             /**< Sea (container ship). */
    TRANSPORT_MODE_AIR,             /**< Air (freight aircraft). */
    TRANSPORT_MODE_COUNT            /**< Sentinel count value. */
} TransportMode;

/**
 * @brief A single transport leg in a shipment journey.
 */
typedef struct {
    TransportMode mode;     /**< Transport mode (default ROAD if unknown). */
    double distance_km;     /**< Distance travelled in kilometers. */
    int mode_known;         /**< 1 if mode is explicitly set, 0 for estimation. */
} TransportLeg;

/**
 * @brief A complete transport journey of one or more legs.
 */
typedef struct {
    TransportLeg legs[MAX_TRANSPORT_LEGS]; /**< Transport legs array. */
    size_t leg_count;                      /**< Number of legs. */
    double shipment_weight_kg;             /**< Total shipment weight (kg). */
} TransportJourney;

/**
 * @brief Results from a transport footprint calculation.
 */
typedef struct {
    double carbon_footprint_kg_CO2eq;              /**< Total footprint (kg CO2-eq). */
    double total_distance_km;                       /**< Sum of all leg distances (km). */
    double weighted_ef_gCO2e_tkm;                   /**< Weighted average EF (gCO2e/tkm). */
    double mode_probabilities[TRANSPORT_MODE_COUNT]; /**< Per-mode probability. */
} TransportResult;

/**
 * @brief Initialize an empty transport journey.
 *
 * @param[out] journey             Pointer to TransportJourney to initialize.
 * @param[in]  shipment_weight_kg  Weight of shipment in kilograms.
 * @return 0 on success, -1 on failure (null pointer or invalid weight).
 */
int transport_init_journey(TransportJourney *journey,
                           double shipment_weight_kg);

/**
 * @brief Add a transport leg to journey.
 *
 * @param[in,out] journey      Pointer to TransportJourney.
 * @param[in]     distance_km  Distance for this leg (km).
 * @param[in]     mode         Transport mode (ignored if mode_known is 0).
 * @param[in]     mode_known   1 if mode is known, 0 for logit estimation.
 * @return 0 on success, -1 on failure.
 */
int transport_add_leg(TransportJourney *journey,
                      double distance_km,
                      TransportMode mode, int mode_known);

/**
 * @brief Add a transport leg with unknown mode.
 *
 * Convenience function; mode will be estimated via the
 * multinomial logit model.
 *
 * @param[in,out] journey      Pointer to TransportJourney.
 * @param[in]     distance_km  Distance for this leg (km).
 * @return 0 on success, -1 on failure.
 */
int transport_add_leg_unknown_mode(TransportJourney *journey,
                                   double distance_km);

/**
 * @brief Calculate total transport carbon footprint.
 *
 * Implements: E(D) = (w/1000) * D * (EF_weighted/1000)
 *
 * @param[in]  journey  Pointer to TransportJourney.
 * @param[out] result   Pointer to TransportResult for output.
 * @return 0 on success, -1 on error.
 *
 * @note Calculates only upstream transport. Internal transport
 *       handled separately in the adjustments module.
 */
int transport_calculate_footprint(const TransportJourney *journey,
                                  TransportResult *result);

/**
 * @brief Calculate footprint for a single transport leg.
 *
 * @param[in] distance_km  Travel distance (km).
 * @param[in] weight_kg    Shipment weight (kg).
 * @param[in] mode         Transport mode (used only if mode_known is 1).
 * @param[in] mode_known   1 if mode is explicitly provided.
 * @return Carbon footprint in kg CO2-eq, or -1.0 on error.
 */
double transport_calculate_single_leg(double distance_km,
                                      double weight_kg,
                                      TransportMode mode,
                                      int mode_known);

/**
 * @brief Get string name for transport mode.
 *
 * @param[in] mode  Transport mode enum value.
 * @return Static string with mode name, or "unknown" if invalid.
 */
const char *transport_get_mode_name(TransportMode mode);

/**
 * @brief Parse transport mode from string.
 *
 * Case-insensitive matching of mode names.
 *
 * @param[in] mode_str  String representation (e.g., "road", "sea").
 * @return TransportMode value, or TRANSPORT_MODE_ROAD if unrecognized.
 */
TransportMode transport_mode_from_string(const char *mode_str);

#endif  /* TRANSPORT_H_ */
