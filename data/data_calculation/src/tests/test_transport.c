/**
 * @file test_transport.c
 * @ingroup Tests
 * @brief Tests for transport carbon footprint module.
 */

#include <stdio.h>
#include "transport/transport.h"
#include "transport/emission_factors.h"
#include "transport/mode_probability.h"
#include "tests/test_runner.h"

int test_transport(void)
{
    printf("\n=== Transport Module Test ===\n\n");

    /* Test 1: Emission factors */
    printf("Test 1: Emission Factors\n");
    printf("%-20s | %s\n", "Mode", "EF (gCO2e/tkm)");
    printf("--------------------+---------------\n");
    for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
        printf("%-20s | %.1f\n",
               transport_get_mode_name((TransportMode)m),
               emission_factor_get((TransportMode)m));
    }
    printf("\n");

    /* Test 2: Mode probabilities at different distances */
    printf("Test 2: Mode Probabilities "
           "(Multinomial Logit Model)\n");
    double test_distances[] = {100, 500, 1000, 3000, 8000};
    int num_distances =
        sizeof(test_distances) / sizeof(test_distances[0]);

    printf("%-10s", "Distance");
    for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
        printf(" | %-8s",
               transport_get_mode_name((TransportMode)m));
    }
    printf("\n");
    printf("----------");
    for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
        printf("-+----------");
    }
    printf("\n");

    for (int d = 0; d < num_distances; d++) {
        double probs[TRANSPORT_MODE_COUNT];
        mode_probability_calculate_all(
            test_distances[d], probs);

        printf("%-10.0f", test_distances[d]);
        for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
            printf(" | %7.1f%%", probs[m] * 100.0);
        }
        printf("\n");
    }
    printf("\n");

    /* Test 3: Single leg calculation with known mode */
    printf("Test 3: Single Leg Calculation "
           "(Known Mode)\n");
    printf("Scenario: 500 kg shipment, "
           "1000 km by road\n");
    double cf_road = transport_calculate_single_leg(
        1000.0, 500.0, TRANSPORT_MODE_ROAD, 1);
    printf("Carbon footprint: %.4f kg CO2-eq\n", cf_road);
    printf("Verification: (500/1000) * 1000 * (72.9/1000)"
           " = %.4f kg CO2-eq\n\n",
           0.5 * 1000.0 * 0.0729);

    /* Test 4: Single leg with unknown mode */
    printf("Test 4: Single Leg Calculation "
           "(Unknown Mode - Estimated)\n");
    printf("Scenario: 500 kg shipment, 1000 km "
           "(mode estimated via logit model)\n");
    double cf_estimated = transport_calculate_single_leg(
        1000.0, 500.0, TRANSPORT_MODE_ROAD, 0);
    printf("Carbon footprint: %.4f kg CO2-eq\n\n",
           cf_estimated);

    /* Test 5: Complete journey calculation */
    printf("Test 5: Complete Journey Calculation\n");
    printf("Scenario: T-shirt supply chain "
           "(250g shipment)\n");
    printf("  Leg 1: Cotton farm to spinning mill "
           "- 200 km (road, known)\n");
    printf("  Leg 2: Spinning mill to fabric factory "
           "- 8000 km (sea, known)\n");
    printf("  Leg 3: Fabric factory to assembly "
           "- 500 km (mode unknown)\n");
    printf("  Leg 4: Assembly to port "
           "- 100 km (road, known)\n\n");

    TransportJourney journey;
    TransportResult result;

    if (transport_init_journey(&journey, 0.25) != 0) {
        printf("ERROR: Failed to initialize journey\n");
        return -1;
    }

    transport_add_leg(&journey, 200.0,
                      TRANSPORT_MODE_ROAD, 1);
    transport_add_leg(&journey, 8000.0,
                      TRANSPORT_MODE_SEA, 1);
    transport_add_leg(&journey, 500.0,
                      TRANSPORT_MODE_ROAD, 0);
    transport_add_leg(&journey, 100.0,
                      TRANSPORT_MODE_ROAD, 1);

    if (transport_calculate_footprint(
            &journey, &result) != 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    printf("Results:\n");
    printf("  Total distance:      %.1f km\n",
           result.total_distance_km);
    printf("  Weighted avg EF:     %.2f gCO2e/tkm\n",
           result.weighted_ef_gCO2e_tkm);
    printf("  Carbon footprint:    %.6f kg CO2-eq\n\n",
           result.carbon_footprint_kg_CO2eq);

    printf("Mode probability breakdown "
           "(weighted by tonne-km):\n");
    for (int m = 0; m < TRANSPORT_MODE_COUNT; m++) {
        printf("  %-20s: %6.2f%%\n",
               transport_get_mode_name((TransportMode)m),
               result.mode_probabilities[m] * 100.0);
    }
    printf("\n");

    /* Test 6: Mode string parsing */
    printf("Test 6: Mode String Parsing\n");
    const char *test_modes[] = {
        "road", "RAIL", "Ship", "iww",
        "plane", "truck", "unknown"
    };
    int num_modes =
        sizeof(test_modes) / sizeof(test_modes[0]);

    for (int i = 0; i < num_modes; i++) {
        TransportMode parsed =
            transport_mode_from_string(test_modes[i]);
        printf("  \"%s\" -> %s\n",
               test_modes[i],
               transport_get_mode_name(parsed));
    }
    printf("\n");

    printf("=== Transport Test Complete ===\n");

    return 0;
}
