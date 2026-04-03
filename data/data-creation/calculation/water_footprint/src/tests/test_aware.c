/**
 * @file test_aware.c
 * @ingroup Tests
 * @brief Tests for AWARE characterization factor module.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "aware/aware.h"
#include "tests/test_runner.h"

int test_aware(void)
{
    printf("=== AWARE Module Test ===\n\n");

    AwareDatabase db;

    /* Test 1: Initialize database with agri fallback */
    printf("Test 1: Database Initialization\n");
    if (aware_init_database(&db, AWARE_GLOBAL_FALLBACK_AGRI) != 0) {
        printf("ERROR: Failed to initialize AWARE database\n");
        return -1;
    }
    printf("Initialized with fallback: %.1f\n\n",
           db.global_fallback);

    /* Test 2: Load built-in aliases */
    printf("Test 2: Loading Built-in Aliases\n");
    if (aware_load_aliases(&db) != 0) {
        printf("ERROR: Failed to load aliases\n");
        return -1;
    }
    printf("Loaded %zu aliases.\n\n", db.alias_count);

    /* Test 3: Alias resolution */
    printf("Test 3: Alias Resolution\n");
    struct {
        const char *input;
        const char *expected;
    } alias_tests[] = {
        {"Turkey",         "Turkiye"},
        {"USA",            "United States of America"},
        {"UK",             "United Kingdom"},
        {"England",        "United Kingdom"},
        {"Scotland",       "United Kingdom"},
        {"Czech Republic", "Czechia"},
        {"Kitwe",          "Zambia"},
        {"Lusaka",         "Zambia"},
        {"Tashkent",       "Uzbekistan"},
        {"Fergana Valley", "Uzbekistan"},
        {"France",         "France"},
        {"China",          "China"}
    };
    int num_alias_tests =
        (int)(sizeof(alias_tests) / sizeof(alias_tests[0]));

    for (int i = 0; i < num_alias_tests; i++) {
        const char *resolved =
            aware_resolve_alias(&db, alias_tests[i].input);
        int pass = (strcmp(resolved, alias_tests[i].expected) == 0);
        printf("  \"%s\" -> \"%s\" %s\n",
               alias_tests[i].input, resolved,
               pass ? "[OK]" : "[FAIL]");
        if (!pass) {
            printf("    Expected: \"%s\"\n",
                   alias_tests[i].expected);
        }
    }
    printf("\n");

    /* Test 4: Country extraction from location strings */
    printf("Test 4: Country Extraction (D6 Logic)\n");
    struct {
        const char *location;
        const char *expected;
    } extract_tests[] = {
        {"Istanbul, Turkey",           "Turkiye"},
        {"Shanghai, China",            "China"},
        {"New York, USA",              "United States of America"},
        {"London, England",            "United Kingdom"},
        {"Singapore",                  "Singapore"},
        {"Lusaka",                     "Zambia"},
        {"Dhaka, Bangladesh",          "Bangladesh"},
        {"Seoul, South Korea",         "South Korea"},
        {"Mumbai, Maharashtra, India", "India"}
    };
    int num_extract_tests =
        (int)(sizeof(extract_tests) / sizeof(extract_tests[0]));

    for (int i = 0; i < num_extract_tests; i++) {
        char *country = aware_extract_country(
            &db, extract_tests[i].location);
        if (country != NULL) {
            int pass = (strcmp(country, extract_tests[i].expected) == 0);
            printf("  \"%s\" -> \"%s\" %s\n",
                   extract_tests[i].location, country,
                   pass ? "[OK]" : "[FAIL]");
            if (!pass) {
                printf("    Expected: \"%s\"\n",
                       extract_tests[i].expected);
            }
            free(country);
        } else {
            printf("  \"%s\" -> NULL [FAIL]\n",
                   extract_tests[i].location);
        }
    }
    printf("\n");

    /* Test 5: Fallback for unknown country */
    printf("Test 5: Global Fallback\n");
    double factor = aware_get_factor(&db, "Atlantis");
    printf("  Unknown country 'Atlantis': %.1f ", factor);
    if (fabs(factor - AWARE_GLOBAL_FALLBACK_AGRI) < 0.01) {
        printf("[OK]\n");
    } else {
        printf("[FAIL] Expected %.1f\n",
               AWARE_GLOBAL_FALLBACK_AGRI);
    }

    factor = aware_get_factor(&db, NULL);
    printf("  NULL country: %.1f ", factor);
    if (fabs(factor - AWARE_GLOBAL_FALLBACK_AGRI) < 0.01) {
        printf("[OK]\n");
    } else {
        printf("[FAIL] Expected %.1f\n",
               AWARE_GLOBAL_FALLBACK_AGRI);
    }
    printf("\n");

    /* Test 6: CSV loading (skip if file not present) */
    printf("Test 6: CSV Loading\n");
    AwareDatabase csv_db;
    aware_init_database(&csv_db, AWARE_GLOBAL_FALLBACK_AGRI);
    int loaded = aware_load_csv(&csv_db, AWARE_AGRI_PATH);
    if (loaded > 0) {
        printf("Loaded %d AWARE entries from CSV.\n", loaded);
        aware_load_aliases(&csv_db);
        /* Test a known country lookup */
        double china_factor = aware_get_factor(&csv_db, "China");
        printf("  China AWARE factor: %.2f\n", china_factor);
    } else {
        printf("CSV file not found (expected for unit tests "
               "without data files). Skipping.\n");
    }
    aware_free_database(&csv_db);
    printf("\n");

    /* Cleanup */
    aware_free_database(&db);
    printf("=== AWARE Test Complete ===\n\n");

    return 0;
}
