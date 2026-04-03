/**
 * @file test_raw_materials.c
 * @ingroup Tests
 * @brief Tests for raw materials water footprint module.
 */

#include <stdio.h>
#include <string.h>
#include "raw_materials/raw_materials.h"
#include "aware/aware.h"
#include "tests/test_runner.h"

int test_raw_materials(void)
{
    printf("=== Raw Materials Module Test ===\n\n");

    MaterialDatabase db;
    ProductMaterialList product;

    /* Initialize database */
    if (raw_materials_init_database(&db) != 0) {
        printf("ERROR: Failed to initialize database\n");
        return -1;
    }
    printf("Database initialized.\n");

    /* Load materials from CSV */
    int loaded = raw_materials_load_csv(&db, DATASET_PATH);
    if (loaded < 0) {
        printf("CSV file not found: %s\n", DATASET_PATH);
        printf("Skipping CSV-dependent tests.\n\n");
    } else {
        printf("Loaded %d materials from dataset.\n\n", loaded);

        /* Display first 5 materials */
        printf("Sample materials (first 5):\n");
        printf("%-40s | %-10s | %s\n", "Name", "Type",
               "WU (m3/kg)");
        printf("----------------------------------------"
               "+------------+------------------\n");
        for (size_t i = 0; i < 5 && i < db.count; i++) {
            printf("%-40.40s | %-10s | %.6f\n",
                   db.materials[i].name,
                   db.materials[i].type == MATERIAL_TYPE_FLOW
                       ? "flow" : "process",
                   db.materials[i].water_consumption_m3_per_kg);
        }
        printf("\n");

        /* Test material search */
        printf("Testing material search...\n");
        int cotton_idx = raw_materials_find_by_name(&db, "cotton");
        if (cotton_idx >= 0) {
            printf("Found 'cotton' at index %d: %s "
                   "(WU: %.6f m3/kg)\n",
                   cotton_idx,
                   db.materials[cotton_idx].name,
                   db.materials[cotton_idx]
                       .water_consumption_m3_per_kg);
        } else {
            printf("Material 'cotton' not found.\n");
        }
        printf("\n");
    }

    /* Test water footprint calculation with synthetic data */
    printf("Testing footprint calculation "
           "(synthetic data)...\n");

    /* Create synthetic materials */
    MaterialDatabase synth_db;
    raw_materials_init_database(&synth_db);

    /* Material 0: cotton, WU = 10.0 m3/kg */
    strncpy(synth_db.materials[0].name, "cotton",
            MAX_MATERIAL_NAME_LEN);
    synth_db.materials[0].water_consumption_m3_per_kg = 10.0;
    synth_db.materials[0].type = MATERIAL_TYPE_FLOW;
    synth_db.count = 1;

    /* Material 1: polyester, WU = 0.1 m3/kg */
    strncpy(synth_db.materials[1].name, "polyester",
            MAX_MATERIAL_NAME_LEN);
    synth_db.materials[1].water_consumption_m3_per_kg = 0.1;
    synth_db.materials[1].type = MATERIAL_TYPE_FLOW;
    synth_db.count = 2;

    /* Setup AWARE database for test */
    AwareDatabase aware_db;
    aware_init_database(&aware_db, AWARE_GLOBAL_FALLBACK_AGRI);
    aware_load_aliases(&aware_db);

    /* Add a synthetic AWARE entry for India */
    strncpy(aware_db.entries[0].country_name, "India",
            AWARE_MAX_COUNTRY_LEN);
    aware_db.entries[0].aware_cf_annual = 85.0;
    aware_db.count = 1;

    if (raw_materials_init_product_list(&product) != 0) {
        printf("ERROR: Failed to initialize product list\n");
        return -1;
    }

    /* 0.2 kg cotton from India (AWARE=85.0) */
    raw_materials_add_to_product(&product, 0, 0.2, "India");
    /* 0.05 kg polyester, unknown origin (uses GLO fallback) */
    raw_materials_add_to_product(&product, 1, 0.05, NULL);

    double footprint =
        raw_materials_calculate_footprint(
            &synth_db, &product, &aware_db);
    if (footprint < 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    printf("Example product: T-shirt\n");
    printf("  Cotton:    0.20 kg * 10.0 m3/kg * 85.0 "
           "= %.4f m3 world-eq\n", 0.2 * 10.0 * 85.0);
    printf("  Polyester: 0.05 kg * 0.1 m3/kg * %.1f "
           "= %.4f m3 world-eq\n",
           AWARE_GLOBAL_FALLBACK_AGRI,
           0.05 * 0.1 * AWARE_GLOBAL_FALLBACK_AGRI);
    printf("  -----------------------------------------\n");
    printf("  Total raw material footprint: "
           "%.4f m3 world-eq\n", footprint);
    printf("  Expected: %.4f m3 world-eq\n\n",
           (0.2 * 10.0 * 85.0)
           + (0.05 * 0.1 * AWARE_GLOBAL_FALLBACK_AGRI));

    /* Cleanup */
    raw_materials_free_database(&synth_db);
    aware_free_database(&aware_db);
    printf("=== Test Complete ===\n");

    return 0;
}
