/**
 * @file test_raw_materials.c
 * @ingroup Tests
 * @brief Tests for raw materials carbon footprint module.
 */

#include <stdio.h>
#include "raw_materials/raw_materials.h"
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
        printf("ERROR: Failed to load CSV file: %s\n", DATASET_PATH);
        return -1;
    }
    printf("Loaded %d materials from dataset.\n\n", loaded);

    /* Display first 5 materials as sample */
    printf("Sample materials (first 5):\n");
    printf("%-40s | %-10s | %s\n", "Name", "Type",
           "EF (kg CO2eq/kg)");
    printf("----------------------------------------"
           "+------------+------------------\n");
    for (size_t i = 0; i < 5 && i < db.count; i++) {
        printf("%-40.40s | %-10s | %.2f\n",
               db.materials[i].name,
               db.materials[i].type == MATERIAL_TYPE_FLOW
                   ? "flow" : "process",
               db.materials[i].carbon_footprint_kg_CO2eq_per_kg);
    }
    printf("\n");

    /* Test material search */
    printf("Testing material search...\n");
    int cotton_idx = raw_materials_find_by_name(&db, "cotton");
    if (cotton_idx >= 0) {
        printf("Found 'cotton' at index %d: %s "
               "(EF: %.2f kg CO2eq/kg)\n",
               cotton_idx,
               db.materials[cotton_idx].name,
               db.materials[cotton_idx]
                   .carbon_footprint_kg_CO2eq_per_kg);
    } else {
        printf("Material 'cotton' not found.\n");
    }

    int polyester_idx =
        raw_materials_find_by_name(&db, "polyester");
    if (polyester_idx >= 0) {
        printf("Found 'polyester' at index %d: %s "
               "(EF: %.2f kg CO2eq/kg)\n",
               polyester_idx,
               db.materials[polyester_idx].name,
               db.materials[polyester_idx]
                   .carbon_footprint_kg_CO2eq_per_kg);
    } else {
        printf("Material 'polyester' not found.\n");
    }
    printf("\n");

    /* Test carbon footprint calculation */
    printf("Testing footprint calculation...\n");
    printf("Example product: T-shirt "
           "(0.2 kg cotton, 0.05 kg polyester)\n\n");

    if (raw_materials_init_product_list(&product) != 0) {
        printf("ERROR: Failed to initialize product list\n");
        return -1;
    }

    if (cotton_idx >= 0) {
        raw_materials_add_to_product(
            &product, (size_t)cotton_idx, 0.2);
    }
    if (polyester_idx >= 0) {
        raw_materials_add_to_product(
            &product, (size_t)polyester_idx, 0.05);
    }

    double footprint =
        raw_materials_calculate_footprint(&db, &product);
    if (footprint < 0) {
        printf("ERROR: Failed to calculate footprint\n");
        return -1;
    }

    printf("Calculation breakdown:\n");
    if (cotton_idx >= 0) {
        double cotton_ef = db.materials[cotton_idx]
            .carbon_footprint_kg_CO2eq_per_kg;
        printf("  Cotton:    0.20 kg * %.2f kg CO2eq/kg "
               "= %.4f kg CO2eq\n",
               cotton_ef, 0.2 * cotton_ef);
    }
    if (polyester_idx >= 0) {
        double polyester_ef = db.materials[polyester_idx]
            .carbon_footprint_kg_CO2eq_per_kg;
        printf("  Polyester: 0.05 kg * %.2f kg CO2eq/kg "
               "= %.4f kg CO2eq\n",
               polyester_ef, 0.05 * polyester_ef);
    }
    printf("  -----------------------------------------\n");
    printf("  Total raw material footprint: "
           "%.4f kg CO2eq\n\n", footprint);

    /* Cleanup */
    raw_materials_free_database(&db);
    printf("=== Test Complete ===\n");

    return 0;
}
