/**
 * @file raw_materials.c
 * @ingroup RawMaterials
 * @brief Raw material water footprint calculation.
 *
 * Formula: WF_raw = sum(w_i * WU_material_i * AWARE_CF_origin_i)
 * Unit: m3 world-eq
 */

#include "raw_materials/raw_materials.h"
#include "utils/csv_parser.h"
#include <string.h>
#include <strings.h>
#include <stdlib.h>

/* CSV column indices for base_materials_water.csv */
#define COL_MATERIAL_NAME 0
#define COL_TYPE 1
#define COL_REFERENCE_ID 2
#define COL_DESCRIPTION 3
#define COL_WATER_CONSUMPTION 4
#define COL_NOTES 5

int raw_materials_init_database(MaterialDatabase *db)
{
    if (db == NULL) {
        return -1;
    }

    memset(db->materials, 0, sizeof(db->materials));
    db->count = 0;

    return 0;
}

int raw_materials_load_csv(MaterialDatabase *db, const char *filepath)
{
    if (db == NULL || filepath == NULL) {
        return -1;
    }

    CsvParser parser;
    CsvRow row;

    if (csv_parser_open(&parser, filepath, 1) != 0) {
        return -1;
    }

    int materials_loaded = 0;
    int result;

    while ((result = csv_parser_read_row(&parser, &row)) == 1) {
        if (db->count >= MAX_MATERIALS) {
            break;
        }

        Material *mat = &db->materials[db->count];

        if (csv_parser_get_field_str(&row, COL_MATERIAL_NAME,
                                      mat->name, MAX_MATERIAL_NAME_LEN) != 0) {
            continue;
        }

        char type_str[32];
        if (csv_parser_get_field_str(&row, COL_TYPE, type_str, sizeof(type_str)) != 0) {
            continue;
        }
        csv_str_to_lower(type_str);
        if (strcmp(type_str, "flow") == 0) {
            mat->type = MATERIAL_TYPE_FLOW;
        } else if (strcmp(type_str, "process") == 0) {
            mat->type = MATERIAL_TYPE_PROCESS;
        } else {
            mat->type = MATERIAL_TYPE_FLOW;
        }

        if (csv_parser_get_field_str(&row, COL_REFERENCE_ID,
                                      mat->reference_id, MAX_REFERENCE_ID_LEN) != 0) {
            mat->reference_id[0] = '\0';
        }

        /* Parse water consumption value (m3/kg) */
        if (csv_parser_get_field_double(&row, COL_WATER_CONSUMPTION,
                                         &mat->water_consumption_m3_per_kg) != 0) {
            mat->water_consumption_m3_per_kg = 0.0;
        }

        if (csv_parser_get_field_str(&row, COL_NOTES,
                                      mat->notes, MAX_NOTES_LEN) != 0) {
            mat->notes[0] = '\0';
        }

        db->count++;
        materials_loaded++;
    }

    csv_parser_close(&parser);

    if (result == -1) {
        return -1;
    }

    return materials_loaded;
}

int raw_materials_find_by_name(const MaterialDatabase *db, const char *name)
{
    if (db == NULL || name == NULL) {
        return -1;
    }

    char search_lower[MAX_MATERIAL_NAME_LEN];
    strncpy(search_lower, name, MAX_MATERIAL_NAME_LEN - 1);
    search_lower[MAX_MATERIAL_NAME_LEN - 1] = '\0';
    csv_str_to_lower(search_lower);

    for (size_t i = 0; i < db->count; i++) {
        char mat_name_lower[MAX_MATERIAL_NAME_LEN];
        strncpy(mat_name_lower, db->materials[i].name, MAX_MATERIAL_NAME_LEN - 1);
        mat_name_lower[MAX_MATERIAL_NAME_LEN - 1] = '\0';
        csv_str_to_lower(mat_name_lower);

        if (strstr(mat_name_lower, search_lower) != NULL) {
            return (int)i;
        }
    }

    return -1;
}

int raw_materials_find_by_id(const MaterialDatabase *db, const char *reference_id)
{
    if (db == NULL || reference_id == NULL) {
        return -1;
    }

    for (size_t i = 0; i < db->count; i++) {
        if (strcmp(db->materials[i].reference_id, reference_id) == 0) {
            return (int)i;
        }
    }

    return -1;
}

double raw_materials_get_water_consumption(const MaterialDatabase *db, size_t index)
{
    if (db == NULL || index >= db->count) {
        return -1.0;
    }

    return db->materials[index].water_consumption_m3_per_kg;
}

int raw_materials_init_product_list(ProductMaterialList *list)
{
    if (list == NULL) {
        return -1;
    }

    memset(list->items, 0, sizeof(list->items));
    list->count = 0;

    return 0;
}

int raw_materials_add_to_product(ProductMaterialList *list,
                                  size_t material_index,
                                  double weight_kg,
                                  const char *origin_country)
{
    if (list == NULL || weight_kg < 0.0) {
        return -1;
    }

    if (list->count >= MAX_PRODUCT_MATERIALS) {
        return -1;
    }

    ProductMaterial *item = &list->items[list->count];
    item->material_index = material_index;
    item->weight_kg = weight_kg;

    if (origin_country != NULL) {
        strncpy(item->origin_country, origin_country, MAX_ORIGIN_LEN - 1);
        item->origin_country[MAX_ORIGIN_LEN - 1] = '\0';
    } else {
        item->origin_country[0] = '\0';
    }

    list->count++;

    return 0;
}

double raw_materials_calculate_footprint(
    const MaterialDatabase *db,
    const ProductMaterialList *list,
    const AwareDatabase *aware_db)
{
    if (db == NULL || list == NULL) {
        return -1.0;
    }

    /*
     * WF_raw = sum(weight_i * water_consumption_i * AWARE_CF_origin_i)
     * Unit: m3 world-eq
     *
     * If aware_db is NULL, AWARE factor defaults to 1.0 (unweighted).
     */
    double total_footprint_m3_world_eq = 0.0;

    for (size_t i = 0; i < list->count; i++) {
        const ProductMaterial *item = &list->items[i];

        if (item->material_index >= db->count) {
            return -1.0;
        }

        double water_consumption =
            db->materials[item->material_index].water_consumption_m3_per_kg;

        double aware_factor = 1.0;
        if (aware_db != NULL && item->origin_country[0] != '\0') {
            aware_factor = aware_get_factor(aware_db, item->origin_country);
        } else if (aware_db != NULL) {
            aware_factor = aware_db->global_fallback;
        }

        double material_footprint =
            item->weight_kg * water_consumption * aware_factor;

        total_footprint_m3_world_eq += material_footprint;
    }

    return total_footprint_m3_world_eq;
}

void raw_materials_free_database(MaterialDatabase *db)
{
    if (db != NULL) {
        db->count = 0;
    }
}
