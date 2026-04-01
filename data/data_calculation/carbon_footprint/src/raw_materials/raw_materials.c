/**
 * @file raw_materials.c
 * @ingroup RawMaterials
 * @brief Raw material carbon footprint calculation.
 *
 * @see research_paper.tex Section 3.1
 */

#include "raw_materials/raw_materials.h"
#include "utils/csv_parser.h"
#include <string.h>
#include <strings.h>
#include <stdlib.h>

/* CSV column indices for Product_materials.csv */
#define COL_MATERIAL_NAME 0
#define COL_TYPE 1
#define COL_REFERENCE_ID 2
#define COL_DESCRIPTION 3
#define COL_CARBON_FOOTPRINT 4
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

    /* Open CSV file, skip header row */
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

        /* Parse material name */
        if (csv_parser_get_field_str(&row, COL_MATERIAL_NAME,
                                      mat->name, MAX_MATERIAL_NAME_LEN) != 0) {
            continue;
        }

        /* Parse type (flow or process) */
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

        /* Parse reference ID */
        if (csv_parser_get_field_str(&row, COL_REFERENCE_ID,
                                      mat->reference_id, MAX_REFERENCE_ID_LEN) != 0) {
            mat->reference_id[0] = '\0';
        }

        /* Parse carbon footprint value */
        if (csv_parser_get_field_double(&row, COL_CARBON_FOOTPRINT,
                                         &mat->carbon_footprint_kg_CO2eq_per_kg) != 0) {
            mat->carbon_footprint_kg_CO2eq_per_kg = 0.0;
        }

        /* Parse notes */
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

    /* Create lowercase copy of search term */
    char search_lower[MAX_MATERIAL_NAME_LEN];
    strncpy(search_lower, name, MAX_MATERIAL_NAME_LEN - 1);
    search_lower[MAX_MATERIAL_NAME_LEN - 1] = '\0';
    csv_str_to_lower(search_lower);

    for (size_t i = 0; i < db->count; i++) {
        /* Create lowercase copy of material name */
        char mat_name_lower[MAX_MATERIAL_NAME_LEN];
        strncpy(mat_name_lower, db->materials[i].name, MAX_MATERIAL_NAME_LEN - 1);
        mat_name_lower[MAX_MATERIAL_NAME_LEN - 1] = '\0';
        csv_str_to_lower(mat_name_lower);

        /* Check for partial match */
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

double raw_materials_get_emission_factor(const MaterialDatabase *db, size_t index)
{
    if (db == NULL || index >= db->count) {
        return -1.0;
    }

    return db->materials[index].carbon_footprint_kg_CO2eq_per_kg;
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
                                  double weight_kg)
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

    list->count++;

    return 0;
}

double raw_materials_calculate_footprint(const MaterialDatabase *db,
                                          const ProductMaterialList *list)
{
    if (db == NULL || list == NULL) {
        return -1.0;
    }

    /*
     * Implementation of formula from research_paper.tex Section 3.1:
     * CF_raw = sum(weight_material_kg * carbon_footprint_material_kg_CO2eq_per_kg)
     */
    double total_footprint_kg_CO2eq = 0.0;

    for (size_t i = 0; i < list->count; i++) {
        const ProductMaterial *item = &list->items[i];

        /* Validate material index */
        if (item->material_index >= db->count) {
            return -1.0;
        }

        double emission_factor = db->materials[item->material_index].carbon_footprint_kg_CO2eq_per_kg;
        double material_footprint = item->weight_kg * emission_factor;

        total_footprint_kg_CO2eq += material_footprint;
    }

    return total_footprint_kg_CO2eq;
}

void raw_materials_free_database(MaterialDatabase *db)
{
    if (db != NULL) {
        db->count = 0;
    }
}
