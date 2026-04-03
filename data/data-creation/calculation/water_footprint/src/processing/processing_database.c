/**
 * @file processing_database.c
 * @ingroup Processing
 * @brief Database initialization, CSV loading, and category helpers.
 *
 * Handles loading of processing steps and material-process
 * combinations from CSV datasets with water consumption values,
 * category string conversion, and resource cleanup.
 */

#include "processing/material_processing.h"
#include "utils/csv_parser.h"
#include <string.h>
#include <strings.h>
#include <stdlib.h>

/** @brief CSV column indices for processing_steps_water.csv. */
#define STEP_COL_NAME 0
#define STEP_COL_PROCESS_ID 1
#define STEP_COL_CATEGORY 2
#define STEP_COL_WATER_CONSUMPTION 3
#define STEP_COL_APPLICABLE_MATERIALS 4
#define STEP_COL_DESCRIPTION 5

/** @brief CSV column indices for material_processing_water.csv. */
#define COMBO_COL_MATERIAL_NAME 0
#define COMBO_COL_MATERIAL_ID 1
#define COMBO_COL_MATERIAL_TYPE 2
#define COMBO_COL_MATERIAL_CATEGORY 3
#define COMBO_COL_PROCESS_NAME 4
#define COMBO_COL_PROCESS_ID 5
#define COMBO_COL_PROCESS_DESC 6
#define COMBO_COL_REF_MASS 7
#define COMBO_COL_WATER_CONSUMPTION 8

/** @brief Category name strings indexed by ProcessCategory. */
static const char *CATEGORY_NAMES[] = {
    "Pre-processing",
    "Primary processing",
    "Wet processing",
    "Finishing",
    "Special treatments",
    "Synthetic fibre production",
    "Glass/mineral fibre processing",
    "Composite processing",
    "Construction materials",
    "Unknown"
};

int processing_init_step_database(ProcessingStepDatabase *db)
{
    if (db == NULL) {
        return -1;
    }

    memset(db->steps, 0, sizeof(db->steps));
    db->count = 0;

    return 0;
}

int processing_load_steps_csv(ProcessingStepDatabase *db,
                              const char *filepath)
{
    if (db == NULL || filepath == NULL) {
        return -1;
    }

    CsvParser parser;
    CsvRow row;

    if (csv_parser_open(&parser, filepath, 1) != 0) {
        return -1;
    }

    int steps_loaded = 0;
    int result;

    while ((result = csv_parser_read_row(&parser, &row)) == 1) {
        if (db->count >= MAX_PROCESSING_STEPS) {
            break;
        }

        ProcessingStep *step = &db->steps[db->count];

        if (csv_parser_get_field_str(
                &row, STEP_COL_NAME,
                step->name, MAX_PROCESS_NAME_LEN) != 0) {
            continue;
        }

        if (csv_parser_get_field_str(
                &row, STEP_COL_PROCESS_ID,
                step->process_id,
                MAX_PROCESS_ID_LEN) != 0) {
            step->process_id[0] = '\0';
        }

        char category_str[MAX_CATEGORY_LEN];
        if (csv_parser_get_field_str(
                &row, STEP_COL_CATEGORY,
                category_str, MAX_CATEGORY_LEN) == 0) {
            step->category =
                processing_category_from_string(
                    category_str);
        } else {
            step->category = PROCESS_CAT_UNKNOWN;
        }

        /* Water consumption (m3/kg) */
        if (csv_parser_get_field_double(
                &row, STEP_COL_WATER_CONSUMPTION,
                &step->water_consumption_m3_per_kg)
                != 0) {
            step->water_consumption_m3_per_kg = 0.0;
        }

        if (csv_parser_get_field_str(
                &row, STEP_COL_DESCRIPTION,
                step->description,
                MAX_PROCESS_DESC_LEN) != 0) {
            step->description[0] = '\0';
        }

        db->count++;
        steps_loaded++;
    }

    csv_parser_close(&parser);

    if (result == -1) {
        return -1;
    }

    return steps_loaded;
}

int processing_init_combo_database(MaterialProcessDatabase *db)
{
    if (db == NULL) {
        return -1;
    }

    memset(db->combinations, 0, sizeof(db->combinations));
    db->count = 0;

    return 0;
}

int processing_load_combinations_csv(
    MaterialProcessDatabase *db, const char *filepath)
{
    if (db == NULL || filepath == NULL) {
        return -1;
    }

    CsvParser parser;
    CsvRow row;

    if (csv_parser_open(&parser, filepath, 1) != 0) {
        return -1;
    }

    int combos_loaded = 0;
    int result;

    while ((result = csv_parser_read_row(&parser, &row)) == 1) {
        if (db->count >= MAX_PROCESSING_COMBINATIONS) {
            break;
        }

        MaterialProcessCombination *combo =
            &db->combinations[db->count];

        if (csv_parser_get_field_str(
                &row, COMBO_COL_MATERIAL_NAME,
                combo->material_name,
                MAX_COMBO_MATERIAL_NAME_LEN) != 0) {
            continue;
        }

        if (csv_parser_get_field_str(
                &row, COMBO_COL_MATERIAL_ID,
                combo->material_id,
                MAX_PROCESS_ID_LEN) != 0) {
            combo->material_id[0] = '\0';
        }

        if (csv_parser_get_field_str(
                &row, COMBO_COL_MATERIAL_CATEGORY,
                combo->material_category,
                MAX_MATERIAL_CATEGORY_LEN) != 0) {
            combo->material_category[0] = '\0';
        }

        if (csv_parser_get_field_str(
                &row, COMBO_COL_PROCESS_NAME,
                combo->process_name,
                MAX_PROCESS_NAME_LEN) != 0) {
            continue;
        }

        if (csv_parser_get_field_str(
                &row, COMBO_COL_PROCESS_ID,
                combo->process_id,
                MAX_PROCESS_ID_LEN) != 0) {
            combo->process_id[0] = '\0';
        }

        /* Water consumption (m3/kg) */
        if (csv_parser_get_field_double(
                &row, COMBO_COL_WATER_CONSUMPTION,
                &combo->water_consumption_m3_per_kg)
                != 0) {
            combo->water_consumption_m3_per_kg = 0.0;
        }

        db->count++;
        combos_loaded++;
    }

    csv_parser_close(&parser);

    if (result == -1) {
        return -1;
    }

    return combos_loaded;
}

const char *processing_get_category_name(
    ProcessCategory category)
{
    if (category < 0
        || category > PROCESS_CAT_UNKNOWN) {
        return CATEGORY_NAMES[PROCESS_CAT_UNKNOWN];
    }

    return CATEGORY_NAMES[category];
}

ProcessCategory processing_category_from_string(
    const char *category_str)
{
    if (category_str == NULL) {
        return PROCESS_CAT_UNKNOWN;
    }

    if (strcasecmp(category_str,
                   "Pre-processing") == 0) {
        return PROCESS_CAT_PRE_PROCESSING;
    }
    if (strcasecmp(category_str,
                   "Primary processing") == 0) {
        return PROCESS_CAT_PRIMARY_PROCESSING;
    }
    if (strcasecmp(category_str,
                   "Wet processing") == 0) {
        return PROCESS_CAT_WET_PROCESSING;
    }
    if (strcasecmp(category_str, "Finishing") == 0) {
        return PROCESS_CAT_FINISHING;
    }
    if (strcasecmp(category_str,
                   "Special treatments") == 0) {
        return PROCESS_CAT_SPECIAL_TREATMENTS;
    }
    if (strcasecmp(category_str,
                   "Synthetic fibre production") == 0) {
        return PROCESS_CAT_SYNTHETIC_FIBRE;
    }
    if (strcasecmp(category_str,
                   "Glass/mineral fibre processing") == 0) {
        return PROCESS_CAT_GLASS_MINERAL_FIBRE;
    }
    if (strcasecmp(category_str,
                   "Composite processing") == 0) {
        return PROCESS_CAT_COMPOSITE_PROCESSING;
    }
    if (strcasecmp(category_str,
                   "Construction materials") == 0) {
        return PROCESS_CAT_CONSTRUCTION_MATERIALS;
    }

    return PROCESS_CAT_UNKNOWN;
}

void processing_free_step_database(ProcessingStepDatabase *db)
{
    if (db != NULL) { db->count = 0; }
}

void processing_free_combo_database(MaterialProcessDatabase *db)
{
    if (db != NULL) { db->count = 0; }
}
