/**
 * @file processing_lookup.c
 * @ingroup Processing
 * @brief Search and lookup functions for processing databases.
 *
 * Provides name-based and ID-based search across processing step
 * and material-process combination databases for water consumption.
 */

#include "processing/material_processing.h"
#include "utils/csv_parser.h"
#include <string.h>

int processing_find_step_by_name(
    const ProcessingStepDatabase *db, const char *name)
{
    if (db == NULL || name == NULL) {
        return -1;
    }

    char search_lower[MAX_PROCESS_NAME_LEN];
    strncpy(search_lower, name,
            MAX_PROCESS_NAME_LEN - 1);
    search_lower[MAX_PROCESS_NAME_LEN - 1] = '\0';
    csv_str_to_lower(search_lower);

    /* Exact match first */
    for (size_t i = 0; i < db->count; i++) {
        char step_lower[MAX_PROCESS_NAME_LEN];
        strncpy(step_lower, db->steps[i].name,
                MAX_PROCESS_NAME_LEN - 1);
        step_lower[MAX_PROCESS_NAME_LEN - 1] = '\0';
        csv_str_to_lower(step_lower);

        if (strcmp(step_lower, search_lower) == 0) {
            return (int)i;
        }
    }

    /* Try partial match if exact not found */
    for (size_t i = 0; i < db->count; i++) {
        char step_lower[MAX_PROCESS_NAME_LEN];
        strncpy(step_lower, db->steps[i].name,
                MAX_PROCESS_NAME_LEN - 1);
        step_lower[MAX_PROCESS_NAME_LEN - 1] = '\0';
        csv_str_to_lower(step_lower);

        if (strstr(step_lower, search_lower) != NULL) {
            return (int)i;
        }
    }

    return -1;
}

int processing_find_step_by_id(
    const ProcessingStepDatabase *db,
    const char *process_id)
{
    if (db == NULL || process_id == NULL) {
        return -1;
    }

    for (size_t i = 0; i < db->count; i++) {
        if (strcmp(db->steps[i].process_id,
                   process_id) == 0) {
            return (int)i;
        }
    }

    return -1;
}

double processing_get_water_consumption(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name)
{
    if (combo_db == NULL || material_name == NULL
        || process_name == NULL) {
        return -1.0;
    }

    char mat_lower[MAX_COMBO_MATERIAL_NAME_LEN];
    char proc_lower[MAX_PROCESS_NAME_LEN];

    strncpy(mat_lower, material_name,
            MAX_COMBO_MATERIAL_NAME_LEN - 1);
    mat_lower[MAX_COMBO_MATERIAL_NAME_LEN - 1] = '\0';
    csv_str_to_lower(mat_lower);

    strncpy(proc_lower, process_name,
            MAX_PROCESS_NAME_LEN - 1);
    proc_lower[MAX_PROCESS_NAME_LEN - 1] = '\0';
    csv_str_to_lower(proc_lower);

    for (size_t i = 0; i < combo_db->count; i++) {
        char combo_mat[MAX_COMBO_MATERIAL_NAME_LEN];
        char combo_proc[MAX_PROCESS_NAME_LEN];

        strncpy(combo_mat,
                combo_db->combinations[i].material_name,
                MAX_COMBO_MATERIAL_NAME_LEN - 1);
        combo_mat[MAX_COMBO_MATERIAL_NAME_LEN - 1] = '\0';
        csv_str_to_lower(combo_mat);

        strncpy(combo_proc,
                combo_db->combinations[i].process_name,
                MAX_PROCESS_NAME_LEN - 1);
        combo_proc[MAX_PROCESS_NAME_LEN - 1] = '\0';
        csv_str_to_lower(combo_proc);

        int mat_match =
            (strstr(combo_mat, mat_lower) != NULL)
            || (strstr(mat_lower, combo_mat) != NULL);
        int proc_match =
            (strcmp(combo_proc, proc_lower) == 0);

        if (mat_match && proc_match) {
            return combo_db->combinations[i]
                .water_consumption_m3_per_kg;
        }
    }

    return -1.0;
}
