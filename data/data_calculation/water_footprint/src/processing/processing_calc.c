/**
 * @file processing_calc.c
 * @ingroup Processing
 * @brief Calculation and product list management functions.
 *
 * Implements the processing water footprint calculation formula
 * and product material list operations.
 *
 * Formula: WF_processing = sum_m sum_p (w_m * WU_p * AWARE_nonagri)
 * Unit: m3 world-eq
 */

#include "processing/material_processing.h"
#include <string.h>

int processing_init_product_list(ProductProcessingList *list)
{
    if (list == NULL) {
        return -1;
    }

    memset(list->materials, 0, sizeof(list->materials));
    list->count = 0;

    return 0;
}

int processing_add_material(ProductProcessingList *list,
                            const char *material_name,
                            double weight_kg,
                            const char *factory_country)
{
    if (list == NULL || material_name == NULL
        || weight_kg < 0.0) {
        return -1;
    }

    if (list->count >= MAX_PRODUCT_MATERIALS_PROC) {
        return -1;
    }

    ProductMaterialProcessing *mat =
        &list->materials[list->count];

    strncpy(mat->material_name, material_name,
            MAX_COMBO_MATERIAL_NAME_LEN - 1);
    mat->material_name[MAX_COMBO_MATERIAL_NAME_LEN - 1]
        = '\0';
    mat->material_id[0] = '\0';
    mat->weight_kg = weight_kg;
    mat->process_count = 0;

    if (factory_country != NULL) {
        strncpy(mat->factory_country, factory_country,
                MAX_FACTORY_COUNTRY_LEN - 1);
        mat->factory_country[MAX_FACTORY_COUNTRY_LEN - 1]
            = '\0';
    } else {
        mat->factory_country[0] = '\0';
    }

    int index = (int)list->count;
    list->count++;

    return index;
}

int processing_add_step_to_material(
    ProductProcessingList *list,
    size_t material_index,
    const char *process_name)
{
    if (list == NULL || process_name == NULL) {
        return -1;
    }

    if (material_index >= list->count) {
        return -1;
    }

    ProductMaterialProcessing *mat =
        &list->materials[material_index];

    if (mat->process_count >= MAX_PRODUCT_PROCESSES) {
        return -1;
    }

    ProductProcessingStep *step =
        &mat->processes[mat->process_count];
    step->process_index = -1;
    strncpy(step->process_name, process_name,
            MAX_PROCESS_NAME_LEN - 1);
    step->process_name[MAX_PROCESS_NAME_LEN - 1] = '\0';

    mat->process_count++;

    return 0;
}

int processing_calculate_footprint(
    const MaterialProcessDatabase *combo_db,
    const ProductProcessingList *list,
    const AwareDatabase *aware_db,
    ProcessingResult *result)
{
    if (combo_db == NULL || list == NULL
        || result == NULL) {
        return -1;
    }

    result->total_footprint_m3_world_eq = 0.0;
    memset(result->step_contributions, 0,
           sizeof(result->step_contributions));
    result->step_count = 0;

    if (list->count == 0) {
        return 0;
    }

    /*
     * WF_processing = sum_m sum_p (w_m * WU_p * AWARE_nonagri)
     * Unit: m3 world-eq
     *
     * If aware_db is NULL, AWARE factor defaults to 1.0.
     */
    for (size_t m = 0; m < list->count; m++) {
        const ProductMaterialProcessing *mat =
            &list->materials[m];

        /* Get AWARE factor for this material's factory */
        double aware_factor = 1.0;
        if (aware_db != NULL
            && mat->factory_country[0] != '\0') {
            aware_factor = aware_get_factor(
                aware_db, mat->factory_country);
        } else if (aware_db != NULL) {
            aware_factor = aware_db->global_fallback;
        }

        for (size_t p = 0; p < mat->process_count; p++) {
            const ProductProcessingStep *step =
                &mat->processes[p];

            double wu = processing_get_water_consumption(
                combo_db, mat->material_name,
                step->process_name);

            if (wu >= 0.0) {
                double contribution =
                    mat->weight_kg * wu * aware_factor;
                result->total_footprint_m3_world_eq
                    += contribution;

                if (result->step_count
                    < MAX_PROCESSING_STEPS) {
                    result->step_contributions[
                        result->step_count]
                        = contribution;
                    result->step_count++;
                }
            }
        }
    }

    return 0;
}

double processing_calculate_single(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name,
    double weight_kg,
    double aware_factor)
{
    if (combo_db == NULL || material_name == NULL
        || process_name == NULL || weight_kg < 0.0) {
        return -1.0;
    }

    double wu = processing_get_water_consumption(
        combo_db, material_name, process_name);

    if (wu < 0.0) {
        return -1.0;
    }

    return weight_kg * wu * aware_factor;
}
