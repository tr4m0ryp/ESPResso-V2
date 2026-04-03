/**
 * @file processing_calc.c
 * @ingroup Processing
 * @brief Calculation and product list management functions.
 *
 * Implements the processing carbon footprint calculation formula
 * and product material list operations.
 *
 * @see research_paper.tex Section 3.4
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
                            double weight_kg)
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
    ProcessingResult *result)
{
    if (combo_db == NULL || list == NULL
        || result == NULL) {
        return -1;
    }

    result->total_footprint_kg_CO2e = 0.0;
    memset(result->step_contributions, 0,
           sizeof(result->step_contributions));
    result->step_count = 0;

    if (list->count == 0) {
        return 0;
    }

    /*
     * CF_processing = sum_m sum_p (w_m * EF_m,p)
     * Reference: research_paper.tex Section 3.4
     */
    for (size_t m = 0; m < list->count; m++) {
        const ProductMaterialProcessing *mat =
            &list->materials[m];

        for (size_t p = 0; p < mat->process_count; p++) {
            const ProductProcessingStep *step =
                &mat->processes[p];

            double ef = processing_get_emission_factor(
                combo_db, mat->material_name,
                step->process_name);

            if (ef >= 0.0) {
                double contribution =
                    mat->weight_kg * ef;
                result->total_footprint_kg_CO2e
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
    double weight_kg)
{
    if (combo_db == NULL || material_name == NULL
        || process_name == NULL || weight_kg < 0.0) {
        return -1.0;
    }

    double ef = processing_get_emission_factor(
        combo_db, material_name, process_name);

    if (ef < 0.0) {
        return -1.0;
    }

    return weight_kg * ef;
}
