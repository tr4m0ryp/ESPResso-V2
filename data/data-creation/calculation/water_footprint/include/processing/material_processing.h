/**
 * @file material_processing.h
 * @ingroup Processing
 * @brief Material processing water footprint calculation API.
 *
 * Implements the material processing phase of the water
 * footprint calculation using AWARE non-agricultural factors.
 *
 * Formula: WF_processing = sum_m sum_p (w_m * WU_p * AWARE_nonagri_p)
 *
 * @see processing_types.h for data structure definitions.
 */

#ifndef MATERIAL_PROCESSING_H_
#define MATERIAL_PROCESSING_H_

#include "processing_types.h"
#include "aware/aware.h"

/**
 * @brief Initialize an empty processing step database.
 */
int processing_init_step_database(ProcessingStepDatabase *db);

/**
 * @brief Load processing steps from CSV file.
 *
 * Reads processing_steps_water.csv with water consumption values.
 */
int processing_load_steps_csv(ProcessingStepDatabase *db,
                              const char *filepath);

/**
 * @brief Initialize material-process combination database.
 */
int processing_init_combo_database(MaterialProcessDatabase *db);

/**
 * @brief Load material-process combinations from CSV file.
 *
 * Reads material_processing_water.csv with water consumption values.
 */
int processing_load_combinations_csv(MaterialProcessDatabase *db,
                                     const char *filepath);

/**
 * @brief Find processing step by name (case-insensitive partial match).
 */
int processing_find_step_by_name(const ProcessingStepDatabase *db,
                                 const char *name);

/**
 * @brief Find processing step by process ID.
 */
int processing_find_step_by_id(const ProcessingStepDatabase *db,
                               const char *process_id);

/**
 * @brief Get water consumption for a material-process pair.
 *
 * @return Water consumption in m3/kg, or -1.0 if not found.
 */
double processing_get_water_consumption(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name);

/**
 * @brief Initialize empty product processing list.
 */
int processing_init_product_list(ProductProcessingList *list);

/**
 * @brief Add a material to product processing list.
 *
 * @param[in] factory_country  Factory country for AWARE lookup (can be NULL).
 * @return Index of added material on success, -1 on failure.
 */
int processing_add_material(ProductProcessingList *list,
                            const char *material_name,
                            double weight_kg,
                            const char *factory_country);

/**
 * @brief Add processing step to a material.
 */
int processing_add_step_to_material(ProductProcessingList *list,
                                    size_t material_index,
                                    const char *process_name);

/**
 * @brief Calculate total processing water footprint.
 *
 * Implements: WF_processing = sum_m sum_p (w_m * WU_p * AWARE_nonagri)
 *
 * @param[in]  combo_db  Pointer to MaterialProcessDatabase.
 * @param[in]  list      Pointer to ProductProcessingList.
 * @param[in]  aware_db  Pointer to AwareDatabase (nonagri factors).
 * @param[out] result    Pointer to ProcessingResult to store results.
 * @return 0 on success, -1 on error.
 */
int processing_calculate_footprint(
    const MaterialProcessDatabase *combo_db,
    const ProductProcessingList *list,
    const AwareDatabase *aware_db,
    ProcessingResult *result);

/**
 * @brief Calculate footprint for single material-process pair.
 *
 * @param[in] aware_factor  AWARE CF for the factory country.
 * @return Water footprint in m3 world-eq, or -1.0 if invalid.
 */
double processing_calculate_single(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name,
    double weight_kg,
    double aware_factor);

/**
 * @brief Get string name for process category.
 */
const char *processing_get_category_name(ProcessCategory category);

/**
 * @brief Parse process category from string.
 */
ProcessCategory processing_category_from_string(
    const char *category_str);

/**
 * @brief Release resources held by step database.
 */
void processing_free_step_database(ProcessingStepDatabase *db);

/**
 * @brief Release resources held by combo database.
 */
void processing_free_combo_database(MaterialProcessDatabase *db);

#endif  /* MATERIAL_PROCESSING_H_ */
