/**
 * @file material_processing.h
 * @ingroup Processing
 * @brief Material processing carbon footprint calculation API.
 *
 * Implements the material processing phase of the cradle-to-gate
 * carbon footprint calculation as defined in ISO 14040/14044 and
 * PEFCR for Apparel and Footwear v3.1.
 *
 * Formula: CF_processing = sum_m sum_p (w_m * EF_m,p)
 *
 * @see processing_types.h for data structure definitions.
 * @see research_paper.tex Section 3.4
 */

#ifndef MATERIAL_PROCESSING_H_
#define MATERIAL_PROCESSING_H_

#include "processing_types.h"

/**
 * @brief Initialize an empty processing step database.
 *
 * @param[out] db  Pointer to ProcessingStepDatabase to initialize.
 * @return 0 on success, -1 on failure (null pointer).
 */
int processing_init_step_database(ProcessingStepDatabase *db);

/**
 * @brief Load processing steps from CSV file.
 *
 * Reads processing_steps_overview.csv containing 41 processing
 * step definitions.
 *
 * @param[out] db        Pointer to ProcessingStepDatabase to populate.
 * @param[in]  filepath  Path to CSV file.
 * @return Number of steps loaded on success, -1 on failure.
 */
int processing_load_steps_csv(ProcessingStepDatabase *db,
                              const char *filepath);

/**
 * @brief Initialize material-process combination database.
 *
 * @param[out] db  Pointer to MaterialProcessDatabase to initialize.
 * @return 0 on success, -1 on failure.
 */
int processing_init_combo_database(MaterialProcessDatabase *db);

/**
 * @brief Load material-process combinations from CSV file.
 *
 * Reads material_processing_emissions.csv containing 3084
 * valid combinations.
 *
 * @param[out] db        Pointer to MaterialProcessDatabase to populate.
 * @param[in]  filepath  Path to CSV file.
 * @return Number of combinations loaded on success, -1 on failure.
 */
int processing_load_combinations_csv(MaterialProcessDatabase *db,
                                     const char *filepath);

/**
 * @brief Find processing step by name.
 *
 * Performs case-insensitive partial match search.
 *
 * @param[in] db    Pointer to ProcessingStepDatabase to search.
 * @param[in] name  Processing step name or partial name.
 * @return Index of first matching step, or -1 if not found.
 */
int processing_find_step_by_name(const ProcessingStepDatabase *db,
                                 const char *name);

/**
 * @brief Find processing step by process ID.
 *
 * @param[in] db          Pointer to ProcessingStepDatabase to search.
 * @param[in] process_id  EcoInvent process ID.
 * @return Index of matching step, or -1 if not found.
 */
int processing_find_step_by_id(const ProcessingStepDatabase *db,
                               const char *process_id);

/**
 * @brief Get emission factor for a material-process pair.
 *
 * Looks up the emission factor for a specific material undergoing
 * a specific processing step from the combinations database.
 *
 * @param[in] combo_db       Pointer to MaterialProcessDatabase.
 * @param[in] material_name  Name of the material (partial match).
 * @param[in] process_name   Name of the processing step (partial match).
 * @return Emission factor in kg CO2e/kg, or -1.0 if not found.
 */
double processing_get_emission_factor(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name);

/**
 * @brief Initialize empty product processing list.
 *
 * @param[out] list  Pointer to ProductProcessingList to initialize.
 * @return 0 on success, -1 on failure.
 */
int processing_init_product_list(ProductProcessingList *list);

/**
 * @brief Add a material to product processing list.
 *
 * @param[in,out] list           Pointer to ProductProcessingList.
 * @param[in]     material_name  Name of the material.
 * @param[in]     weight_kg      Mass of material (kg).
 * @return Index of added material on success, -1 on failure.
 */
int processing_add_material(ProductProcessingList *list,
                            const char *material_name,
                            double weight_kg);

/**
 * @brief Add processing step to a material.
 *
 * @param[in,out] list            Pointer to ProductProcessingList.
 * @param[in]     material_index  Index of material in list.
 * @param[in]     process_name    Name of processing step to add.
 * @return 0 on success, -1 on failure.
 */
int processing_add_step_to_material(ProductProcessingList *list,
                                    size_t material_index,
                                    const char *process_name);

/**
 * @brief Calculate total processing carbon footprint.
 *
 * Implements: CF_processing = sum_m sum_p (w_m * EF_m,p)
 *
 * @param[in]  combo_db  Pointer to MaterialProcessDatabase.
 * @param[in]  list      Pointer to ProductProcessingList.
 * @param[out] result    Pointer to ProcessingResult to store results.
 * @return 0 on success, -1 on error.
 *
 * @note Invalid material-process pairs are silently skipped.
 */
int processing_calculate_footprint(
    const MaterialProcessDatabase *combo_db,
    const ProductProcessingList *list,
    ProcessingResult *result);

/**
 * @brief Calculate footprint for single material-process pair.
 *
 * @param[in] combo_db       Pointer to MaterialProcessDatabase.
 * @param[in] material_name  Name of the material.
 * @param[in] process_name   Name of the processing step.
 * @param[in] weight_kg      Mass of material (kg).
 * @return Carbon footprint in kg CO2e, or -1.0 if invalid.
 */
double processing_calculate_single(
    const MaterialProcessDatabase *combo_db,
    const char *material_name,
    const char *process_name,
    double weight_kg);

/**
 * @brief Get string name for process category.
 *
 * @param[in] category  ProcessCategory enum value.
 * @return Pointer to static string containing category name.
 */
const char *processing_get_category_name(ProcessCategory category);

/**
 * @brief Parse process category from string.
 *
 * @param[in] category_str  String representation of category.
 * @return ProcessCategory enum value.
 */
ProcessCategory processing_category_from_string(
    const char *category_str);

/**
 * @brief Release resources held by step database.
 *
 * @param[in,out] db  Pointer to ProcessingStepDatabase to free.
 */
void processing_free_step_database(ProcessingStepDatabase *db);

/**
 * @brief Release resources held by combo database.
 *
 * @param[in,out] db  Pointer to MaterialProcessDatabase to free.
 */
void processing_free_combo_database(MaterialProcessDatabase *db);

#endif  /* MATERIAL_PROCESSING_H_ */
