/**
 * @file processing_types.h
 * @ingroup Processing
 * @brief Type definitions for the material processing module.
 *
 * Defines all data structures, enumerations, and size constants
 * used by the material processing water footprint calculation.
 * Separated from function declarations to keep each header
 * under the 300-line limit.
 */

#ifndef PROCESSING_TYPES_H_
#define PROCESSING_TYPES_H_

#include <stddef.h>

/** @brief Maximum length for processing step name. */
#define MAX_PROCESS_NAME_LEN 64

/** @brief Maximum length for process ID string. */
#define MAX_PROCESS_ID_LEN 64

/** @brief Maximum length for category string. */
#define MAX_CATEGORY_LEN 64

/** @brief Maximum length for description string. */
#define MAX_PROCESS_DESC_LEN 256

/** @brief Maximum length for material name in combinations. */
#define MAX_COMBO_MATERIAL_NAME_LEN 256

/** @brief Maximum length for material category. */
#define MAX_MATERIAL_CATEGORY_LEN 64

/** @brief Maximum length for factory country field. */
#define MAX_FACTORY_COUNTRY_LEN 256

/** @brief Maximum number of processing steps. */
#define MAX_PROCESSING_STEPS 50

/** @brief Maximum number of material-processing combinations. */
#define MAX_PROCESSING_COMBINATIONS 4000

/** @brief Maximum processing steps for a single product. */
#define MAX_PRODUCT_PROCESSES 20

/** @brief Maximum materials in a single product for processing. */
#define MAX_PRODUCT_MATERIALS_PROC 20

/**
 * @brief Classification of processing step categories.
 */
typedef enum {
    PROCESS_CAT_PRE_PROCESSING,
    PROCESS_CAT_PRIMARY_PROCESSING,
    PROCESS_CAT_WET_PROCESSING,
    PROCESS_CAT_FINISHING,
    PROCESS_CAT_SPECIAL_TREATMENTS,
    PROCESS_CAT_SYNTHETIC_FIBRE,
    PROCESS_CAT_GLASS_MINERAL_FIBRE,
    PROCESS_CAT_COMPOSITE_PROCESSING,
    PROCESS_CAT_CONSTRUCTION_MATERIALS,
    PROCESS_CAT_UNKNOWN
} ProcessCategory;

/**
 * @brief A processing step from the dataset.
 */
typedef struct {
    char name[MAX_PROCESS_NAME_LEN];
    char process_id[MAX_PROCESS_ID_LEN];
    ProcessCategory category;
    double water_consumption_m3_per_kg;     /**< Water consumption (m3/kg). */
    char description[MAX_PROCESS_DESC_LEN];
} ProcessingStep;

/**
 * @brief Container for processing steps dataset.
 */
typedef struct {
    ProcessingStep steps[MAX_PROCESSING_STEPS];
    size_t count;
} ProcessingStepDatabase;

/**
 * @brief A valid material-process pair from the dataset.
 */
typedef struct {
    char material_name[MAX_COMBO_MATERIAL_NAME_LEN];
    char material_id[MAX_PROCESS_ID_LEN];
    char material_category[MAX_MATERIAL_CATEGORY_LEN];
    char process_name[MAX_PROCESS_NAME_LEN];
    char process_id[MAX_PROCESS_ID_LEN];
    double water_consumption_m3_per_kg;     /**< Water consumption (m3/kg). */
} MaterialProcessCombination;

/**
 * @brief Container for material-process combinations.
 */
typedef struct {
    MaterialProcessCombination combinations[MAX_PROCESSING_COMBINATIONS];
    size_t count;
} MaterialProcessDatabase;

/**
 * @brief A processing step in a product's journey.
 */
typedef struct {
    int process_index;
    char process_name[MAX_PROCESS_NAME_LEN];
} ProductProcessingStep;

/**
 * @brief A material with its processing steps and factory country.
 */
typedef struct {
    char material_name[MAX_COMBO_MATERIAL_NAME_LEN];
    char material_id[MAX_PROCESS_ID_LEN];
    double weight_kg;
    char factory_country[MAX_FACTORY_COUNTRY_LEN];  /**< Factory location for AWARE lookup. */
    ProductProcessingStep processes[MAX_PRODUCT_PROCESSES];
    size_t process_count;
} ProductMaterialProcessing;

/**
 * @brief List of materials with their processing steps.
 */
typedef struct {
    ProductMaterialProcessing materials[MAX_PRODUCT_MATERIALS_PROC];
    size_t count;
} ProductProcessingList;

/**
 * @brief Calculation results from processing water footprint.
 */
typedef struct {
    double total_footprint_m3_world_eq;             /**< Total water footprint (m3 world-eq). */
    double step_contributions[MAX_PROCESSING_STEPS];
    size_t step_count;
} ProcessingResult;

#endif  /* PROCESSING_TYPES_H_ */
