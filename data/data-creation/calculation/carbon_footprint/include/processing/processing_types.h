/**
 * @file processing_types.h
 * @ingroup Processing
 * @brief Type definitions for the material processing module.
 *
 * Defines all data structures, enumerations, and size constants
 * used by the material processing carbon footprint calculation.
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

/** @brief Maximum number of processing steps (41 per research paper). */
#define MAX_PROCESSING_STEPS 50

/** @brief Maximum number of material-processing combinations (3084 per research paper). */
#define MAX_PROCESSING_COMBINATIONS 4000

/** @brief Maximum processing steps for a single product. */
#define MAX_PRODUCT_PROCESSES 20

/** @brief Maximum materials in a single product for processing. */
#define MAX_PRODUCT_MATERIALS_PROC 20

/**
 * @brief Classification of processing step categories.
 *
 * Categories as defined in processing_steps_overview.csv.
 */
typedef enum {
    PROCESS_CAT_PRE_PROCESSING,         /**< Pre-processing. */
    PROCESS_CAT_PRIMARY_PROCESSING,     /**< Primary processing. */
    PROCESS_CAT_WET_PROCESSING,         /**< Wet processing. */
    PROCESS_CAT_FINISHING,              /**< Finishing. */
    PROCESS_CAT_SPECIAL_TREATMENTS,     /**< Special treatments. */
    PROCESS_CAT_SYNTHETIC_FIBRE,        /**< Synthetic fibre production. */
    PROCESS_CAT_GLASS_MINERAL_FIBRE,    /**< Glass/mineral fibre processing. */
    PROCESS_CAT_COMPOSITE_PROCESSING,   /**< Composite processing. */
    PROCESS_CAT_CONSTRUCTION_MATERIALS, /**< Construction materials. */
    PROCESS_CAT_UNKNOWN                 /**< Unknown category. */
} ProcessCategory;

/**
 * @brief A processing step from the dataset.
 */
typedef struct {
    char name[MAX_PROCESS_NAME_LEN];                /**< Processing step name. */
    char process_id[MAX_PROCESS_ID_LEN];            /**< EcoInvent unique identifier. */
    ProcessCategory category;                        /**< Processing category classification. */
    double emission_factor_kg_CO2e_per_kg;           /**< Emission factor (kg CO2e/kg). */
    char description[MAX_PROCESS_DESC_LEN];          /**< Brief description. */
} ProcessingStep;

/**
 * @brief Container for processing steps dataset.
 */
typedef struct {
    ProcessingStep steps[MAX_PROCESSING_STEPS];     /**< Array of processing step entries. */
    size_t count;                                    /**< Number of steps loaded. */
} ProcessingStepDatabase;

/**
 * @brief A valid material-process pair from the dataset.
 */
typedef struct {
    char material_name[MAX_COMBO_MATERIAL_NAME_LEN]; /**< Material name. */
    char material_id[MAX_PROCESS_ID_LEN];            /**< Material unique identifier. */
    char material_category[MAX_MATERIAL_CATEGORY_LEN]; /**< Material category. */
    char process_name[MAX_PROCESS_NAME_LEN];         /**< Processing step name. */
    char process_id[MAX_PROCESS_ID_LEN];             /**< Process unique identifier. */
    double emission_factor_kg_CO2e_per_kg;           /**< Emission factor for this combination. */
} MaterialProcessCombination;

/**
 * @brief Container for material-process combinations.
 */
typedef struct {
    MaterialProcessCombination combinations[MAX_PROCESSING_COMBINATIONS]; /**< Combination array. */
    size_t count;                                    /**< Number of combinations loaded. */
} MaterialProcessDatabase;

/**
 * @brief A processing step in a product's journey.
 */
typedef struct {
    int process_index;                               /**< Index into step database (-1 for name lookup). */
    char process_name[MAX_PROCESS_NAME_LEN];         /**< Processing step name. */
} ProductProcessingStep;

/**
 * @brief A material with its processing steps.
 */
typedef struct {
    char material_name[MAX_COMBO_MATERIAL_NAME_LEN]; /**< Material name. */
    char material_id[MAX_PROCESS_ID_LEN];            /**< Optional unique identifier. */
    double weight_kg;                                /**< Mass of material (kg). */
    ProductProcessingStep processes[MAX_PRODUCT_PROCESSES]; /**< Processing steps array. */
    size_t process_count;                            /**< Number of processing steps. */
} ProductMaterialProcessing;

/**
 * @brief List of materials with their processing steps.
 */
typedef struct {
    ProductMaterialProcessing materials[MAX_PRODUCT_MATERIALS_PROC]; /**< Materials array. */
    size_t count;                                    /**< Number of materials. */
} ProductProcessingList;

/**
 * @brief Calculation results from processing footprint.
 */
typedef struct {
    double total_footprint_kg_CO2e;                  /**< Total processing carbon footprint (kg CO2e). */
    double step_contributions[MAX_PROCESSING_STEPS]; /**< Contribution per processing step. */
    size_t step_count;                               /**< Number of unique steps applied. */
} ProcessingResult;

#endif  /* PROCESSING_TYPES_H_ */
