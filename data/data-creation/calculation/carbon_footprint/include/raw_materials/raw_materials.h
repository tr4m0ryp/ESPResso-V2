/**
 * @file raw_materials.h
 * @ingroup RawMaterials
 * @brief Raw material carbon footprint calculation module.
 *
 * Implements the raw material acquisition phase of the
 * cradle-to-gate carbon footprint calculation as defined in
 * ISO 14040/14044 and PEFCR for Apparel and Footwear v3.1.
 *
 * Formula: CF_raw = sum(w_m * EF_m)
 *
 * Data source: EcoInvent v3.12 dataset (187 high-confidence materials).
 *
 * @see research_paper.tex Section 3.1
 */

#ifndef RAW_MATERIALS_H_
#define RAW_MATERIALS_H_

#include <stddef.h>

/** @brief Maximum length for material name string. */
#define MAX_MATERIAL_NAME_LEN 256

/** @brief Maximum length for reference ID string. */
#define MAX_REFERENCE_ID_LEN 64

/** @brief Maximum length for notes field. */
#define MAX_NOTES_LEN 128

/** @brief Maximum number of materials in dataset. */
#define MAX_MATERIALS 200

/** @brief Maximum number of materials in a single product. */
#define MAX_PRODUCT_MATERIALS 20

/**
 * @brief Classification of material entry type.
 */
typedef enum {
    MATERIAL_TYPE_FLOW,    /**< Material flow entry from EcoInvent. */
    MATERIAL_TYPE_PROCESS  /**< Process-based entry from EcoInvent. */
} MaterialType;

/**
 * @brief A single material from EcoInvent dataset.
 */
typedef struct {
    char name[MAX_MATERIAL_NAME_LEN];            /**< Material name. */
    MaterialType type;                            /**< Flow or process. */
    char reference_id[MAX_REFERENCE_ID_LEN];     /**< EcoInvent reference ID. */
    double carbon_footprint_kg_CO2eq_per_kg;     /**< Emission factor (kg CO2-eq/kg). */
    char notes[MAX_NOTES_LEN];                   /**< Additional information. */
} Material;

/**
 * @brief Container for loaded material dataset.
 */
typedef struct {
    Material materials[MAX_MATERIALS]; /**< Material entries. */
    size_t count;                      /**< Number of materials loaded. */
} MaterialDatabase;

/**
 * @brief A material used in a product.
 */
typedef struct {
    size_t material_index; /**< Index into MaterialDatabase.materials. */
    double weight_kg;      /**< Mass of material in product (kg). */
} ProductMaterial;

/**
 * @brief List of materials comprising a product.
 */
typedef struct {
    ProductMaterial items[MAX_PRODUCT_MATERIALS]; /**< Material entries. */
    size_t count;                                 /**< Number of materials. */
} ProductMaterialList;

/**
 * @brief Initialize an empty material database.
 *
 * @param[out] db  Pointer to MaterialDatabase to initialize.
 * @return 0 on success, -1 on failure (null pointer).
 */
int raw_materials_init_database(MaterialDatabase *db);

/**
 * @brief Load material data from CSV file.
 *
 * Expected CSV: Name, Type, Reference ID, Description,
 *               Carbon_Footprint_kg_CO2eq_per_kg, Notes
 *
 * @param[out] db        Pointer to MaterialDatabase to populate.
 * @param[in]  filepath  Path to CSV file.
 * @return Number of materials loaded, or -1 on failure.
 *
 * @note CSV must have header row (skipped during parsing).
 */
int raw_materials_load_csv(MaterialDatabase *db,
                           const char *filepath);

/**
 * @brief Search for material by name.
 *
 * Case-insensitive partial match.
 *
 * @param[in] db    Pointer to MaterialDatabase.
 * @param[in] name  Material name or partial name.
 * @return Index of first match, or -1 if not found.
 */
int raw_materials_find_by_name(const MaterialDatabase *db,
                               const char *name);

/**
 * @brief Search for material by reference ID.
 *
 * Exact match.
 *
 * @param[in] db            Pointer to MaterialDatabase.
 * @param[in] reference_id  EcoInvent reference ID.
 * @return Index of match, or -1 if not found.
 */
int raw_materials_find_by_id(const MaterialDatabase *db,
                             const char *reference_id);

/**
 * @brief Get emission factor for material at given index.
 *
 * @param[in] db     Pointer to MaterialDatabase.
 * @param[in] index  Index of material.
 * @return Emission factor in kg CO2-eq/kg, or -1.0 if invalid.
 */
double raw_materials_get_emission_factor(
    const MaterialDatabase *db, size_t index);

/**
 * @brief Initialize empty product material list.
 *
 * @param[out] list  Pointer to ProductMaterialList.
 * @return 0 on success, -1 on failure.
 */
int raw_materials_init_product_list(ProductMaterialList *list);

/**
 * @brief Add material to product material list.
 *
 * @param[in,out] list            Pointer to ProductMaterialList.
 * @param[in]     material_index  Index in MaterialDatabase.
 * @param[in]     weight_kg       Mass of material (kg).
 * @return 0 on success, -1 on failure.
 */
int raw_materials_add_to_product(ProductMaterialList *list,
                                 size_t material_index,
                                 double weight_kg);

/**
 * @brief Calculate total raw material carbon footprint.
 *
 * Implements: CF_raw = sum(w_m * EF_m)
 *
 * @param[in] db    Pointer to MaterialDatabase with emission factors.
 * @param[in] list  Pointer to ProductMaterialList with composition.
 * @return Total footprint in kg CO2-eq, or -1.0 on error.
 *
 * @note Calculates only raw material acquisition phase.
 */
double raw_materials_calculate_footprint(
    const MaterialDatabase *db,
    const ProductMaterialList *list);

/**
 * @brief Release resources held by database.
 *
 * @param[in,out] db  Pointer to MaterialDatabase to free.
 *
 * @note Current implementation uses static arrays (no-op).
 */
void raw_materials_free_database(MaterialDatabase *db);

#endif  /* RAW_MATERIALS_H_ */
