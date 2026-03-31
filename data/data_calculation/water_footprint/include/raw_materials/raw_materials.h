/**
 * @file raw_materials.h
 * @ingroup RawMaterials
 * @brief Raw material water footprint calculation module.
 *
 * Implements the raw material acquisition phase of the
 * water footprint calculation using AWARE-weighted
 * water consumption factors.
 *
 * Formula: WF_raw = sum(w_i * WU_material_i * AWARE_CF_i)
 *
 * Data source: EcoInvent v3.12 water consumption values.
 * AWARE weighting: country-specific agricultural factors.
 */

#ifndef RAW_MATERIALS_H_
#define RAW_MATERIALS_H_

#include <stddef.h>
#include "aware/aware.h"

/** @brief Maximum length for material name string. */
#define MAX_MATERIAL_NAME_LEN 256

/** @brief Maximum length for reference ID string. */
#define MAX_REFERENCE_ID_LEN 64

/** @brief Maximum length for notes field. */
#define MAX_NOTES_LEN 128

/** @brief Maximum length for origin country field. */
#define MAX_ORIGIN_LEN 256

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
    double water_consumption_m3_per_kg;           /**< Water consumption (m3/kg). */
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
 * @brief A material used in a product, with origin country.
 */
typedef struct {
    size_t material_index;           /**< Index into MaterialDatabase.materials. */
    double weight_kg;                /**< Mass of material in product (kg). */
    char origin_country[MAX_ORIGIN_LEN]; /**< Origin country for AWARE lookup. */
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
 * Expected CSV: Name, Type, Reference_ID, Description,
 *               Water_Consumption_m3_per_kg, Notes
 *
 * @param[out] db        Pointer to MaterialDatabase to populate.
 * @param[in]  filepath  Path to CSV file.
 * @return Number of materials loaded, or -1 on failure.
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
 * @brief Get water consumption factor for material at given index.
 *
 * @param[in] db     Pointer to MaterialDatabase.
 * @param[in] index  Index of material.
 * @return Water consumption in m3/kg, or -1.0 if invalid.
 */
double raw_materials_get_water_consumption(
    const MaterialDatabase *db, size_t index);

/**
 * @brief Initialize empty product material list.
 *
 * @param[out] list  Pointer to ProductMaterialList.
 * @return 0 on success, -1 on failure.
 */
int raw_materials_init_product_list(ProductMaterialList *list);

/**
 * @brief Add material to product material list with origin.
 *
 * @param[in,out] list            Pointer to ProductMaterialList.
 * @param[in]     material_index  Index in MaterialDatabase.
 * @param[in]     weight_kg       Mass of material (kg).
 * @param[in]     origin_country  Origin country name (can be NULL for GLO).
 * @return 0 on success, -1 on failure.
 */
int raw_materials_add_to_product(ProductMaterialList *list,
                                 size_t material_index,
                                 double weight_kg,
                                 const char *origin_country);

/**
 * @brief Calculate total raw material water footprint.
 *
 * Implements: WF_raw = sum(w_i * WU_i * AWARE_CF_i)
 *
 * @param[in] db       Pointer to MaterialDatabase with water factors.
 * @param[in] list     Pointer to ProductMaterialList with composition.
 * @param[in] aware_db Pointer to AwareDatabase for country factors.
 * @return Total footprint in m3 world-eq, or -1.0 on error.
 */
double raw_materials_calculate_footprint(
    const MaterialDatabase *db,
    const ProductMaterialList *list,
    const AwareDatabase *aware_db);

/**
 * @brief Release resources held by database.
 *
 * @param[in,out] db  Pointer to MaterialDatabase to free.
 */
void raw_materials_free_database(MaterialDatabase *db);

#endif  /* RAW_MATERIALS_H_ */
