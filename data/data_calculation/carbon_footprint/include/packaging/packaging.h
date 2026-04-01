/**
 * @file packaging.h
 * @ingroup Packaging
 * @brief Packaging carbon footprint calculation module.
 *
 * Implements the packaging phase of the cradle-to-gate carbon
 * footprint calculation as defined in ISO 14040/14044 and PEFCR
 * for Apparel and Footwear v3.1.
 *
 * Formula: CF_packaging = sum_i (m_i * EF_i)
 *
 * @see research_paper.tex Section 3.5
 */

#ifndef PACKAGING_H_
#define PACKAGING_H_

#include <stddef.h>

/** @brief Maximum number of packaging items per product. */
#define MAX_PACKAGING_ITEMS 10

/** @brief Maximum length for packaging material description. */
#define MAX_PACKAGING_DESC_LEN 128

/**
 * @brief Classification of packaging material categories.
 *
 * Emission factors (kg CO2e/kg) from research_paper.tex Table 3:
 * - Paper/Cardboard: 1.3
 * - Plastic: 3.5
 * - Glass: 1.1
 * - Other: 2.5
 */
typedef enum {
    PACKAGING_PAPER_CARDBOARD,  /**< Paper and cardboard. */
    PACKAGING_PLASTIC,          /**< Plastic materials. */
    PACKAGING_GLASS,            /**< Glass materials. */
    PACKAGING_OTHER,            /**< Other materials. */
    PACKAGING_CATEGORY_COUNT    /**< Sentinel count value. */
} PackagingCategory;

/**
 * @name Emission factors for packaging categories (kg CO2e/kg).
 * Source: research_paper.tex Table 3 and EcoInvent v3.12.
 * @{
 */
#define EF_PACKAGING_PAPER_CARDBOARD_kgCO2e_kg  1.3
#define EF_PACKAGING_PLASTIC_kgCO2e_kg          3.5
#define EF_PACKAGING_GLASS_kgCO2e_kg            1.1
#define EF_PACKAGING_OTHER_kgCO2e_kg            2.5
/** @} */

/**
 * @brief A single packaging component.
 */
typedef struct {
    PackagingCategory category;              /**< Material category. */
    double mass_kg;                          /**< Mass (kg). */
    char description[MAX_PACKAGING_DESC_LEN]; /**< Optional description. */
} PackagingItem;

/**
 * @brief List of packaging components for a product.
 */
typedef struct {
    PackagingItem items[MAX_PACKAGING_ITEMS]; /**< Packaging items. */
    size_t count;                             /**< Number of items. */
} PackagingList;

/**
 * @brief Packaging calculation results.
 */
typedef struct {
    double total_footprint_kg_CO2e;                       /**< Total footprint (kg CO2e). */
    double total_mass_kg;                                  /**< Total packaging mass (kg). */
    double category_contributions[PACKAGING_CATEGORY_COUNT]; /**< Footprint by category. */
    double category_masses[PACKAGING_CATEGORY_COUNT];      /**< Mass by category. */
} PackagingResult;

/**
 * @brief Initialize an empty packaging list.
 *
 * @param[out] list  Pointer to PackagingList.
 * @return 0 on success, -1 on failure.
 */
int packaging_init_list(PackagingList *list);

/**
 * @brief Add a packaging item to the list.
 *
 * @param[in,out] list         Pointer to PackagingList.
 * @param[in]     category     Packaging material category.
 * @param[in]     mass_kg      Mass of packaging (kg).
 * @param[in]     description  Optional description (can be NULL).
 * @return 0 on success, -1 on failure.
 */
int packaging_add_item(PackagingList *list,
                       PackagingCategory category,
                       double mass_kg,
                       const char *description);

/**
 * @brief Add packaging item using category name string.
 *
 * @param[in,out] list           Pointer to PackagingList.
 * @param[in]     category_name  Category name string.
 * @param[in]     mass_kg        Mass of packaging (kg).
 * @param[in]     description    Optional description (can be NULL).
 * @return 0 on success, -1 on failure.
 */
int packaging_add_item_by_name(PackagingList *list,
                               const char *category_name,
                               double mass_kg,
                               const char *description);

/**
 * @brief Get emission factor for packaging category.
 *
 * @param[in] category  PackagingCategory enum value.
 * @return Emission factor in kg CO2e/kg, or -1.0 if invalid.
 */
double packaging_get_emission_factor(PackagingCategory category);

/**
 * @brief Calculate total packaging carbon footprint.
 *
 * Implements: CF_packaging = sum_i (m_i * EF_i)
 *
 * @param[in]  list    Pointer to PackagingList.
 * @param[out] result  Pointer to PackagingResult.
 * @return 0 on success, -1 on error.
 */
int packaging_calculate_footprint(const PackagingList *list,
                                  PackagingResult *result);

/**
 * @brief Calculate footprint for a single packaging item.
 *
 * @param[in] category  Material category.
 * @param[in] mass_kg   Mass of packaging (kg).
 * @return Carbon footprint in kg CO2e, or -1.0 on error.
 */
double packaging_calculate_single(PackagingCategory category,
                                  double mass_kg);

/**
 * @brief Calculate footprint using category name string.
 *
 * @param[in] category_name  Category name string.
 * @param[in] mass_kg        Mass of packaging (kg).
 * @return Carbon footprint in kg CO2e, or -1.0 on error.
 */
double packaging_calculate_single_by_name(
    const char *category_name, double mass_kg);

/**
 * @brief Get string name for packaging category.
 *
 * @param[in] category  PackagingCategory enum value.
 * @return Static string with category name.
 */
const char *packaging_get_category_name(
    PackagingCategory category);

/**
 * @brief Parse packaging category from string.
 *
 * Case-insensitive; recognizes aliases like "cardboard", "carton".
 *
 * @param[in] category_str  String representation.
 * @return PackagingCategory value, or PACKAGING_OTHER if unrecognized.
 */
PackagingCategory packaging_category_from_string(
    const char *category_str);

/**
 * @brief Clear all items from packaging list.
 *
 * @param[in,out] list  Pointer to PackagingList.
 * @return 0 on success, -1 on failure.
 */
int packaging_clear_list(PackagingList *list);

/**
 * @brief Get total mass of all packaging items.
 *
 * @param[in] list  Pointer to PackagingList.
 * @return Total mass in kg, or -1.0 on error.
 */
double packaging_get_total_mass(const PackagingList *list);

#endif  /* PACKAGING_H_ */
