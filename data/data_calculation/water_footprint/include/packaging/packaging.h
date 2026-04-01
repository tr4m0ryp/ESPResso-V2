/**
 * @file packaging.h
 * @ingroup Packaging
 * @brief Packaging water footprint calculation module.
 *
 * Implements the packaging phase of the water footprint
 * calculation. No AWARE weighting is applied to packaging.
 *
 * Formula: WF_packaging = sum_j (mass_j * WU_packaging_j)
 * Unit: m3
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
 * Water consumption factors (m3/kg) from EcoInvent v3.12:
 * - Paper/Cardboard: 0.0072
 * - Plastic: 0.0035
 * - Glass: 0.0018
 * - Other: 0.0050
 */
typedef enum {
    PACKAGING_PAPER_CARDBOARD,  /**< Paper and cardboard. */
    PACKAGING_PLASTIC,          /**< Plastic materials. */
    PACKAGING_GLASS,            /**< Glass materials. */
    PACKAGING_OTHER,            /**< Other materials. */
    PACKAGING_CATEGORY_COUNT    /**< Sentinel count value. */
} PackagingCategory;

/**
 * @name Water consumption factors for packaging categories (m3/kg).
 * Source: EcoInvent v3.12 net water consumption values.
 * @{
 */
#define WU_PACKAGING_PAPER_CARDBOARD_m3_kg  0.0072
#define WU_PACKAGING_PLASTIC_m3_kg          0.0035
#define WU_PACKAGING_GLASS_m3_kg            0.0018
#define WU_PACKAGING_OTHER_m3_kg            0.0050
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
    double total_footprint_m3;                            /**< Total footprint (m3). */
    double total_mass_kg;                                  /**< Total packaging mass (kg). */
    double category_contributions[PACKAGING_CATEGORY_COUNT]; /**< Footprint by category. */
    double category_masses[PACKAGING_CATEGORY_COUNT];      /**< Mass by category. */
} PackagingResult;

/**
 * @brief Initialize an empty packaging list.
 */
int packaging_init_list(PackagingList *list);

/**
 * @brief Add a packaging item to the list.
 */
int packaging_add_item(PackagingList *list,
                       PackagingCategory category,
                       double mass_kg,
                       const char *description);

/**
 * @brief Add packaging item using category name string.
 */
int packaging_add_item_by_name(PackagingList *list,
                               const char *category_name,
                               double mass_kg,
                               const char *description);

/**
 * @brief Get water consumption factor for packaging category.
 *
 * @return Water consumption in m3/kg, or -1.0 if invalid.
 */
double packaging_get_water_consumption(PackagingCategory category);

/**
 * @brief Calculate total packaging water footprint.
 *
 * Implements: WF_packaging = sum_j (mass_j * WU_j)
 * No AWARE weighting applied.
 */
int packaging_calculate_footprint(const PackagingList *list,
                                  PackagingResult *result);

/**
 * @brief Calculate footprint for a single packaging item.
 *
 * @return Water footprint in m3, or -1.0 on error.
 */
double packaging_calculate_single(PackagingCategory category,
                                  double mass_kg);

/**
 * @brief Calculate footprint using category name string.
 *
 * @return Water footprint in m3, or -1.0 on error.
 */
double packaging_calculate_single_by_name(
    const char *category_name, double mass_kg);

/**
 * @brief Get string name for packaging category.
 */
const char *packaging_get_category_name(
    PackagingCategory category);

/**
 * @brief Parse packaging category from string.
 */
PackagingCategory packaging_category_from_string(
    const char *category_str);

/**
 * @brief Clear all items from packaging list.
 */
int packaging_clear_list(PackagingList *list);

/**
 * @brief Get total mass of all packaging items.
 */
double packaging_get_total_mass(const PackagingList *list);

#endif  /* PACKAGING_H_ */
