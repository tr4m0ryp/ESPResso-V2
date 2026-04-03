/**
 * @file packaging.c
 * @ingroup Packaging
 * @brief Packaging carbon footprint calculation.
 *
 * @see research_paper.tex Section 3.5
 */

#include "packaging/packaging.h"
#include <string.h>
#include <strings.h>
#include <stddef.h>

/* Category name strings */
static const char *CATEGORY_NAMES[PACKAGING_CATEGORY_COUNT] = {
    "Paper/Cardboard",
    "Plastic",
    "Glass",
    "Other/Unspecified"
};

/* Emission factors array indexed by PackagingCategory */
static const double EMISSION_FACTORS[PACKAGING_CATEGORY_COUNT] = {
    EF_PACKAGING_PAPER_CARDBOARD_kgCO2e_kg,
    EF_PACKAGING_PLASTIC_kgCO2e_kg,
    EF_PACKAGING_GLASS_kgCO2e_kg,
    EF_PACKAGING_OTHER_kgCO2e_kg
};

int packaging_init_list(PackagingList *list)
{
    if (list == NULL) {
        return -1;
    }

    memset(list->items, 0, sizeof(list->items));
    list->count = 0;

    return 0;
}

int packaging_add_item(PackagingList *list, PackagingCategory category,
                       double mass_kg, const char *description)
{
    if (list == NULL || mass_kg < 0.0) {
        return -1;
    }

    if (category < 0 || category >= PACKAGING_CATEGORY_COUNT) {
        return -1;
    }

    if (list->count >= MAX_PACKAGING_ITEMS) {
        return -1;
    }

    PackagingItem *item = &list->items[list->count];
    item->category = category;
    item->mass_kg = mass_kg;

    if (description != NULL) {
        strncpy(item->description, description, MAX_PACKAGING_DESC_LEN - 1);
        item->description[MAX_PACKAGING_DESC_LEN - 1] = '\0';
    } else {
        item->description[0] = '\0';
    }

    list->count++;

    return 0;
}

int packaging_add_item_by_name(PackagingList *list, const char *category_name,
                                double mass_kg, const char *description)
{
    if (list == NULL || category_name == NULL) {
        return -1;
    }

    PackagingCategory category = packaging_category_from_string(category_name);
    return packaging_add_item(list, category, mass_kg, description);
}

double packaging_get_emission_factor(PackagingCategory category)
{
    if (category < 0 || category >= PACKAGING_CATEGORY_COUNT) {
        return -1.0;
    }

    return EMISSION_FACTORS[category];
}

int packaging_calculate_footprint(const PackagingList *list,
                                   PackagingResult *result)
{
    if (list == NULL || result == NULL) {
        return -1;
    }

    /* Initialize result */
    result->total_footprint_kg_CO2e = 0.0;
    result->total_mass_kg = 0.0;
    memset(result->category_contributions, 0, sizeof(result->category_contributions));
    memset(result->category_masses, 0, sizeof(result->category_masses));

    if (list->count == 0) {
        return 0;
    }

    /*
     * Implementation of formula from research_paper.tex Section 3.5:
     * CF_packaging = sum_i (m_i * EF_i)
     */
    for (size_t i = 0; i < list->count; i++) {
        const PackagingItem *item = &list->items[i];

        double ef = EMISSION_FACTORS[item->category];
        double contribution = item->mass_kg * ef;

        result->total_footprint_kg_CO2e += contribution;
        result->total_mass_kg += item->mass_kg;

        /* Track per-category contributions */
        result->category_contributions[item->category] += contribution;
        result->category_masses[item->category] += item->mass_kg;
    }

    return 0;
}

double packaging_calculate_single(PackagingCategory category, double mass_kg)
{
    if (category < 0 || category >= PACKAGING_CATEGORY_COUNT || mass_kg < 0.0) {
        return -1.0;
    }

    return mass_kg * EMISSION_FACTORS[category];
}

double packaging_calculate_single_by_name(const char *category_name,
                                           double mass_kg)
{
    if (category_name == NULL || mass_kg < 0.0) {
        return -1.0;
    }

    PackagingCategory category = packaging_category_from_string(category_name);
    return packaging_calculate_single(category, mass_kg);
}

const char *packaging_get_category_name(PackagingCategory category)
{
    if (category < 0 || category >= PACKAGING_CATEGORY_COUNT) {
        return "Unknown";
    }

    return CATEGORY_NAMES[category];
}

PackagingCategory packaging_category_from_string(const char *category_str)
{
    if (category_str == NULL) {
        return PACKAGING_OTHER;
    }

    /* Paper/Cardboard aliases */
    if (strcasecmp(category_str, "paper") == 0 ||
        strcasecmp(category_str, "cardboard") == 0 ||
        strcasecmp(category_str, "paper/cardboard") == 0 ||
        strcasecmp(category_str, "card") == 0 ||
        strcasecmp(category_str, "carton") == 0 ||
        strcasecmp(category_str, "paperboard") == 0 ||
        strcasecmp(category_str, "corrugated") == 0) {
        return PACKAGING_PAPER_CARDBOARD;
    }

    /* Plastic aliases */
    if (strcasecmp(category_str, "plastic") == 0 ||
        strcasecmp(category_str, "polyethylene") == 0 ||
        strcasecmp(category_str, "pe") == 0 ||
        strcasecmp(category_str, "polypropylene") == 0 ||
        strcasecmp(category_str, "pp") == 0 ||
        strcasecmp(category_str, "pet") == 0 ||
        strcasecmp(category_str, "polystyrene") == 0 ||
        strcasecmp(category_str, "ps") == 0 ||
        strcasecmp(category_str, "pvc") == 0 ||
        strcasecmp(category_str, "ldpe") == 0 ||
        strcasecmp(category_str, "hdpe") == 0 ||
        strcasecmp(category_str, "film") == 0 ||
        strcasecmp(category_str, "wrap") == 0 ||
        strcasecmp(category_str, "bag") == 0 ||
        strcasecmp(category_str, "polybag") == 0) {
        return PACKAGING_PLASTIC;
    }

    /* Glass aliases */
    if (strcasecmp(category_str, "glass") == 0 ||
        strcasecmp(category_str, "bottle") == 0 ||
        strcasecmp(category_str, "jar") == 0) {
        return PACKAGING_GLASS;
    }

    /* Other aliases */
    if (strcasecmp(category_str, "other") == 0 ||
        strcasecmp(category_str, "other/unspecified") == 0 ||
        strcasecmp(category_str, "unspecified") == 0 ||
        strcasecmp(category_str, "mixed") == 0 ||
        strcasecmp(category_str, "composite") == 0 ||
        strcasecmp(category_str, "metal") == 0 ||
        strcasecmp(category_str, "aluminium") == 0 ||
        strcasecmp(category_str, "aluminum") == 0 ||
        strcasecmp(category_str, "tin") == 0 ||
        strcasecmp(category_str, "wood") == 0 ||
        strcasecmp(category_str, "fabric") == 0 ||
        strcasecmp(category_str, "textile") == 0) {
        return PACKAGING_OTHER;
    }

    /* Default to Other if not recognized */
    return PACKAGING_OTHER;
}

int packaging_clear_list(PackagingList *list)
{
    if (list == NULL) {
        return -1;
    }

    memset(list->items, 0, sizeof(list->items));
    list->count = 0;

    return 0;
}

double packaging_get_total_mass(const PackagingList *list)
{
    if (list == NULL) {
        return -1.0;
    }

    double total = 0.0;
    for (size_t i = 0; i < list->count; i++) {
        total += list->items[i].mass_kg;
    }

    return total;
}
