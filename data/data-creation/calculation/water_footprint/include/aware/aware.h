/**
 * @file aware.h
 * @ingroup AWARE
 * @brief AWARE characterization factor lookup module.
 *
 * Provides country-level AWARE factors for water scarcity
 * weighting. Supports agricultural (raw materials) and
 * non-agricultural (processing) factor types via separate
 * CSV databases.
 *
 * Data source: AWARE 2.0 Countries and Regions dataset.
 *
 * Country resolution (D6 logic):
 * 1. Split location on comma, take last part, strip whitespace.
 * 2. Check alias table for known mismatches.
 * 3. Lookup in AWARE database.
 * 4. Fallback to GLO if not found.
 */

#ifndef AWARE_H_
#define AWARE_H_

#include <stddef.h>

/** @brief Maximum length for country name string. */
#define AWARE_MAX_COUNTRY_LEN 256

/** @brief Maximum length for short country code. */
#define AWARE_MAX_SHORT_LEN 8

/** @brief Maximum length for ISO3 code. */
#define AWARE_MAX_ISO3_LEN 4

/** @brief Maximum number of AWARE entries. */
#define AWARE_MAX_ENTRIES 300

/** @brief Maximum number of country aliases. */
#define AWARE_MAX_ALIASES 20

/** @brief Global fallback AWARE CF for agricultural use. */
#define AWARE_GLOBAL_FALLBACK_AGRI 43.1

/** @brief Global fallback AWARE CF for non-agricultural use. */
#define AWARE_GLOBAL_FALLBACK_NONAGRI 17.9

/**
 * @brief A single AWARE database entry.
 */
typedef struct {
    char country_name[AWARE_MAX_COUNTRY_LEN];
    char ecoinvent_shortname[AWARE_MAX_SHORT_LEN];
    char iso3[AWARE_MAX_ISO3_LEN];
    double aware_cf_annual;
} AwareEntry;

/**
 * @brief A country name alias mapping.
 */
typedef struct {
    char alias[AWARE_MAX_COUNTRY_LEN];
    char canonical[AWARE_MAX_COUNTRY_LEN];
} AwareAlias;

/**
 * @brief AWARE factor database with alias resolution.
 */
typedef struct {
    AwareEntry entries[AWARE_MAX_ENTRIES];
    size_t count;
    AwareAlias aliases[AWARE_MAX_ALIASES];
    size_t alias_count;
    double global_fallback;
} AwareDatabase;

/**
 * @brief Initialize an empty AWARE database.
 *
 * @param[out] db        Pointer to AwareDatabase to initialize.
 * @param[in]  fallback  Global fallback CF value (agri or nonagri).
 * @return 0 on success, -1 on failure (null pointer).
 */
int aware_init_database(AwareDatabase *db, double fallback);

/**
 * @brief Load AWARE factors from CSV file.
 *
 * Expected CSV columns:
 *   country_name, ecoinvent_shortname, iso3, aware_cf_annual
 *
 * @param[out] db        Pointer to AwareDatabase to populate.
 * @param[in]  filepath  Path to CSV file.
 * @return Number of entries loaded, or -1 on failure.
 */
int aware_load_csv(AwareDatabase *db, const char *filepath);

/**
 * @brief Load built-in country aliases into database.
 *
 * Registers the 10 known mismatches from D6 spec:
 * Turkey->Turkiye, USA->United States of America, etc.
 *
 * @param[out] db  Pointer to AwareDatabase.
 * @return 0 on success, -1 on failure.
 */
int aware_load_aliases(AwareDatabase *db);

/**
 * @brief Get AWARE factor for a country name.
 *
 * Performs case-insensitive lookup. If not found, returns
 * the global fallback value.
 *
 * @param[in] db            Pointer to AwareDatabase.
 * @param[in] country_name  Country name to look up.
 * @return AWARE CF value, or global fallback if not found.
 */
double aware_get_factor(const AwareDatabase *db,
                        const char *country_name);

/**
 * @brief Resolve a country name through the alias table.
 *
 * @param[in] db    Pointer to AwareDatabase.
 * @param[in] name  Name to resolve.
 * @return Canonical name if alias found, otherwise input name.
 */
const char *aware_resolve_alias(const AwareDatabase *db,
                                const char *name);

/**
 * @brief Extract country from a location string.
 *
 * Implements D6 logic:
 * 1. Split on comma, take last part, strip whitespace.
 * 2. Resolve through alias table.
 *
 * Caller must free the returned string.
 *
 * @param[in] db               Pointer to AwareDatabase (for alias resolution).
 * @param[in] location_string  Location like "Istanbul, Turkey".
 * @return Heap-allocated resolved country name, or NULL on error.
 */
char *aware_extract_country(const AwareDatabase *db,
                            const char *location_string);

/**
 * @brief Release resources held by AWARE database.
 *
 * @param[in,out] db  Pointer to AwareDatabase to free.
 */
void aware_free_database(AwareDatabase *db);

#endif  /* AWARE_H_ */
