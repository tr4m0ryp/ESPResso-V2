/**
 * @file aware.c
 * @ingroup AWARE
 * @brief AWARE characterization factor lookup implementation.
 *
 * Loads country-level AWARE factors from CSV and provides
 * lookup with alias resolution and global fallback.
 *
 * @see rules.md Section "Country Matching (D6)"
 */

#include "aware/aware.h"
#include "utils/csv_parser.h"
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <ctype.h>

/* CSV column indices for AWARE CSV */
#define COL_COUNTRY_NAME 0
#define COL_ECOINVENT_SHORT 1
#define COL_ISO3 2
#define COL_AWARE_CF 3

/**
 * Static alias table: known mismatches from D6 spec.
 * Format: { alias, canonical_name }
 */
static const char *BUILTIN_ALIASES[][2] = {
    {"Turkey",          "Turkiye"},
    {"USA",             "United States of America"},
    {"UK",              "United Kingdom"},
    {"England",         "United Kingdom"},
    {"Scotland",        "United Kingdom"},
    {"Czech Republic",  "Czechia"},
    {"Kitwe",           "Zambia"},
    {"Lusaka",          "Zambia"},
    {"Tashkent",        "Uzbekistan"},
    {"Fergana Valley",  "Uzbekistan"}
};

#define NUM_BUILTIN_ALIASES \
    (sizeof(BUILTIN_ALIASES) / sizeof(BUILTIN_ALIASES[0]))

int aware_init_database(AwareDatabase *db, double fallback)
{
    if (db == NULL) {
        return -1;
    }

    memset(db->entries, 0, sizeof(db->entries));
    db->count = 0;
    memset(db->aliases, 0, sizeof(db->aliases));
    db->alias_count = 0;
    db->global_fallback = fallback;

    return 0;
}

int aware_load_csv(AwareDatabase *db, const char *filepath)
{
    if (db == NULL || filepath == NULL) {
        return -1;
    }

    CsvParser parser;
    CsvRow row;

    if (csv_parser_open(&parser, filepath, 1) != 0) {
        return -1;
    }

    int loaded = 0;
    int result;

    while ((result = csv_parser_read_row(&parser, &row)) == 1) {
        if (db->count >= AWARE_MAX_ENTRIES) {
            break;
        }

        AwareEntry *entry = &db->entries[db->count];

        if (csv_parser_get_field_str(
                &row, COL_COUNTRY_NAME,
                entry->country_name,
                AWARE_MAX_COUNTRY_LEN) != 0) {
            continue;
        }

        if (csv_parser_get_field_str(
                &row, COL_ECOINVENT_SHORT,
                entry->ecoinvent_shortname,
                AWARE_MAX_SHORT_LEN) != 0) {
            entry->ecoinvent_shortname[0] = '\0';
        }

        if (csv_parser_get_field_str(
                &row, COL_ISO3,
                entry->iso3,
                AWARE_MAX_ISO3_LEN) != 0) {
            entry->iso3[0] = '\0';
        }

        if (csv_parser_get_field_double(
                &row, COL_AWARE_CF,
                &entry->aware_cf_annual) != 0) {
            entry->aware_cf_annual = 0.0;
        }

        db->count++;
        loaded++;
    }

    csv_parser_close(&parser);

    if (result == -1) {
        return -1;
    }

    return loaded;
}

int aware_load_aliases(AwareDatabase *db)
{
    if (db == NULL) {
        return -1;
    }

    for (size_t i = 0; i < NUM_BUILTIN_ALIASES; i++) {
        if (db->alias_count >= AWARE_MAX_ALIASES) {
            break;
        }

        AwareAlias *a = &db->aliases[db->alias_count];
        strncpy(a->alias,
                BUILTIN_ALIASES[i][0],
                AWARE_MAX_COUNTRY_LEN - 1);
        a->alias[AWARE_MAX_COUNTRY_LEN - 1] = '\0';

        strncpy(a->canonical,
                BUILTIN_ALIASES[i][1],
                AWARE_MAX_COUNTRY_LEN - 1);
        a->canonical[AWARE_MAX_COUNTRY_LEN - 1] = '\0';

        db->alias_count++;
    }

    return 0;
}

const char *aware_resolve_alias(const AwareDatabase *db,
                                const char *name)
{
    if (db == NULL || name == NULL) {
        return name;
    }

    for (size_t i = 0; i < db->alias_count; i++) {
        if (strcasecmp(db->aliases[i].alias, name) == 0) {
            return db->aliases[i].canonical;
        }
    }

    return name;
}

double aware_get_factor(const AwareDatabase *db,
                        const char *country_name)
{
    if (db == NULL || country_name == NULL) {
        return (db != NULL) ? db->global_fallback : 0.0;
    }

    /* Resolve alias first */
    const char *resolved = aware_resolve_alias(db, country_name);

    /* Case-insensitive lookup */
    for (size_t i = 0; i < db->count; i++) {
        if (strcasecmp(db->entries[i].country_name,
                       resolved) == 0) {
            return db->entries[i].aware_cf_annual;
        }
    }

    /* Fallback to GLO */
    return db->global_fallback;
}

char *aware_extract_country(const AwareDatabase *db,
                            const char *location_string)
{
    if (location_string == NULL) {
        return NULL;
    }

    /* Find last comma */
    const char *last_comma = strrchr(location_string, ',');
    const char *country_part;

    if (last_comma != NULL) {
        country_part = last_comma + 1;
    } else {
        /* No comma: entire string is the country */
        country_part = location_string;
    }

    /* Strip leading whitespace */
    while (*country_part && isspace((unsigned char)*country_part)) {
        country_part++;
    }

    /* Copy and strip trailing whitespace */
    size_t len = strlen(country_part);
    char *result = malloc(len + 1);
    if (result == NULL) {
        return NULL;
    }
    strncpy(result, country_part, len);
    result[len] = '\0';

    /* Trim trailing whitespace and newlines */
    char *end = result + strlen(result) - 1;
    while (end > result && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }

    /* Resolve through alias table */
    if (db != NULL) {
        const char *canonical = aware_resolve_alias(db, result);
        if (canonical != result) {
            size_t clen = strlen(canonical);
            char *resolved = malloc(clen + 1);
            if (resolved != NULL) {
                strncpy(resolved, canonical, clen);
                resolved[clen] = '\0';
                free(result);
                return resolved;
            }
        }
    }

    return result;
}

void aware_free_database(AwareDatabase *db)
{
    if (db != NULL) {
        db->count = 0;
        db->alias_count = 0;
    }
}
