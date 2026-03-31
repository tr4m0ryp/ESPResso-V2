/*
 * layer7_parsing.c - Layer 7: JSON parsing, configuration, and logging
 *
 * Provides JSON array parsing for materials, weights, processing
 * steps, transport legs, and packaging fields, plus configuration
 * initialization, argument parsing, validation, and logging.
 */

#include "layer7_calculation.h"
#include <ctype.h>

/* -- Logging --------------------------------------------------------- */

void layer7_log_error(const char *func, const char *msg, int rec)
{
    if (rec >= 0)
        fprintf(stderr, "[ERROR][Record %d][%s] %s\n", rec, func, msg);
    else
        fprintf(stderr, "[ERROR][%s] %s\n", func, msg);
}

void layer7_log_warning(const char *func, const char *msg, int rec)
{
    if (rec >= 0)
        fprintf(stderr, "[WARNING][Record %d][%s] %s\n", rec, func, msg);
    else
        fprintf(stderr, "[WARNING][%s] %s\n", func, msg);
}

void layer7_print_progress(int current, int total, int step)
{
    static int last_pct = -1;
    int pct;
    if (total <= 0) return;
    pct = (current * 100) / total;
    if (pct != last_pct && pct % step == 0) {
        printf("Progress: %d%% (%d/%d records)\n", pct, current, total);
        fflush(stdout);
        last_pct = pct;
    }
}

/* -- Configuration --------------------------------------------------- */

void layer7_init_config(Layer7Config *config)
{
    memset(config, 0, sizeof(Layer7Config));
    snprintf(config->input_path, sizeof(config->input_path),
             "data/datasets/pre-model/generated/layer_5/layer_5_validated.csv");
    snprintf(config->output_path, sizeof(config->output_path),
             "data/datasets/pre-model/generated/layer_7/layer_7_water_footprint.csv");
    snprintf(config->summary_path, sizeof(config->summary_path),
             "data/datasets/pre-model/generated/layer_7/calculation_summary.json");
    snprintf(config->materials_path, sizeof(config->materials_path),
             "data/datasets/pre-model/final/base_materials_water.csv");
    snprintf(config->processing_steps_path, sizeof(config->processing_steps_path),
             "data/datasets/pre-model/final/processing_steps_water.csv");
    snprintf(config->processing_combinations_path,
             sizeof(config->processing_combinations_path),
             "data/datasets/pre-model/final/material_processing_water.csv");
    snprintf(config->aware_agri_path, sizeof(config->aware_agri_path),
             "data/datasets/pre-model/final/aware_factors_agri.csv");
    snprintf(config->aware_nonagri_path, sizeof(config->aware_nonagri_path),
             "data/datasets/pre-model/final/aware_factors_nonagri.csv");
    snprintf(config->calculation_version,
             sizeof(config->calculation_version), "v1.0");
    config->enable_validation = 1;
    config->enable_statistics = 1;
    config->verbose = 0;
}

int layer7_parse_args(int argc, char *argv[], Layer7Config *config)
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Layer 7: Water Footprint Calculation\n");
            printf("Usage: %s [options]\n\n", argv[0]);
            printf("Options:\n");
            printf("  --input <path>          Input CSV\n");
            printf("  --output <path>         Output CSV\n");
            printf("  --summary <path>        Summary JSON\n");
            printf("  --materials <path>      Materials water CSV\n");
            printf("  --steps <path>          Processing steps water CSV\n");
            printf("  --combinations <path>   Material-process water CSV\n");
            printf("  --aware-agri <path>     AWARE agri factors CSV\n");
            printf("  --aware-nonagri <path>  AWARE nonagri factors CSV\n");
            printf("  --no-validation         Skip validation checks\n");
            printf("  --no-stats              Skip statistics output\n");
            printf("  --verbose, -v           Enable verbose logging\n");
            printf("  --help, -h              Show this help\n");
            exit(0);
        } else if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            strncpy(config->input_path, argv[++i], L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            strncpy(config->output_path, argv[++i], L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--summary") == 0 && i + 1 < argc) {
            strncpy(config->summary_path, argv[++i], L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--materials") == 0 && i + 1 < argc) {
            strncpy(config->materials_path, argv[++i], L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--steps") == 0 && i + 1 < argc) {
            strncpy(config->processing_steps_path, argv[++i],
                    L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--combinations") == 0 && i + 1 < argc) {
            strncpy(config->processing_combinations_path, argv[++i],
                    L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--aware-agri") == 0 && i + 1 < argc) {
            strncpy(config->aware_agri_path, argv[++i],
                    L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--aware-nonagri") == 0 && i + 1 < argc) {
            strncpy(config->aware_nonagri_path, argv[++i],
                    L7_MAX_FIELD_LENGTH - 1);
        } else if (strcmp(argv[i], "--no-validation") == 0) {
            config->enable_validation = 0;
        } else if (strcmp(argv[i], "--no-stats") == 0) {
            config->enable_statistics = 0;
        } else if (strcmp(argv[i], "-v") == 0 ||
                   strcmp(argv[i], "--verbose") == 0) {
            config->verbose = 1;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return -1;
        }
    }
    return 0;
}

int layer7_validate_config(const Layer7Config *config)
{
    const char *paths[] = {
        config->input_path, config->output_path,
        config->materials_path, config->processing_steps_path,
        config->processing_combinations_path,
        config->aware_agri_path, config->aware_nonagri_path
    };
    const char *names[] = {
        "input", "output", "materials", "processing_steps",
        "processing_combinations", "aware_agri", "aware_nonagri"
    };
    for (int i = 0; i < 7; i++) {
        if (strlen(paths[i]) == 0) {
            fprintf(stderr, "ERROR: %s path is required\n", names[i]);
            return -1;
        }
    }
    return 0;
}

/* -- JSON double-array parser: "[1.23, 4.56, 7.89]" ----------------- */

int layer7_parse_double_array(const char *json, double *out, int max_values)
{
    int count = 0;
    const char *p = json;
    char buf[64];
    int bi = 0, in_q = 0;

    if (!json || !out || max_values <= 0) return 0;
    while (*p && (*p == '[' || isspace((unsigned char)*p))) p++;

    while (*p && count < max_values) {
        if (*p == '"') {
            in_q = !in_q;
        } else if ((*p == ',' || *p == ']') && !in_q) {
            buf[bi] = '\0';
            if (bi > 0) {
                char *end;
                double v = strtod(buf, &end);
                if (*end == '\0') out[count++] = v;
            }
            bi = 0;
            if (*p == ']') break;
        } else if (!isspace((unsigned char)*p) || in_q) {
            if (bi < (int)sizeof(buf) - 1) buf[bi++] = *p;
        }
        p++;
    }
    return count;
}

/* -- JSON string-array parser: '["foo","bar","baz"]' ----------------- */

int layer7_parse_string_array(const char *json,
                              char out[][L7_MAX_ITEM_LEN], int max_items)
{
    int count = 0;
    const char *p = json;
    char buf[L7_MAX_ITEM_LEN];
    int bi = 0, in_str = 0;

    if (!json || !out || max_items <= 0) return 0;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    while (*p && count < max_items) {
        if (*p == '"') {
            if (in_str) {
                buf[bi] = '\0';
                strncpy(out[count], buf, L7_MAX_ITEM_LEN - 1);
                out[count][L7_MAX_ITEM_LEN - 1] = '\0';
                count++;
                bi = 0; in_str = 0;
            } else {
                in_str = 1; bi = 0;
            }
        } else if (in_str) {
            if (bi < L7_MAX_ITEM_LEN - 1) buf[bi++] = *p;
        } else if (*p == ']') {
            break;
        }
        p++;
    }
    return count;
}

/* -- Extract country from transport_legs JSON at given leg index ------ */
/*                                                                       */
/* transport_legs_json is an array of objects with "from_location" fields */
/* like "Istanbul, Turkey". Returns the last comma-separated part.       */

int layer7_extract_transport_country(const char *json, int leg_index,
                                     char *country_buf, size_t buf_size)
{
    const char *p;
    int obj_depth = 0, current_obj = -1;
    int found_key = 0, in_val = 0, in_q = 0;
    char location[L7_MAX_ITEM_LEN];
    int loc_i = 0;

    if (!json || !country_buf || buf_size == 0) return -1;
    country_buf[0] = '\0';

    for (p = json; *p; p++) {
        if ((*p == '[' || *p == ']') && !in_q) continue;
        if (*p == '{' && !in_q) {
            obj_depth++;
            if (obj_depth == 1) {
                current_obj++;
                found_key = 0; in_val = 0;
            }
            continue;
        }
        if (*p == '}' && !in_q) { obj_depth--; continue; }
        if (obj_depth != 1 || current_obj != leg_index) continue;

        if (*p == '"') {
            in_q = !in_q;
            if (in_val && !in_q) {
                location[loc_i] = '\0';
                char *lc = strrchr(location, ',');
                const char *ctry = lc ? lc + 1 : location;
                while (*ctry && isspace((unsigned char)*ctry)) ctry++;
                size_t clen = strlen(ctry);
                while (clen > 0 && isspace((unsigned char)ctry[clen - 1]))
                    clen--;
                if (clen >= buf_size) clen = buf_size - 1;
                memcpy(country_buf, ctry, clen);
                country_buf[clen] = '\0';
                return 0;
            }
            continue;
        }
        if (in_q && !in_val) {
            if (strncmp(p, "from_location", 13) == 0) {
                found_key = 1;
                p += 12;
                continue;
            }
        }
        if (found_key && *p == ':') {
            in_val = 1; loc_i = 0; found_key = 0;
            continue;
        }
        if (in_val && in_q) {
            if (loc_i < L7_MAX_ITEM_LEN - 1) location[loc_i++] = *p;
        }
    }
    return -1;
}
