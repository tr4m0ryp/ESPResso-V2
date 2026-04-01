/**
 * @file csv_parser.c
 * @ingroup Utils
 * @brief CSV file parsing utilities implementation.
 */

#include "utils/csv_parser.h"
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

int csv_parser_open(CsvParser *parser, const char *filepath, int skip_header)
{
    if (parser == NULL || filepath == NULL) {
        return -1;
    }

    parser->file = fopen(filepath, "r");
    if (parser->file == NULL) {
        return -1;
    }

    parser->current_line = 0;
    parser->has_header = skip_header;

    /* Skip header row if requested */
    if (skip_header) {
        if (fgets(parser->line_buffer, CSV_MAX_LINE_LEN, parser->file) == NULL) {
            fclose(parser->file);
            parser->file = NULL;
            return -1;
        }
        parser->current_line = 1;
    }

    return 0;
}

/*
 * parse_csv_line - Internal function to parse a single CSV line into fields
 *
 * Handles quoted fields with embedded commas and escaped quotes.
 */
static int parse_csv_line(const char *line, CsvRow *row)
{
    size_t field_idx = 0;
    size_t char_idx = 0;
    int in_quotes = 0;
    size_t line_len = strlen(line);

    row->field_count = 0;

    while (char_idx <= line_len && field_idx < CSV_MAX_FIELDS) {
        CsvField *field = &row->fields[field_idx];
        size_t field_char_idx = 0;

        /* Skip leading whitespace before field */
        while (char_idx < line_len && isspace((unsigned char)line[char_idx]) &&
               line[char_idx] != ',' && line[char_idx] != '\n') {
            char_idx++;
        }

        /* Check if field starts with quote */
        if (char_idx < line_len && line[char_idx] == '"') {
            in_quotes = 1;
            char_idx++;
        }

        /* Parse field content */
        while (char_idx < line_len) {
            char c = line[char_idx];

            if (in_quotes) {
                if (c == '"') {
                    /* Check for escaped quote */
                    if (char_idx + 1 < line_len && line[char_idx + 1] == '"') {
                        if (field_char_idx < CSV_MAX_FIELD_LEN - 1) {
                            field->value[field_char_idx++] = '"';
                        }
                        char_idx += 2;
                        continue;
                    } else {
                        /* End of quoted field */
                        in_quotes = 0;
                        char_idx++;
                        /* Skip to comma or end */
                        while (char_idx < line_len &&
                               line[char_idx] != ',' &&
                               line[char_idx] != '\n' &&
                               line[char_idx] != '\r') {
                            char_idx++;
                        }
                        break;
                    }
                } else {
                    if (field_char_idx < CSV_MAX_FIELD_LEN - 1) {
                        field->value[field_char_idx++] = c;
                    }
                    char_idx++;
                }
            } else {
                if (c == ',' || c == '\n' || c == '\r' || c == '\0') {
                    break;
                }
                if (field_char_idx < CSV_MAX_FIELD_LEN - 1) {
                    field->value[field_char_idx++] = c;
                }
                char_idx++;
            }
        }

        /* Null-terminate field */
        field->value[field_char_idx] = '\0';
        field->length = field_char_idx;

        /* Trim trailing whitespace */
        csv_str_trim(field->value);
        field->length = strlen(field->value);

        field_idx++;
        row->field_count = field_idx;

        /* Skip delimiter */
        if (char_idx < line_len && line[char_idx] == ',') {
            char_idx++;
        } else {
            break;
        }
    }

    return 0;
}

int csv_parser_read_row(CsvParser *parser, CsvRow *row)
{
    if (parser == NULL || row == NULL || parser->file == NULL) {
        return -1;
    }

    /* Read next line */
    if (fgets(parser->line_buffer, CSV_MAX_LINE_LEN, parser->file) == NULL) {
        if (feof(parser->file)) {
            return 0;
        }
        return -1;
    }

    parser->current_line++;

    /* Skip empty lines */
    size_t len = strlen(parser->line_buffer);
    if (len == 0 || (len == 1 && parser->line_buffer[0] == '\n')) {
        return csv_parser_read_row(parser, row);
    }

    /* Parse the line */
    if (parse_csv_line(parser->line_buffer, row) != 0) {
        return -1;
    }

    return 1;
}

int csv_parser_get_field_str(const CsvRow *row, size_t index,
                              char *dest, size_t dest_size)
{
    if (row == NULL || dest == NULL || index >= row->field_count) {
        return -1;
    }

    const CsvField *field = &row->fields[index];
    if (field->length >= dest_size) {
        return -1;
    }

    strncpy(dest, field->value, dest_size - 1);
    dest[dest_size - 1] = '\0';

    return 0;
}

int csv_parser_get_field_double(const CsvRow *row, size_t index, double *value)
{
    if (row == NULL || value == NULL || index >= row->field_count) {
        return -1;
    }

    const CsvField *field = &row->fields[index];
    if (field->length == 0) {
        *value = 0.0;
        return 0;
    }

    char *endptr;
    *value = strtod(field->value, &endptr);

    /* Check if conversion was successful */
    if (endptr == field->value) {
        return -1;
    }

    return 0;
}

int csv_parser_get_field_int(const CsvRow *row, size_t index, int *value)
{
    if (row == NULL || value == NULL || index >= row->field_count) {
        return -1;
    }

    const CsvField *field = &row->fields[index];
    if (field->length == 0) {
        *value = 0;
        return 0;
    }

    char *endptr;
    long result = strtol(field->value, &endptr, 10);

    if (endptr == field->value) {
        return -1;
    }

    *value = (int)result;
    return 0;
}

void csv_parser_close(CsvParser *parser)
{
    if (parser != NULL && parser->file != NULL) {
        fclose(parser->file);
        parser->file = NULL;
    }
}

char *csv_str_trim(char *str)
{
    if (str == NULL) {
        return NULL;
    }

    /* Trim leading whitespace */
    char *start = str;
    while (*start && isspace((unsigned char)*start)) {
        start++;
    }

    /* If all whitespace */
    if (*start == '\0') {
        *str = '\0';
        return str;
    }

    /* Trim trailing whitespace */
    char *end = start + strlen(start) - 1;
    while (end > start && isspace((unsigned char)*end)) {
        end--;
    }
    *(end + 1) = '\0';

    /* Move string to beginning if needed */
    if (start != str) {
        memmove(str, start, end - start + 2);
    }

    return str;
}

char *csv_str_to_lower(char *str)
{
    if (str == NULL) {
        return NULL;
    }

    for (char *p = str; *p; p++) {
        *p = (char)tolower((unsigned char)*p);
    }

    return str;
}
