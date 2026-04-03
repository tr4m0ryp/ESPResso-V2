/**
 * @file csv_parser.h
 * @ingroup Utils
 * @brief CSV file parsing utilities.
 *
 * Provides functions for parsing CSV files with support for
 * quoted fields containing commas, header row handling, and
 * field extraction by index.
 *
 * Designed for parsing EcoInvent-compliant datasets.
 */

#ifndef CSV_PARSER_H_
#define CSV_PARSER_H_

#include <stddef.h>
#include <stdio.h>

/** @brief Maximum length of a single CSV line. */
#define CSV_MAX_LINE_LEN 2048

/** @brief Maximum number of fields per CSV row. */
#define CSV_MAX_FIELDS 32

/** @brief Maximum length of a single field. */
#define CSV_MAX_FIELD_LEN 512

/**
 * @brief A parsed CSV field.
 */
typedef struct {
    char value[CSV_MAX_FIELD_LEN]; /**< Null-terminated field value. */
    size_t length;                 /**< Length excluding null terminator. */
} CsvField;

/**
 * @brief A parsed CSV row.
 */
typedef struct {
    CsvField fields[CSV_MAX_FIELDS]; /**< Parsed fields. */
    size_t field_count;              /**< Number of fields. */
} CsvRow;

/**
 * @brief State for CSV parsing operations.
 */
typedef struct {
    FILE *file;                        /**< Open file pointer. */
    char line_buffer[CSV_MAX_LINE_LEN]; /**< Line read buffer. */
    size_t current_line;               /**< Current line number (1-based). */
    int has_header;                    /**< 1 if header was skipped. */
} CsvParser;

/**
 * @brief Open CSV file for parsing.
 *
 * @param[out] parser       Pointer to CsvParser to initialize.
 * @param[in]  filepath     Path to CSV file.
 * @param[in]  skip_header  1 to skip first row, 0 otherwise.
 * @return 0 on success, -1 on failure.
 */
int csv_parser_open(CsvParser *parser, const char *filepath,
                    int skip_header);

/**
 * @brief Read and parse next row from CSV file.
 *
 * Handles quoted fields containing commas.
 *
 * @param[in]  parser  Pointer to initialized CsvParser.
 * @param[out] row     Pointer to CsvRow to populate.
 * @retval 1  Row read successfully.
 * @retval 0  End of file.
 * @retval -1 Error (malformed CSV or read error).
 */
int csv_parser_read_row(CsvParser *parser, CsvRow *row);

/**
 * @brief Get string value from parsed row.
 *
 * @param[in]  row        Pointer to parsed CsvRow.
 * @param[in]  index      Zero-based field index.
 * @param[out] dest       Destination buffer.
 * @param[in]  dest_size  Size of destination buffer.
 * @return 0 on success, -1 if index out of bounds.
 */
int csv_parser_get_field_str(const CsvRow *row, size_t index,
                             char *dest, size_t dest_size);

/**
 * @brief Get double value from parsed row.
 *
 * @param[in]  row    Pointer to parsed CsvRow.
 * @param[in]  index  Zero-based field index.
 * @param[out] value  Pointer to double for result.
 * @return 0 on success, -1 on failure.
 */
int csv_parser_get_field_double(const CsvRow *row,
                                size_t index, double *value);

/**
 * @brief Get integer value from parsed row.
 *
 * @param[in]  row    Pointer to parsed CsvRow.
 * @param[in]  index  Zero-based field index.
 * @param[out] value  Pointer to int for result.
 * @return 0 on success, -1 on failure.
 */
int csv_parser_get_field_int(const CsvRow *row,
                             size_t index, int *value);

/**
 * @brief Close CSV file and release resources.
 *
 * @param[in,out] parser  Pointer to CsvParser to close.
 */
void csv_parser_close(CsvParser *parser);

/**
 * @brief Remove leading and trailing whitespace in place.
 *
 * @param[in,out] str  String to trim.
 * @return Same pointer as input.
 */
char *csv_str_trim(char *str);

/**
 * @brief Convert string to lowercase in place.
 *
 * @param[in,out] str  String to convert.
 * @return Same pointer as input.
 */
char *csv_str_to_lower(char *str);

#endif  /* CSV_PARSER_H_ */
