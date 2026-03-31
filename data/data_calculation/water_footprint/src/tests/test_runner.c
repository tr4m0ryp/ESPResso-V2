/**
 * @file test_runner.c
 * @ingroup Tests
 * @brief Orchestrates execution of all water footprint test suites.
 */

#include <stdio.h>
#include <stdlib.h>
#include "tests/test_runner.h"

int run_all_tests(void)
{
    printf("========================================\n");
    printf(" Water Footprint Calculator Test Suite\n");
    printf("========================================\n\n");

    if (test_aware() != 0) {
        return EXIT_FAILURE;
    }

    if (test_raw_materials() != 0) {
        return EXIT_FAILURE;
    }

    if (test_processing() != 0) {
        return EXIT_FAILURE;
    }

    if (test_packaging() != 0) {
        return EXIT_FAILURE;
    }

    printf("\n========================================\n");
    printf(" All tests passed.\n");
    printf("========================================\n");

    return EXIT_SUCCESS;
}
