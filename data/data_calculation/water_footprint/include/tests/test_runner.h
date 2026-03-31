/**
 * @file test_runner.h
 * @ingroup Tests
 * @brief Test suite declarations for water footprint calculation modules.
 *
 * Declares test functions for each calculation module and an
 * orchestrator that runs all tests sequentially.
 */

#ifndef TEST_RUNNER_H_
#define TEST_RUNNER_H_

/** @brief Path to raw material water consumption dataset. */
#define DATASET_PATH \
    "../datasets/final/base_materials_water.csv"

/** @brief Path to processing steps water dataset. */
#define PROCESSING_STEPS_PATH \
    "../datasets/final/processing_steps_water.csv"

/** @brief Path to material-process water combination dataset. */
#define PROCESSING_COMBOS_PATH \
    "../datasets/final/material_processing_water.csv"

/** @brief Path to AWARE agricultural factors CSV. */
#define AWARE_AGRI_PATH \
    "../datasets/final/aware_agri.csv"

/** @brief Path to AWARE non-agricultural factors CSV. */
#define AWARE_NONAGRI_PATH \
    "../datasets/final/aware_nonagri.csv"

/**
 * @brief Test AWARE module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_aware(void);

/**
 * @brief Test raw materials module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_raw_materials(void);

/**
 * @brief Test material processing module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_processing(void);

/**
 * @brief Test packaging module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_packaging(void);

/**
 * @brief Run all module tests sequentially.
 * @return EXIT_SUCCESS if all pass, EXIT_FAILURE on first failure.
 */
int run_all_tests(void);

#endif  /* TEST_RUNNER_H_ */
