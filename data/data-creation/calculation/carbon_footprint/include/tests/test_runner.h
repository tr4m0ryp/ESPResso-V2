/**
 * @file test_runner.h
 * @ingroup Tests
 * @brief Test suite declarations for carbon footprint calculation modules.
 *
 * Declares test functions for each calculation module and an
 * orchestrator that runs all tests sequentially.
 */

#ifndef TEST_RUNNER_H_
#define TEST_RUNNER_H_

/** @brief Path to raw material emission factor dataset. */
#define DATASET_PATH \
    "../datasets/final/Product_materials.csv"

/** @brief Path to processing steps overview dataset. */
#define PROCESSING_STEPS_PATH \
    "../datasets/final/processing_steps_overview.csv"

/** @brief Path to material-process combination dataset. */
#define PROCESSING_COMBOS_PATH \
    "../datasets/final/material_processing_emissions.csv"

/**
 * @brief Test raw materials module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_raw_materials(void);

/**
 * @brief Test transport module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_transport(void);

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
 * @brief Test additional adjustments module functionality.
 * @return 0 on success, -1 on failure.
 */
int test_adjustments(void);

/**
 * @brief Run all module tests sequentially.
 * @return EXIT_SUCCESS if all pass, EXIT_FAILURE on first failure.
 */
int run_all_tests(void);

#endif  /* TEST_RUNNER_H_ */
