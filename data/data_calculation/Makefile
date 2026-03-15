# Carbon Footprint Calculator - Build System
#
# Usage:
#   make        - Build all modules
#   make clean  - Remove build artifacts
#   make test   - Build and run tests
#   make docs   - Generate Doxygen documentation

CC = gcc
CFLAGS = -Wall -Wextra -Werror -pedantic -std=c11 -O2
CFLAGS_DEBUG = -Wall -Wextra -pedantic -std=c11 -g -DDEBUG

# Directories
SRC_DIR = src
INC_DIR = include
BUILD_DIR = build/obj
BIN_DIR = bin

# Include paths
INCLUDES = -I$(INC_DIR)

# Source files by module
UTILS_SRC = $(SRC_DIR)/utils/csv_parser.c
RAW_MATERIALS_SRC = $(SRC_DIR)/raw_materials/raw_materials.c
TRANSPORT_SRC = $(SRC_DIR)/transport/transport.c \
                $(SRC_DIR)/transport/emission_factors.c \
                $(SRC_DIR)/transport/mode_probability.c
PROCESSING_SRC = $(SRC_DIR)/processing/processing_database.c \
                 $(SRC_DIR)/processing/processing_lookup.c \
                 $(SRC_DIR)/processing/processing_calc.c
PACKAGING_SRC = $(SRC_DIR)/packaging/packaging.c
ADJUSTMENTS_SRC = $(SRC_DIR)/adjustments/adjustments.c

# Test source files
TEST_SRC = $(SRC_DIR)/tests/test_runner.c \
           $(SRC_DIR)/tests/test_raw_materials.c \
           $(SRC_DIR)/tests/test_transport.c \
           $(SRC_DIR)/tests/test_processing.c \
           $(SRC_DIR)/tests/test_packaging.c \
           $(SRC_DIR)/tests/test_adjustments.c

# Object files
UTILS_OBJ = $(BUILD_DIR)/csv_parser.o
RAW_MATERIALS_OBJ = $(BUILD_DIR)/raw_materials.o
TRANSPORT_OBJ = $(BUILD_DIR)/transport.o \
                $(BUILD_DIR)/emission_factors.o \
                $(BUILD_DIR)/mode_probability.o
PROCESSING_OBJ = $(BUILD_DIR)/processing_database.o \
                 $(BUILD_DIR)/processing_lookup.o \
                 $(BUILD_DIR)/processing_calc.o
PACKAGING_OBJ = $(BUILD_DIR)/packaging.o
ADJUSTMENTS_OBJ = $(BUILD_DIR)/adjustments.o

# Test object files
TEST_OBJ = $(BUILD_DIR)/test_runner.o \
           $(BUILD_DIR)/test_raw_materials.o \
           $(BUILD_DIR)/test_transport.o \
           $(BUILD_DIR)/test_processing.o \
           $(BUILD_DIR)/test_packaging.o \
           $(BUILD_DIR)/test_adjustments.o

# All module object files (excludes tests)
ALL_OBJ = $(UTILS_OBJ) $(RAW_MATERIALS_OBJ) $(TRANSPORT_OBJ) \
          $(PROCESSING_OBJ) $(PACKAGING_OBJ) $(ADJUSTMENTS_OBJ)

# Default target
all: dirs $(ALL_OBJ)
	@echo "Build complete."

# Create build directories
dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BIN_DIR)

# --- Utility module ---
$(BUILD_DIR)/csv_parser.o: $(SRC_DIR)/utils/csv_parser.c \
                           $(INC_DIR)/utils/csv_parser.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Raw materials module ---
$(BUILD_DIR)/raw_materials.o: $(SRC_DIR)/raw_materials/raw_materials.c \
                              $(INC_DIR)/raw_materials/raw_materials.h \
                              $(INC_DIR)/utils/csv_parser.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Transport module ---
$(BUILD_DIR)/transport.o: $(SRC_DIR)/transport/transport.c \
                          $(INC_DIR)/transport/transport.h \
                          $(INC_DIR)/transport/emission_factors.h \
                          $(INC_DIR)/transport/mode_probability.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/emission_factors.o: $(SRC_DIR)/transport/emission_factors.c \
                                 $(INC_DIR)/transport/emission_factors.h \
                                 $(INC_DIR)/transport/transport.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/mode_probability.o: $(SRC_DIR)/transport/mode_probability.c \
                                 $(INC_DIR)/transport/mode_probability.h \
                                 $(INC_DIR)/transport/transport.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Processing module ---
$(BUILD_DIR)/processing_database.o: $(SRC_DIR)/processing/processing_database.c \
                                    $(INC_DIR)/processing/material_processing.h \
                                    $(INC_DIR)/processing/processing_types.h \
                                    $(INC_DIR)/utils/csv_parser.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/processing_lookup.o: $(SRC_DIR)/processing/processing_lookup.c \
                                 $(INC_DIR)/processing/material_processing.h \
                                 $(INC_DIR)/processing/processing_types.h \
                                 $(INC_DIR)/utils/csv_parser.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/processing_calc.o: $(SRC_DIR)/processing/processing_calc.c \
                                $(INC_DIR)/processing/material_processing.h \
                                $(INC_DIR)/processing/processing_types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Packaging module ---
$(BUILD_DIR)/packaging.o: $(SRC_DIR)/packaging/packaging.c \
                          $(INC_DIR)/packaging/packaging.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Adjustments module ---
$(BUILD_DIR)/adjustments.o: $(SRC_DIR)/adjustments/adjustments.c \
                            $(INC_DIR)/adjustments/adjustments.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Test object files ---
$(BUILD_DIR)/test_runner.o: $(SRC_DIR)/tests/test_runner.c \
                            $(INC_DIR)/tests/test_runner.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_raw_materials.o: $(SRC_DIR)/tests/test_raw_materials.c \
                                   $(INC_DIR)/tests/test_runner.h \
                                   $(INC_DIR)/raw_materials/raw_materials.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_transport.o: $(SRC_DIR)/tests/test_transport.c \
                               $(INC_DIR)/tests/test_runner.h \
                               $(INC_DIR)/transport/transport.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_processing.o: $(SRC_DIR)/tests/test_processing.c \
                                $(INC_DIR)/tests/test_runner.h \
                                $(INC_DIR)/processing/material_processing.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_packaging.o: $(SRC_DIR)/tests/test_packaging.c \
                               $(INC_DIR)/tests/test_runner.h \
                               $(INC_DIR)/packaging/packaging.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_adjustments.o: $(SRC_DIR)/tests/test_adjustments.c \
                                 $(INC_DIR)/tests/test_runner.h \
                                 $(INC_DIR)/adjustments/adjustments.h
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) -c $< -o $@

# --- Test executable ---
test: dirs $(ALL_OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS_DEBUG) $(INCLUDES) $(ALL_OBJ) $(TEST_OBJ) \
	    $(SRC_DIR)/main.c -o $(BIN_DIR)/test_footprint -lm
	@echo "Running tests..."
	@$(BIN_DIR)/test_footprint

# --- Documentation ---
docs:
	@doxygen Doxyfile
	@echo "Documentation generated in docs/html/"

# --- Clean ---
clean:
	rm -rf $(BUILD_DIR)/*.o
	rm -rf $(BIN_DIR)/*

# --- Debug build ---
debug: CFLAGS = $(CFLAGS_DEBUG)
debug: all

.PHONY: all dirs clean test debug docs
