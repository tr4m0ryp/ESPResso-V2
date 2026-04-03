#
# Makefile for Layer 6: Carbon Footprint Calculation
#
# Builds the Layer 6 calculation executable that processes validated
# records from Layer 5 and calculates complete carbon footprints.
#

# Compiler settings
CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c99 -I. -I../../data_calculation/include
LDFLAGS = -lm

# Source files
SRCS = main.c layer6_calculation.c
OBJS = $(SRCS:.c=.o)

# Target executable
TARGET = bin/layer6_calculate

# Include existing calculation modules
CALC_SRCS = ../../data_calculation/src/raw_materials/raw_materials.c \
           ../../data_calculation/src/transport/transport.c \
           ../../data_calculation/src/transport/emission_factors.c \
           ../../data_calculation/src/transport/mode_probability.c \
           ../../data_calculation/src/processing/material_processing.c \
           ../../data_calculation/src/packaging/packaging.c \
           ../../data_calculation/src/adjustments/adjustments.c \
           ../../data_calculation/src/utils/csv_parser.c

CALC_OBJS = $(CALC_SRCS:.c=.o)

# Default target
all: $(TARGET)

# Create necessary directories
bin:
	@mkdir -p bin

# Build the target executable
$(TARGET): bin $(OBJS) $(CALC_OBJS)
	$(CC) $(OBJS) $(CALC_OBJS) -o $(TARGET) $(LDFLAGS)
	@echo "✅ Layer 6 executable built: $(TARGET)"

# Compile source files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Compile calculation module files (handle different directory structure)
../../data_calculation/src/%.o: ../../data_calculation/src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(OBJS) $(CALC_OBJS) $(TARGET)
	rm -rf bin/
	@echo "🧹 Cleaned build artifacts"

# Clean everything including calculation modules
clean-all: clean
	rm -f ../../data_calculation/src/**/*.o
	@echo "🧹 Cleaned all build artifacts"

# Install the executable
install: $(TARGET)
	@mkdir -p ../../bin
	cp $(TARGET) ../../bin/
	@echo "📦 Installed to ../../bin/"

# Test the executable
test: $(TARGET)
	@echo "🧪 Testing Layer 6 calculation..."
	$(TARGET) --help
	@echo "✅ Basic test completed"

# Run with sample data (requires existing Layer 5 output)
run: $(TARGET)
	@echo "🚀 Running Layer 6 calculation..."
	$(TARGET) --verbose
	@echo "✅ Calculation completed"

# Debug build
debug: CFLAGS += -g -DDEBUG
debug: $(TARGET)

# Static analysis
analyze:
	@echo "🔍 Running static analysis..."
	@command -v cppcheck >/dev/null 2>&1 && cppcheck --enable=all --suppress=missingIncludeSystem $(SRCS) || echo "cppcheck not found, skipping static analysis"
	@command -v clang-tidy >/dev/null 2>&1 && clang-tidy $(SRCS) -- $(CFLAGS) || echo "clang-tidy not found, skipping analysis"

# Show usage
help:
	@echo "Layer 6 Carbon Footprint Calculation - Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build the executable (default)"
	@echo "  clean        - Remove build artifacts"
	@echo "  clean-all    - Remove all build artifacts including dependencies"
	@echo "  test         - Run basic functionality test"
	@echo "  run          - Run calculation with verbose output"
	@echo "  debug        - Build with debug symbols"
	@echo "  analyze      - Run static analysis tools"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  make                    # Build executable"
	@echo "  make test              # Test the build"
	@echo "  make run               # Run calculation"
	@echo "  make clean             # Clean build files"

.PHONY: all clean clean-all install test run debug analyze help