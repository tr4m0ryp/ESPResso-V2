#
# Makefile for Layer 7: Water Footprint Calculation
#
# Builds the Layer 7 executable that calculates water footprints
# for validated records using AWARE-weighted water consumption.
#

CC = gcc
CFLAGS = -Wall -Wextra -Werror -pedantic -std=c99 -O2 -I. \
         -I../../data_calculation/water_footprint/include
LDFLAGS = -lm

# Layer 7 source files
SRCS = main.c layer7_calculation.c layer7_csv.c layer7_parsing.c

# Water footprint C modules (no transport, no adjustments)
CALC_SRCS = ../../data_calculation/water_footprint/src/raw_materials/raw_materials.c \
            ../../data_calculation/water_footprint/src/processing/processing_database.c \
            ../../data_calculation/water_footprint/src/processing/processing_lookup.c \
            ../../data_calculation/water_footprint/src/processing/processing_calc.c \
            ../../data_calculation/water_footprint/src/packaging/packaging.c \
            ../../data_calculation/water_footprint/src/aware/aware.c \
            ../../data_calculation/water_footprint/src/utils/csv_parser.c

TARGET = bin/layer7_calculate

OBJS = $(SRCS:.c=.o)
CALC_OBJS = $(CALC_SRCS:.c=.o)

all: $(TARGET)

bin:
	@mkdir -p bin

$(TARGET): bin $(OBJS) $(CALC_OBJS)
	$(CC) $(OBJS) $(CALC_OBJS) -o $(TARGET) $(LDFLAGS)
	@echo "Built: $(TARGET)"

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

../../data_calculation/water_footprint/src/%.o: ../../data_calculation/water_footprint/src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(CALC_OBJS) $(TARGET)
	rm -rf bin/

test: $(TARGET)
	$(TARGET) --help
	@echo "Basic test completed."

run: $(TARGET)
	$(TARGET) --verbose

debug: CFLAGS += -g -DDEBUG
debug: clean $(TARGET)

.PHONY: all clean test run debug
