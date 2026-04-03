"""Reporting helpers for the Layer 4 orchestrator."""

import logging

logger = logging.getLogger(__name__)


def log_batch_summary(summary: dict) -> None:
    """Log key metrics from the validator batch summary."""
    logger.info("=== Validation Batch Summary ===")
    logger.info(
        "Records: %d total, %d with warnings",
        summary.get("total_records", 0),
        summary.get("records_with_warnings", 0),
    )
    logger.info(
        "Duplicates: %d (%.2f%%)",
        summary.get("duplicate_count", 0),
        summary.get("duplicate_percentage", 0.0),
    )
    if summary.get("mean_packaging_ratio") is not None:
        logger.info(
            "Mean packaging ratio: %.6f",
            summary["mean_packaging_ratio"],
        )
    usage = summary.get("category_usage", {})
    if usage:
        for category, pct in usage.items():
            logger.info(
                "Category '%s' nonzero in %.2f%% of records",
                category,
                pct,
            )


def log_output_summary(summary: dict) -> None:
    """Log key metrics from the output file summary."""
    if not summary.get("exists", False):
        logger.warning("Output file does not exist; no output summary available")
        return
    logger.info("=== Output File Summary ===")
    logger.info("Total records written: %d", summary.get("total_records", 0))
    if "mean_total_weight_kg" in summary:
        logger.info(
            "Mean product weight: %.4f kg", summary["mean_total_weight_kg"]
        )
    if "mean_total_distance_km" in summary:
        logger.info(
            "Mean transport distance: %.1f km",
            summary["mean_total_distance_km"],
        )
    if "mean_packaging_mass_kg" in summary:
        logger.info(
            "Mean packaging mass: %.6f kg",
            summary["mean_packaging_mass_kg"],
        )
    if "mean_packaging_ratio" in summary:
        logger.info(
            "Mean packaging ratio: %.6f", summary["mean_packaging_ratio"]
        )
