"""Database loading helpers for Layer 7 calculator.

Extracts the CSV-loading logic from calculator.py to keep that
file under the 300-line limit.

Primary functions:
    load_materials_db -- Load material water consumption factors.
    load_processing_db -- Load step and combination water factors.
    load_packaging_db -- Load packaging water consumption factors.
    load_aware_databases -- Load AWARE agri and nonagri CFs.

Dependencies:
    pandas for CSV I/O.
    databases module for database wrapper classes.
    config module for AWARE fallback values.
"""

import logging
import pandas as pd

from data.data_generation.layer_7.config.config import (
    Layer7Config,
    AWARE_FALLBACK_AGRI,
    AWARE_FALLBACK_NONAGRI,
)
from data.data_generation.layer_7.core.databases import (
    WaterMaterialDatabase,
    WaterProcessingDatabase,
    WaterPackagingDatabase,
    AWAREDatabase,
)

logger = logging.getLogger(__name__)


def load_materials_db(
    config: Layer7Config,
) -> WaterMaterialDatabase:
    """Load materials water consumption database from CSV.

    Args:
        config: Layer 7 configuration with file paths.

    Returns:
        Populated WaterMaterialDatabase.
    """
    db = WaterMaterialDatabase()
    df = pd.read_csv(config.materials_water_path)
    for _, row in df.iterrows():
        name = str(row.get('material_name', ''))
        try:
            wu = float(row.get('water_consumption_m3_per_kg', 0))
            db.materials[name] = wu
        except (ValueError, TypeError):
            continue
    logger.info("Loaded %d materials (water)", len(db.materials))
    return db


def load_processing_db(
    config: Layer7Config,
) -> WaterProcessingDatabase:
    """Load processing step and combination water databases.

    Args:
        config: Layer 7 configuration with file paths.

    Returns:
        Populated WaterProcessingDatabase.
    """
    db = WaterProcessingDatabase()

    # Step-level factors
    df = pd.read_csv(config.processing_water_path)
    for _, row in df.iterrows():
        name = str(row.get('process_name', ''))
        try:
            wu = float(row.get('water_consumption_m3_per_kg', 0))
            db.steps[name] = wu
        except (ValueError, TypeError):
            continue
    logger.info("Loaded %d processing steps (water)", len(db.steps))

    # Material-process combinations
    try:
        combo_df = pd.read_csv(config.material_processing_water_path)
        for _, row in combo_df.iterrows():
            mat = str(row.get('material_name', ''))
            proc = str(row.get('process_name', ''))
            try:
                wu = float(row.get('water_consumption_m3_per_kg', 0))
                db.combinations[(mat, proc)] = wu
            except (ValueError, TypeError):
                continue
        logger.info(
            "Loaded %d material-process combos (water)",
            len(db.combinations)
        )
    except FileNotFoundError:
        logger.warning(
            "Material-process water file not found, "
            "using step-level factors only"
        )

    return db


def load_packaging_db(
    config: Layer7Config,
) -> WaterPackagingDatabase:
    """Load packaging water consumption database from CSV.

    Args:
        config: Layer 7 configuration with file paths.

    Returns:
        Populated WaterPackagingDatabase.
    """
    db = WaterPackagingDatabase()
    df = pd.read_csv(config.packaging_water_path)
    for _, row in df.iterrows():
        cat = str(row.get('category', ''))
        try:
            wu = float(row.get('water_consumption_m3_per_kg', 0))
            db.categories[cat] = wu
        except (ValueError, TypeError):
            continue
    logger.info(
        "Loaded %d packaging categories (water)",
        len(db.categories)
    )
    return db


def load_aware_databases(
    config: Layer7Config,
) -> tuple:
    """Load AWARE agri and nonagri characterization factor databases.

    Args:
        config: Layer 7 configuration with file paths.

    Returns:
        Tuple of (aware_agri_db, aware_nonagri_db).
    """
    aware_agri = AWAREDatabase(fallback=AWARE_FALLBACK_AGRI)
    df = pd.read_csv(config.aware_agri_path)
    for _, row in df.iterrows():
        name = str(row.get('country_name', ''))
        try:
            cf = float(row.get('aware_cf_annual', 0))
            aware_agri.factors[name] = cf
        except (ValueError, TypeError):
            continue
    logger.info(
        "Loaded %d AWARE agri factors", len(aware_agri.factors)
    )

    aware_nonagri = AWAREDatabase(fallback=AWARE_FALLBACK_NONAGRI)
    df = pd.read_csv(config.aware_nonagri_path)
    for _, row in df.iterrows():
        name = str(row.get('country_name', ''))
        try:
            cf = float(row.get('aware_cf_annual', 0))
            aware_nonagri.factors[name] = cf
        except (ValueError, TypeError):
            continue
    logger.info(
        "Loaded %d AWARE nonagri factors",
        len(aware_nonagri.factors)
    )

    return aware_agri, aware_nonagri
