"""Target transforms and train/val/test splitting for the carbon footprint model.

Log1pZScoreTransform: fit on training targets (4 heads), apply/inverse at
train/eval time. Supports state_dict for checkpointing.

create_splits: stratified 70/15/15 split by category + material-count bin,
with forced representation for ultra-rare categories.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset, Subset

logger = logging.getLogger(__name__)

HEAD_NAMES: List[str] = ["raw_materials", "transport", "processing", "packaging"]

# Ultra-rare categories that need forced val/test representation (<15 samples)
ULTRA_RARE_CATEGORIES = frozenset({
    "Mules",
    "Sunglasses",
    "Lace-up Shoes",
    "Blue-light Glasses",
    "Business Shoes",
})

# Material-count bin edges: 1-2, 3, 4, 5, 6+
_MAT_BIN_LABELS = {1: "1-2", 2: "1-2", 3: "3", 4: "4", 5: "5"}


def _material_bin(n: int) -> str:
    """Map a material count to its stratification bin label."""
    return _MAT_BIN_LABELS.get(n, "6+")


# ---------------------------------------------------------------------------
# Log1p + Z-score target transform
# ---------------------------------------------------------------------------

class Log1pZScoreTransform:
    """Log1p followed by z-score normalization per target head.

    4 targets: raw_materials, transport, processing, packaging.
    Transform: y_z = (log1p(y) - mu) / sigma
    Inverse:   y   = expm1(y_z * sigma + mu)

    Usage:
        transform = Log1pZScoreTransform()
        transform.fit(raw_materials, transport, processing, packaging)
        z = transform.transform(values, "transport")
        original = transform.inverse(z, "transport")
    """

    def __init__(self) -> None:
        self.means: Dict[str, float] = {}
        self.stds: Dict[str, float] = {}
        self._fitted = False

    def fit(
        self,
        raw_materials: np.ndarray,
        transport: np.ndarray,
        processing: np.ndarray,
        packaging: np.ndarray,
    ) -> "Log1pZScoreTransform":
        """Compute log1p mean/std from training targets for all 4 heads."""
        targets = [
            ("raw_materials", raw_materials),
            ("transport", transport),
            ("processing", processing),
            ("packaging", packaging),
        ]
        for name, values in targets:
            log_vals = np.log1p(values.astype(np.float64))
            self.means[name] = float(np.mean(log_vals))
            self.stds[name] = float(np.std(log_vals))
            if self.stds[name] < 1e-8:
                self.stds[name] = 1.0
                logger.warning("Std near zero for %s, defaulting to 1.0", name)
            logger.info(
                "Transform %s: mean=%.4f, std=%.4f",
                name, self.means[name], self.stds[name],
            )
        self._fitted = True
        return self

    def _check_fitted(self) -> None:
        if not self._fitted:
            raise RuntimeError(
                "Log1pZScoreTransform not fitted. Call fit() first."
            )

    def transform(self, values: np.ndarray, target: str) -> np.ndarray:
        """Apply log1p then z-score for the named target."""
        self._check_fitted()
        if target not in self.means:
            raise KeyError(f"Unknown target '{target}'. Expected one of {HEAD_NAMES}")
        return (np.log1p(values) - self.means[target]) / self.stds[target]

    def inverse(self, z_values: np.ndarray, target: str) -> np.ndarray:
        """Reverse z-score then expm1 for the named target."""
        self._check_fitted()
        if target not in self.means:
            raise KeyError(f"Unknown target '{target}'. Expected one of {HEAD_NAMES}")
        return np.expm1(z_values * self.stds[target] + self.means[target])

    def state_dict(self) -> Dict[str, Dict[str, float]]:
        """Serialize transform parameters for checkpointing."""
        return {"means": dict(self.means), "stds": dict(self.stds)}

    def load_state_dict(self, state: Dict[str, Dict[str, float]]) -> None:
        """Restore from a state dict."""
        self.means = state["means"]
        self.stds = state["stds"]
        self._fitted = True


# ---------------------------------------------------------------------------
# Train / val / test split
# ---------------------------------------------------------------------------

def _build_strat_keys(
    category_names: List[str],
    n_materials_list: List[int],
) -> np.ndarray:
    """Build composite stratification keys: '{category}_{mat_bin}'."""
    keys = []
    for cat, n_mat in zip(category_names, n_materials_list):
        keys.append(f"{cat}_{_material_bin(n_mat)}")
    return np.array(keys)


def _force_ultra_rare(
    train_idx: np.ndarray,
    val_idx: np.ndarray,
    test_idx: np.ndarray,
    category_names: List[str],
    rng: np.random.RandomState,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Ensure ultra-rare categories have >=1 sample in val and test."""
    cats = np.array(category_names)
    train_list = train_idx.tolist()
    val_list = val_idx.tolist()
    test_list = test_idx.tolist()

    for rare_cat in ULTRA_RARE_CATEGORIES:
        val_mask = cats[val_idx] == rare_cat
        test_mask = cats[test_idx] == rare_cat
        train_mask = cats[train_idx] == rare_cat

        needs_val = not np.any(val_mask)
        needs_test = not np.any(test_mask)

        if not needs_val and not needs_test:
            continue

        train_rare = train_idx[train_mask]
        if len(train_rare) == 0:
            logger.warning(
                "Ultra-rare category '%s' has no training samples to move",
                rare_cat,
            )
            continue

        rng.shuffle(train_rare)
        moved = 0
        if needs_val and moved < len(train_rare):
            idx_to_move = int(train_rare[moved])
            train_list.remove(idx_to_move)
            val_list.append(idx_to_move)
            moved += 1
            logger.info("Forced 1 sample of '%s' into val", rare_cat)

        if needs_test and moved < len(train_rare):
            idx_to_move = int(train_rare[moved])
            train_list.remove(idx_to_move)
            test_list.append(idx_to_move)
            moved += 1
            logger.info("Forced 1 sample of '%s' into test", rare_cat)

    return (
        np.array(train_list),
        np.array(val_list),
        np.array(test_list),
    )


def create_splits(
    dataset: Dataset,
    category_names: List[str],
    n_materials_list: List[int],
    seed: int = 42,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    save_path: Optional[str] = None,
) -> Tuple[Subset, Subset, Subset]:
    """Stratified 70/15/15 split by category + material-count bin.

    Stratification key: '{category}_{n_materials_bin}'
    Bins: 1-2, 3, 4, 5, 6+

    Ultra-rare categories (<15 samples) get forced min 1 in val and test.
    Optionally saves split indices to a JSON file for reproducibility.

    Returns (train_subset, val_subset, test_subset).
    """
    n = len(dataset)
    indices = np.arange(n)
    strat_keys = _build_strat_keys(category_names, n_materials_list)
    rng = np.random.RandomState(seed)

    val_test_ratio = val_ratio + test_ratio

    # First split: train vs (val+test)
    try:
        train_idx, valtest_idx = train_test_split(
            indices,
            test_size=val_test_ratio,
            random_state=seed,
            stratify=strat_keys,
        )
    except ValueError:
        logger.warning(
            "Stratified first split failed (rare strata); "
            "falling back to non-stratified split"
        )
        train_idx, valtest_idx = train_test_split(
            indices, test_size=val_test_ratio, random_state=seed,
        )

    # Second split: val vs test
    val_frac_of_remainder = val_ratio / val_test_ratio
    try:
        val_idx, test_idx = train_test_split(
            valtest_idx,
            test_size=(1.0 - val_frac_of_remainder),
            random_state=seed,
            stratify=strat_keys[valtest_idx],
        )
    except ValueError:
        logger.warning(
            "Stratified val/test split failed (rare strata); "
            "falling back to non-stratified split"
        )
        val_idx, test_idx = train_test_split(
            valtest_idx,
            test_size=(1.0 - val_frac_of_remainder),
            random_state=seed,
        )

    # Force ultra-rare categories into val and test
    train_idx, val_idx, test_idx = _force_ultra_rare(
        train_idx, val_idx, test_idx, category_names, rng,
    )

    logger.info(
        "Split sizes: train=%d, val=%d, test=%d",
        len(train_idx), len(val_idx), len(test_idx),
    )

    if save_path is not None:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump({
                "train": train_idx.tolist(),
                "val": val_idx.tolist(),
                "test": test_idx.tolist(),
                "seed": seed,
                "stratify": "category_n_materials_bin",
            }, f)
        logger.info("Saved split indices to %s", save_path)

    return (
        Subset(dataset, train_idx.tolist()),
        Subset(dataset, val_idx.tolist()),
        Subset(dataset, test_idx.tolist()),
    )
