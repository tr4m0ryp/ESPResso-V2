"""Target transforms and train/val/test splitting for the water footprint model.

Log1pZScoreTransform: fit on training targets, apply/inverse at train/eval time.
create_splits: stratified 70/15/15 split by category, returns Subset objects.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset, Subset

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Log1p + Z-score target transform
# ---------------------------------------------------------------------------

class Log1pZScoreTransform:
    """Log1p followed by z-score normalization per target column.

    Usage:
        transform = Log1pZScoreTransform()
        transform.fit(train_raw, train_proc, train_pkg)
        z_raw = transform.transform_raw(raw_values)
        original = transform.inverse_raw(z_raw)
    """

    def __init__(self) -> None:
        self.means: Dict[str, float] = {}
        self.stds: Dict[str, float] = {}
        self._fitted = False

    def fit(self, wf_raw: np.ndarray, wf_processing: np.ndarray,
            wf_packaging: np.ndarray) -> "Log1pZScoreTransform":
        """Compute log1p mean/std from training targets."""
        for name, values in [("raw", wf_raw), ("processing", wf_processing),
                             ("packaging", wf_packaging)]:
            log_vals = np.log1p(values)
            self.means[name] = float(np.mean(log_vals))
            self.stds[name] = float(np.std(log_vals))
            if self.stds[name] < 1e-8:
                self.stds[name] = 1.0
                logger.warning("Std near zero for %s, defaulting to 1.0", name)
            logger.info("Transform %s: mean=%.4f, std=%.4f",
                        name, self.means[name], self.stds[name])
        self._fitted = True
        return self

    def _check_fitted(self) -> None:
        if not self._fitted:
            raise RuntimeError("Log1pZScoreTransform not fitted. Call fit() first.")

    def transform(self, values: np.ndarray, target: str) -> np.ndarray:
        """Apply log1p then z-score for the named target."""
        self._check_fitted()
        return (np.log1p(values) - self.means[target]) / self.stds[target]

    def inverse(self, z_values: np.ndarray, target: str) -> np.ndarray:
        """Reverse z-score then expm1 for the named target."""
        self._check_fitted()
        return np.expm1(z_values * self.stds[target] + self.means[target])

    def transform_raw(self, v: np.ndarray) -> np.ndarray:
        """Shorthand for transform(v, 'raw')."""
        return self.transform(v, "raw")

    def transform_processing(self, v: np.ndarray) -> np.ndarray:
        """Shorthand for transform(v, 'processing')."""
        return self.transform(v, "processing")

    def transform_packaging(self, v: np.ndarray) -> np.ndarray:
        """Shorthand for transform(v, 'packaging')."""
        return self.transform(v, "packaging")

    def inverse_raw(self, z: np.ndarray) -> np.ndarray:
        """Shorthand for inverse(z, 'raw')."""
        return self.inverse(z, "raw")

    def inverse_processing(self, z: np.ndarray) -> np.ndarray:
        """Shorthand for inverse(z, 'processing')."""
        return self.inverse(z, "processing")

    def inverse_packaging(self, z: np.ndarray) -> np.ndarray:
        """Shorthand for inverse(z, 'packaging')."""
        return self.inverse(z, "packaging")

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

def create_splits(
    dataset: Dataset,
    category_names: List[str],
    seed: int = 42,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    save_path: Optional[str] = None,
) -> Tuple[Subset, Subset, Subset]:
    """Stratified split by category_name into train/val/test Subsets.

    Returns (train_subset, val_subset, test_subset).
    Optionally saves split indices to a JSON file for reproducibility.
    """
    n = len(dataset)
    indices = np.arange(n)
    labels = np.array(category_names)

    val_test_ratio = val_ratio + test_ratio
    try:
        train_idx, valtest_idx = train_test_split(
            indices, test_size=val_test_ratio, random_state=seed,
            stratify=labels[indices],
        )
    except ValueError:
        logger.warning("Stratified first split failed (rare categories); "
                        "falling back to non-stratified split")
        train_idx, valtest_idx = train_test_split(
            indices, test_size=val_test_ratio, random_state=seed,
        )
    val_frac_of_remainder = val_ratio / val_test_ratio
    try:
        val_idx, test_idx = train_test_split(
            valtest_idx, test_size=(1.0 - val_frac_of_remainder),
            random_state=seed, stratify=labels[valtest_idx],
        )
    except ValueError:
        logger.warning("Stratified val/test split failed (rare categories); "
                        "falling back to non-stratified split")
        val_idx, test_idx = train_test_split(
            valtest_idx, test_size=(1.0 - val_frac_of_remainder),
            random_state=seed,
        )

    logger.info("Split sizes: train=%d, val=%d, test=%d",
                len(train_idx), len(val_idx), len(test_idx))

    if save_path is not None:
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump({
                "train": train_idx.tolist(),
                "val": val_idx.tolist(),
                "test": test_idx.tolist(),
                "seed": seed,
            }, f)
        logger.info("Saved split indices to %s", save_path)

    return (
        Subset(dataset, train_idx.tolist()),
        Subset(dataset, val_idx.tolist()),
        Subset(dataset, test_idx.tolist()),
    )
