"""
Deduplication checker for Layer 5 statistical validation.

Provides hash-based exact duplicate detection and similarity scoring.
Hash is computed from upstream data only (product definition + transport),
excluding downstream generated data (packaging) since PEFCR baselines
produce intentionally similar packaging across records.
"""

import json
import hashlib
from typing import Any, Dict, Set

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import CompleteProductRecord


class DedupChecker:
    """Hash-based deduplication for product records."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self.record_hashes: Set[str] = set()
        self.exact_duplicates: int = 0
        self.near_duplicates: int = 0

    def check_duplicates(
        self, record: CompleteProductRecord
    ) -> Dict[str, Any]:
        """Check for exact and near duplicates."""
        record_hash = self._compute_record_hash(record)

        # Exact duplicate
        if record_hash in self.record_hashes:
            self.exact_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": 1.0,
                "duplicate_type": "exact",
            }

        # Near duplicate via similarity metric
        similarity = self._compute_similarity_score(record, record_hash)

        if similarity >= self.config.dedup_similarity_threshold:
            self.near_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": similarity,
                "duplicate_type": "near",
            }

        # New unique record
        self.record_hashes.add(record_hash)

        return {
            "is_duplicate": False,
            "similarity": similarity,
            "duplicate_type": None,
        }

    def _compute_record_hash(
        self, record: CompleteProductRecord
    ) -> str:
        """Create MD5 hash for deduplication detection.

        Hashes upstream data only: product definition + transport.
        Packaging is excluded because PEFCR baselines produce narrow
        per-piece ranges, making packaging intentionally similar.
        """
        key_parts = [
            record.subcategory_id,
            tuple(sorted(record.materials)),
            round(record.total_weight_kg, 1),
            tuple(sorted(record.preprocessing_steps)),
            round(record.total_transport_distance_km, -2),
        ]

        key_str = json.dumps(key_parts, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _compute_similarity_score(
        self, record: CompleteProductRecord, record_hash: str
    ) -> float:
        """Compute similarity score against existing records.

        Currently returns 0 for new records (hash-based dedup handles
        exact matches). Could be extended with Jaccard similarity on
        materials, weighted distance on numerics, etc.
        """
        return 0.0

    def reset(self) -> None:
        """Reset dedup tracking state."""
        self.record_hashes = set()
        self.exact_duplicates = 0
        self.near_duplicates = 0
