"""
Incremental Output Writer for Layer 5: Controlling and Validation Layer

Handles writing validation results to CSV files incrementally during processing
for better visibility, crash protection, and memory efficiency.
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import CompleteValidationResult, ValidationSummary

logger = logging.getLogger(__name__)


def _safe_getattr(obj: Any, attr: str, default: Any = None) -> Any:
    """
    Safely get an attribute from an object with a default fallback.

    Args:
        obj: Object to get attribute from
        attr: Attribute name (supports nested attrs with dot notation)
        default: Default value if attribute doesn't exist

    Returns:
        Attribute value or default
    """
    if obj is None:
        return default
    try:
        for part in attr.split('.'):
            obj = getattr(obj, part, None)
            if obj is None:
                return default
        return obj
    except (AttributeError, TypeError):
        return default


class IncrementalValidationOutputWriter:
    """Handles incremental output writing for Layer 5 validation results."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self.write_counter = 0
        self.temp_dir = Path(config.output_dir) / "temp_files"
        self.start_time = time.time()
        self.last_write_time = self.start_time
        self.batch_files: Dict[str, List[str]] = {
            'accepted': [],
            'review': [],
            'rejected': []
        }
        
        # Ensure temp directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized incremental writer. Temp files will be written to: {self.temp_dir}")

    def _validate_result(self, result: CompleteValidationResult) -> Tuple[bool, List[str]]:
        """
        Validate that a CompleteValidationResult has all required attributes.

        Args:
            result: Validation result to check

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []

        if result is None:
            return False, ["Result is None"]

        # Check required top-level attributes
        required_attrs = [
            ('record_id', str),
            ('complete_record', object),
            ('passport', object),
            ('final_decision', str),
        ]

        for attr_name, expected_type in required_attrs:
            if not hasattr(result, attr_name):
                issues.append(f"Missing required attribute: {attr_name}")
            elif getattr(result, attr_name) is None and attr_name != 'metadata':
                issues.append(f"Required attribute is None: {attr_name}")

        # Check complete_record sub-attributes
        if hasattr(result, 'complete_record') and result.complete_record:
            record = result.complete_record
            record_attrs = [
                'category_name', 'subcategory_name', 'materials',
                'material_weights_kg', 'material_percentages',
                'preprocessing_steps', 'total_weight_kg',
                'total_transport_distance_km', 'supply_chain_type',
                'transport_items', 'packaging_items', 'packaging_categories',
                'total_packaging_mass_kg'
            ]
            for attr in record_attrs:
                if not hasattr(record, attr):
                    issues.append(f"complete_record missing: {attr}")

        # Check passport sub-attributes
        if hasattr(result, 'passport') and result.passport:
            if not hasattr(result.passport, 'is_valid'):
                issues.append("passport missing: is_valid")
            if not hasattr(result.passport, 'errors'):
                issues.append("passport missing: errors")
        elif not hasattr(result, 'passport') or result.passport is None:
            issues.append("passport verification result is missing")

        return len(issues) == 0, issues

    def write_batch(self, batch_results: List[CompleteValidationResult], 
                   batch_num: int) -> Dict[str, Optional[str]]:
        """
        Write a batch of validation results to temporary CSV files.
        
        Args:
            batch_results: List of validation results for this batch
            batch_num: Current batch number (for file naming)
            
        Returns:
            Dictionary with paths to written temp files
        """
        if not batch_results:
            logger.debug(f"Batch {batch_num}: No results to write")
            return {'accepted': None, 'review': None, 'rejected': None}
        
        # Separate results by decision type
        accepted = [r for r in batch_results if r.final_decision == "accept"]
        review_queue = [r for r in batch_results if r.final_decision == "review"]
        rejected = [r for r in batch_results if r.final_decision == "reject"]
        
        written_files = {}
        
        # Write each category to temp file
        if accepted:
            file_path = self._write_temp_file(accepted, 'accepted', batch_num)
            self.batch_files['accepted'].append(file_path)
            written_files['accepted'] = file_path
        else:
            written_files['accepted'] = None
            
        if review_queue:
            file_path = self._write_temp_file(review_queue, 'review', batch_num)
            self.batch_files['review'].append(file_path)
            written_files['review'] = file_path
        else:
            written_files['review'] = None
            
        if rejected:
            file_path = self._write_temp_file(rejected, 'rejected', batch_num)
            self.batch_files['rejected'].append(file_path)
            written_files['rejected'] = file_path
        else:
            written_files['rejected'] = None
        
        # Update counter and log progress
        self.write_counter += len(batch_results)
        self._log_progress_if_needed()
        
        return written_files
    
    def _write_temp_file(self, results: List[CompleteValidationResult], 
                        decision_type: str, batch_num: int) -> str:
        """
        Write results to a temporary CSV file.
        
        Args:
            results: List of validation results
            decision_type: 'accepted', 'review', or 'rejected'
            batch_num: Batch number for file naming
            
        Returns:
            Path to the written temp file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"layer_5_{decision_type}_batch_{batch_num:04d}_{timestamp}.csv"
        file_path = self.temp_dir / filename
        
        try:
            fieldnames = self._get_extended_fieldnames()
            if decision_type == 'rejected':
                fieldnames += ['rejection_reason', 'validation_errors']
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                records_written = 0
                records_skipped = 0

                for idx, result in enumerate(results):
                    try:
                        # Validate result before writing
                        is_valid, issues = self._validate_result(result)
                        if not is_valid:
                            logger.warning(
                                f"Skipping invalid result at index {idx}: {issues}"
                            )
                            records_skipped += 1
                            continue

                        row = self._create_extended_row(result)
                        if decision_type == 'rejected':
                            row['rejection_reason'] = '; '.join(
                                _safe_getattr(result, 'decision_factors', []) or []
                            )
                            row['validation_errors'] = json.dumps(
                                _safe_getattr(result, 'passport.errors', []) or []
                            )
                        writer.writerow(row)
                        records_written += 1

                    except Exception as row_error:
                        record_id = _safe_getattr(result, 'record_id', f'unknown_{idx}')
                        logger.error(
                            f"Failed to write record {record_id}: {row_error}"
                        )
                        records_skipped += 1
                        continue

            if records_skipped > 0:
                logger.warning(
                    f"Batch {batch_num}: Skipped {records_skipped}/{len(results)} "
                    f"records due to errors"
                )

            logger.debug(
                f"Wrote {records_written} {decision_type} records to {file_path}"
            )
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Failed to write temp file {file_path}: {e}")
            raise

    def merge_final_outputs(self, validation_summary: ValidationSummary) -> Dict[str, str]:
        """
        Merge all temporary CSV files into final output files.
        
        Args:
            validation_summary: Summary of validation results
            
        Returns:
            Dictionary with paths to final output files
        """
        logger.info("Merging temporary files into final outputs...")
        
        final_files = {}
        
        # Merge each decision type
        for decision_type in ['accepted', 'review', 'rejected']:
            if self.batch_files[decision_type]:
                final_path = self._merge_category_files(decision_type, validation_summary)
                final_files[decision_type] = final_path
            else:
                final_files[decision_type] = ""
        
        # Clean up temp directory
        self._cleanup_temp_files()
        
        return final_files
    
    def _merge_category_files(self, decision_type: str, 
                             validation_summary: ValidationSummary) -> str:
        """
        Merge all temp files for a decision category into final CSV.
        
        Args:
            decision_type: 'accepted', 'review', or 'rejected'
            validation_summary: Validation summary for metadata
            
        Returns:
            Path to final merged file
        """
        # Determine final output path
        if decision_type == 'accepted':
            final_path = self.config.accepted_output_path
        elif decision_type == 'review':
            final_path = self.config.review_queue_path
        else:
            final_path = self.config.rejected_output_path
        
        # Add timestamp to final filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_path = Path(str(final_path).replace('.csv', f'_{timestamp}.csv'))
        
        # Ensure output directory exists
        final_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get fieldnames (from first temp file)
        first_file = self.batch_files[decision_type][0]
        fieldnames = self._get_fieldnames_from_file(first_file)
        if decision_type == 'rejected':
            fieldnames += ['rejection_reason', 'validation_errors']
        
        try:
            with open(final_path, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                total_records = 0
                for temp_file in sorted(self.batch_files[decision_type]):
                    records_written = self._append_file_to_writer(temp_file, writer)
                    total_records += records_written
            
            logger.info(f"Merged {len(self.batch_files[decision_type])} temp files "
                       f"into {final_path} ({total_records} records)")
            return str(final_path)
            
        except Exception as e:
            logger.error(f"Failed to merge {decision_type} files: {e}")
            raise
    
    def _append_file_to_writer(self, input_file: str, writer: csv.DictWriter) -> int:
        """Append all rows from input file to writer. Returns record count."""
        record_count = 0
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                # Skip header rows from temp files (they all have headers)
                if set(row.keys()) == set(writer.fieldnames):
                    continue
                writer.writerow(row)
                record_count += 1
        return record_count
    
    def _get_fieldnames_from_file(self, file_path: str) -> List[str]:
        """Extract fieldnames from a CSV file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames or []
    
    def _log_progress_if_needed(self) -> None:
        """Log progress every 1,000 records."""
        if self.write_counter % 1000 == 0:
            elapsed = time.time() - self.last_write_time
            rec_per_min = 1000 / elapsed * 60 if elapsed > 0 else 0
            
            pct_complete = (self.write_counter / 102000) * 100 if hasattr(self, 'total_records') else 0
            
            logger.info(f"Processed {self.write_counter:,} records "
                       f"(@ {rec_per_min:.0f} rec/min, {pct_complete:.1f}% complete)")
            
            self.last_write_time = time.time()
    
    def _cleanup_temp_files(self) -> None:
        """Remove temporary files after successful merge."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temp directory: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp directory: {e}")

    # Existing methods from original writer (copied for compatibility)
    def _get_extended_fieldnames(self) -> List[str]:
        """Get extended fieldnames for CSV output."""
        return [
            'record_id', 'subcategory_name', 'category_name',
            'materials', 'material_weights_kg', 'material_percentages',
            'preprocessing_steps',
            'total_weight_kg', 'total_transport_distance_km', 'supply_chain_type',
            'transport_items', 'total_packaging_mass_kg',
            'packaging_items', 'packaging_categories',
            'passport_valid',
            'lifecycle_coherence_score', 'cross_layer_contradiction_score',
            'overall_coherence_score', 'coherence_recommendation',
            'is_duplicate', 'is_outlier',
            'reward_sampled', 'reward_score', 'dataset_quality_estimate',
            'final_decision', 'final_score', 'validation_timestamp'
        ]
    
    def _create_extended_row(self, result: CompleteValidationResult) -> Dict[str, Any]:
        """
        Create extended row for CSV output with defensive attribute access.

        Args:
            result: Validation result to convert to row

        Returns:
            Dictionary suitable for CSV writing
        """
        record = result.complete_record

        # Safe list conversion with float casting
        weights = _safe_getattr(record, 'material_weights_kg', []) or []
        percentages = _safe_getattr(record, 'material_percentages', []) or []

        try:
            weights_float = [float(w) for w in weights]
        except (TypeError, ValueError):
            weights_float = []
            logger.warning(f"Invalid material_weights_kg for {result.record_id}")

        try:
            percentages_float = [float(p) for p in percentages]
        except (TypeError, ValueError):
            percentages_float = []
            logger.warning(f"Invalid material_percentages for {result.record_id}")

        return {
            'record_id': _safe_getattr(result, 'record_id', ''),
            'subcategory_name': _safe_getattr(record, 'subcategory_name', ''),
            'category_name': _safe_getattr(record, 'category_name', ''),
            'materials': json.dumps(_safe_getattr(record, 'materials', []) or []),
            'material_weights_kg': json.dumps(weights_float),
            'material_percentages': json.dumps(percentages_float),
            'preprocessing_steps': json.dumps(
                _safe_getattr(record, 'preprocessing_steps', []) or []
            ),
            'total_weight_kg': float(
                _safe_getattr(record, 'total_weight_kg', 0.0) or 0.0
            ),
            'total_transport_distance_km': float(
                _safe_getattr(record, 'total_transport_distance_km', 0.0) or 0.0
            ),
            'supply_chain_type': _safe_getattr(record, 'supply_chain_type', '') or '',
            'transport_items': json.dumps(
                _safe_getattr(record, 'transport_items', []) or []
            ),
            'total_packaging_mass_kg': float(
                _safe_getattr(record, 'total_packaging_mass_kg', 0.0) or 0.0
            ),
            'packaging_items': json.dumps(
                _safe_getattr(record, 'packaging_items', []) or []
            ),
            'packaging_categories': json.dumps(
                _safe_getattr(record, 'packaging_categories', []) or []
            ),
            # Passport verification
            'passport_valid': _safe_getattr(
                result, 'passport.is_valid', False
            ),
            # Cross-layer coherence
            'lifecycle_coherence_score': float(
                _safe_getattr(result, 'coherence.lifecycle_coherence_score', 0.0) or 0.0
            ),
            'cross_layer_contradiction_score': float(
                _safe_getattr(result, 'coherence.cross_layer_contradiction_score', 0.0) or 0.0
            ),
            'overall_coherence_score': float(
                _safe_getattr(result, 'coherence.overall_coherence_score', 0.0) or 0.0
            ),
            'coherence_recommendation': _safe_getattr(
                result, 'coherence.recommendation', 'review'
            ) or 'review',
            # Statistical quality
            'is_duplicate': _safe_getattr(
                result, 'statistical.is_duplicate', False
            ),
            'is_outlier': _safe_getattr(
                result, 'statistical.is_outlier', False
            ),
            # Sampled reward scoring
            'reward_sampled': _safe_getattr(
                result, 'reward.was_sampled', False
            ),
            'reward_score': float(
                _safe_getattr(result, 'reward.reward_score', 0.0) or 0.0
            ),
            'dataset_quality_estimate': float(
                _safe_getattr(result, 'reward.dataset_estimated_quality', 0.0) or 0.0
            ),
            # Final decision
            'final_decision': _safe_getattr(result, 'final_decision', 'review'),
            'final_score': float(
                _safe_getattr(result, 'final_score', 0.0) or 0.0
            ),
            'validation_timestamp': _safe_getattr(
                result, 'metadata.validation_timestamp', datetime.now().isoformat()
            ) or datetime.now().isoformat()
        }
    
    def write_validation_summary(self, results: List[CompleteValidationResult], 
                                stats: Any) -> str:
        """Write validation summary report to JSON."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_path = Path(str(self.config.validation_report_path).replace('.json', f'_{timestamp}.json'))
        
        # Ensure directory exists
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        summary = {
            'validation_summary': {
                'total_records': len(results),
                'accepted': len([r for r in results if r.final_decision == 'accept']),
                'review': len([r for r in results if r.final_decision == 'review']),
                'rejected': len([r for r in results if r.final_decision == 'reject']),
                'processing_stats': {
                    'write_counter': self.write_counter,
                    'temp_files_created': len(self.batch_files['accepted']) + 
                                        len(self.batch_files['review']) + 
                                        len(self.batch_files['rejected'])
                }
            },
            'timestamp': timestamp
        }
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Written validation summary to {summary_path}")
        return str(summary_path)
