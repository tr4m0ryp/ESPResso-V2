"""
Validation logic for Layer 3 Transport Scenario Generator.

Validates generated transport scenarios for plausibility and consistency.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from data.data_generation.layer_3.io.layer2_reader import Layer2Record
from .generator import TransportScenario
from data.data_generation.layer_3.config.config import Layer3Config

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validation checks."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]


class TransportScenarioValidator:
    """Validates transport scenarios for plausibility and consistency."""

    def __init__(self, config: Layer3Config):
        self.config = config

    def validate_scenario(self, 
                         scenario: TransportScenario, 
                         base_record: Layer2Record) -> ValidationResult:
        """
        Comprehensive validation of a transport scenario.
        
        Args:
            scenario: Transport scenario to validate
            base_record: Original Layer 2 record for context
            
        Returns:
            ValidationResult with errors, warnings, and suggestions
        """
        errors = []
        warnings = []
        suggestions = []

        try:
            # 1. Distance validation
            distance_errors, distance_warnings = self._validate_distance(scenario)
            errors.extend(distance_errors)
            warnings.extend(distance_warnings)

            # 2. Supply chain type validation
            type_errors, type_warnings = self._validate_supply_chain_type(scenario)
            errors.extend(type_errors)
            warnings.extend(type_warnings)

            # 3. Origin region validation
            origin_errors, origin_warnings, origin_suggestions = self._validate_origin_region(scenario, base_record)
            errors.extend(origin_errors)
            warnings.extend(origin_warnings)
            suggestions.extend(origin_suggestions)

            # 4. Transport modes validation
            mode_errors, mode_warnings = self._validate_transport_modes(scenario)
            errors.extend(mode_errors)
            warnings.extend(mode_warnings)

            # 5. Cross-layer consistency validation
            consistency_errors, consistency_warnings = self._validate_consistency(scenario, base_record)
            errors.extend(consistency_errors)
            warnings.extend(consistency_warnings)

            # 6. Reasoning validation
            reasoning_warnings = self._validate_reasoning(scenario)
            warnings.extend(reasoning_warnings)

            is_valid = len(errors) == 0

            return ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                suggestions=suggestions
            )

        except Exception as e:
            logger.error(f"Validation failed for scenario {scenario.transport_scenario_id}: {e}")
            return ValidationResult(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                warnings=[],
                suggestions=[]
            )

    def _validate_distance(self, scenario: TransportScenario) -> Tuple[List[str], List[str]]:
        """Validate transport distance."""
        errors = []
        warnings = []
        
        distance = scenario.total_transport_distance_km
        
        # Basic range checks
        if distance <= 0:
            errors.append("Transport distance must be positive")
        elif distance < 10:  # Unrealistic minimum for textile supply chains
            errors.append(f"Transport distance {distance}km is unrealistically low for textile supply chains")
        elif distance > 25000:  # Maximum realistic for extreme cases
            errors.append(f"Transport distance {distance}km exceeds realistic maximum for textile supply chains")
        
        # Realistic range checks based on supply chain type
        supply_type = scenario.supply_chain_type
        if supply_type == "short_haul" and distance > 500:
            warnings.append(f"Short haul supply chain with distance {distance}km exceeds 500km threshold")
        elif supply_type == "medium_haul" and (distance < 500 or distance > 2000):
            warnings.append(f"Medium haul supply chain with distance {distance}km outside 500-2000km range")
        elif supply_type == "long_haul" and distance < 2000:
            warnings.append(f"Long haul supply chain with distance {distance}km below 2000km threshold")
        
        # Reasonableness checks for textile industry
        if supply_type == "short_haul" and distance > 200:
            warnings.append(f"Short haul distance {distance}km may be high for regional textile supply chains")
        elif supply_type == "long_haul" and distance > 15000:
            warnings.append(f"Long haul distance {distance}km is very high - verify if realistic")
        
        return errors, warnings

    def _validate_supply_chain_type(self, scenario: TransportScenario) -> Tuple[List[str], List[str]]:
        """Validate supply chain type classification."""
        errors = []
        warnings = []
        
        supply_type = scenario.supply_chain_type
        distance = scenario.total_transport_distance_km
        
        valid_types = ["short_haul", "medium_haul", "long_haul"]
        if supply_type not in valid_types:
            errors.append(f"Invalid supply chain type: {supply_type}")
            return errors, warnings
        
        # Check consistency with distance
        if supply_type == "short_haul" and distance > 600:
            warnings.append(f"Short haul classification for {distance}km distance (threshold: 500km)")
        elif supply_type == "medium_haul" and (distance < 400 or distance > 2200):
            warnings.append(f"Medium haul classification for {distance}km distance (range: 500-2000km)")
        elif supply_type == "long_haul" and distance < 1800:
            warnings.append(f"Long haul classification for {distance}km distance (threshold: 2000km)")
        
        return errors, warnings

    def _validate_origin_region(self, scenario: TransportScenario, base_record: Layer2Record) -> Tuple[List[str], List[str], List[str]]:
        """Validate origin region for plausibility."""
        errors = []
        warnings = []
        suggestions = []
        
        origin = scenario.origin_region
        
        # Check if it's a known manufacturing region
        known_origins = set()
        for origins in self.config.material_origins.values():
            known_origins.update(origins)
        for hubs in self.config.manufacturing_hubs.values():
            known_origins.update(hubs)
        
        if origin not in known_origins:
            warnings.append(f"Origin region '{origin}' not in known manufacturing regions database")
            
            # Suggest alternatives based on materials and processes
            suggested_origins = self._suggest_origin_regions(base_record)
            if suggested_origins:
                suggestions.append(f"Consider these alternative origins: {', '.join(suggested_origins[:3])}")
        
        # Check origin consistency with materials
        materials_text = " ".join(base_record.materials).lower()
        steps_text = " ".join(base_record.preprocessing_steps).lower()
        
        # Specific material-origin mismatches
        if "leather" in materials_text and origin not in ["Italy", "India", "Brazil", "China"]:
            warnings.append(f"Leather products typically manufactured in specialized regions, not '{origin}'")
        
        if "down" in materials_text and origin not in ["China", "Hungary", "Poland", "France", "Canada"]:
            warnings.append(f"Down products often manufactured near source regions, '{origin}' may be unusual")
        
        # Check if origin makes sense for the distance
        # This is a simplified check - in reality, we'd need more sophisticated geographic logic
        if origin in ["China", "India", "Bangladesh", "Vietnam"] and scenario.total_transport_distance_km < 1000:
            warnings.append(f"Short distance {scenario.total_transport_distance_km}km from Asian origin '{origin}' - verify if realistic")
        
        return errors, warnings, suggestions

    def _suggest_origin_regions(self, base_record: Layer2Record) -> List[str]:
        """Suggest plausible origin regions based on materials and processes."""
        suggestions = []
        
        materials_text = " ".join(base_record.materials).lower()
        steps_text = " ".join(base_record.preprocessing_steps).lower()
        
        # Leather processing
        if "leather" in materials_text or "tanning" in steps_text:
            suggestions.extend(["Italy", "India", "Brazil", "China"])
        
        # Synthetic fiber production
        if "extrusion" in steps_text or any(syn in materials_text for syn in ["polyester", "nylon", "acrylic"]):
            suggestions.extend(["China", "Taiwan", "South Korea", "Japan"])
        
        # Footwear assembly
        if "footwear" in base_record.category_name.lower():
            suggestions.extend(["Vietnam", "China", "Indonesia", "India"])
        
        # General textile manufacturing
        suggestions.extend(["China", "India", "Bangladesh", "Vietnam", "Pakistan", "Turkey"])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_suggestions = []
        for suggestion in suggestions:
            if suggestion not in seen:
                seen.add(suggestion)
                unique_suggestions.append(suggestion)
        
        return unique_suggestions[:5]  # Return top 5 suggestions

    def _validate_transport_modes(self, scenario: TransportScenario) -> Tuple[List[str], List[str]]:
        """Validate transport modes selection."""
        errors = []
        warnings = []
        
        modes = scenario.transport_modes
        distance = scenario.total_transport_distance_km
        supply_type = scenario.supply_chain_type
        
        # Check for valid modes
        valid_modes = ["road", "rail", "inland_waterway", "sea", "air"]
        for mode in modes:
            if mode not in valid_modes:
                errors.append(f"Invalid transport mode: {mode}")
        
        # Check mode consistency with distance/supply chain type
        if supply_type == "short_haul":
            # Short haul should primarily use road/rail
            if not any(mode in modes for mode in ["road", "rail"]):
                warnings.append(f"Short haul supply chain with no road/rail modes: {modes}")
            if "sea" in modes and distance < 200:
                warnings.append(f"Sea transport for short distance {distance}km may be unrealistic")
            
        elif supply_type == "long_haul":
            # Long haul should include sea or air
            if not any(mode in modes for mode in ["sea", "air"]):
                warnings.append(f"Long haul supply chain with no sea/air modes: {modes}")
            
            # Check for first/last mile modes
            if "sea" in modes and not any(mode in modes for mode in ["road", "rail"]):
                warnings.append(f"Sea transport without road/rail for first/last mile")
        
        # Air transport should be rare and expensive
        if "air" in modes:
            if distance > 5000:  # Very long distance air freight is extremely expensive
                warnings.append(f"Air transport for very long distance {distance}km - verify economic feasibility")
            if len(modes) == 1:  # Only air transport
                warnings.append("Air transport as sole mode - typically only for high-value/time-sensitive goods")
        
        return errors, warnings

    def _validate_consistency(self, scenario: TransportScenario, base_record: Layer2Record) -> Tuple[List[str], List[str]]:
        """Validate cross-layer consistency."""
        errors = []
        warnings = []
        
        # Check if transport modes are consistent with product weight
        total_weight = base_record.total_weight_kg
        
        if "air" in scenario.transport_modes and total_weight > 1.0:
            warnings.append(f"Air transport for relatively heavy product ({total_weight}kg) - may be uneconomical")
        
        # Check if distance is consistent with material types
        materials_text = " ".join(base_record.materials).lower()
        distance = scenario.total_transport_distance_km
        
        # Heavy/bulky materials typically don't travel very far by air
        if "air" in scenario.transport_modes:
            if any(material in materials_text for material in ["cotton", "wool", "rubber", "leather"]):
                if distance > 2000:
                    warnings.append(f"Air transport for bulky natural materials over long distance {distance}km")
        
        # Check origin consistency with preprocessing steps
        origin = scenario.origin_region
        steps_text = " ".join(base_record.preprocessing_steps).lower()
        
        # Specialized processes should be in appropriate regions
        if "tanning" in steps_text and origin not in ["Italy", "India", "Brazil", "China", "Pakistan"]:
            warnings.append(f"Leather tanning processes in non-traditional region '{origin}'")
        
        if "extrusion" in steps_text and origin not in ["China", "Taiwan", "South Korea", "Japan", "USA", "Germany"]:
            warnings.append(f"Synthetic fiber extrusion in non-major region '{origin}'")
        
        return errors, warnings

    def _validate_reasoning(self, scenario: TransportScenario) -> List[str]:
        """Validate the reasoning quality."""
        warnings = []
        
        reasoning = scenario.reasoning
        
        if not reasoning or reasoning.strip() == "":
            warnings.append("No reasoning provided for transport scenario")
        elif len(reasoning.strip()) < 20:
            warnings.append("Reasoning too brief to be meaningful")
        elif "Generated" in reasoning and "LLM" in reasoning:
            warnings.append("Generic reasoning - lacks specific supply chain logic")
        
        # Check if reasoning mentions relevant factors
        if reasoning:
            if not any(keyword in reasoning.lower() for keyword in 
                      ["material", "manufacturing", "transport", "supply", "origin", "distance"]):
                warnings.append("Reasoning lacks transport-relevant keywords")
        
        return warnings

    def validate_batch(self, scenarios: List[TransportScenario], base_records: List[Layer2Record]) -> List[ValidationResult]:
        """Validate a batch of scenarios."""
        results = []
        
        if len(scenarios) != len(base_records):
            logger.error("Mismatched scenario and record counts in batch validation")
            return []
        
        for scenario, record in zip(scenarios, base_records):
            result = self.validate_scenario(scenario, record)
            results.append(result)
        
        return results

    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Get summary statistics for validation results."""
        total = len(results)
        valid_count = sum(1 for r in results if r.is_valid)
        
        total_errors = sum(len(r.errors) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)
        
        # Most common errors and warnings
        error_counts = {}
        warning_counts = {}
        
        for result in results:
            for error in result.errors:
                error_counts[error] = error_counts.get(error, 0) + 1
            for warning in result.warnings:
                warning_counts[warning] = warning_counts.get(warning, 0) + 1
        
        return {
            "total_scenarios": total,
            "valid_scenarios": valid_count,
            "invalid_scenarios": total - valid_count,
            "validation_rate": valid_count / total if total > 0 else 0,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "most_common_errors": sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5],
            "most_common_warnings": sorted(warning_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        }