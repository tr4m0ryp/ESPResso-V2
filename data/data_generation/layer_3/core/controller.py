"""
Extra Controlling Layer for Layer 3 Transport Scenario Generation.

Provides additional validation, consistency checking, and quality control
for generated transport scenarios before they are written to output.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from data.data_generation.layer_3.io.layer2_reader import Layer2Record
from .generator import TransportScenario

logger = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Quality metrics for a transport scenario."""
    scenario_id: str
    completeness_score: float  # 0-1
    consistency_score: float   # 0-1
    realism_score: float       # 0-1
    uniqueness_score: float    # 0-1
    overall_score: float       # 0-1
    issues: List[str]
    suggestions: List[str]


class Layer3Controller:
    """
    Extra controlling layer for transport scenario validation.
    Ensures consistency and quality before final output.
    """

    def __init__(self):
        self.validation_rules = self._initialize_validation_rules()
        self.seen_scenarios = set()  # Track scenario IDs for uniqueness
        
    def _initialize_validation_rules(self) -> Dict[str, callable]:
        """Initialize validation rules."""
        return {
            "required_fields": self._validate_required_fields,
            "scenario_id_format": self._validate_scenario_id_format,
            "distance_realism": self._validate_distance_realism,
            "transport_modes": self._validate_transport_modes,
            "reasoning_quality": self._validate_reasoning_quality,
            "strategy_consistency": self._validate_strategy_consistency,
            "geographic_plausibility": self._validate_geographic_plausibility,
        }
    
    def control_and_validate(self, 
                           record: Layer2Record, 
                           scenarios: List[TransportScenario]) -> Tuple[List[TransportScenario], List[QualityMetrics]]:
        """
        Apply extra controlling layer to validate and potentially fix scenarios.
        
        Args:
            record: Layer 2 input record
            scenarios: Generated transport scenarios
            
        Returns:
            Tuple of (validated_scenarios, quality_metrics)
        """
        logger.info(f"Applying controlling layer to {len(scenarios)} scenarios for {record.preprocessing_path_id}")
        
        validated_scenarios = []
        quality_metrics = []
        
        for scenario in scenarios:
            # Apply all validation rules
            metrics = self._validate_scenario(record, scenario)
            quality_metrics.append(metrics)
            
            # If scenario has major issues, attempt to fix them
            if metrics.issues:
                logger.warning(f"Scenario {scenario.transport_scenario_id} has issues: {metrics.issues}")
                fixed_scenario = self._attempt_fix_scenario(scenario, metrics)
                if fixed_scenario:
                    # Re-validate after fixing
                    metrics = self._validate_scenario(record, fixed_scenario)
                    if not metrics.issues or metrics.overall_score >= 0.7:
                        logger.info(f"Successfully fixed scenario {scenario.transport_scenario_id}")
                        validated_scenarios.append(fixed_scenario)
                        continue
            
            # If no major issues or fixing failed, include original if score is acceptable
            if metrics.overall_score >= 0.6:
                validated_scenarios.append(scenario)
            else:
                logger.error(f"Scenario {scenario.transport_scenario_id} failed quality control (score: {metrics.overall_score:.2f})")
        
        # Ensure we have enough valid scenarios
        if len(validated_scenarios) < 3:
            logger.warning(f"Only {len(validated_scenarios)} valid scenarios out of {len(scenarios)} - generating additional fallback scenarios")
            fallback_scenarios = self._generate_fallback_scenarios(record, count=5-len(validated_scenarios))
            validated_scenarios.extend(fallback_scenarios)
        
        logger.info(f"Controlling layer complete: {len(validated_scenarios)} valid scenarios out of {len(scenarios)} generated")
        return validated_scenarios, quality_metrics
    
    def _validate_scenario(self, record: Layer2Record, scenario: TransportScenario) -> QualityMetrics:
        """Run all validation rules on a scenario."""
        issues = []
        suggestions = []
        rule_scores = {}
        
        for rule_name, rule_func in self.validation_rules.items():
            is_valid, message = rule_func(record, scenario)
            if not is_valid:
                issues.append(f"{rule_name}: {message}")
                rule_scores[rule_name] = 0.0
            else:
                # Full score if passed, partial if with suggestions
                if "suggestion" in message.lower():
                    suggestions.append(f"{rule_name}: {message}")
                    rule_scores[rule_name] = 0.7
                else:
                    rule_scores[rule_name] = 1.0
        
        # Calculate scores
        completeness_score = rule_scores.get("required_fields", 0)
        consistency_score = (rule_scores.get("scenario_id_format", 0) + 
                           rule_scores.get("strategy_consistency", 0)) / 2
        realism_score = (rule_scores.get("distance_realism", 0) + 
                       rule_scores.get("transport_modes", 0) + 
                       rule_scores.get("geographic_plausibility", 0)) / 3
        uniqueness_score = rule_scores.get("scenario_id_format", 0)  # Unique ID check
        
        overall_score = (completeness_score * 0.2 + 
                        consistency_score * 0.2 + 
                        realism_score * 0.4 + 
                        uniqueness_score * 0.2)
        
        return QualityMetrics(
            scenario_id=scenario.transport_scenario_id,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            realism_score=realism_score,
            uniqueness_score=uniqueness_score,
            overall_score=overall_score,
            issues=issues,
            suggestions=suggestions
        )
    
    def _validate_required_fields(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate all required fields are present and not empty."""
        required_fields = [
            scenario.transport_scenario_id,
            scenario.total_transport_distance_km,
            scenario.supply_chain_type,
            scenario.origin_region,
            scenario.transport_modes,
            scenario.reasoning
        ]
        
        field_names = ["scenario_id", "distance", "supply_chain_type", "origin_region", "transport_modes", "reasoning"]
        
        for value, name in zip(required_fields, field_names):
            if not value:
                return False, f"Missing required field: {name}"
        
        return True, "All required fields present"
    
    def _validate_scenario_id_format(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate scenario ID format."""
        scenario_id = scenario.transport_scenario_id
        
        expected_suffixes = ["cost", "speed", "eco", "risk", "regional"]
        if not any(scenario_id.endswith(suffix) for suffix in expected_suffixes):
            return False, f"Scenario ID '{scenario_id}' doesn't end with valid strategy suffix"
        
        if scenario_id in self.seen_scenarios:
            return False, f"Duplicate scenario ID: {scenario_id}"
        
        self.seen_scenarios.add(scenario_id)
        return True, "Scenario ID format valid"
    
    def _validate_distance_realism(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate distance is realistic."""
        distance = scenario.total_transport_distance_km
        
        if distance < 100:
            return False, f"Distance {distance:.1f} km is too short (<100km)"
        
        if distance > 25000:
            return False, f"Distance {distance:.1f} km is unrealistic (>25000km)"
        
        # Validate against supply chain type
        expected_type = self._get_expected_supply_chain_type(distance)
        if scenario.supply_chain_type != expected_type:
            return False, f"Distance {distance:.0f} km doesn't match supply_chain_type '{scenario.supply_chain_type}', expected '{expected_type}'"
        
        return True, "Distance is realistic"
    
    def _validate_transport_modes(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate transport modes are realistic."""
        modes = scenario.transport_modes
        
        # Check mode count
        if len(modes) < 2:
            return False, f"Only {len(modes)} transport mode(s) specified, need at least 2"
        
        if len(modes) > 4:
            return False, f"Too many transport modes ({len(modes)}), maximum 4"
        
        # Check for valid mode names
        valid_modes = {"road", "rail", "sea", "air", "inland_waterway"}
        invalid_modes = [m for m in modes if m not in valid_modes]
        if invalid_modes:
            return False, f"Invalid transport mode(s): {invalid_modes}"
        
        # Check mode consistency with distance
        distance = scenario.total_transport_distance_km
        if distance > 2000 and "air" in modes and len(modes) > 2:
            # Air freight rarely combined with multiple other modes for long distances
            return False, "Air freight typically used with 1-2 other modes for long distances"
        
        # Strategy-specific validation
        strategy = self._extract_strategy(scenario.transport_scenario_id)
        expected_modes = self._get_expected_modes_for_strategy(strategy, distance)
        
        if not any(mode in modes for mode in expected_modes):
            return True, f"Suggestion: Consider including expected mode(s) {expected_modes} for {strategy} strategy"
        
        return True, "Transport modes are realistic"
    
    def _validate_reasoning_quality(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate reasoning quality."""
        reasoning = scenario.reasoning
        
        if not reasoning or len(reasoning) < 50:
            return False, "Reasoning is too short or missing (<50 characters)"
        
        if len(reasoning) < 100:
            return True, "Suggestion: Reasoning could be more detailed (currently <100 chars)"
        
        # Check if reasoning mentions key elements
        strategy = self._extract_strategy(scenario.transport_scenario_id)
        expected_elements = self._get_expected_reasoning_elements(strategy)
        
        reasoning_lower = reasoning.lower()
        element_count = sum(1 for element in expected_elements if element in reasoning_lower)
        
        if element_count < 2:
            return True, f"Suggestion: Reasoning could better explain strategy logic for {strategy}"
        
        return True, "Reasoning is detailed and strategy-appropriate"
    
    def _validate_strategy_consistency(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate strategy consistency across scenario attributes."""
        strategy = self._extract_strategy(scenario.transport_scenario_id)
        
        # Check if strategy is reflected in transport modes
        distance = scenario.total_transport_distance_km
        modes = scenario.transport_modes
        
        inconsistencies = []
        
        if strategy == "cost" and "air" in modes and distance > 5000:
            inconsistencies.append("Cost strategy uses expensive air freight for long distance")
        
        if strategy == "speed" and "sea" in modes and distance < 3000:
            inconsistencies.append("Speed strategy uses slow sea freight")
        
        if strategy == "eco" and "air" in modes:
            inconsistencies.append("Eco strategy uses high-emission air transport")
        
        if strategy == "regional" and distance > 8000:
            inconsistencies.append("Regional strategy has very long distance")
        
        if inconsistencies:
            return False, "Strategy inconsistency: " + "; ".join(inconsistencies)
        
        return True, "Strategy is consistently applied"
    
    def _validate_geographic_plausibility(self, record: Layer2Record, scenario: TransportScenario) -> Tuple[bool, str]:
        """Validate geographic plausibility."""
        origin = scenario.origin_region
        
        # Check if origin is a known region
        if len(origin) < 2 or not origin[0].isalpha():
            return False, f"Origin region name '{origin}' appears invalid"
        
        # For specific strategies, origin should match expectations
        strategy = self._extract_strategy(scenario.transport_scenario_id)
        
        if strategy == "regional":
            # Regional strategy should use nearshore origins
            if origin in ["China", "India", "Vietnam"] and "Europe" in " ".join(record.materials):
                return True, "Suggestion: Consider using nearer origin for regional strategy to Europe"
        
        if strategy == "cost":
            # Cost strategy should use low-cost manufacturing regions
            low_cost_regions = {"Bangladesh", "Vietnam", "Myanmar", "Pakistan", "India"}
            if origin not in low_cost_regions and any(lc in origin for lc in low_cost_regions):
                return True, "Suggestion: Cost strategy might benefit from lower-cost origin region"
        
        return True, "Geographic choices are plausible"
    
    def _attempt_fix_scenario(self, scenario: TransportScenario, metrics: QualityMetrics) -> Optional[TransportScenario]:
        """Attempt to fix issues in a scenario."""
        try:
            # Simple fixes based on specific issues
            if "transport_modes" in str(metrics.issues):
                # Fix mode count if too many/too few
                current_modes = scenario.transport_modes
                if len(current_modes) > 4:
                    # Remove least appropriate modes
                    strategy = self._extract_strategy(scenario.transport_scenario_id)
                    scenario.transport_modes = self._get_expected_modes_for_strategy(strategy, scenario.total_transport_distance_km)
                    scenario.reasoning += " [Modes adjusted for consistency]"
                elif len(current_modes) < 2:
                    # Add complementary mode
                    if "road" not in current_modes:
                        scenario.transport_modes.append("road")
                    elif "sea" in current_modes:
                        scenario.transport_modes.append("rail")
                    else:
                        scenario.transport_modes.append("rail")
                    scenario.reasoning += " [Modes complemented for realism]"
            
            if "distance" in str(metrics.issues):
                # Adjust distance if unrealistic
                current_distance = scenario.total_transport_distance_km
                if current_distance < 100:
                    scenario.total_transport_distance_km = 500.0
                    scenario.reasoning += " [Distance adjusted to minimum realistic value]"
                elif current_distance > 25000:
                    scenario.total_transport_distance_km = 15000.0
                    scenario.reasoning += " [Distance adjusted to maximum realistic value]"
            
            if "reasoning" in str(metrics.issues) and len(scenario.reasoning) < 50:
                # Expand reasoning
                strategy = self._extract_strategy(scenario.transport_scenario_id)
                expanded = self._generate_fallback_reasoning(strategy, scenario)
                scenario.reasoning = expanded
            
            return scenario
            
        except Exception as e:
            logger.warning(f"Failed to fix scenario {scenario.transport_scenario_id}: {e}")
            return None
    
    def _generate_fallback_scenarios(self, record: Layer2Record, count: int = 1) -> List[TransportScenario]:
        """Generate fallback scenarios when validation fails."""
        fallbacks = []
        
        strategies = ["cost", "speed", "eco", "risk", "regional"][:count]
        base_distance = 8000.0  # Default realistic distance
        
        for i, strategy in enumerate(strategies):
            # Generate scenario based on strategy
            if strategy == "regional":
                distance = 2500.0
                modes = ["road", "rail"]
                origin = "Turkey"
            elif strategy == "speed":
                distance = 8500.0
                modes = ["air", "road"]
                origin = "China"
            elif strategy == "eco":
                distance = 9000.0
                modes = ["rail", "sea"]
                origin = "Germany"
            elif strategy == "cost":
                distance = 10000.0
                modes = ["sea", "rail"]
                origin = "Bangladesh"
            else:  # risk
                distance = 8000.0
                modes = ["sea", "rail", "road"]
                origin = "India"
            
            scenario = TransportScenario(
                transport_scenario_id=f"{record.preprocessing_path_id}_ts-{strategy}_fallback",
                total_transport_distance_km=distance,
                supply_chain_type=self._get_expected_supply_chain_type(distance),
                origin_region=origin,
                transport_modes=modes,
                reasoning=f"Fallback {strategy} scenario generated by controlling layer. "
                         f"Uses {', '.join(modes)} from {origin} for consistent quality."
            )
            
            fallbacks.append(scenario)
        
        return fallbacks
    
    def _extract_strategy(self, scenario_id: str) -> str:
        """Extract strategy from scenario ID."""
        if "ts-cost" in scenario_id:
            return "cost"
        elif "ts-speed" in scenario_id:
            return "speed"
        elif "ts-eco" in scenario_id:
            return "eco"
        elif "ts-risk" in scenario_id:
            return "risk"
        elif "ts-regional" in scenario_id:
            return "regional"
        else:
            return "unknown"
    
    def _get_expected_supply_chain_type(self, distance: float) -> str:
        """Get expected supply chain type for a distance."""
        if distance <= 500:
            return "short_haul"
        elif distance <= 4000:
            return "medium_haul"
        else:
            return "long_haul"
    
    def _get_expected_modes_for_strategy(self, strategy: str, distance: float) -> List[str]:
        """Get expected transport modes for a strategy."""
        if strategy == "cost":
            return ["sea", "rail"]
        elif strategy == "speed":
            return ["air", "road"]
        elif strategy == "eco":
            return ["rail", "sea"]
        elif strategy == "regional":
            return ["road", "rail"]
        else:  # risk
            return ["sea", "rail", "road"]
    
    def _get_expected_reasoning_elements(self, strategy: str) -> List[str]:
        """Get expected elements that should appear in reasoning."""
        if strategy == "cost":
            return ["cost", "low", "cheap", "sea", "economical"]
        elif strategy == "speed":
            return ["fast", "quick", "air", "delivery", "time"]
        elif strategy == "eco":
            return ["green", "sustainable", "environment", "rail", "low emission"]
        elif strategy == "regional":
            return ["nearby", "short", "regional", "close", "local"]
        else:  # risk
            return ["diverse", "multiple", "flexible", "resilient", "spread"]
    
    def _generate_fallback_reasoning(self, strategy: str, scenario: TransportScenario) -> str:
        """Generate fallback reasoning text."""
        strategy_names = {
            "cost": "Cost-Optimized",
            "speed": "Speed-Optimized",
            "eco": "Eco-Optimized",
            "risk": "Risk-Diversified",
            "regional": "Regional-Proximity"
        }
        
        strategy_name = strategy_names.get(strategy, "Unknown")
        
        return (
            f"{strategy_name} supply chain strategy with fallback configuration. "
            f"Sourcing materials through {', '.join(scenario.transport_modes[:2])} transport "
            f"from {scenario.origin_region}. Total transport distance {scenario.total_transport_distance_km:.0f}km "
            f"ensures realistic and consistent supply chain modeling. This approach balances "
            f"{'cost and efficiency' if strategy == 'cost' else 'speed and delivery' if strategy == 'speed' else 'environmental impact and logistics' if strategy == 'eco' else 'flexibility and resilience' if strategy == 'risk' else 'proximity and responsiveness'}."
        )
