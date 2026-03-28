"""
Transport Mode Selection Model for Layer 6.

Implements the multinomial logit model for transport mode selection
based on distance, as described in the research paper.

NOTE: When enriched transport data is available (use_enriched_transport),
the pipeline uses calculate_transport_from_actuals() in components.py
instead of this model. This class is retained for legacy/comparison use.
"""

import math
from typing import Dict

from data.data_generation.layer_6.config.config import (
    TRANSPORT_MODE_PARAMS,
    TRANSPORT_EMISSION_FACTORS
)


class TransportModeModel:
    """
    Multinomial logit model for transport mode selection.

    Calculates mode probabilities P_m(D) as a function of transport distance,
    then computes the weighted average emission factor.
    """

    def __init__(
        self,
        mode_params: Dict[str, Dict[str, float]] = None,
        emission_factors: Dict[str, float] = None
    ):
        """
        Initialize transport mode model.

        Args:
            mode_params: Parameters for utility functions per mode
            emission_factors: Emission factors per mode (g CO2e/tkm)
        """
        self.mode_params = mode_params or TRANSPORT_MODE_PARAMS
        self.emission_factors = emission_factors or TRANSPORT_EMISSION_FACTORS

    def calculate_utility(self, mode: str, distance_km: float) -> float:
        """
        Calculate utility V_m(D) for a transport mode at given distance.

        The utility function is: V_m(D) = alpha_m + beta_m * (D - d_ref_m)

        Args:
            mode: Transport mode name
            distance_km: Transport distance in kilometers

        Returns:
            Utility value for the mode
        """
        params = self.mode_params.get(mode, {'alpha': 0, 'beta': 0, 'd_ref': 1000})
        alpha = params['alpha']
        beta = params['beta']
        d_ref = params['d_ref']

        return alpha + beta * (distance_km - d_ref)

    def calculate_mode_probabilities(self, distance_km: float) -> Dict[str, float]:
        """
        Calculate probability of each transport mode given distance.

        Uses multinomial logit model: P_m(D) = exp(V_m) / sum(exp(V_k))

        Args:
            distance_km: Transport distance in kilometers

        Returns:
            Dictionary mapping mode names to probabilities
        """
        # Calculate utilities for all modes
        utilities = {}
        for mode in self.mode_params.keys():
            utilities[mode] = self.calculate_utility(mode, distance_km)

        # Calculate exp(V_m) for numerical stability, subtract max utility
        max_utility = max(utilities.values())
        exp_utilities = {}
        for mode, utility in utilities.items():
            exp_utilities[mode] = math.exp(utility - max_utility)

        # Calculate denominator (sum of exp(V_k))
        denominator = sum(exp_utilities.values())

        # Calculate probabilities
        probabilities = {}
        for mode, exp_v in exp_utilities.items():
            probabilities[mode] = exp_v / denominator

        return probabilities

    def calculate_weighted_emission_factor(self, distance_km: float) -> float:
        """
        Calculate distance-weighted emission factor.

        EF_weighted(D) = sum(P_m(D) * EF_m)

        Args:
            distance_km: Transport distance in kilometers

        Returns:
            Weighted emission factor in g CO2e/tkm
        """
        probabilities = self.calculate_mode_probabilities(distance_km)

        weighted_ef = 0.0
        for mode, prob in probabilities.items():
            ef = self.emission_factors.get(mode, 50.0)  # Default fallback
            weighted_ef += prob * ef

        return weighted_ef

    def calculate_transport_footprint(
        self,
        weight_kg: float,
        distance_km: float
    ) -> Dict[str, float]:
        """
        Calculate transport carbon footprint with mode breakdown.

        CF_transport = (W/1000) * D * (EF_weighted/1000)

        Args:
            weight_kg: Product weight in kilograms
            distance_km: Transport distance in kilometers

        Returns:
            Dictionary with footprint and mode probabilities
        """
        probabilities = self.calculate_mode_probabilities(distance_km)
        weighted_ef = sum(
            probabilities[m] * self.emission_factors.get(m, 50.0)
            for m in probabilities
        )

        # Convert: weight to tonnes, EF from g/tkm to kg/tkm
        # CF = (W/1000) * D * (EF/1000)
        footprint = (weight_kg / 1000.0) * distance_km * (weighted_ef / 1000.0)

        return {
            'footprint_kg_co2e': footprint,
            'weighted_ef_g_co2e_tkm': weighted_ef,
            'mode_probabilities': probabilities
        }
