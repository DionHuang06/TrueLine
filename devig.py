"""
De-vig methods: convert decimal odds to fair probabilities.

- multiplicative: scale implied probs so they sum to 1.
- power: fair_h = pi_h^z / (pi_h^z + pi_a^z), z=2.
- shin: closed-form two-outcome Shin (margin derived from overround).
"""
from __future__ import annotations

import math
from typing import Tuple


def _implied(home_dec: float, away_dec: float) -> Tuple[float, float]:
    if home_dec <= 0 or away_dec <= 0:
        return (0.5, 0.5)
    return (1 / home_dec, 1 / away_dec)


def multiplicative(home_dec: float, away_dec: float) -> Tuple[float, float]:
    """Scale implied probs so they sum to 1."""
    pi_h, pi_a = _implied(home_dec, away_dec)
    s = pi_h + pi_a
    if s <= 0:
        return (0.5, 0.5)
    return (pi_h / s, pi_a / s)


def power(home_dec: float, away_dec: float, z: float = 2.0) -> Tuple[float, float]:
    """Power method: fair_h = pi_h^z / (pi_h^z + pi_a^z)."""
    pi_h, pi_a = _implied(home_dec, away_dec)
    s = pi_h + pi_a
    if s <= 0:
        return (0.5, 0.5)
    n_h, n_a = pi_h / s, pi_a / s
    if n_h <= 0 or n_a <= 0:
        return (0.5, 0.5)
    hz = n_h ** z
    az = n_a ** z
    denom = hz + az
    if denom <= 0:
        return (0.5, 0.5)
    return (hz / denom, az / denom)


def shin(home_dec: float, away_dec: float) -> Tuple[float, float]:
    """Two-outcome Shin: solve for implicit margin and return fair probs."""
    pi_h, pi_a = _implied(home_dec, away_dec)
    s = pi_h + pi_a
    if s <= 0:
        return (0.5, 0.5)
    overround = s - 1.0
    if overround <= 0:
        return (pi_h / s, pi_a / s)
    # Shin: z = (sqrt(1 + 4*overround*(1/pi_h + 1/pi_a)) - 1) / (2*overround)
    # then fair = (z + 1/pi) / (2*z + 1/pi_h + 1/pi_a). Simplified two-outcome:
    try:
        rad = 1.0 + 4.0 * overround * (1.0 / pi_h + 1.0 / pi_a)
        z = (math.sqrt(rad) - 1.0) / (2.0 * overround)
        denom = 2.0 * z + (1.0 / pi_h) + (1.0 / pi_a)
        h_fair = (z + 1.0 / pi_h) / denom
        a_fair = (z + 1.0 / pi_a) / denom
        if 0 <= h_fair <= 1 and 0 <= a_fair <= 1:
            return (h_fair, a_fair)
    except (ValueError, ZeroDivisionError):
        pass
    return (pi_h / s, pi_a / s)


def devig(home_dec: float, away_dec: float, method: str = "multiplicative") -> Tuple[float, float]:
    """Dispatch to multiplicative, power, or shin."""
    if method == "power":
        return power(home_dec, away_dec)
    if method == "shin":
        return shin(home_dec, away_dec)
    return multiplicative(home_dec, away_dec)
