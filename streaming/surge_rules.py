"""Surge pricing rules: demand/supply ratio -> multiplier (plus rain / extreme temperature), capped and smoothed."""
from typing import Optional, Tuple

CAP = 3.0                 # final multiplier never exceeds 3.0x
MAX_STEP = 0.3            # and moves at most +/-0.3x per minute (no price flicker)
RAIN_THRESHOLD_MM = 2.5
RAIN_BONUS = 0.2
EXTREME_TEMP_BONUS = 0.1  # < 5 C or > 35 C


def base_multiplier(ratio: float) -> float:
    """demand/supply ratio -> base multiplier.

    < 1.0 -> 1.0x | [1.0,1.5) -> 1.2x | [1.5,2.0) -> 1.5x | [2.0,3.0] -> 2.0x | > 3.0 -> 2.5x
    """
    if ratio < 1.0:
        return 1.0
    if ratio < 1.5:
        return 1.2
    if ratio < 2.0:
        return 1.5
    if ratio <= 3.0:
        return 2.0
    return 2.5


def weather_adjustment(rain_mm: Optional[float], temp_c: Optional[float],
                       rain_threshold: float = RAIN_THRESHOLD_MM) -> float:
    adj = 0.0
    if rain_mm is not None and rain_mm >= rain_threshold:
        adj += RAIN_BONUS
    if temp_c is not None and (temp_c < 5.0 or temp_c > 35.0):
        adj += EXTREME_TEMP_BONUS
    return adj


def surge_multiplier(demand: float, supply: float, rain_mm: Optional[float] = None,
                     temp_c: Optional[float] = None, prev: Optional[float] = None,
                     rain_threshold: float = RAIN_THRESHOLD_MM) -> Tuple[float, float]:
    """Returns (multiplier, ratio)."""
    ratio = demand / max(supply, 1)
    raw = min(base_multiplier(ratio) + weather_adjustment(rain_mm, temp_c, rain_threshold), CAP)
    if prev is not None:
        raw = max(prev - MAX_STEP, min(prev + MAX_STEP, raw))
    return round(min(max(raw, 1.0), CAP), 2), round(ratio, 4)
