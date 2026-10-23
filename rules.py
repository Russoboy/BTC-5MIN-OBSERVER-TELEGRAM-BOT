"""Pure event-rule helpers for the BTC 5-minute observation bot."""

from __future__ import annotations

from typing import Optional

SWEET_SPOT_THRESHOLD = 0.80
PULLBACK_THRESHOLD = 0.55
SWITCH_OPPOSITE_THRESHOLD = 0.80


def remaining_label(end_ts: float, event_ts: float) -> str:
    """Format time remaining as [minutes, seconds], e.g. [2, 07]."""
    remaining = max(0, int(round(end_ts - event_ts)))
    minutes, seconds = divmod(remaining, 60)
    return f"[{minutes}, {seconds:02d}]"


def sweet_spot_category(end_ts: float, event_ts: float) -> str:
    """Return '3 min+' before the final three minutes, else the time label."""
    remaining = max(0, int(round(end_ts - event_ts)))
    if remaining > 180:
        return "3 min+"
    return remaining_label(end_ts, event_ts)


def opposite_direction(direction: Optional[str]) -> Optional[str]:
    if direction == "UP":
        return "DOWN"
    if direction == "DOWN":
        return "UP"
    return None


def is_sweet_spot(price: Optional[float]) -> bool:
    """Sweet Spot is strictly above $0.80 (not equal to $0.80)."""
    return price is not None and price > SWEET_SPOT_THRESHOLD


def is_pullback(initial_direction: Optional[str], current_direction: str,
                price: Optional[float]) -> bool:
    """Pullback when the initial Sweet Spot direction later reaches <= $0.55."""
    return (
        initial_direction is not None
        and current_direction == initial_direction
        and price is not None
        and price <= PULLBACK_THRESHOLD
    )


def is_opposite_80(direction: str, initial_direction: Optional[str],
                   price: Optional[float]) -> bool:
    """The opposite side hits $0.80 or higher after the initial Sweet Spot."""
    return (
        initial_direction is not None
        and direction == opposite_direction(initial_direction)
        and price is not None
        and price >= SWITCH_OPPOSITE_THRESHOLD
    )


def switch_up_result(initial_direction: Optional[str], opposite_hit_80: bool,
                     final_resolution: Optional[str]) -> Optional[str]:
    """Resolve Switch Up only when outcome is known; YES if opposite wins."""
    if final_resolution not in {"UP", "DOWN"}:
        return None
    opposite = opposite_direction(initial_direction)
    if opposite is not None and opposite_hit_80 and final_resolution == opposite:
        return "YES"
    return "NO"
