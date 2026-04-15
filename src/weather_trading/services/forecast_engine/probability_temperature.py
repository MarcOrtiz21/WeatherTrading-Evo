from __future__ import annotations

from weather_trading.domain.models import ForecastDistribution
from weather_trading.infrastructure.config import ConfigLoader


def normalize_temperature_unit(unit: str | None) -> str:
    normalized = (unit or "").strip().lower()
    if normalized in {"f", "°f", "fahrenheit"}:
        return "fahrenheit"
    if normalized in {"c", "°c", "celsius"}:
        return "celsius"
    return "unknown"


def infer_temperature_unit(question: str | None) -> str:
    normalized = (question or "").lower()
    if "°f" in normalized or "fahrenheit" in normalized:
        return "fahrenheit"
    if "°c" in normalized or "celsius" in normalized:
        return "celsius"
    return "unknown"


def get_probability_temperature_alpha(default: float = 1.0) -> float:
    raw_value = ConfigLoader.get("forecast_policy.probability_temperature_alpha", default)
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return default
    return value


def get_probability_temperature_alpha_by_unit() -> dict[str, float]:
    raw_mapping = ConfigLoader.get("forecast_policy.probability_temperature_alpha_by_unit", {}) or {}
    if not isinstance(raw_mapping, dict):
        return {}

    normalized: dict[str, float] = {}
    for raw_unit, raw_value in raw_mapping.items():
        unit = normalize_temperature_unit(str(raw_unit))
        if unit == "unknown":
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue
        normalized[unit] = value
    return normalized


def get_probability_temperature_alpha_for_unit(unit: str | None, default: float = 1.0) -> float:
    normalized_unit = normalize_temperature_unit(unit)
    mapping = get_probability_temperature_alpha_by_unit()
    if normalized_unit in mapping:
        return mapping[normalized_unit]
    return get_probability_temperature_alpha(default=default)


def apply_probability_temperature(
    distribution: ForecastDistribution,
    alpha: float | None = None,
    unit: str | None = None,
) -> ForecastDistribution:
    calibrated_alpha = (
        get_probability_temperature_alpha_for_unit(unit)
        if alpha is None
        else float(alpha)
    )
    if calibrated_alpha <= 0 or abs(calibrated_alpha - 1.0) < 1e-9:
        return distribution

    adjusted = {
        int(temperature_c): probability ** calibrated_alpha
        for temperature_c, probability in distribution.probabilities_by_temp_c.items()
        if probability > 0
    }
    total = sum(adjusted.values()) or 1.0
    normalized = {
        temperature_c: probability / total
        for temperature_c, probability in adjusted.items()
    }
    notes = tuple(
        dict.fromkeys(
            distribution.notes + (f"probability_temperature_alpha={calibrated_alpha:.3f}",)
        )
    )
    return ForecastDistribution(
        market_id=distribution.market_id,
        generated_at_utc=distribution.generated_at_utc,
        model_name=distribution.model_name,
        calibration_version=f"{distribution.calibration_version}+temp_alpha",
        probabilities_by_temp_c=normalized,
        notes=notes,
    )
