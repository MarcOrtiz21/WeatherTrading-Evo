from typing import Literal

from weather_trading.infrastructure.config import ConfigLoader


ForecastStrategy = Literal["baseline_short_horizon", "calibrated_long_horizon"]
ForecastSelectionMode = Literal["cutoff", "horizon_overrides"]

DEFAULT_BASELINE_MAX_HORIZON_DAYS = 2
VALID_STRATEGIES = {"baseline_short_horizon", "calibrated_long_horizon"}
DEFAULT_SELECTION_MODE: ForecastSelectionMode = "cutoff"


def get_adaptive_baseline_max_horizon_days(default: int = DEFAULT_BASELINE_MAX_HORIZON_DAYS) -> int:
    value = ConfigLoader.get("forecast_policy.adaptive_baseline_max_horizon_days", default)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def get_forecast_policy_selection_mode(
    default: ForecastSelectionMode = DEFAULT_SELECTION_MODE,
) -> ForecastSelectionMode:
    value = ConfigLoader.get("forecast_policy.selection_mode", default)
    return "horizon_overrides" if value == "horizon_overrides" else "cutoff"


def get_horizon_strategy_overrides() -> dict[int, ForecastStrategy]:
    raw_value = ConfigLoader.get("forecast_policy.horizon_strategy_overrides", {})
    if not isinstance(raw_value, dict):
        return {}

    overrides: dict[int, ForecastStrategy] = {}
    for horizon_key, strategy in raw_value.items():
        try:
            horizon_days = max(0, int(horizon_key))
        except (TypeError, ValueError):
            continue

        if horizon_days <= 0 or strategy not in VALID_STRATEGIES:
            continue
        overrides[horizon_days] = strategy

    return overrides


def select_adaptive_forecast_strategy(
    horizon_days: int,
    baseline_max_horizon_days: int | None = None,
    horizon_strategy_overrides: dict[int | str, ForecastStrategy] | None = None,
    selection_mode: ForecastSelectionMode | None = None,
) -> ForecastStrategy:
    normalized_horizon_days = max(0, int(horizon_days))
    active_selection_mode = get_forecast_policy_selection_mode() if selection_mode is None else selection_mode

    if horizon_strategy_overrides is None:
        overrides = get_horizon_strategy_overrides()
    else:
        overrides = {}
        for horizon_key, strategy in horizon_strategy_overrides.items():
            try:
                parsed_horizon = max(0, int(horizon_key))
            except (TypeError, ValueError):
                continue
            if parsed_horizon <= 0 or strategy not in VALID_STRATEGIES:
                continue
            overrides[parsed_horizon] = strategy

    if active_selection_mode == "horizon_overrides":
        selected_override = overrides.get(normalized_horizon_days)
        if selected_override is not None:
            return selected_override

    cutoff = (
        get_adaptive_baseline_max_horizon_days()
        if baseline_max_horizon_days is None
        else max(0, baseline_max_horizon_days)
    )
    if normalized_horizon_days <= cutoff:
        return "baseline_short_horizon"
    return "calibrated_long_horizon"
