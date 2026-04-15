from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.forecast_engine.strategy_selection import (
    get_adaptive_baseline_max_horizon_days,
    get_forecast_policy_selection_mode,
    get_horizon_strategy_overrides,
    select_adaptive_forecast_strategy,
)


def test_select_adaptive_forecast_strategy_uses_baseline_for_short_horizons():
    assert select_adaptive_forecast_strategy(1, baseline_max_horizon_days=2) == "baseline_short_horizon"
    assert select_adaptive_forecast_strategy(2, baseline_max_horizon_days=2) == "baseline_short_horizon"


def test_select_adaptive_forecast_strategy_uses_calibrated_for_long_horizons():
    assert select_adaptive_forecast_strategy(3, baseline_max_horizon_days=2) == "calibrated_long_horizon"
    assert select_adaptive_forecast_strategy(4, baseline_max_horizon_days=2) == "calibrated_long_horizon"


def test_select_adaptive_forecast_strategy_uses_configured_threshold():
    ConfigLoader._config = {"forecast_policy": {"adaptive_baseline_max_horizon_days": 1}}

    assert get_adaptive_baseline_max_horizon_days() == 1
    assert select_adaptive_forecast_strategy(1) == "baseline_short_horizon"
    assert select_adaptive_forecast_strategy(2) == "calibrated_long_horizon"


def test_select_adaptive_forecast_strategy_uses_horizon_overrides_from_config():
    ConfigLoader._config = {
        "forecast_policy": {
            "adaptive_baseline_max_horizon_days": 0,
            "selection_mode": "horizon_overrides",
            "horizon_strategy_overrides": {
                "1": "baseline_short_horizon",
                "2": "calibrated_long_horizon",
                "bad": "baseline_short_horizon",
                "3": "unknown",
            },
        }
    }

    assert get_forecast_policy_selection_mode() == "horizon_overrides"
    assert get_horizon_strategy_overrides() == {
        1: "baseline_short_horizon",
        2: "calibrated_long_horizon",
    }
    assert select_adaptive_forecast_strategy(1) == "baseline_short_horizon"
    assert select_adaptive_forecast_strategy(2) == "calibrated_long_horizon"
    assert select_adaptive_forecast_strategy(4) == "calibrated_long_horizon"


def test_select_adaptive_forecast_strategy_allows_explicit_horizon_overrides():
    ConfigLoader._config = {}

    assert (
        select_adaptive_forecast_strategy(
            3,
            baseline_max_horizon_days=0,
            selection_mode="horizon_overrides",
            horizon_strategy_overrides={3: "baseline_short_horizon"},
        )
        == "baseline_short_horizon"
    )
