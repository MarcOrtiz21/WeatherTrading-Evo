from datetime import date

import pytest

from weather_trading.services.forecast_engine.backtest_support import compute_previous_runs_past_days


def test_compute_previous_runs_past_days_matches_current_day_window():
    result = compute_previous_runs_past_days(
        date(2026, 4, 5),
        lookback_days=30,
        max_horizon_days=4,
        reference_today=date(2026, 4, 5),
    )

    assert result == 34


def test_compute_previous_runs_past_days_extends_window_for_older_reference_dates():
    result = compute_previous_runs_past_days(
        date(2026, 4, 3),
        lookback_days=30,
        max_horizon_days=4,
        reference_today=date(2026, 4, 5),
    )

    assert result == 36


def test_compute_previous_runs_past_days_rejects_future_reference_dates():
    with pytest.raises(ValueError):
        compute_previous_runs_past_days(
            date(2026, 4, 6),
            lookback_days=30,
            max_horizon_days=4,
            reference_today=date(2026, 4, 5),
        )
