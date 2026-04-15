from datetime import date, timedelta

import pytest

from weather_trading.services.weather_ingestion.openmeteo_client import OpenMeteoClient


@pytest.mark.asyncio
async def test_fetch_horizon_calibration_window_filters_previous_runs_to_resolved_days(monkeypatch):
    client = OpenMeteoClient()
    today = date.today()
    resolved_day = today - timedelta(days=2)
    older_day = today - timedelta(days=4)

    async def fake_previous_runs_history(**kwargs):
        return {
            older_day.isoformat(): {"best_match": 21.0},
            resolved_day.isoformat(): {"best_match": 22.0},
            today.isoformat(): {"best_match": 23.0},
            (today + timedelta(days=1)).isoformat(): {"best_match": 24.0},
        }

    async def fake_archive_daily_max_history(**kwargs):
        return {
            older_day.isoformat(): 20.5,
            resolved_day.isoformat(): 21.5,
            today.isoformat(): 23.0,
        }

    monkeypatch.setattr(client, "fetch_previous_runs_history", fake_previous_runs_history)
    monkeypatch.setattr(client, "fetch_archive_daily_max_history", fake_archive_daily_max_history)

    forecast_history, actual_history = await client.fetch_horizon_calibration_window(
        latitude=0.0,
        longitude=0.0,
        as_of_date=today,
        horizon_days=2,
        lookback_days=7,
    )

    assert set(forecast_history) == {older_day.isoformat(), resolved_day.isoformat()}
    assert set(actual_history) == {older_day.isoformat(), resolved_day.isoformat()}


@pytest.mark.asyncio
async def test_fetch_horizon_calibration_window_falls_back_for_non_today_as_of(monkeypatch):
    client = OpenMeteoClient()
    as_of_date = date.today() - timedelta(days=3)

    async def fake_recent_calibration_window(**kwargs):
        return (
            {"2026-04-01": {"best_match": 20.0}},
            {"2026-04-01": 21.0},
        )

    monkeypatch.setattr(client, "fetch_recent_calibration_window", fake_recent_calibration_window)

    forecast_history, actual_history = await client.fetch_horizon_calibration_window(
        latitude=0.0,
        longitude=0.0,
        as_of_date=as_of_date,
        horizon_days=3,
        lookback_days=7,
    )

    assert forecast_history == {"2026-04-01": {"best_match": 20.0}}
    assert actual_history == {"2026-04-01": 21.0}
