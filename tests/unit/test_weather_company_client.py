from datetime import date

import pytest

from weather_trading.services.weather_ingestion.weather_company_client import WeatherCompanyClient


@pytest.mark.asyncio
async def test_fetch_forecast_normalizes_daily_and_hourly_payloads(monkeypatch):
    client = WeatherCompanyClient(api_key="test-key")

    async def fake_request_json(path, params):
        if "daily" in path:
            return {
                "validTimeLocal": [
                    "2026-04-08T07:00:00-0400",
                    "2026-04-09T07:00:00-0400",
                ],
                "calendarDayTemperatureMax": [13, 16],
                "calendarDayTemperatureMin": [6, 7],
            }
        return {
            "validTimeLocal": [
                "2026-04-08T00:00:00-0400",
                "2026-04-08T06:00:00-0400",
                "2026-04-08T12:00:00-0400",
                "2026-04-09T00:00:00-0400",
            ],
            "temperature": [8, 10, 13, 9],
            "cloudCover": [40, 60, 80, 20],
        }

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    payload = await client.fetch_forecast(
        latitude=40.7,
        longitude=-73.9,
        local_date=date(2026, 4, 8),
    )

    assert payload is not None
    assert payload["model_max_temp"] == 13.0
    assert payload["model_min_temp"] == 6.0
    assert payload["model_hourly_temps"] == [8.0, 10.0, 13.0]
    assert payload["model_cloud_cover_avg"] == pytest.approx(60.0)
    assert payload["provider"] == "weather_company"


@pytest.mark.asyncio
async def test_fetch_forecast_returns_none_when_target_day_missing(monkeypatch):
    client = WeatherCompanyClient(api_key="test-key")

    async def fake_request_json(path, params):
        if "daily" in path:
            return {"validTimeLocal": ["2026-04-09T07:00:00-0400"], "temperatureMax": [16]}
        return {"validTimeLocal": [], "temperature": []}

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    payload = await client.fetch_forecast(
        latitude=40.7,
        longitude=-73.9,
        local_date=date(2026, 4, 8),
    )

    assert payload is None


def test_weather_company_client_requires_api_key():
    client = WeatherCompanyClient(api_key=None)

    with pytest.raises(ValueError, match="Missing The Weather Company API key"):
        client._require_api_key()
