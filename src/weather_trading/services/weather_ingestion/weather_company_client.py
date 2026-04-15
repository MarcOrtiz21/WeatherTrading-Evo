import os
from datetime import date
from typing import Any, Dict, Optional

import httpx

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import retry_async


class WeatherCompanyClient:
    """Cliente oficial para The Weather Company Data APIs."""

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 30,
        base_url: str | None = None,
        language: str = "en-US",
        units: str = "m",
    ):
        self.api_key = (
            api_key
            or os.getenv("WEATHER_COMPANY_API_KEY")
            or os.getenv("TWC_API_KEY")
        )
        self.timeout = timeout
        self.base_url = (
            base_url
            or ConfigLoader.get("weather_apis.weather_company_url", "https://api.weather.com")
        ).rstrip("/")
        self.language = language
        self.units = units

    @retry_async
    async def fetch_forecast(
        self,
        latitude: float,
        longitude: float,
        local_date: date,
    ) -> Optional[Dict[str, Any]]:
        geocode = self._format_geocode(latitude, longitude)
        daily_payload = await self._request_json(
            "/v3/wx/forecast/daily/15day",
            {"geocode": geocode},
        )
        hourly_payload = await self._request_json(
            "/v3/wx/forecast/hourly/15day",
            {"geocode": geocode},
        )

        day_index = self._find_local_date_index(daily_payload.get("validTimeLocal", []), local_date)
        if day_index is None:
            return None

        daily_max = self._value_at(
            daily_payload,
            (
                "calendarDayTemperatureMax",
                "temperatureMax",
            ),
            day_index,
        )
        if daily_max is None:
            return None

        hourly_times = []
        hourly_temps = []
        for timestamp, temp in zip(
            hourly_payload.get("validTimeLocal", []),
            hourly_payload.get("temperature", []),
            strict=False,
        ):
            if temp is None:
                continue
            if self._local_timestamp_matches_date(timestamp, local_date):
                hourly_times.append(timestamp)
                hourly_temps.append(float(temp))

        cloud_cover_values = [
            float(value)
            for timestamp, value in zip(
                hourly_payload.get("validTimeLocal", []),
                hourly_payload.get("cloudCover", []),
                strict=False,
            )
            if value is not None and self._local_timestamp_matches_date(timestamp, local_date)
        ]

        return {
            "model_max_temp": float(daily_max),
            "model_min_temp": self._value_at(
                daily_payload,
                ("calendarDayTemperatureMin", "temperatureMin"),
                day_index,
            ),
            "model_hourly_times": hourly_times,
            "model_hourly_temps": hourly_temps,
            "model_cloud_cover_avg": (
                sum(cloud_cover_values) / len(cloud_cover_values) if cloud_cover_values else 0.0
            ),
            "model_timezone": self._timezone_from_timestamp(
                self._value_at(hourly_payload, ("validTimeLocal",), 0)
                or self._value_at(daily_payload, ("validTimeLocal",), day_index)
            ),
            "provider": "weather_company",
        }

    async def _request_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        api_key = self._require_api_key()
        request_params = {
            "api_key": api_key,
            "language": self.language,
            "format": "json",
            "units": self.units,
            **params,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}{path}", params=request_params)
            response.raise_for_status()
            return response.json()

    def _require_api_key(self) -> str:
        if self.api_key:
            return self.api_key
        raise ValueError(
            "Missing The Weather Company API key. Set WEATHER_COMPANY_API_KEY or TWC_API_KEY."
        )

    def _format_geocode(self, latitude: float, longitude: float) -> str:
        return f"{float(latitude):.4f},{float(longitude):.4f}"

    def _find_local_date_index(self, values: list[Any], local_date: date) -> int | None:
        for index, value in enumerate(values):
            if self._local_timestamp_matches_date(value, local_date):
                return index
        return None

    def _local_timestamp_matches_date(self, timestamp: Any, local_date: date) -> bool:
        if not isinstance(timestamp, str) or len(timestamp) < 10:
            return False
        return timestamp[:10] == local_date.isoformat()

    def _timezone_from_timestamp(self, timestamp: Any) -> str | None:
        if not isinstance(timestamp, str):
            return None
        if len(timestamp) >= 5 and timestamp[-5] in {"+", "-"}:
            return timestamp[-5:]
        return None

    def _value_at(
        self,
        payload: Dict[str, Any],
        candidate_keys: tuple[str, ...],
        index: int,
    ) -> float | str | None:
        for key in candidate_keys:
            values = payload.get(key, [])
            if index < len(values) and values[index] is not None:
                value = values[index]
                if isinstance(value, (int, float)):
                    return float(value)
                return value
        return None
