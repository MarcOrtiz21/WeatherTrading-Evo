import httpx
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any
import logging
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import retry_async

logger = logging.getLogger(__name__)

DEFAULT_MODELS = ("best_match", "gfs_seamless", "ecmwf_ifs025", "icon_seamless")

class OpenMeteoClient:
    """Cliente para obtener forecasts numéricos de Open-Meteo con reintentos."""

    def __init__(self, timeout: int = 30):
        self.base_url = ConfigLoader.get("weather_apis.open_meteo_url", "https://api.open-meteo.com/v1/forecast")
        self.historical_forecast_url = ConfigLoader.get(
            "weather_apis.open_meteo_historical_forecast_url",
            "https://historical-forecast-api.open-meteo.com/v1/forecast",
        )
        self.archive_url = ConfigLoader.get(
            "weather_apis.open_meteo_archive_url",
            "https://archive-api.open-meteo.com/v1/archive",
        )
        self.previous_runs_url = ConfigLoader.get(
            "weather_apis.open_meteo_previous_runs_url",
            "https://previous-runs-api.open-meteo.com/v1/forecast",
        )
        self.ensemble_url = ConfigLoader.get(
            "weather_apis.open_meteo_ensemble_url",
            "https://ensemble-api.open-meteo.com/v1/ensemble",
        )
        self.timeout = timeout

    @retry_async
    async def fetch_forecast(
        self, 
        latitude: float, 
        longitude: float, 
        local_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene el pronóstico para una ubicación y fecha específica.
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "hourly": "temperature_2m,cloudcover,windspeed_10m",
            "timezone": "auto",
            "start_date": local_date.isoformat(),
            "end_date": local_date.isoformat()
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            daily = data.get("daily", {})
            hourly = data.get("hourly", {})
            
            return {
                "model_max_temp": daily.get("temperature_2m_max", [None])[0],
                "model_min_temp": daily.get("temperature_2m_min", [None])[0],
                "model_hourly_times": hourly.get("time", []),
                "model_hourly_temps": hourly.get("temperature_2m", []),
                "model_cloud_cover_avg": sum(hourly.get("cloudcover", [])) / 24 if hourly.get("cloudcover") else 0,
                "model_timezone": data.get("timezone"),
                "provider": "open_meteo"
            }

    @retry_async
    async def fetch_multimodel_forecast(
        self,
        latitude: float,
        longitude: float,
        local_date: date,
        models: tuple[str, ...] = DEFAULT_MODELS,
        historical: bool = False,
    ) -> Dict[str, float]:
        url = self.historical_forecast_url if historical else self.base_url
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "start_date": local_date.isoformat(),
            "end_date": local_date.isoformat(),
            "models": ",".join(models),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()

        daily = payload.get("daily", {})
        result: Dict[str, float] = {}
        for model_name in models:
            key = f"temperature_2m_max_{model_name}"
            values = daily.get(key, [])
            if values and values[0] is not None:
                result[model_name] = float(values[0])
        return result

    @retry_async
    async def fetch_ensemble_members(
        self,
        latitude: float,
        longitude: float,
        local_date: date,
    ) -> list[float]:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "start_date": local_date.isoformat(),
            "end_date": local_date.isoformat(),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.ensemble_url, params=params)
            response.raise_for_status()
            payload = response.json()

        daily = payload.get("daily", {})
        members: list[float] = []
        for key, values in daily.items():
            if key.startswith("temperature_2m_max_member") and values and values[0] is not None:
                members.append(float(values[0]))
        return members

    @retry_async
    async def fetch_archive_daily_max(
        self,
        latitude: float,
        longitude: float,
        local_date: date,
    ) -> float | None:
        history = await self.fetch_archive_daily_max_history(
            latitude=latitude,
            longitude=longitude,
            start_date=local_date,
            end_date=local_date,
        )
        return history.get(local_date.isoformat())

    @retry_async
    async def fetch_archive_daily_max_history(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
    ) -> Dict[str, float]:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.archive_url, params=params)
            response.raise_for_status()
            payload = response.json()

        daily = payload.get("daily", {})
        return {
            day: float(value)
            for day, value in zip(daily.get("time", []), daily.get("temperature_2m_max", []))
            if value is not None
        }

    @retry_async
    async def fetch_historical_multimodel_history(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        models: tuple[str, ...] = DEFAULT_MODELS,
    ) -> Dict[str, Dict[str, float]]:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "models": ",".join(models),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.historical_forecast_url, params=params)
            response.raise_for_status()
            payload = response.json()

        daily = payload.get("daily", {})
        days = daily.get("time", [])
        history: Dict[str, Dict[str, float]] = {}
        for index, day in enumerate(days):
            values_by_model: Dict[str, float] = {}
            for model_name in models:
                key = f"temperature_2m_max_{model_name}"
                values = daily.get(key, [])
                if index < len(values) and values[index] is not None:
                    values_by_model[model_name] = float(values[index])
            if values_by_model:
                history[day] = values_by_model
        return history

    @retry_async
    async def fetch_previous_runs_history(
        self,
        latitude: float,
        longitude: float,
        horizon_days: int,
        past_days: int = 7,
        models: tuple[str, ...] = DEFAULT_MODELS,
    ) -> Dict[str, Dict[str, float]]:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "past_days": max(past_days, 1),
            "forecast_days": max(horizon_days, 1),
            "models": ",".join(models),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.previous_runs_url, params=params)
            response.raise_for_status()
            payload = response.json()

        daily = payload.get("daily", {})
        days = daily.get("time", [])
        history: Dict[str, Dict[str, float]] = {}
        for index, day in enumerate(days):
            values_by_model: Dict[str, float] = {}
            for model_name in models:
                candidate_keys = (
                    f"temperature_2m_max_{model_name}",
                    "temperature_2m_max" if model_name == "best_match" else "",
                )
                for key in candidate_keys:
                    if not key:
                        continue
                    values = daily.get(key, [])
                    if index < len(values) and values[index] is not None:
                        values_by_model[model_name] = float(values[index])
                        break
            if values_by_model:
                history[day] = values_by_model
        return history

    async def fetch_recent_calibration_window(
        self,
        latitude: float,
        longitude: float,
        calibration_end_date: date,
        lookback_days: int = 7,
        models: tuple[str, ...] = DEFAULT_MODELS,
    ) -> tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
        start_date = calibration_end_date - timedelta(days=lookback_days - 1)
        forecast_history = await self.fetch_historical_multimodel_history(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=calibration_end_date,
            models=models,
        )
        actual_history = await self.fetch_archive_daily_max_history(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=calibration_end_date,
        )
        return forecast_history, actual_history

    async def fetch_horizon_calibration_window(
        self,
        latitude: float,
        longitude: float,
        as_of_date: date,
        horizon_days: int,
        lookback_days: int = 7,
        models: tuple[str, ...] = DEFAULT_MODELS,
    ) -> tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
        if as_of_date == date.today():
            previous_runs_history = await self.fetch_previous_runs_history(
                latitude=latitude,
                longitude=longitude,
                horizon_days=horizon_days,
                past_days=lookback_days,
                models=models,
            )
            filtered_forecast_history = {
                target_date: values_by_model
                for target_date, values_by_model in previous_runs_history.items()
                if date.fromisoformat(target_date) < as_of_date
            }
            if filtered_forecast_history:
                start_date = min(date.fromisoformat(day) for day in filtered_forecast_history)
                end_date = max(date.fromisoformat(day) for day in filtered_forecast_history)
                actual_history = await self.fetch_archive_daily_max_history(
                    latitude=latitude,
                    longitude=longitude,
                    start_date=start_date,
                    end_date=end_date,
                )
                filtered_actual_history = {
                    target_date: value
                    for target_date, value in actual_history.items()
                    if target_date in filtered_forecast_history
                }
                return filtered_forecast_history, filtered_actual_history

        calibration_end_date = as_of_date - timedelta(days=1)
        return await self.fetch_recent_calibration_window(
            latitude=latitude,
            longitude=longitude,
            calibration_end_date=calibration_end_date,
            lookback_days=lookback_days,
            models=models,
        )
