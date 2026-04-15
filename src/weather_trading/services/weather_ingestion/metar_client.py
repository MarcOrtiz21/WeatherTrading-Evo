import httpx
import re
from datetime import datetime, timedelta
from typing import Optional
import logging
from weather_trading.domain.models import WeatherObservation, ResolutionSource
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import retry_async, utc_now

logger = logging.getLogger(__name__)

class MetarIngestor:
    """Cliente para obtener y parsear METARs con reintentos."""

    def __init__(self, timeout: int = 20):
        self.base_url = ConfigLoader.get("weather_apis.aviation_weather_url", "https://aviationweather.gov/api/data/metar")
        self.timeout = timeout

    @retry_async
    async def fetch_metar(self, station_code: str) -> Optional[str]:
        """Obtiene el último METAR bruto para una estación."""
        params = {"ids": station_code}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            return response.text.strip()

    def parse_metar(
        self,
        raw_metar: str,
        reference_time_utc: datetime | None = None,
    ) -> Optional[WeatherObservation]:
        """Parseo simplificado de un METAR bruto."""
        if not raw_metar:
            return None
        parts = raw_metar.split()
        if len(parts) < 2:
            return None

        station_code = parts[0]
        time_match = re.search(r"(\d{2})(\d{2})(\d{2})z", raw_metar.lower())
        observed_at = reference_time_utc or utc_now()
        if time_match:
            day, hour, minute = map(int, time_match.groups())
            observed_at = self._resolve_report_timestamp(
                day=day,
                hour=hour,
                minute=minute,
                reference_time_utc=observed_at,
            )

        temp_match = re.search(r"\b(m?\d{2})/(m?\d{2})\b", raw_metar.lower())
        temp_c = 0.0
        dewpoint_c = None
        if temp_match:
            def parse_val(v: str) -> float:
                if v.startswith('m'): return -float(v[1:])
                return float(v)
            temp_c = parse_val(temp_match.group(1))
            dewpoint_c = parse_val(temp_match.group(2))

        pressure_match = re.search(r"q(\d{4})", raw_metar.lower())
        pressure_hpa = float(pressure_match.group(1)) if pressure_match else None

        return WeatherObservation(
            station_code=station_code,
            provider=ResolutionSource.METAR,
            observed_at_utc=observed_at,
            temp_c=temp_c,
            dewpoint_c=dewpoint_c,
            pressure_hpa=pressure_hpa,
            raw_reference=raw_metar
        )

    def _resolve_report_timestamp(
        self,
        day: int,
        hour: int,
        minute: int,
        reference_time_utc: datetime,
    ) -> datetime:
        candidates: list[datetime] = []
        for year, month in self._candidate_months(reference_time_utc):
            try:
                candidates.append(datetime(year, month, day, hour, minute, 0, 0))
            except ValueError:
                continue

        if not candidates:
            raise ValueError(f"No se pudo resolver la fecha del METAR para el día {day:02d}")

        latest_reasonable = [
            candidate
            for candidate in candidates
            if candidate <= reference_time_utc + timedelta(hours=12)
        ]
        pool = latest_reasonable or candidates
        return min(pool, key=lambda candidate: abs(reference_time_utc - candidate))

    def _candidate_months(self, reference_time_utc: datetime) -> list[tuple[int, int]]:
        current_year = reference_time_utc.year
        current_month = reference_time_utc.month

        previous_month = current_month - 1 or 12
        previous_year = current_year - 1 if current_month == 1 else current_year

        next_month = 1 if current_month == 12 else current_month + 1
        next_year = current_year + 1 if current_month == 12 else current_year

        return [
            (previous_year, previous_month),
            (current_year, current_month),
            (next_year, next_month),
        ]
