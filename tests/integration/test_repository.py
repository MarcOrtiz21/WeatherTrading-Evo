from datetime import date

import pytest

from weather_trading.domain.models import (
    MarketSpec,
    MetricKind,
    ResolutionSource,
    RoundingMethod,
    TimeAggregation,
    WeatherObservation,
)
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.persistence.repository import WeatherRepository


@pytest.mark.asyncio
async def test_repository_round_trip(session_factory):
    async with session_factory() as session:
        repo = WeatherRepository(session)
        spec = MarketSpec(
            market_id="test-market-123",
            question="¿Test?",
            rules_text="Reglas...",
            city="Madrid",
            country="Spain",
            station_code="LEMD",
            timezone="Europe/Madrid",
            local_date=date(2026, 4, 15),
            resolution_source=ResolutionSource.WUNDERGROUND,
            metric=MetricKind.MAX_TEMP_C,
            aggregation=TimeAggregation.DAILY_MAX,
            rounding_method=RoundingMethod.STRICT_DECIMAL,
            threshold_c=33.5,
            outcomes=("Yes", "No"),
            confidence_score=0.95,
        )

        await repo.save_market_spec(spec)
        retrieved_spec = await repo.get_market_spec(spec.market_id)

        assert retrieved_spec is not None
        assert retrieved_spec.city == "Madrid"
        assert retrieved_spec.threshold_c == 33.5

        obs = WeatherObservation(
            station_code="LEMD",
            provider=ResolutionSource.METAR,
            observed_at_utc=utc_now(),
            temp_c=25.5,
            dewpoint_c=12.0,
            pressure_hpa=1013.0,
            raw_reference="RAW_METAR_STRING",
        )
        await repo.save_observation(obs)

        latest_obs = await repo.get_latest_observations("LEMD", limit=1)
        assert len(latest_obs) == 1
        assert latest_obs[0].temp_c == 25.5


@pytest.mark.asyncio
async def test_repository_upsert_observation_reuses_station_provider_timestamp_key(session_factory):
    async with session_factory() as session:
        repo = WeatherRepository(session)
        observed_at = utc_now()
        first = WeatherObservation(
            station_code="LEMD",
            provider=ResolutionSource.OPEN_METEO,
            observed_at_utc=observed_at,
            temp_c=24.8,
            raw_reference="archive_daily_max_backfill:2026-04-06",
        )
        second = WeatherObservation(
            station_code="LEMD",
            provider=ResolutionSource.OPEN_METEO,
            observed_at_utc=observed_at,
            temp_c=25.2,
            raw_reference="archive_daily_max_backfill:2026-04-06",
        )

        created = await repo.upsert_observation(first)
        updated = await repo.upsert_observation(second)

        latest_obs = await repo.get_latest_observations("LEMD", limit=5)

        assert created is True
        assert updated is False
        assert len(latest_obs) == 1
        assert latest_obs[0].temp_c == 25.2
