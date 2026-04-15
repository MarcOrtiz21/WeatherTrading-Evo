import sys
import os
import asyncio
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.domain.models import MarketSpec, WeatherObservation, ResolutionSource, MetricKind, TimeAggregation, RoundingMethod
from weather_trading.infrastructure.database import init_db, AsyncSessionLocal
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.persistence.repository import WeatherRepository

async def test_persistence():
    # 1. Inicializar DB
    print("Inicializando base de datos...")
    await init_db()

    async with AsyncSessionLocal() as session:
        repo = WeatherRepository(session)

        # 2. Test MarketSpec
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
            confidence_score=0.95
        )

        print(f"Guardando MarketSpec: {spec.market_id}")
        await repo.save_market_spec(spec)

        retrieved_spec = await repo.get_market_spec("test-market-123")
        print(f"Recuperado MarketSpec: {retrieved_spec.market_id}")
        assert retrieved_spec.city == "Madrid"
        assert retrieved_spec.threshold_c == 33.5
        assert retrieved_spec.outcomes == ("Yes", "No")

        # 3. Test WeatherObservation
        obs = WeatherObservation(
            station_code="LEMD",
            provider=ResolutionSource.METAR,
            observed_at_utc=utc_now(),
            temp_c=25.5,
            dewpoint_c=12.0,
            pressure_hpa=1013.0,
            raw_reference="RAW_METAR_STRING"
        )

        print(f"Guardando observación para {obs.station_code}")
        await repo.save_observation(obs)

        latest_obs = await repo.get_latest_observations("LEMD", limit=1)
        print(f"Recuperada última observación: {latest_obs[0].temp_c}°C")
        assert len(latest_obs) == 1
        assert latest_obs[0].temp_c == 25.5
        assert latest_obs[0].raw_reference == "RAW_METAR_STRING"

    print("\n¡Pruebas de persistencia completadas con éxito!")

if __name__ == "__main__":
    asyncio.run(test_persistence())
