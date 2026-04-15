from datetime import datetime

import pytest

from weather_trading.domain.models import MarketQuote, ResolutionSource, WeatherObservation
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.baseline import BaselineForecastModel
from weather_trading.services.persistence.repository import WeatherRepository
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService


@pytest.mark.asyncio
async def test_pipeline_smoke_blocks_negative_edge(session_factory):
    parser = DeterministicParser()
    mapper = StationMapperService()
    forecaster = BaselineForecastModel()
    pricing = PricingEngine()

    market_data = {
        "id": "madrid-max-32-apr-15",
        "question": "¿Hará 32°C o más en Madrid el 15 de abril de 2026?",
        "description": "Máxima diaria en Aeropuerto de Madrid (LEMD) el 15 de abril de 2026 según Wunderground.",
        "rules": "Se usará el valor reportado por Wunderground para LEMD el 15 de abril de 2026.",
        "outcomes": ["Yes", "No"],
    }

    spec = mapper.enrich(parser.parse(market_data))
    assert spec is not None
    assert not spec.requires_manual_review

    observation = WeatherObservation(
        station_code=spec.station_code,
        provider=ResolutionSource.METAR,
        observed_at_utc=utc_now(),
        temp_c=28.5,
    )

    async with session_factory() as session:
        repo = WeatherRepository(session)
        await repo.save_market_spec(spec)
        await repo.save_observation(observation)
        history = await repo.get_latest_observations(spec.station_code, limit=24)

    forecast = forecaster.estimate_max_distribution(spec.market_id, history, datetime.now().replace(hour=13, minute=0))
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.54,
        best_ask=0.56,
        captured_at_utc=utc_now(),
    )

    signal = pricing.generate_signal(spec, forecast, quote)

    assert signal.market_probability == 0.55
    assert signal.fair_probability < signal.market_probability
    assert not signal.is_tradeable
