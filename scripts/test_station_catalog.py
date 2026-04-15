import sys
import os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.domain.models import MarketSpec, MetricKind, ResolutionSource, TimeAggregation
from weather_trading.services.station_mapper.service import StationMapperService


def test_station_catalog():
    mapper = StationMapperService()

    station = mapper.get_station("LEMD")
    assert station is not None
    assert station["city"] == "Madrid"

    mapping = mapper.get_provider_mapping("LEMD", "wunderground")
    assert mapping is not None
    assert mapping["airport_code"] == "LEMD"

    source = mapper.get_source_definition("open_meteo")
    assert source is not None
    assert source["role"] == "forecast_auxiliary"

    spec = MarketSpec(
        market_id="alias-inference-test",
        question="¿Hará 31°C o más en Barcelona Airport el 15 de abril de 2026?",
        rules_text="Resolución por Wunderground para la máxima diaria.",
        city="UNKNOWN",
        country="UNKNOWN",
        station_code="UNKNOWN",
        timezone="UTC",
        local_date=date(2026, 4, 15),
        resolution_source=ResolutionSource.WUNDERGROUND,
        metric=MetricKind.MAX_TEMP_C,
        aggregation=TimeAggregation.DAILY_MAX,
        threshold_c=31.0,
        confidence_score=0.7,
    )

    enriched = mapper.enrich(spec)
    print(f"Station inferred: {enriched.station_code}")
    print(f"Notes: {enriched.notes}")

    assert enriched.station_code == "LEBL"
    assert enriched.city == "Barcelona"
    assert any(note.startswith("inferred_station_code:") for note in enriched.notes)

    print("\n¡Pruebas del catálogo de estaciones completadas con éxito!")


if __name__ == "__main__":
    test_station_catalog()
