from datetime import date

from weather_trading.domain.models import (
    MarketSpec,
    MetricKind,
    ResolutionSource,
    TimeAggregation,
)
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.station_mapper.service import StationMapperService


def make_spec(**overrides) -> MarketSpec:
    payload = {
        "market_id": "m1",
        "question": "Will it be hot in Madrid?",
        "rules_text": "Wunderground Madrid airport rules.",
        "city": "",
        "country": "",
        "station_code": "UNKNOWN",
        "timezone": "UTC",
        "local_date": date(2026, 4, 6),
        "resolution_source": ResolutionSource.WUNDERGROUND,
        "metric": MetricKind.MAX_TEMP_C,
        "aggregation": TimeAggregation.DAILY_MAX,
        "threshold_c": 30.0,
        "confidence_score": 0.75,
        "notes": (),
    }
    payload.update(overrides)
    return MarketSpec(**payload)


def test_station_mapper_enrich_infers_station_and_populates_metadata():
    ConfigLoader._config = {
        "stations": {
            "LEMD": {
                "city": "Madrid",
                "country": "Spain",
                "timezone": "Europe/Madrid",
                "aliases": ["Madrid Airport", "Madrid"],
                "provider_mappings": {
                    "wunderground": {"airport_code": "LEMD"},
                },
            }
        }
    }
    mapper = StationMapperService()

    enriched = mapper.enrich(make_spec(question="Will Madrid hit 30C?", rules_text="Use Madrid Airport station."))

    assert enriched.station_code == "LEMD"
    assert enriched.city == "Madrid"
    assert enriched.country == "Spain"
    assert enriched.timezone == "Europe/Madrid"
    assert enriched.confidence_score == 0.9
    assert "inferred_station_code:LEMD" in enriched.notes


def test_station_mapper_adds_missing_provider_mapping_note():
    ConfigLoader._config = {
        "stations": {
            "LEMD": {
                "city": "Madrid",
                "country": "Spain",
                "timezone": "Europe/Madrid",
                "aliases": ["Madrid"],
                "provider_mappings": {},
            }
        }
    }
    mapper = StationMapperService()

    enriched = mapper.enrich(make_spec(station_code="LEMD", city="Madrid", confidence_score=0.95))

    assert "missing_provider_mapping:wunderground" in enriched.notes


def test_station_mapper_prefers_longest_alias_match():
    ConfigLoader._config = {
        "stations": {
            "KJFK": {
                "city": "New York",
                "country": "US",
                "timezone": "America/New_York",
                "aliases": ["New York", "JFK"],
                "provider_mappings": {},
            },
            "KORK": {
                "city": "York",
                "country": "US",
                "timezone": "America/New_York",
                "aliases": ["York"],
                "provider_mappings": {},
            },
        }
    }
    mapper = StationMapperService()

    inferred = mapper.infer_station_code_from_text("Highest temperature in New York tomorrow")

    assert inferred == "KJFK"


def test_station_mapper_region_policy_defaults_to_americas_and_europe():
    ConfigLoader._config = {
        "stations": {
            "LEMD": {"region": "Europe"},
            "KJFK": {"region": "North America"},
            "SAEZ": {"region": "South America"},
            "ZSPD": {"region": "Asia"},
        }
    }
    mapper = StationMapperService()

    assert mapper.is_station_allowed("LEMD") is True
    assert mapper.is_station_allowed("KJFK") is True
    assert mapper.is_station_allowed("SAEZ") is True
    assert mapper.is_station_allowed("ZSPD") is False


def test_station_mapper_region_policy_respects_configured_allowed_regions():
    ConfigLoader._config = {
        "operating_universe": {
            "allowed_regions": ["Europe", "North America"],
        },
        "stations": {
            "SAEZ": {"region": "South America"},
            "LEMD": {"region": "Europe"},
        },
    }
    mapper = StationMapperService()

    assert mapper.is_station_allowed("LEMD") is True
    assert mapper.is_station_allowed("SAEZ") is False
