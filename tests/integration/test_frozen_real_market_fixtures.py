import json
from pathlib import Path

from weather_trading.services.market_discovery.resolved_markets import find_resolved_winner_market
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService


ROOT = Path(__file__).resolve().parents[2]


def load_fixture(name: str) -> dict:
    path = ROOT / "tests" / "fixtures" / "markets" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_frozen_amsterdam_market_fixture_parses_and_maps_without_network():
    payload = load_fixture("polymarket_amsterdam_active_2026-04-05.json")
    parser = DeterministicParser()
    mapper = StationMapperService()

    market = payload["markets"][0]
    spec = parser.parse(
        {
            "id": market["id"],
            "question": market["question"],
            "description": market["description"],
            "rules": payload["description"],
            "outcomes": ("Yes", "No"),
            "event_date": payload["eventDate"],
            "resolution_source_url": market["resolutionSource"],
            "endDate": market["endDate"],
        }
    )
    enriched = mapper.enrich(spec)

    assert enriched is not None
    assert enriched.station_code == "EHAM"
    assert enriched.city == "Amsterdam"
    assert enriched.resolution_source.value == "wunderground"
    assert not enriched.requires_manual_review


def test_frozen_seoul_resolved_fixture_preserves_contract_winner():
    payload = load_fixture("polymarket_seoul_resolved_2026-04-05.json")

    winner = find_resolved_winner_market(payload["markets"])

    assert winner is not None
    assert winner["id"] == "1056401"
    assert winner["question"] == "Will the highest temperature in Seoul be -1°C or higher on December 31?"
