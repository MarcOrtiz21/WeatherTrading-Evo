from datetime import date

import pytest

from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient


def test_normalize_event_payload_falls_back_to_end_date():
    client = PolymarketGammaClient()

    payload = client.normalize_event_payload(
        {
            "slug": "highest-temperature-in-hong-kong-on-april-4-2026",
            "title": "Highest temperature in Hong Kong on April 4?",
            "description": "Resolution source.",
            "endDate": "2026-04-04T12:00:00Z",
            "tags": [{"slug": "weather"}, {"slug": "temperature"}],
            "markets": [{"id": "m1", "question": "Q1"}],
        }
    )

    assert payload["event_date"] == "2026-04-04"
    assert payload["tags"] == ("weather", "temperature")
    assert payload["markets"][0]["question"] == "Q1"
    assert payload["resolution_source_url"] == ""
    assert payload["active"] is None
    assert payload["closed"] is None


def test_normalize_event_payload_preserves_resolution_metadata():
    client = PolymarketGammaClient()

    payload = client.normalize_event_payload(
        {
            "id": "e1",
            "slug": "highest-temperature-in-hong-kong-on-april-4-2026",
            "title": "HK",
            "eventDate": "2026-04-04",
            "resolutionSource": "https://wunderground.com/example",
            "active": False,
            "closed": True,
            "archived": True,
            "markets": [],
        }
    )

    assert payload["event_id"] == "e1"
    assert payload["resolution_source_url"] == "https://wunderground.com/example"
    assert payload["active"] is False
    assert payload["closed"] is True
    assert payload["archived"] is True


@pytest.mark.asyncio
async def test_discover_temperature_event_payloads_filters_by_range_and_prefix(monkeypatch):
    client = PolymarketGammaClient()

    async def fake_fetch_events_page(**kwargs):
        offset = kwargs["offset"]
        tag_id = kwargs["tag_id"]
        if offset == 0 and tag_id == client.temperature_tag_id:
            return [
                {
                    "slug": "highest-temperature-in-hong-kong-on-april-4-2026",
                    "title": "HK",
                    "eventDate": "2026-04-04",
                    "markets": [],
                    "tags": [{"slug": "temperature"}],
                },
                {
                    "slug": "will-bitcoin-hit-100k",
                    "title": "BTC",
                    "eventDate": "2026-04-04",
                    "markets": [],
                    "tags": [{"slug": "crypto"}],
                },
            ]
        if offset == 0 and tag_id == client.weather_tag_id:
            return [
                {
                    "slug": "highest-temperature-in-hong-kong-on-april-4-2026",
                    "title": "HK duplicate",
                    "eventDate": "2026-04-04",
                    "markets": [],
                    "tags": [{"slug": "weather"}],
                },
                {
                    "slug": "will-it-rain-in-london",
                    "title": "Rain",
                    "eventDate": "2026-04-04",
                    "markets": [],
                    "tags": [{"slug": "weather"}],
                },
            ]
        return []

    monkeypatch.setattr(client, "fetch_events_page", fake_fetch_events_page)

    payloads = await client.discover_temperature_event_payloads(
        active=None,
        closed=True,
        start_date=date(2026, 4, 4),
        end_date=date(2026, 4, 4),
        max_pages=2,
    )

    assert [payload["event_slug"] for payload in payloads] == [
        "highest-temperature-in-hong-kong-on-april-4-2026"
    ]
