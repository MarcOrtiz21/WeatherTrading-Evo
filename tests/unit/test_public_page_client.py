import json

from weather_trading.services.market_discovery.public_page_client import PolymarketPublicPageClient


def test_extract_event_slugs_from_category_html():
    html = """
    <a href="/es/event/highest-temperature-in-madrid-on-april-6-2026">Madrid</a>
    <a href="/event/highest-temperature-in-london-on-april-6-2026">London</a>
    <a href="/es/event/highest-temperature-in-madrid-on-april-6-2026">Madrid duplicated</a>
    """

    client = PolymarketPublicPageClient()
    slugs = client.extract_event_slugs(html)

    assert slugs == [
        "highest-temperature-in-london-on-april-6-2026",
        "highest-temperature-in-madrid-on-april-6-2026",
    ]


def test_parse_event_page_extracts_event_payload():
    payload = {
        "props": {
            "pageProps": {
                "eslug": "highest-temperature-in-madrid-on-april-6-2026",
                "eventDate": "2026-04-06",
                "dehydratedState": {
                    "queries": [
                        {
                            "state": {
                                "data": {
                                    "title": "Highest temperature in Madrid on April 6?",
                                    "description": "Resolution source: Wunderground for Madrid.",
                                    "tags": [{"slug": "weather"}, {"slug": "temperature"}],
                                    "markets": [
                                        {
                                            "id": "market-1",
                                            "question": "Will the highest temperature in Madrid be 21°C on April 6?",
                                            "bestBid": 0.21,
                                            "bestAsk": 0.24,
                                        }
                                    ],
                                }
                            }
                        }
                    ]
                },
            }
        }
    }
    html = (
        '<html><body><script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">'
        f"{json.dumps(payload)}"
        "</script></body></html>"
    )

    client = PolymarketPublicPageClient()
    parsed = client.parse_event_page(html)

    assert parsed["event_slug"] == "highest-temperature-in-madrid-on-april-6-2026"
    assert parsed["event_title"] == "Highest temperature in Madrid on April 6?"
    assert parsed["event_date"] == "2026-04-06"
    assert parsed["tags"] == ("weather", "temperature")
    assert parsed["markets"][0]["question"] == "Will the highest temperature in Madrid be 21°C on April 6?"


def test_parse_event_page_falls_back_to_date_in_slug():
    payload = {
        "props": {
            "pageProps": {
                "eslug": "highest-temperature-in-london-on-april-9-2026",
                "dehydratedState": {
                    "queries": [
                        {
                            "state": {
                                "data": {
                                    "title": "Highest temperature in London on April 9?",
                                    "description": "Resolution source: Wunderground for London.",
                                    "tags": [{"slug": "weather"}],
                                    "markets": [],
                                }
                            }
                        }
                    ]
                },
            }
        }
    }
    html = (
        '<html><body><script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">'
        f"{json.dumps(payload)}"
        "</script></body></html>"
    )

    client = PolymarketPublicPageClient()
    parsed = client.parse_event_page(html)

    assert parsed["event_date"] == "2026-04-09"
