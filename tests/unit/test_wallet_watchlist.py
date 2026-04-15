from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.market_discovery.wallet_watchlist import WalletWatchlistService


class FakeDataClient:
    async def fetch_leaderboard(self, **kwargs):
        period = kwargs["time_period"]
        if period == "MONTH":
            return [
                {
                    "rank": "2",
                    "proxyWallet": "0xcold",
                    "userName": "ColdMath",
                    "vol": 672000,
                    "pnl": 51565,
                },
                {
                    "rank": "9",
                    "proxyWallet": "0xpoly",
                    "userName": "Poligarch",
                    "vol": 1040000,
                    "pnl": 21677,
                },
            ]
        return [
            {
                "rank": "4",
                "proxyWallet": "0xcold",
                "userName": "ColdMath",
                "vol": 7350000,
                "pnl": 105264,
            },
            {
                "rank": "20",
                "proxyWallet": "0xpoly",
                "userName": "Poligarch",
                "vol": 4630000,
                "pnl": 41564,
            },
        ]

    async def fetch_user_trades(self, *, user, limit, offset=0):
        if user == "0xcold":
            return [
                {
                    "eventSlug": "highest-temperature-in-madrid-on-april-12-2026",
                    "slug": "madrid-10-11c-apr-12",
                    "title": "Will the highest temperature in Madrid be 10-11°C on April 12?",
                    "outcome": "Yes",
                    "side": "BUY",
                    "price": 0.21,
                    "size": 15,
                    "timestamp": 1775862000,
                }
            ]
        if user == "0xpoly":
            return [
                {
                    "eventSlug": "highest-temperature-in-madrid-on-april-12-2026",
                    "slug": "madrid-12-13c-apr-12",
                    "title": "Will the highest temperature in Madrid be 12-13°C on April 12?",
                    "outcome": "Yes",
                    "side": "BUY",
                    "price": 0.31,
                    "size": 20,
                    "timestamp": 1775862100,
                }
            ]
        return []


@dataclass
class FakeRow:
    market_id: str
    market_slug: str | None
    question: str


@pytest.mark.asyncio
async def test_build_watchlist_snapshot_resolves_traders_and_groups_trades():
    ConfigLoader._config = {
        "wallet_watchlist": {
            "enabled": True,
            "category": "WEATHER",
            "leaderboard_periods": ["MONTH", "ALL"],
            "leaderboard_limit": 50,
            "recent_trade_limit": 100,
            "recent_trade_lookback_hours": 24000,
            "traders": [
                {"username": "ColdMath", "label": "ColdMath"},
                {"username": "Poligarch", "label": "Poligarch"},
            ],
        }
    }
    service = WalletWatchlistService()

    snapshot = await service.build_watchlist_snapshot(
        data_client=FakeDataClient(),
        event_slugs={"highest-temperature-in-madrid-on-april-12-2026"},
    )

    assert snapshot["enabled"] is True
    assert len(snapshot["tracked_traders"]) == 2
    assert snapshot["unresolved_entries"] == []
    assert "highest-temperature-in-madrid-on-april-12-2026" in snapshot["trades_by_event_slug"]


@pytest.mark.asyncio
async def test_build_watchlist_snapshot_excludes_future_trades():
    class FutureTradeClient(FakeDataClient):
        async def fetch_user_trades(self, *, user, limit, offset=0):
            return [
                {
                    "eventSlug": "highest-temperature-in-madrid-on-april-12-2026",
                    "slug": "madrid-10-11c-apr-12",
                    "title": "Will the highest temperature in Madrid be 10-11°C on April 12?",
                    "outcome": "Yes",
                    "side": "BUY",
                    "price": 0.21,
                    "size": 15,
                    "timestamp": 1775952000,
                }
            ]

    ConfigLoader._config = {
        "wallet_watchlist": {
            "enabled": True,
            "category": "WEATHER",
            "leaderboard_periods": ["MONTH"],
            "leaderboard_limit": 50,
            "recent_trade_limit": 100,
            "recent_trade_lookback_hours": 24000,
            "traders": [{"username": "ColdMath", "label": "ColdMath"}],
        }
    }
    service = WalletWatchlistService()

    snapshot = await service.build_watchlist_snapshot(
        data_client=FutureTradeClient(),
        event_slugs={"highest-temperature-in-madrid-on-april-12-2026"},
        as_of_utc=datetime.fromtimestamp(1775862600, tz=timezone.utc),
    )

    assert snapshot["trades_by_event_slug"] == {}


def test_summarize_event_alignment_classifies_aligned_and_opposed():
    ConfigLoader._config = {
        "wallet_watchlist": {
            "enabled": True,
            "traders": [
                {"username": "ColdMath", "label": "ColdMath"},
                {"username": "Poligarch", "label": "Poligarch"},
            ],
        }
    }
    service = WalletWatchlistService()
    service.remember_snapshot(
        {
            "trades_by_event_slug": {
                "highest-temperature-in-madrid-on-april-12-2026": [
                    {
                        "username": "ColdMath",
                        "label": "ColdMath",
                        "proxy_wallet": "0xcold",
                        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
                        "market_slug": "madrid-10-11c-apr-12",
                        "market_title": "will the highest temperature in madrid be 10-11°c on april 12?",
                        "outcome": "yes",
                        "side": "buy",
                    },
                    {
                        "username": "Poligarch",
                        "label": "Poligarch",
                        "proxy_wallet": "0xpoly",
                        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
                        "market_slug": "madrid-12-13c-apr-12",
                        "market_title": "will the highest temperature in madrid be 12-13°c on april 12?",
                        "outcome": "yes",
                        "side": "buy",
                    },
                ]
            }
        }
    )

    rows = [
        FakeRow(
            market_id="m1",
            market_slug="madrid-10-11c-apr-12",
            question="Will the highest temperature in Madrid be 10-11°C on April 12?",
        ),
        FakeRow(
            market_id="m2",
            market_slug="madrid-12-13c-apr-12",
            question="Will the highest temperature in Madrid be 12-13°C on April 12?",
        ),
    ]

    summary = service.summarize_event_alignment(
        event_slug="highest-temperature-in-madrid-on-april-12-2026",
        rows=rows,
        top_edge_market_id="m1",
    )

    assert summary["signal"] == "mixed"
    assert summary["aligned_traders"] == ["ColdMath"]
    assert summary["opposed_traders"] == ["Poligarch"]
    assert summary["match_count"] == 2


def test_summarize_event_alignment_returns_silent_without_trades():
    ConfigLoader._config = {
        "wallet_watchlist": {
            "enabled": True,
            "traders": [{"username": "ColdMath", "label": "ColdMath"}],
        }
    }
    service = WalletWatchlistService()
    service.remember_snapshot({"trades_by_event_slug": {}})

    summary = service.summarize_event_alignment(
        event_slug="highest-temperature-in-nyc-on-april-12-2026",
        rows=[],
        top_edge_market_id="m1",
    )

    assert summary["signal"] == "silent"
    assert summary["match_count"] == 0
