import pytest

from weather_trading.services.market_discovery.data_api_client import PolymarketDataApiClient


@pytest.mark.asyncio
async def test_fetch_leaderboard_returns_list(monkeypatch):
    client = PolymarketDataApiClient()

    async def fake_fetch_json(path, params=None):
        assert path == "/v1/leaderboard"
        assert params["category"] == "WEATHER"
        assert params["timePeriod"] == "MONTH"
        return [{"userName": "ColdMath", "proxyWallet": "0xabc"}]

    monkeypatch.setattr(client, "fetch_json", fake_fetch_json)

    rows = await client.fetch_leaderboard(category="WEATHER", time_period="MONTH", limit=10)

    assert rows == [{"userName": "ColdMath", "proxyWallet": "0xabc"}]


@pytest.mark.asyncio
async def test_fetch_user_trades_returns_list(monkeypatch):
    client = PolymarketDataApiClient()

    async def fake_fetch_json(path, params=None):
        assert path == "/trades"
        assert params["user"] == "0xabc"
        assert params["offset"] == 0
        return [{"eventSlug": "highest-temperature-in-madrid-on-april-12-2026"}]

    monkeypatch.setattr(client, "fetch_json", fake_fetch_json)

    rows = await client.fetch_user_trades(user="0xabc", limit=25)

    assert rows == [{"eventSlug": "highest-temperature-in-madrid-on-april-12-2026"}]
