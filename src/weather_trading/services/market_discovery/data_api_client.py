from typing import Any

import httpx

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import retry_async


class PolymarketDataApiClient:
    """Cliente ligero para consultar el Data API público de Polymarket."""

    def __init__(self, base_url: str | None = None, timeout: int = 30):
        self.base_url = (
            base_url or ConfigLoader.get("polymarket.data_api_url", "https://data-api.polymarket.com")
        ).rstrip("/")
        self.timeout = timeout

    @retry_async
    async def fetch_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = path if path.startswith("http://") or path.startswith("https://") else f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    async def fetch_leaderboard(
        self,
        *,
        category: str = "WEATHER",
        time_period: str = "MONTH",
        order_by: str = "PNL",
        limit: int = 25,
        offset: int = 0,
        user: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "category": category,
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset,
        }
        if user:
            params["user"] = user
        payload = await self.fetch_json("/v1/leaderboard", params=params)
        return payload if isinstance(payload, list) else []

    async def fetch_user_trades(
        self,
        *,
        user: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        payload = await self.fetch_json(
            "/trades",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )
        return payload if isinstance(payload, list) else []

    async def fetch_user_positions(
        self,
        *,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        payload = await self.fetch_json(
            "/positions",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )
        return payload if isinstance(payload, list) else []

    async def fetch_user_closed_positions(
        self,
        *,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        payload = await self.fetch_json(
            "/closed-positions",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )
        return payload if isinstance(payload, list) else []

    async def fetch_user_activity(
        self,
        *,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        payload = await self.fetch_json(
            "/activity",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )
        return payload if isinstance(payload, list) else []

    async def fetch_user_value(self, *, user: str) -> dict[str, Any]:
        payload = await self.fetch_json(
            "/value",
            params={
                "user": user,
            },
        )
        return payload if isinstance(payload, dict) else {}
