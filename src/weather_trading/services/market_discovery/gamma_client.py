from datetime import date
from typing import Any

import httpx

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import retry_async


class PolymarketGammaClient:
    """Cliente oficial de discovery contra el Gamma API de Polymarket."""

    def __init__(self, base_url: str | None = None, timeout: int = 30):
        self.base_url = (base_url or ConfigLoader.get("polymarket.gamma_api_url", "https://gamma-api.polymarket.com")).rstrip("/")
        self.timeout = timeout
        self.temperature_tag_id = str(ConfigLoader.get("market_discovery.temperature_tag_id", "103040"))
        self.weather_tag_id = str(ConfigLoader.get("market_discovery.weather_tag_id", "84"))
        configured_tag_ids = ConfigLoader.get(
            "market_discovery.temperature_discovery_tag_ids",
            [self.temperature_tag_id, self.weather_tag_id],
        )
        self.discovery_tag_ids = tuple(
            dict.fromkeys(str(tag_id) for tag_id in configured_tag_ids if str(tag_id).strip())
        )
        self.page_size = int(ConfigLoader.get("market_discovery.gamma_page_size", 100))
        self.max_pages = int(ConfigLoader.get("market_discovery.gamma_max_pages", 20))

    @retry_async
    async def fetch_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = path if path.startswith("http://") or path.startswith("https://") else f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    async def fetch_events_page(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit or self.page_size,
            "offset": offset,
            "tag_id": tag_id or self.temperature_tag_id,
        }
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        if order:
            params["order"] = order
        if ascending is not None:
            params["ascending"] = str(ascending).lower()
        payload = await self.fetch_json("/events", params=params)
        return payload if isinstance(payload, list) else []

    async def fetch_event_by_slug(self, event_slug: str) -> dict[str, Any]:
        payload = await self.fetch_json(f"/events/slug/{event_slug}")
        return self.normalize_event_payload(payload)

    async def discover_temperature_event_payloads(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        max_pages: int | None = None,
        tag_ids: tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for tag_id in tag_ids or self.discovery_tag_ids:
            payloads.extend(
                await self._discover_temperature_event_payloads_for_tag(
                    active=active,
                    closed=closed,
                    start_date=start_date,
                    end_date=end_date,
                    max_pages=max_pages,
                    tag_id=tag_id,
                )
            )

        unique_payloads = {
            payload["event_slug"]: payload
            for payload in payloads
        }
        return sorted(unique_payloads.values(), key=lambda payload: (payload["event_date"] or "", payload["event_slug"]))

    async def _discover_temperature_event_payloads_for_tag(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        max_pages: int | None = None,
        tag_id: str,
    ) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        pages = max_pages or self.max_pages
        order = "endDate"
        ascending = False if closed else True

        for page_index in range(pages):
            events = await self.fetch_events_page(
                active=active,
                closed=closed,
                tag_id=tag_id,
                offset=page_index * self.page_size,
                order=order,
                ascending=ascending,
            )
            if not events:
                break

            page_payloads: list[dict[str, Any]] = []
            for event in events:
                payload = self.normalize_event_payload(event)
                if not self._is_supported_temperature_event(payload):
                    continue
                event_day = self.parse_event_date(payload)
                if event_day is None:
                    continue
                if start_date and event_day < start_date:
                    continue
                if end_date and event_day > end_date:
                    continue
                page_payloads.append(payload)

            payloads.extend(page_payloads)

            if start_date and closed:
                normalized_dates = [self.parse_event_date(self.normalize_event_payload(event)) for event in events]
                valid_dates = [event_day for event_day in normalized_dates if event_day is not None]
                if valid_dates and min(valid_dates) < start_date:
                    break
            if end_date and not closed:
                normalized_dates = [self.parse_event_date(self.normalize_event_payload(event)) for event in events]
                valid_dates = [event_day for event_day in normalized_dates if event_day is not None]
                if valid_dates and max(valid_dates) > end_date:
                    break

        return payloads

    def normalize_event_payload(self, event: dict[str, Any]) -> dict[str, Any]:
        event_slug = event.get("slug") or ""
        return {
            "event_id": str(event.get("id") or ""),
            "event_slug": event_slug,
            "event_title": event.get("title"),
            "event_description": event.get("description", ""),
            "event_date": event.get("eventDate")
            or self._parse_iso_date_prefix(event.get("endDate"))
            or self._extract_iso_date_from_slug(event_slug),
            "resolution_source_url": event.get("resolutionSource") or "",
            "active": event.get("active"),
            "closed": event.get("closed"),
            "archived": event.get("archived"),
            "series_slug": event.get("seriesSlug") or event.get("series", [{}])[0].get("slug"),
            "tags": tuple(tag.get("slug") for tag in event.get("tags", []) if tag.get("slug")),
            "markets": event.get("markets", []),
        }

    def parse_event_date(self, payload: dict[str, Any]) -> date | None:
        value = payload.get("event_date")
        if not value:
            return None
        return date.fromisoformat(value)

    def _parse_iso_date_prefix(self, value: str | None) -> str | None:
        if not value:
            return None
        prefix = value[:10]
        try:
            return date.fromisoformat(prefix).isoformat()
        except ValueError:
            return None

    def _extract_iso_date_from_slug(self, event_slug: str | None) -> str | None:
        if not event_slug:
            return None

        import re

        match = re.search(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$", event_slug)
        if not match:
            return None

        month_name, day_s, year_s = match.groups()
        month = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }.get(month_name.lower())
        if month is None:
            return None
        return date(int(year_s), month, int(day_s)).isoformat()

    def _is_supported_temperature_event(self, payload: dict[str, Any]) -> bool:
        event_slug = (payload.get("event_slug") or "").lower()
        event_title = (payload.get("event_title") or "").lower()
        return event_slug.startswith("highest-temperature-in-") or event_title.startswith("highest temperature in ")
