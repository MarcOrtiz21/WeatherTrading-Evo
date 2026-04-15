import json
import re
from datetime import date
from typing import Any

import httpx

from weather_trading.infrastructure.utils import retry_async


class PolymarketPublicPageClient:
    """Lee páginas públicas de Polymarket y extrae los datos hidratados de Next.js."""

    def __init__(self, base_url: str = "https://polymarket.com", locale: str = "es", timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.locale = locale
        self.timeout = timeout

    @retry_async
    async def fetch_text(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    async def fetch_category_event_slugs(self, category_path: str) -> list[str]:
        html = await self.fetch_text(self._qualify_url(category_path))
        return self.extract_event_slugs(html)

    async def fetch_event_payload(self, event_slug: str) -> dict[str, Any]:
        html = await self.fetch_text(f"{self.base_url}/{self.locale}/event/{event_slug}")
        return self.parse_event_page(html)

    def extract_event_slugs(self, html: str) -> list[str]:
        matches = re.findall(r"/(?:[a-z-]+/)?event/([a-z0-9-]+)", html)
        unique_slugs = sorted(dict.fromkeys(matches))
        return unique_slugs

    def parse_event_page(self, html: str) -> dict[str, Any]:
        next_data = self._extract_next_data(html)
        page_props = next_data["props"]["pageProps"]
        queries = page_props["dehydratedState"]["queries"]

        event_state = None
        for query in queries:
            state = query["state"]["data"]
            if isinstance(state, dict) and "markets" in state and "title" in state:
                event_state = state
                break

        if event_state is None:
            raise ValueError("No se ha encontrado el payload principal del evento en la página.")

        event_slug = page_props.get("eslug") or event_state.get("slug")
        return {
            "event_slug": event_slug,
            "event_title": event_state.get("title"),
            "event_description": event_state.get("description", ""),
            "event_date": page_props.get("eventDate")
            or event_state.get("eventDate")
            or self._extract_iso_date_from_slug(event_slug),
            "series_slug": event_state.get("seriesSlug"),
            "tags": tuple(tag.get("slug") for tag in event_state.get("tags", [])),
            "markets": event_state.get("markets", []),
        }

    def _extract_next_data(self, html: str) -> dict[str, Any]:
        marker = '<script id="__NEXT_DATA__"'
        start = html.find(marker)
        if start == -1:
            raise ValueError("No se ha encontrado __NEXT_DATA__ en la página de Polymarket.")

        tag_end = html.find(">", start)
        script_end = html.find("</script>", tag_end)
        if tag_end == -1 or script_end == -1:
            raise ValueError("La página de Polymarket no contiene un bloque JSON completo.")

        return json.loads(html[tag_end + 1:script_end])

    def _qualify_url(self, value: str) -> str:
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if value.startswith("/"):
            return f"{self.base_url}{value}"
        return f"{self.base_url}/{value}"

    def _extract_iso_date_from_slug(self, event_slug: str | None) -> str | None:
        if not event_slug:
            return None

        match = re.search(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$", event_slug)
        if not match:
            return None

        month_name, day_s, year_s = match.groups()
        month = ENGLISH_MONTHS.get(month_name.lower())
        if month is None:
            return None

        return date(int(year_s), month, int(day_s)).isoformat()


ENGLISH_MONTHS = {
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
}
