from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from weather_trading.infrastructure.config import ConfigLoader


@dataclass(slots=True)
class WatchlistTrader:
    username: str
    label: str
    proxy_wallet: str
    monthly_rank: int | None = None
    monthly_pnl: float | None = None
    monthly_volume: float | None = None
    all_time_rank: int | None = None
    all_time_pnl: float | None = None
    all_time_volume: float | None = None


@dataclass(slots=True)
class WatchlistTrade:
    username: str
    label: str
    proxy_wallet: str
    event_slug: str
    market_slug: str | None
    market_title: str | None
    outcome: str | None
    side: str | None
    price: float | None
    size: float | None
    timestamp: int | None


class WalletWatchlistService:
    """Sigue wallets de referencia y las cruza con nuestros eventos live."""

    def __init__(self) -> None:
        self.config = ConfigLoader.get("wallet_watchlist", {}) or {}

    def is_enabled(self) -> bool:
        return bool(self.config.get("enabled", False))

    def get_entries(self) -> list[dict[str, Any]]:
        raw_entries = self.config.get("traders", [])
        if not isinstance(raw_entries, list):
            return []
        normalized: list[dict[str, Any]] = []
        for entry in raw_entries:
            if isinstance(entry, str):
                normalized.append({"username": entry, "label": entry})
            elif isinstance(entry, dict) and entry.get("username"):
                normalized.append(
                    {
                        "username": str(entry["username"]).strip(),
                        "label": str(entry.get("label") or entry["username"]).strip(),
                    }
                )
        return [entry for entry in normalized if entry["username"]]

    def get_recent_trade_limit(self) -> int:
        return int(self.config.get("recent_trade_limit", 150))

    def get_recent_trade_pages(self) -> int:
        return int(self.config.get("recent_trade_pages", 8))

    def get_recent_trade_lookback_hours(self) -> int:
        return int(self.config.get("recent_trade_lookback_hours", 168))

    def get_leaderboard_periods(self) -> tuple[str, ...]:
        raw = self.config.get("leaderboard_periods", ("MONTH", "ALL"))
        if not isinstance(raw, (list, tuple)):
            return ("MONTH", "ALL")
        normalized = tuple(str(item).strip().upper() for item in raw if str(item).strip())
        return normalized or ("MONTH", "ALL")

    def get_leaderboard_limit(self) -> int:
        return int(self.config.get("leaderboard_limit", 50))

    def get_category(self) -> str:
        return str(self.config.get("category", "WEATHER")).strip().upper()

    async def build_watchlist_snapshot(
        self,
        *,
        data_client,
        event_slugs: set[str],
        as_of_utc: datetime | None = None,
    ) -> dict[str, Any]:
        if not self.is_enabled() or not event_slugs:
            return {
                "enabled": self.is_enabled(),
                "tracked_traders": [],
                "unresolved_entries": [],
                "trades_by_event_slug": {},
                "recent_trade_limit": self.get_recent_trade_limit(),
                "recent_trade_lookback_hours": self.get_recent_trade_lookback_hours(),
            }

        leaderboard_rows_by_period = await self._fetch_leaderboard_rows(data_client)
        traders, unresolved = self._resolve_traders(leaderboard_rows_by_period)

        upper_bound = as_of_utc or datetime.now(timezone.utc)
        if upper_bound.tzinfo is None:
            upper_bound = upper_bound.replace(tzinfo=timezone.utc)
        cutoff = upper_bound - timedelta(hours=self.get_recent_trade_lookback_hours())
        grouped_trades: dict[str, list[WatchlistTrade]] = defaultdict(list)
        for trader in traders:
            payloads = await self._fetch_recent_trades_for_trader(
                data_client=data_client,
                proxy_wallet=trader.proxy_wallet,
                cutoff=cutoff,
                upper_bound=upper_bound,
            )
            for payload in payloads:
                event_slug = str(payload.get("eventSlug") or "").strip()
                if event_slug not in event_slugs:
                    continue
                timestamp = self._to_int(payload.get("timestamp"))
                if timestamp is not None:
                    trade_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    if trade_dt < cutoff or trade_dt > upper_bound:
                        continue
                grouped_trades[event_slug].append(
                    WatchlistTrade(
                        username=trader.username,
                        label=trader.label,
                        proxy_wallet=trader.proxy_wallet,
                        event_slug=event_slug,
                        market_slug=self._normalized_text(payload.get("slug")),
                        market_title=self._normalized_text(payload.get("title")),
                        outcome=self._normalized_text(payload.get("outcome")),
                        side=self._normalized_text(payload.get("side")),
                        price=self._to_float(payload.get("price")),
                        size=self._to_float(payload.get("size")),
                        timestamp=timestamp,
                    )
                )

        return {
            "enabled": True,
            "category": self.get_category(),
            "tracked_traders": [asdict(trader) for trader in traders],
            "unresolved_entries": unresolved,
            "recent_trade_limit": self.get_recent_trade_limit(),
            "recent_trade_pages": self.get_recent_trade_pages(),
            "recent_trade_lookback_hours": self.get_recent_trade_lookback_hours(),
            "trades_by_event_slug": {
                event_slug: [asdict(trade) for trade in trades]
                for event_slug, trades in sorted(grouped_trades.items())
            },
        }

    def summarize_event_alignment(
        self,
        *,
        event_slug: str,
        rows: list[Any],
        top_edge_market_id: str,
    ) -> dict[str, Any]:
        trades = self.configured_trades_for_event(event_slug)
        if not trades:
            return {
                "signal": "silent",
                "alignment_score": 0.0,
                "match_count": 0,
                "active_traders": [],
                "aligned_traders": [],
                "opposed_traders": [],
                "event_only_traders": [],
                "trades": [],
            }

        market_slug_map = {
            self._normalized_text(getattr(row, "market_slug", None)): row
            for row in rows
            if self._normalized_text(getattr(row, "market_slug", None))
        }
        question_map = {
            self._normalized_text(getattr(row, "question", None)): row
            for row in rows
            if self._normalized_text(getattr(row, "question", None))
        }

        aligned_traders: set[str] = set()
        opposed_traders: set[str] = set()
        event_only_traders: set[str] = set()
        classified: list[dict[str, Any]] = []

        for trade in trades:
            matched_row = None
            market_slug = self._normalized_text(trade.get("market_slug"))
            market_title = self._normalized_text(trade.get("market_title"))
            if market_slug and market_slug in market_slug_map:
                matched_row = market_slug_map[market_slug]
            elif market_title and market_title in question_map:
                matched_row = question_map[market_title]

            classification = self._classify_trade(trade, matched_row, top_edge_market_id)
            trader_label = str(trade.get("label") or trade.get("username") or trade.get("proxy_wallet"))
            if classification == "aligned":
                aligned_traders.add(trader_label)
            elif classification == "opposed":
                opposed_traders.add(trader_label)
            elif classification == "event_only":
                event_only_traders.add(trader_label)

            classified.append(
                {
                    **trade,
                    "matched_market_id": None if matched_row is None else getattr(matched_row, "market_id", None),
                    "classification": classification,
                }
            )

        directional_count = len(aligned_traders) + len(opposed_traders)
        alignment_score = (
            (len(aligned_traders) - len(opposed_traders)) / directional_count
            if directional_count
            else 0.0
        )
        if len(aligned_traders) and not len(opposed_traders):
            signal = "aligned"
        elif len(opposed_traders) and not len(aligned_traders):
            signal = "opposed"
        elif len(aligned_traders) or len(opposed_traders):
            signal = "mixed"
        else:
            signal = "active_unclassified"

        return {
            "signal": signal,
            "alignment_score": alignment_score,
            "match_count": len(classified),
            "active_traders": sorted(
                {
                    str(trade.get("label") or trade.get("username") or trade.get("proxy_wallet"))
                    for trade in classified
                }
            ),
            "aligned_traders": sorted(aligned_traders),
            "opposed_traders": sorted(opposed_traders),
            "event_only_traders": sorted(event_only_traders),
            "trades": classified,
        }

    def configured_trades_for_event(self, event_slug: str) -> list[dict[str, Any]]:
        snapshot = getattr(self, "_latest_snapshot", None) or {}
        by_event = snapshot.get("trades_by_event_slug", {})
        trades = by_event.get(event_slug, [])
        return trades if isinstance(trades, list) else []

    def remember_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._latest_snapshot = snapshot

    async def _fetch_recent_trades_for_trader(
        self,
        *,
        data_client,
        proxy_wallet: str,
        cutoff: datetime,
        upper_bound: datetime,
    ) -> list[dict[str, Any]]:
        aggregated: list[dict[str, Any]] = []
        limit = self.get_recent_trade_limit()
        max_pages = self.get_recent_trade_pages()

        for page_index in range(max_pages):
            payloads = await data_client.fetch_user_trades(
                user=proxy_wallet,
                limit=limit,
                offset=page_index * limit,
            )
            if not payloads:
                break
            aggregated.extend(payloads)

            timestamps = [
                datetime.fromtimestamp(timestamp, tz=timezone.utc)
                for timestamp in (self._to_int(payload.get("timestamp")) for payload in payloads)
                if timestamp is not None
            ]
            if timestamps and min(timestamps) < cutoff:
                break
            if len(payloads) < limit:
                break

        return aggregated

    async def _fetch_leaderboard_rows(self, data_client) -> dict[str, list[dict[str, Any]]]:
        period_rows: dict[str, list[dict[str, Any]]] = {}
        for period in self.get_leaderboard_periods():
            rows = await data_client.fetch_leaderboard(
                category=self.get_category(),
                time_period=period,
                order_by="PNL",
                limit=self.get_leaderboard_limit(),
            )
            period_rows[period] = rows
        return period_rows

    def _resolve_traders(
        self,
        leaderboard_rows_by_period: dict[str, list[dict[str, Any]]],
    ) -> tuple[list[WatchlistTrader], list[dict[str, Any]]]:
        rows_by_username: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for period, rows in leaderboard_rows_by_period.items():
            for row in rows:
                username = self._normalized_text(row.get("userName"))
                if username:
                    rows_by_username[username][period] = row

        traders: list[WatchlistTrader] = []
        unresolved: list[dict[str, Any]] = []
        for entry in self.get_entries():
            username_key = self._normalized_text(entry["username"])
            period_rows = rows_by_username.get(username_key, {})
            monthly = period_rows.get("MONTH", {})
            all_time = period_rows.get("ALL", {})
            canonical = monthly or all_time
            proxy_wallet = str(canonical.get("proxyWallet") or "").strip()
            if not proxy_wallet:
                unresolved.append(entry)
                continue
            traders.append(
                WatchlistTrader(
                    username=str(canonical.get("userName") or entry["username"]),
                    label=entry["label"],
                    proxy_wallet=proxy_wallet,
                    monthly_rank=self._to_int(monthly.get("rank")),
                    monthly_pnl=self._to_float(monthly.get("pnl")),
                    monthly_volume=self._to_float(monthly.get("vol")),
                    all_time_rank=self._to_int(all_time.get("rank")),
                    all_time_pnl=self._to_float(all_time.get("pnl")),
                    all_time_volume=self._to_float(all_time.get("vol")),
                )
            )
        return traders, unresolved

    def _classify_trade(self, trade: dict[str, Any], matched_row: Any | None, top_edge_market_id: str) -> str:
        if matched_row is None:
            return "event_only"

        bias = self._infer_yes_bias(trade)
        if bias == 0:
            return "event_only"

        if getattr(matched_row, "market_id", None) == top_edge_market_id:
            return "aligned" if bias > 0 else "opposed"

        return "opposed" if bias > 0 else "event_only"

    def _infer_yes_bias(self, trade: dict[str, Any]) -> int:
        outcome = self._normalized_text(trade.get("outcome"))
        side = self._normalized_text(trade.get("side"))
        if outcome == "yes" and side == "buy":
            return 1
        if outcome == "yes" and side == "sell":
            return -1
        if outcome == "no" and side == "buy":
            return -1
        if outcome == "no" and side == "sell":
            return 1
        return 0

    def _normalized_text(self, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return " ".join(str(value).strip().lower().split())

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
