import argparse
import asyncio
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.market_discovery.data_api_client import PolymarketDataApiClient
from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Perfila el comportamiento operativo de un trader de Polymarket usando la Data API oficial."
    )
    parser.add_argument("--username", default="ColdMath", help="Username exacto del trader en Polymarket.")
    parser.add_argument("--category", default="WEATHER", help="Categoria de leaderboard para resolver el proxy wallet.")
    parser.add_argument("--reference-date", default=date.today().isoformat(), help="Fecha YYYY-MM-DD del snapshot.")
    parser.add_argument("--leaderboard-limit", type=int, default=100)
    parser.add_argument("--trade-limit", type=int, default=200)
    parser.add_argument("--trade-pages", type=int, default=10)
    parser.add_argument("--position-limit", type=int, default=200)
    parser.add_argument("--activity-limit", type=int, default=200)
    parser.add_argument("--lookback-days", type=int, default=60)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    data_api = PolymarketDataApiClient()
    gamma = PolymarketGammaClient()

    profile = await build_trader_profile(
        data_api=data_api,
        gamma=gamma,
        username=args.username,
        category=args.category,
        leaderboard_limit=args.leaderboard_limit,
        trade_limit=args.trade_limit,
        trade_pages=args.trade_pages,
        position_limit=args.position_limit,
        activity_limit=args.activity_limit,
        lookback_days=args.lookback_days,
    )

    output_path = persist_snapshot(reference_date, args.username, profile)
    print(f"Perfil guardado en: {output_path}")
    print("")
    print("=== PERFIL DE TRADER ===")
    print(
        f"{profile['username']} | wallet={profile['proxy_wallet']} | "
        f"trades_recientes={profile['recent_trades_summary']['trade_count']} | "
        f"eventos={profile['recent_trades_summary']['unique_event_count']} | "
        f"notional_medio={profile['recent_trades_summary']['avg_notional_usd']:.2f} USD"
    )
    print(
        f"Anticipacion media={profile['timing_summary']['avg_hours_before_event']:.1f}h | "
        f"mediana={profile['timing_summary']['median_hours_before_event']:.1f}h | "
        f"same_day={profile['timing_summary']['same_day_share']:.1%}"
    )
    print(
        f"Sesgo operativo: yes_buy={profile['trade_direction_summary']['yes_buy_share']:.1%} | "
        f"no_buy={profile['trade_direction_summary']['no_buy_share']:.1%} | "
        f"sell_total={profile['trade_direction_summary']['sell_share']:.1%}"
    )


async def build_trader_profile(
    *,
    data_api: PolymarketDataApiClient,
    gamma: PolymarketGammaClient,
    username: str,
    category: str,
    leaderboard_limit: int,
    trade_limit: int,
    trade_pages: int,
    position_limit: int,
    activity_limit: int,
    lookback_days: int,
) -> dict[str, Any]:
    resolved = await resolve_trader_identity(
        data_api=data_api,
        username=username,
        category=category,
        leaderboard_limit=leaderboard_limit,
    )
    proxy_wallet = resolved["proxy_wallet"]

    trades = await fetch_paginated_trades(
        data_api=data_api,
        user=proxy_wallet,
        trade_limit=trade_limit,
        trade_pages=trade_pages,
        lookback_days=lookback_days,
    )
    positions = await data_api.fetch_user_positions(user=proxy_wallet, limit=position_limit)
    closed_positions = await data_api.fetch_user_closed_positions(user=proxy_wallet, limit=position_limit)
    activity = await data_api.fetch_user_activity(user=proxy_wallet, limit=activity_limit)
    value = await data_api.fetch_user_value(user=proxy_wallet)

    event_dates = build_event_date_map(gamma=gamma, event_slugs={slug for slug in extract_event_slugs(trades)})
    enriched_trades = [enrich_trade(trade, event_dates.get(str(trade.get("eventSlug") or ""))) for trade in trades]

    return {
        "captured_at_utc": utc_now().isoformat(),
        "username": resolved["username"],
        "label": resolved["label"],
        "proxy_wallet": proxy_wallet,
        "category": category,
        "leaderboard": resolved["leaderboard"],
        "recent_trades_summary": summarize_recent_trades(enriched_trades),
        "trade_direction_summary": summarize_trade_direction(enriched_trades),
        "timing_summary": summarize_trade_timing(enriched_trades),
        "market_family_summary": summarize_market_families(enriched_trades),
        "current_positions_summary": summarize_positions(positions),
        "closed_positions_summary": summarize_closed_positions(closed_positions),
        "activity_summary": summarize_activity(activity),
        "portfolio_value_summary": summarize_portfolio_value(value),
        "top_events_by_trade_count": top_counter(
            Counter(
                str(trade.get("eventSlug") or "unknown")
                for trade in enriched_trades
                if trade.get("eventSlug")
            )
        ),
        "top_markets_by_trade_count": top_counter(
            Counter(
                str(trade.get("slug") or "unknown")
                for trade in enriched_trades
                if trade.get("slug")
            )
        ),
        "top_events_by_notional": top_notional_events(enriched_trades),
        "notes": [
            "avg_notional_usd se deriva como price * size cuando ambos campos existen",
            "timing_summary usa eventSlug/eventDate oficial cuando existe y fallback por slug en eventos de temperatura",
            "closed_positions/current_positions dependen del payload actual de la Data API publica",
        ],
    }


async def resolve_trader_identity(
    *,
    data_api: PolymarketDataApiClient,
    username: str,
    category: str,
    leaderboard_limit: int,
) -> dict[str, Any]:
    username_norm = normalize_text(username)
    periods = ("MONTH", "ALL")
    rows_by_period: dict[str, dict[str, Any]] = {}

    for period in periods:
        rows = await data_api.fetch_leaderboard(
            category=category,
            time_period=period,
            order_by="PNL",
            limit=leaderboard_limit,
        )
        matched = next(
            (row for row in rows if normalize_text(row.get("userName")) == username_norm),
            None,
        )
        if matched:
            rows_by_period[period] = matched

    canonical = rows_by_period.get("MONTH") or rows_by_period.get("ALL")
    if not canonical:
        raise SystemExit(f"No he podido resolver al trader '{username}' en el leaderboard oficial de {category}.")

    return {
        "username": str(canonical.get("userName") or username),
        "label": str(canonical.get("userName") or username),
        "proxy_wallet": str(canonical.get("proxyWallet") or ""),
        "leaderboard": {
            "monthly": summarize_leaderboard_row(rows_by_period.get("MONTH")),
            "all_time": summarize_leaderboard_row(rows_by_period.get("ALL")),
        },
    }


def summarize_leaderboard_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "rank": to_int(row.get("rank")),
        "pnl": to_float(row.get("pnl")),
        "volume": to_float(row.get("vol")),
        "proxy_wallet": row.get("proxyWallet"),
    }


async def fetch_paginated_trades(
    *,
    data_api: PolymarketDataApiClient,
    user: str,
    trade_limit: int,
    trade_pages: int,
    lookback_days: int,
) -> list[dict[str, Any]]:
    cutoff_ts = int((datetime.now(timezone.utc).timestamp()) - lookback_days * 24 * 3600)
    aggregated: list[dict[str, Any]] = []
    for page in range(trade_pages):
        payload = await data_api.fetch_user_trades(
            user=user,
            limit=trade_limit,
            offset=page * trade_limit,
        )
        if not payload:
            break
        aggregated.extend(payload)
        timestamps = [to_int(item.get("timestamp")) for item in payload]
        valid_timestamps = [value for value in timestamps if value is not None]
        if valid_timestamps and min(valid_timestamps) < cutoff_ts:
            break
        if len(payload) < trade_limit:
            break

    filtered = [
        trade
        for trade in aggregated
        if (to_int(trade.get("timestamp")) or 0) >= cutoff_ts
    ]
    filtered.sort(key=lambda trade: to_int(trade.get("timestamp")) or 0, reverse=True)
    return filtered


def extract_event_slugs(trades: list[dict[str, Any]]) -> list[str]:
    seen = []
    added = set()
    for trade in trades:
        event_slug = str(trade.get("eventSlug") or "").strip()
        if event_slug and event_slug not in added:
            added.add(event_slug)
            seen.append(event_slug)
    return seen


def build_event_date_map(*, gamma: PolymarketGammaClient, event_slugs: set[str]) -> dict[str, str | None]:
    event_dates: dict[str, str | None] = {}
    for slug in sorted(event_slugs):
        event_date = gamma._extract_iso_date_from_slug(slug)  # reuse the deterministic slug parser we already trust
        event_dates[slug] = event_date
    return event_dates


def enrich_trade(trade: dict[str, Any], event_date_iso: str | None) -> dict[str, Any]:
    enriched = dict(trade)
    price = to_float(trade.get("price"))
    size = to_float(trade.get("size"))
    enriched["derived_notional_usd"] = None if price is None or size is None else price * size
    enriched["event_date"] = event_date_iso
    enriched["trade_datetime_utc"] = timestamp_to_iso(trade.get("timestamp"))
    enriched["hours_before_event"] = compute_hours_before_event(trade.get("timestamp"), event_date_iso)
    enriched["event_family"] = infer_event_family(trade)
    enriched["unit"] = infer_temperature_unit_from_text(
        str(trade.get("title") or trade.get("slug") or "")
    )
    return enriched


def summarize_recent_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    notionals = [float(value) for value in (trade.get("derived_notional_usd") for trade in trades) if value is not None]
    sizes = [float(value) for value in (trade.get("size") for trade in trades) if to_float(value) is not None]
    prices = [float(value) for value in (trade.get("price") for trade in trades) if to_float(value) is not None]
    return {
        "trade_count": len(trades),
        "unique_event_count": len({trade.get("eventSlug") for trade in trades if trade.get("eventSlug")}),
        "unique_market_count": len({trade.get("slug") for trade in trades if trade.get("slug")}),
        "avg_notional_usd": safe_mean(notionals),
        "median_notional_usd": safe_median(notionals),
        "avg_size": safe_mean(sizes),
        "median_size": safe_median(sizes),
        "avg_price": safe_mean(prices),
        "median_price": safe_median(prices),
    }


def summarize_trade_direction(trades: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(trades) or 1
    breakdown = Counter(
        f"{normalize_text(trade.get('outcome')) or 'unknown'}|{normalize_text(trade.get('side')) or 'unknown'}"
        for trade in trades
    )
    sell_count = sum(count for key, count in breakdown.items() if key.endswith("|sell"))
    return {
        "breakdown": dict(sorted(breakdown.items())),
        "yes_buy_share": breakdown.get("yes|buy", 0) / total,
        "no_buy_share": breakdown.get("no|buy", 0) / total,
        "sell_share": sell_count / total,
    }


def summarize_trade_timing(trades: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(value) for value in (trade.get("hours_before_event") for trade in trades) if value is not None]
    total = len(values) or 1
    same_day = sum(1 for value in values if value < 24)
    one_to_three = sum(1 for value in values if 24 <= value < 72)
    more_than_three = sum(1 for value in values if value >= 72)
    by_hour = Counter(
        datetime.fromisoformat(str(trade["trade_datetime_utc"]).replace("Z", "+00:00")).hour
        for trade in trades
        if trade.get("trade_datetime_utc")
    )
    by_weekday = Counter(
        datetime.fromisoformat(str(trade["trade_datetime_utc"]).replace("Z", "+00:00")).strftime("%A")
        for trade in trades
        if trade.get("trade_datetime_utc")
    )
    return {
        "avg_hours_before_event": safe_mean(values),
        "median_hours_before_event": safe_median(values),
        "same_day_share": same_day / total,
        "one_to_three_day_share": one_to_three / total,
        "more_than_three_day_share": more_than_three / total,
        "by_hour_utc": dict(sorted(by_hour.items())),
        "by_weekday_utc": dict(sorted(by_weekday.items())),
    }


def summarize_market_families(trades: list[dict[str, Any]]) -> dict[str, Any]:
    family_counter = Counter(str(trade.get("event_family") or "unknown") for trade in trades)
    unit_counter = Counter(str(trade.get("unit") or "unknown") for trade in trades)
    return {
        "event_family_breakdown": dict(sorted(family_counter.items())),
        "unit_breakdown": dict(sorted(unit_counter.items())),
    }


def summarize_positions(positions: list[dict[str, Any]]) -> dict[str, Any]:
    current_values = [to_float(position.get("currentValue")) for position in positions]
    current_values = [value for value in current_values if value is not None]
    return {
        "position_count": len(positions),
        "total_current_value_usd": sum(current_values),
        "avg_current_value_usd": safe_mean(current_values),
        "top_positions_by_value": top_records(
            positions,
            key="currentValue",
            record_builder=lambda row: {
                "title": row.get("title") or row.get("eventSlug") or row.get("slug"),
                "current_value_usd": to_float(row.get("currentValue")),
                "size": to_float(row.get("size")),
                "outcome": row.get("outcome"),
                "avg_price": to_float(row.get("avgPrice")),
            },
        ),
    }


def summarize_closed_positions(closed_positions: list[dict[str, Any]]) -> dict[str, Any]:
    realized = [to_float(row.get("realizedPnl")) for row in closed_positions]
    realized = [value for value in realized if value is not None]
    percent = [to_float(row.get("percentPnl")) for row in closed_positions]
    percent = [value for value in percent if value is not None]
    wins = sum(1 for value in realized if value is not None and value > 0)
    losses = sum(1 for value in realized if value is not None and value < 0)
    total = len(realized) or 1
    return {
        "closed_position_count": len(closed_positions),
        "total_realized_pnl_usd": sum(realized),
        "avg_realized_pnl_usd": safe_mean(realized),
        "median_realized_pnl_usd": safe_median(realized),
        "avg_percent_pnl": safe_mean(percent),
        "win_rate": wins / total,
        "loss_rate": losses / total,
    }


def summarize_activity(activity: list[dict[str, Any]]) -> dict[str, Any]:
    type_counter = Counter(normalize_text(item.get("type")) or "unknown" for item in activity)
    return {
        "activity_count": len(activity),
        "activity_type_breakdown": dict(sorted(type_counter.items())),
    }


def summarize_portfolio_value(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "raw": value,
        "total_value_usd": first_numeric(value, "totalValue", "value", "total"),
    }


def top_counter(counter: Counter, limit: int = 10) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in counter.most_common(limit)
    ]


def top_notional_events(trades: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        slug = str(trade.get("eventSlug") or "unknown")
        notional = to_float(trade.get("derived_notional_usd"))
        if notional is not None:
            totals[slug] += notional
    return [
        {"event_slug": slug, "total_notional_usd": total}
        for slug, total in sorted(totals.items(), key=lambda item: item[1], reverse=True)[:limit]
    ]


def top_records(records: list[dict[str, Any]], *, key: str, record_builder, limit: int = 10) -> list[dict[str, Any]]:
    sortable = []
    for record in records:
        value = to_float(record.get(key))
        if value is None:
            continue
        sortable.append((value, record))
    sortable.sort(key=lambda item: item[0], reverse=True)
    return [record_builder(record) for _, record in sortable[:limit]]


def compute_hours_before_event(timestamp: Any, event_date_iso: str | None) -> float | None:
    ts = to_int(timestamp)
    if ts is None or not event_date_iso:
        return None
    trade_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    event_dt = datetime.fromisoformat(f"{event_date_iso}T23:59:59+00:00")
    delta = event_dt - trade_dt
    return delta.total_seconds() / 3600.0


def infer_event_family(trade: dict[str, Any]) -> str:
    event_slug = str(trade.get("eventSlug") or "")
    if event_slug.startswith("highest-temperature-in-"):
        return "weather_temperature"
    if "temperature" in event_slug:
        return "temperature_other"
    return "other"


def infer_temperature_unit_from_text(text: str) -> str:
    normalized = text.lower()
    if "°f" in normalized or "fahrenheit" in normalized:
        return "fahrenheit"
    if "°c" in normalized or "celsius" in normalized:
        return "celsius"
    return "unknown"


def timestamp_to_iso(timestamp: Any) -> str | None:
    ts = to_int(timestamp)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def first_numeric(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = to_float(payload.get(key))
        if value is not None:
            return value
    return None


def safe_mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def safe_median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return " ".join(str(value).strip().lower().split())


def persist_snapshot(reference_date: date, username: str, payload: dict[str, Any]) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_username = "".join(ch.lower() if ch.isalnum() else "-" for ch in username).strip("-") or "trader"
    path = output_dir / f"{reference_date.isoformat()}_{safe_username}_behavior_profile.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


if __name__ == "__main__":
    asyncio.run(main())
