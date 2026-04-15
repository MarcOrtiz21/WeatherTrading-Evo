import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.calibrated_multimodel import (
    CalibratedMultiModelDistributionBuilder,
)
from weather_trading.services.execution_engine.operational_evidence import (
    OperationalEvidenceGate,
)
from weather_trading.services.forecast_engine.openmeteo_distribution import OpenMeteoDistributionBuilder
from weather_trading.services.forecast_engine.probability_temperature import (
    get_probability_temperature_alpha,
    get_probability_temperature_alpha_by_unit,
)
from weather_trading.services.forecast_engine.strategy_selection import (
    get_adaptive_baseline_max_horizon_days,
    get_forecast_policy_selection_mode,
    get_horizon_strategy_overrides,
)
from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient
from weather_trading.services.market_discovery.data_api_client import PolymarketDataApiClient
from weather_trading.services.market_discovery.public_page_client import PolymarketPublicPageClient
from weather_trading.services.market_discovery.wallet_watchlist import WalletWatchlistService
from weather_trading.services.evaluation import live_event_evaluator as live_eval
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import OpenMeteoClient


CATEGORY_URLS = (
    "https://polymarket.com/es/predictions/temperature",
    "https://polymarket.com/es/predictions/weather",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Congela una validación ciega sobre mercados live de Polymarket.")
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Fecha de referencia YYYY-MM-DD.")
    parser.add_argument("--max-events", type=int, default=5, help="Número máximo de eventos soportados a evaluar.")
    parser.add_argument("--min-horizon-days", type=int, default=1, help="Horizonte mínimo futuro para evitar mercados ya parcialmente observados.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Máximo horizonte futuro aceptado.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    as_of_date = date.fromisoformat(args.as_of_date)

    client = PolymarketPublicPageClient(locale="es")
    gamma = PolymarketGammaClient()
    data_api = PolymarketDataApiClient()
    parser = DeterministicParser()
    mapper = StationMapperService()
    openmeteo = OpenMeteoClient()
    baseline_builder = OpenMeteoDistributionBuilder()
    optimized_builder = CalibratedMultiModelDistributionBuilder()
    pricing = PricingEngine()
    evidence_gate = OperationalEvidenceGate()
    wallet_watchlist = WalletWatchlistService()

    event_payloads = await discover_temperature_event_payloads(
        gamma=gamma,
        client=client,
        as_of_date=as_of_date,
        min_horizon_days=args.min_horizon_days,
        max_horizon_days=args.max_horizon_days,
    )
    try:
        watchlist_snapshot = await wallet_watchlist.build_watchlist_snapshot(
            data_client=data_api,
            event_slugs={
                str(payload.get("event_slug"))
                for payload in event_payloads
                if payload.get("event_slug")
            },
        )
    except Exception:
        watchlist_snapshot = {
            "enabled": wallet_watchlist.is_enabled(),
            "tracked_traders": [],
            "unresolved_entries": wallet_watchlist.get_entries(),
            "trades_by_event_slug": {},
            "recent_trade_limit": wallet_watchlist.get_recent_trade_limit(),
            "recent_trade_lookback_hours": wallet_watchlist.get_recent_trade_lookback_hours(),
            "error": "watchlist_fetch_failed",
        }
    wallet_watchlist.remember_snapshot(watchlist_snapshot)
    supported_events: list[dict] = []
    skipped_events: list[dict] = []

    for payload in event_payloads:
        if len(supported_events) >= args.max_events:
            break

        event_date = live_eval.parse_event_date(payload.get("event_date"))
        if event_date is None:
            skipped_events.append({"event_slug": payload.get("event_slug"), "reason": "missing_event_date"})
            continue

        horizon_days = (event_date - as_of_date).days
        if horizon_days < args.min_horizon_days or horizon_days > args.max_horizon_days:
            continue

        rows, event_summary = await live_eval.evaluate_event(
            payload=payload,
            parser=parser,
            mapper=mapper,
            openmeteo=openmeteo,
            baseline_builder=baseline_builder,
            optimized_builder=optimized_builder,
            pricing=pricing,
            evidence_gate=evidence_gate,
            wallet_watchlist=wallet_watchlist,
            as_of_date=as_of_date,
        )
        if rows is None:
            skipped_events.append(event_summary)
            continue

        supported_events.append(
            {
                "event_slug": payload["event_slug"],
                "event_title": payload["event_title"],
                "event_date": event_date.isoformat(),
                "station_code": event_summary["station_code"],
                "city": event_summary["city"],
                "forecast_center_c": event_summary["forecast_center_c"],
                "forecast_mode_c": event_summary["forecast_mode_c"],
                "forecast_std_dev_c": event_summary["forecast_std_dev_c"],
                "forecast_model_name": event_summary["forecast_model_name"],
                "forecast_strategy": event_summary["forecast_strategy"],
                "ensemble_members": event_summary["ensemble_members"],
                "calibration_days": event_summary["calibration_days"],
                "market_mode_question": event_summary["market_mode_question"],
                "market_mode_probability": event_summary["market_mode_probability"],
                "model_mode_question": event_summary["model_mode_question"],
                "model_mode_probability": event_summary["model_mode_probability"],
                "top_edge_question": event_summary["top_edge_question"],
                "top_edge_net": event_summary["top_edge_net"],
                "top_edge_tradeable": event_summary["top_edge_tradeable"],
                "top_edge_quality_tier": event_summary["top_edge_quality_tier"],
                "top_edge_market_family": event_summary["top_edge_market_family"],
                "tradeable_markets": event_summary["tradeable_markets"],
                "event_operable": event_summary["event_operable"],
                "event_evidence_score": event_summary["event_evidence_score"],
                "event_evidence_tier": event_summary["event_evidence_tier"],
                "event_blockers": event_summary["event_blockers"],
                "event_evidence_notes": event_summary["event_evidence_notes"],
                "parser_confidence_score": event_summary["parser_confidence_score"],
                "station_region": event_summary["station_region"],
                "temperature_unit": event_summary["temperature_unit"],
                "intraday_active": event_summary["intraday_active"],
                "intraday_source": event_summary["intraday_source"],
                "intraday_max_so_far_c": event_summary["intraday_max_so_far_c"],
                "intraday_remaining_hours": event_summary["intraday_remaining_hours"],
                "station_temperature_bias_c": event_summary["station_temperature_bias_c"],
                "watchlist_signal": event_summary["watchlist_signal"],
                "watchlist_alignment_score": event_summary["watchlist_alignment_score"],
                "watchlist_match_count": event_summary["watchlist_match_count"],
                "watchlist_active_traders": event_summary["watchlist_active_traders"],
                "watchlist_aligned_traders": event_summary["watchlist_aligned_traders"],
                "watchlist_opposed_traders": event_summary["watchlist_opposed_traders"],
                "watchlist_event_only_traders": event_summary["watchlist_event_only_traders"],
                "watchlist_trades": event_summary["watchlist_trades"],
                "watchlist_veto_applied": event_summary["watchlist_veto_applied"],
                "experimental_filter_applied": event_summary["experimental_filter_applied"],
                "markets": [asdict(row) for row in rows],
            }
        )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "as_of_date": as_of_date.isoformat(),
        "category_urls": CATEGORY_URLS,
        "discovery_source": "polymarket_gamma",
        "temperature_tag_id": ConfigLoader.get("market_discovery.temperature_tag_id"),
        "weather_tag_id": ConfigLoader.get("market_discovery.weather_tag_id"),
        "discovery_tag_ids": ConfigLoader.get("market_discovery.temperature_discovery_tag_ids"),
        "adaptive_baseline_max_horizon_days": get_adaptive_baseline_max_horizon_days(),
        "forecast_policy_selection_mode": get_forecast_policy_selection_mode(),
        "forecast_policy_horizon_strategy_overrides": get_horizon_strategy_overrides(),
        "forecast_policy_probability_temperature_alpha": get_probability_temperature_alpha(),
        "forecast_policy_probability_temperature_alpha_by_unit": get_probability_temperature_alpha_by_unit(),
        "wallet_watchlist_enabled": watchlist_snapshot.get("enabled", False),
        "wallet_watchlist_category": watchlist_snapshot.get("category"),
        "wallet_watchlist_recent_trade_limit": watchlist_snapshot.get("recent_trade_limit"),
        "wallet_watchlist_recent_trade_lookback_hours": watchlist_snapshot.get("recent_trade_lookback_hours"),
        "wallet_watchlist_tracked_traders": watchlist_snapshot.get("tracked_traders", []),
        "wallet_watchlist_unresolved_entries": watchlist_snapshot.get("unresolved_entries", []),
        "wallet_watchlist_error": watchlist_snapshot.get("error"),
        "evaluated_events": supported_events,
        "skipped_events": skipped_events,
    }

    snapshot_path = persist_snapshot(snapshot)
    print(f"Snapshot guardado en: {snapshot_path}")
    print("")
    print("=== RESUMEN VALIDACION CIEGA ===")
    print(f"Eventos evaluados: {len(supported_events)}")
    print(f"Eventos descartados: {len(skipped_events)}")
    for event in supported_events:
        print("")
        print(f"- {event['event_title']} [{event['station_code']}] {event['event_date']}")
        print(
            f"  Forecast centro: {event['forecast_center_c']:.1f}C | "
            f"modo modelo: {event['model_mode_question']} ({event['model_mode_probability']:.1%})"
        )
        print(
            f"  Modo mercado: {event['market_mode_question']} ({event['market_mode_probability']:.1%}) | "
            f"top edge: {event['top_edge_question']} ({event['top_edge_net']:.1%})"
        )
        print(
            f"  Evidence: tier {event['event_evidence_tier']} ({event['event_evidence_score']:.2f}) | "
            f"operable={event['event_operable']} | tradeable_markets={event['tradeable_markets']}"
        )
        print(
            f"  Watchlist: {event['watchlist_signal']} | "
            f"active_traders={len(event['watchlist_active_traders'])} | "
            f"aligned={len(event['watchlist_aligned_traders'])} | "
            f"opposed={len(event['watchlist_opposed_traders'])} | "
            f"veto={event['watchlist_veto_applied']}"
        )


async def discover_temperature_event_payloads(
    gamma: PolymarketGammaClient,
    client: PolymarketPublicPageClient,
    as_of_date: date,
    min_horizon_days: int,
    max_horizon_days: int,
) -> list[dict]:
    start_date = as_of_date + timedelta(days=min_horizon_days)
    end_date = as_of_date + timedelta(days=max_horizon_days)
    try:
        return await gamma.discover_temperature_event_payloads(
            active=True,
            closed=False,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception:
        slugs: list[str] = []
        for category_url in CATEGORY_URLS:
            slugs.extend(await client.fetch_category_event_slugs(category_url))
        unique_slugs = sorted(
            {
                slug
                for slug in slugs
                if slug.startswith("highest-temperature-in-")
            }
        )
        payloads = [await client.fetch_event_payload(slug) for slug in unique_slugs]
        return [
            payload
            for payload in payloads
            if payload.get("event_date")
            and start_date <= date.fromisoformat(payload["event_date"]) <= end_date
        ]


def persist_snapshot(snapshot: dict) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{snapshot['as_of_date']}_polymarket_blind_live_validation.json"
    output_path = output_dir / filename
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    asyncio.run(main())
