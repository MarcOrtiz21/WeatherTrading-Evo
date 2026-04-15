import argparse
import asyncio
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.domain.models import MarketQuote, MarketSpec, MetricKind
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.adaptive_threshold_search import (
    search_optimal_baseline_max_horizon_days,
    write_forecast_policy,
)
from weather_trading.services.forecast_engine.backtest_support import compute_previous_runs_past_days
from weather_trading.services.forecast_engine.calibrated_multimodel import (
    CalibratedMultiModelDistributionBuilder,
)
from weather_trading.services.forecast_engine.openmeteo_distribution import OpenMeteoDistributionBuilder
from weather_trading.services.forecast_engine.probability_temperature import (
    apply_probability_temperature,
    get_probability_temperature_alpha,
)
from weather_trading.services.forecast_engine.station_temperature_bias import (
    apply_station_temperature_bias_to_models,
)
from weather_trading.services.forecast_engine.strategy_selection import select_adaptive_forecast_strategy
from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient
from weather_trading.services.market_discovery.public_page_client import PolymarketPublicPageClient
from weather_trading.services.market_discovery.resolved_markets import find_resolved_winner_market
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import DEFAULT_MODELS, OpenMeteoClient


CATEGORY_URLS = (
    "https://polymarket.com/es/predictions/temperature",
    "https://polymarket.com/es/predictions/weather",
)


@dataclass(slots=True)
class HorizonBacktestRow:
    event_slug: str
    event_title: str
    event_date: str
    station_code: str
    horizon_days: int
    actual_temp_c: float | None
    actual_winner_question: str
    baseline_winner_probability: float
    optimized_winner_probability: float
    adaptive_winner_probability: float
    baseline_mode_question: str
    optimized_mode_question: str
    adaptive_mode_question: str
    baseline_mode_hit: bool
    optimized_mode_hit: bool
    adaptive_mode_hit: bool
    baseline_brier: float
    optimized_brier: float
    adaptive_brier: float
    adaptive_strategy: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest reciente por horizonte usando previous runs de Open-Meteo."
    )
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Fecha de referencia YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=7, help="Ventana reciente de eventos resueltos.")
    parser.add_argument("--max-events", type=int, default=20, help="Máximo de eventos a evaluar.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Horizonte máximo a evaluar.")
    parser.add_argument(
        "--learn-adaptive-threshold",
        action="store_true",
        help="Busca el mejor corte adaptive y lo aplica en config/forecast_policy.yaml.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    as_of_date = date.fromisoformat(args.as_of_date)
    today = date.today()
    if as_of_date > today:
        raise ValueError("Este backtest por horizonte solo es valido para fechas de hoy o anteriores.")

    start_date = as_of_date - timedelta(days=args.lookback_days)
    previous_runs_past_days = compute_previous_runs_past_days(
        as_of_date,
        lookback_days=args.lookback_days,
        max_horizon_days=args.max_horizon_days,
        reference_today=today,
    )

    client = PolymarketPublicPageClient(locale="es")
    gamma = PolymarketGammaClient()
    parser = DeterministicParser()
    mapper = StationMapperService()
    openmeteo = OpenMeteoClient()
    baseline_builder = OpenMeteoDistributionBuilder()
    optimized_builder = CalibratedMultiModelDistributionBuilder()
    pricing = PricingEngine()

    historical_payloads = await discover_historical_temperature_event_payloads(
        gamma=gamma,
        client=client,
        start_date=start_date,
        end_date=as_of_date - timedelta(days=1),
    )

    rows: list[HorizonBacktestRow] = []
    skipped_events: list[dict] = []
    previous_runs_cache: dict[tuple[str, int], dict[str, dict[str, float]]] = {}
    actual_history_cache: dict[str, dict[str, float]] = {}

    for payload in historical_payloads[: args.max_events]:
        try:
            event_rows, skipped = await evaluate_event_horizons(
                payload=payload,
                parser=parser,
                mapper=mapper,
                openmeteo=openmeteo,
                baseline_builder=baseline_builder,
                optimized_builder=optimized_builder,
                pricing=pricing,
                as_of_date=as_of_date,
                lookback_days=args.lookback_days,
                max_horizon_days=args.max_horizon_days,
                previous_runs_past_days=previous_runs_past_days,
                previous_runs_cache=previous_runs_cache,
                actual_history_cache=actual_history_cache,
            )
        except Exception as exc:
            event_rows, skipped = [], {
                "event_slug": payload.get("event_slug"),
                "reason": "evaluation_error",
                "detail": str(exc),
            }

        rows.extend(event_rows)
        if skipped:
            skipped_events.append(skipped)

    summary = summarize(rows)
    by_horizon = {
        str(horizon): summarize([row for row in rows if row.horizon_days == horizon])
        for horizon in range(1, args.max_horizon_days + 1)
    }
    policy_search = search_optimal_baseline_max_horizon_days(
        rows,
        max_horizon_days=args.max_horizon_days,
        objective="adaptive_log_loss",
    )
    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "as_of_date": as_of_date.isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": (as_of_date - timedelta(days=1)).isoformat(),
        "days_before_today": (today - as_of_date).days,
        "previous_runs_past_days": previous_runs_past_days,
        "category_urls": CATEGORY_URLS,
        "discovery_source": "polymarket_gamma",
        "temperature_tag_id": ConfigLoader.get("market_discovery.temperature_tag_id"),
        "discovery_tag_ids": ConfigLoader.get("market_discovery.temperature_discovery_tag_ids"),
        "winner_source": "polymarket_market_resolution",
        "probability_temperature_alpha": get_probability_temperature_alpha(),
        "summary": summary,
        "by_horizon": by_horizon,
        "policy_search": policy_search,
        "rows": [asdict(row) for row in rows],
        "skipped_events": skipped_events,
    }

    output_path = persist_snapshot(snapshot, as_of_date)
    print(f"Backtest por horizonte guardado en: {output_path}")
    print("")
    print("=== RESUMEN BACKTEST HORIZONTE ===")
    print(f"Filas evaluadas: {summary['events']}")
    print(f"Eventos descartados: {len(skipped_events)}")
    print(
        f"Baseline hit-rate: {summary['baseline_hit_rate']:.1%} | "
        f"Optimized hit-rate: {summary['optimized_hit_rate']:.1%} | "
        f"Adaptive hit-rate: {summary['adaptive_hit_rate']:.1%}"
    )
    print(
        f"Baseline avg winner prob: {summary['baseline_avg_winner_prob']:.3f} | "
        f"Optimized avg winner prob: {summary['optimized_avg_winner_prob']:.3f} | "
        f"Adaptive avg winner prob: {summary['adaptive_avg_winner_prob']:.3f}"
    )
    selected_policy = policy_search.get("selected_policy")
    if selected_policy:
        print(
            f"Policy search: baseline hasta H<={selected_policy['baseline_max_horizon_days']} | "
            f"log_loss={selected_policy['adaptive_log_loss']:.3f} | "
            f"brier={selected_policy['adaptive_brier']:.3f}"
        )
    for horizon, horizon_summary in by_horizon.items():
        if horizon_summary["events"] == 0:
            continue
        print(
            f"H{horizon}: events={horizon_summary['events']} | "
            f"baseline_hit={horizon_summary['baseline_hit_rate']:.1%} | "
            f"optimized_hit={horizon_summary['optimized_hit_rate']:.1%} | "
            f"adaptive_hit={horizon_summary['adaptive_hit_rate']:.1%} | "
            f"adaptive_log_loss={horizon_summary['adaptive_log_loss']:.3f} | "
            f"adaptive_brier={horizon_summary['adaptive_brier']:.3f}"
        )

    if args.learn_adaptive_threshold and selected_policy:
        policy_path = Path(ROOT) / "config" / "forecast_policy.yaml"
        write_forecast_policy(
            policy_path,
            baseline_max_horizon_days=int(selected_policy["baseline_max_horizon_days"]),
            objective=policy_search["objective"],
            as_of_date=as_of_date.isoformat(),
            lookback_days=args.lookback_days,
            max_events=args.max_events,
            max_horizon_days=args.max_horizon_days,
            learned_at_utc=utc_now(),
        )
        print(f"Umbral adaptive aplicado en config: H<={selected_policy['baseline_max_horizon_days']}")


async def discover_historical_temperature_event_payloads(
    gamma: PolymarketGammaClient,
    client: PolymarketPublicPageClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    try:
        return await gamma.discover_temperature_event_payloads(
            active=None,
            closed=True,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception:
        slugs: list[str] = []
        for category_url in CATEGORY_URLS:
            slugs.extend(await client.fetch_category_event_slugs(category_url))

        payloads: list[dict] = []
        for slug in sorted(set(slugs)):
            if not slug.startswith("highest-temperature-in-"):
                continue
            event_date = parse_event_date_from_slug(slug)
            if event_date is None or not (start_date <= event_date <= end_date):
                continue
            payloads.append(await client.fetch_event_payload(slug))
        return payloads


async def evaluate_event_horizons(
    payload: dict,
    parser: DeterministicParser,
    mapper: StationMapperService,
    openmeteo: OpenMeteoClient,
    baseline_builder: OpenMeteoDistributionBuilder,
    optimized_builder: CalibratedMultiModelDistributionBuilder,
    pricing: PricingEngine,
    as_of_date: date,
    lookback_days: int,
    max_horizon_days: int,
    previous_runs_past_days: int,
    previous_runs_cache: dict[tuple[str, int], dict[str, dict[str, float]]],
    actual_history_cache: dict[str, dict[str, float]],
) -> tuple[list[HorizonBacktestRow], dict | None]:
    parsed_specs: list[MarketSpec] = []
    for market in payload.get("markets", []):
        market_data = {
            "id": str(market.get("id")),
            "question": market.get("question", ""),
            "description": market.get("description") or payload.get("event_description", ""),
            "rules": payload.get("event_description", ""),
            "outcomes": ("Yes", "No"),
            "event_date": payload.get("event_date"),
            "resolution_source_url": market.get("resolution_source") or market.get("resolutionSource") or "",
            "endDate": market.get("endDate"),
        }
        spec = parser.parse(market_data)
        if spec is None:
            continue
        parsed_specs.append(mapper.enrich(spec))

    if not parsed_specs:
        return [], {"event_slug": payload.get("event_slug"), "reason": "no_parseable_markets"}

    representative_spec = next((spec for spec in parsed_specs if not spec.requires_manual_review), parsed_specs[0])
    if representative_spec.requires_manual_review:
        return [], {
            "event_slug": payload.get("event_slug"),
            "reason": "manual_review_required",
            "notes": representative_spec.notes,
        }

    station = mapper.get_station(representative_spec.station_code)
    if not station:
        return [], {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_station_catalog",
            "station_code": representative_spec.station_code,
        }

    station_region = mapper.get_station_region(representative_spec.station_code)
    if not mapper.is_region_allowed(station_region):
        return [], {
            "event_slug": payload.get("event_slug"),
            "reason": "region_not_allowed",
            "station_code": representative_spec.station_code,
            "region": station_region,
        }

    event_date_iso = representative_spec.local_date.isoformat()
    station_code = representative_spec.station_code
    actual_temp_c = await openmeteo.fetch_archive_daily_max(
        latitude=station["latitude"],
        longitude=station["longitude"],
        local_date=representative_spec.local_date,
    )

    winner = find_resolved_winner_spec(parsed_specs, payload.get("markets", []))
    if winner is None:
        return [], {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_resolved_market_winner",
        }

    history_window_start = as_of_date - timedelta(days=lookback_days + max_horizon_days)
    if station_code not in actual_history_cache:
        actual_history_cache[station_code] = await openmeteo.fetch_archive_daily_max_history(
            latitude=station["latitude"],
            longitude=station["longitude"],
            start_date=history_window_start,
            end_date=as_of_date - timedelta(days=1),
        )

    rows: list[HorizonBacktestRow] = []
    for horizon_days in range(1, max_horizon_days + 1):
        cache_key = (station_code, horizon_days)
        if cache_key not in previous_runs_cache:
            previous_runs_cache[cache_key] = await openmeteo.fetch_previous_runs_history(
                latitude=station["latitude"],
                longitude=station["longitude"],
                horizon_days=horizon_days,
                past_days=previous_runs_past_days,
                models=DEFAULT_MODELS,
            )

        previous_runs_history = previous_runs_cache[cache_key]
        model_values_by_name = previous_runs_history.get(event_date_iso)
        if not model_values_by_name:
            continue
        model_values_by_name = apply_station_temperature_bias_to_models(model_values_by_name, station_code)

        calibration_dates = sorted(day for day in previous_runs_history if day < event_date_iso)[-7:]
        calibration_forecasts = {day: previous_runs_history[day] for day in calibration_dates}
        calibration_actuals = {
            day: actual_history_cache[station_code][day]
            for day in calibration_dates
            if day in actual_history_cache[station_code]
        }
        calibration_errors = optimized_builder.extract_calibration_errors(
            calibration_forecasts,
            calibration_actuals,
        )

        best_match = model_values_by_name.get("best_match")
        if best_match is None:
            best_match = next(iter(model_values_by_name.values()))

        baseline_distribution = baseline_builder.build(
            market_id=representative_spec.market_id,
            model_max_temp_c=best_match,
            horizon_days=horizon_days,
        )
        baseline_distribution = apply_probability_temperature(baseline_distribution)
        optimized_distribution = optimized_builder.build(
            market_id=representative_spec.market_id,
            model_values_by_name=model_values_by_name,
            calibration_errors_by_model=calibration_errors,
            horizon_days=horizon_days,
            ensemble_members_c=None,
        )
        optimized_distribution = apply_probability_temperature(optimized_distribution)

        baseline_rows = build_scored_rows(parsed_specs, baseline_distribution, pricing)
        optimized_rows = build_scored_rows(parsed_specs, optimized_distribution, pricing)
        adaptive_strategy = select_adaptive_forecast_strategy(horizon_days)
        adaptive_rows = baseline_rows if adaptive_strategy == "baseline_short_horizon" else optimized_rows
        baseline_mode = max(baseline_rows, key=lambda row: row[1])
        optimized_mode = max(optimized_rows, key=lambda row: row[1])
        adaptive_mode = max(adaptive_rows, key=lambda row: row[1])
        baseline_winner_prob = next(score for spec, score in baseline_rows if spec.market_id == winner.market_id)
        optimized_winner_prob = next(score for spec, score in optimized_rows if spec.market_id == winner.market_id)
        adaptive_winner_prob = next(score for spec, score in adaptive_rows if spec.market_id == winner.market_id)

        rows.append(
            HorizonBacktestRow(
                event_slug=payload["event_slug"],
                event_title=payload["event_title"],
                event_date=event_date_iso,
                station_code=station_code,
                horizon_days=horizon_days,
                actual_temp_c=actual_temp_c,
                actual_winner_question=winner.question,
                baseline_winner_probability=baseline_winner_prob,
                optimized_winner_probability=optimized_winner_prob,
                adaptive_winner_probability=adaptive_winner_prob,
                baseline_mode_question=baseline_mode[0].question,
                optimized_mode_question=optimized_mode[0].question,
                adaptive_mode_question=adaptive_mode[0].question,
                baseline_mode_hit=baseline_mode[0].market_id == winner.market_id,
                optimized_mode_hit=optimized_mode[0].market_id == winner.market_id,
                adaptive_mode_hit=adaptive_mode[0].market_id == winner.market_id,
                baseline_brier=multiclass_brier_score(baseline_rows, winner.market_id),
                optimized_brier=multiclass_brier_score(optimized_rows, winner.market_id),
                adaptive_brier=multiclass_brier_score(adaptive_rows, winner.market_id),
                adaptive_strategy=adaptive_strategy,
            )
        )

    if not rows:
        return [], {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_previous_run_coverage",
        }
    return rows, None


def build_scored_rows(
    specs: list[MarketSpec],
    distribution,
    pricing: PricingEngine,
) -> list[tuple[MarketSpec, float]]:
    rows: list[tuple[MarketSpec, float]] = []
    for spec in specs:
        signal = pricing.generate_signal(
            spec,
            distribution,
            MarketQuote(market_id=spec.market_id, outcome="Yes", last_price=0.0),
        )
        rows.append((spec, signal.fair_probability))
    return rows


def find_resolved_winner_spec(specs: list[MarketSpec], markets: list[dict]) -> MarketSpec | None:
    winner_market = find_resolved_winner_market(markets)
    if winner_market is None:
        return None

    winner_market_id = str(winner_market.get("id"))
    for spec in specs:
        if spec.metric != MetricKind.TEMPERATURE_BIN:
            continue
        if spec.market_id == winner_market_id:
            return spec
    return None


def multiclass_brier_score(rows: list[tuple[MarketSpec, float]], winner_market_id: str) -> float:
    return sum((probability - (1.0 if spec.market_id == winner_market_id else 0.0)) ** 2 for spec, probability in rows)


def summarize(rows: list[HorizonBacktestRow]) -> dict:
    if not rows:
        return {
            "events": 0,
            "baseline_hit_rate": 0.0,
            "optimized_hit_rate": 0.0,
            "adaptive_hit_rate": 0.0,
            "baseline_avg_winner_prob": 0.0,
            "optimized_avg_winner_prob": 0.0,
            "adaptive_avg_winner_prob": 0.0,
            "baseline_log_loss": 0.0,
            "optimized_log_loss": 0.0,
            "adaptive_log_loss": 0.0,
            "baseline_brier": 0.0,
            "optimized_brier": 0.0,
            "adaptive_brier": 0.0,
            "winner_prob_improvement_rate": 0.0,
            "adaptive_winner_prob_improvement_rate": 0.0,
        }

    return {
        "events": len(rows),
        "baseline_hit_rate": sum(row.baseline_mode_hit for row in rows) / len(rows),
        "optimized_hit_rate": sum(row.optimized_mode_hit for row in rows) / len(rows),
        "adaptive_hit_rate": sum(row.adaptive_mode_hit for row in rows) / len(rows),
        "baseline_avg_winner_prob": sum(row.baseline_winner_probability for row in rows) / len(rows),
        "optimized_avg_winner_prob": sum(row.optimized_winner_probability for row in rows) / len(rows),
        "adaptive_avg_winner_prob": sum(row.adaptive_winner_probability for row in rows) / len(rows),
        "baseline_log_loss": sum(-math.log(max(row.baseline_winner_probability, 1e-9)) for row in rows) / len(rows),
        "optimized_log_loss": sum(-math.log(max(row.optimized_winner_probability, 1e-9)) for row in rows) / len(rows),
        "adaptive_log_loss": sum(-math.log(max(row.adaptive_winner_probability, 1e-9)) for row in rows) / len(rows),
        "baseline_brier": sum(row.baseline_brier for row in rows) / len(rows),
        "optimized_brier": sum(row.optimized_brier for row in rows) / len(rows),
        "adaptive_brier": sum(row.adaptive_brier for row in rows) / len(rows),
        "winner_prob_improvement_rate": sum(
            row.optimized_winner_probability > row.baseline_winner_probability for row in rows
        ) / len(rows),
        "adaptive_winner_prob_improvement_rate": sum(
            row.adaptive_winner_probability > row.baseline_winner_probability for row in rows
        ) / len(rows),
    }


def parse_event_date_from_slug(slug: str) -> date | None:
    import re

    match = re.search(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$", slug)
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
    return date(int(year_s), month, int(day_s))


def persist_snapshot(snapshot: dict, as_of_date: date) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{as_of_date.isoformat()}_recent_horizon_temperature_backtest.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    asyncio.run(main())
