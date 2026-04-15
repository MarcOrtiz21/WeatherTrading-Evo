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
class EventBacktestRow:
    event_slug: str
    event_title: str
    event_date: str
    station_code: str
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
    parser = argparse.ArgumentParser(description="Backtest histórico para mercados de temperatura tipo Polymarket.")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Fecha final YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=5, help="Ventana histórica de eventos a evaluar.")
    parser.add_argument("--max-events", type=int, default=20, help="Máximo de eventos históricos.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    end_date = date.fromisoformat(args.end_date)
    start_date = end_date - timedelta(days=args.lookback_days)

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
        end_date=end_date - timedelta(days=1),
    )

    rows: list[EventBacktestRow] = []
    skipped_events: list[dict] = []
    for payload in historical_payloads[: args.max_events]:
        try:
            row, skipped = await evaluate_historical_event(
                payload=payload,
                parser=parser,
                mapper=mapper,
                openmeteo=openmeteo,
                baseline_builder=baseline_builder,
                optimized_builder=optimized_builder,
                pricing=pricing,
            )
        except Exception as exc:
            row, skipped = None, {
                "event_slug": payload.get("event_slug"),
                "reason": "evaluation_error",
                "detail": str(exc),
            }
        if row is not None:
            rows.append(row)
        elif skipped is not None:
            skipped_events.append(skipped)

    summary = summarize(rows)
    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": (end_date - timedelta(days=1)).isoformat(),
        "category_urls": CATEGORY_URLS,
        "discovery_source": "polymarket_gamma",
        "temperature_tag_id": ConfigLoader.get("market_discovery.temperature_tag_id"),
        "discovery_tag_ids": ConfigLoader.get("market_discovery.temperature_discovery_tag_ids"),
        "winner_source": "polymarket_market_resolution",
        "probability_temperature_alpha": get_probability_temperature_alpha(),
        "summary": summary,
        "rows": [asdict(row) for row in rows],
        "skipped_events": skipped_events,
    }

    output_path = persist_snapshot(snapshot, end_date)
    print(f"Backtest guardado en: {output_path}")
    print("")
    print("=== RESUMEN BACKTEST HISTORICO ===")
    print(f"Eventos evaluados: {summary['events']}")
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
    print(
        f"Baseline log loss: {summary['baseline_log_loss']:.3f} | "
        f"Optimized log loss: {summary['optimized_log_loss']:.3f} | "
        f"Adaptive log loss: {summary['adaptive_log_loss']:.3f}"
    )
    print(
        f"Baseline Brier: {summary['baseline_brier']:.3f} | "
        f"Optimized Brier: {summary['optimized_brier']:.3f} | "
        f"Adaptive Brier: {summary['adaptive_brier']:.3f}"
    )
    print(
        f"Mejora prob. ganadora optimized/baseline: {summary['winner_prob_improvement_rate']:.1%} | "
        f"adaptive/baseline: {summary['adaptive_winner_prob_improvement_rate']:.1%}"
    )


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


async def evaluate_historical_event(
    payload: dict,
    parser: DeterministicParser,
    mapper: StationMapperService,
    openmeteo: OpenMeteoClient,
    baseline_builder: OpenMeteoDistributionBuilder,
    optimized_builder: CalibratedMultiModelDistributionBuilder,
    pricing: PricingEngine,
) -> tuple[EventBacktestRow | None, dict | None]:
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
        return None, {"event_slug": payload.get("event_slug"), "reason": "no_parseable_markets"}

    representative_spec = next((spec for spec in parsed_specs if not spec.requires_manual_review), parsed_specs[0])
    if representative_spec.requires_manual_review:
        return None, {
            "event_slug": payload.get("event_slug"),
            "reason": "manual_review_required",
            "notes": representative_spec.notes,
        }

    station = mapper.get_station(representative_spec.station_code)
    if not station:
        return None, {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_station_catalog",
            "station_code": representative_spec.station_code,
        }

    station_region = mapper.get_station_region(representative_spec.station_code)
    if not mapper.is_region_allowed(station_region):
        return None, {
            "event_slug": payload.get("event_slug"),
            "reason": "region_not_allowed",
            "station_code": representative_spec.station_code,
            "region": station_region,
        }

    actual_temp_c = await openmeteo.fetch_archive_daily_max(
        latitude=station["latitude"],
        longitude=station["longitude"],
        local_date=representative_spec.local_date,
    )

    historical_models = await openmeteo.fetch_multimodel_forecast(
        latitude=station["latitude"],
        longitude=station["longitude"],
        local_date=representative_spec.local_date,
        models=DEFAULT_MODELS,
        historical=True,
    )
    if not historical_models:
        return None, {"event_slug": payload.get("event_slug"), "reason": "missing_historical_forecast"}

    historical_models = apply_station_temperature_bias_to_models(
        historical_models,
        representative_spec.station_code,
    )
    best_match = historical_models.get("best_match")
    if best_match is None:
        best_match = next(iter(historical_models.values()))

    calibration_end_date = representative_spec.local_date - timedelta(days=1)
    forecast_history, actual_history = await openmeteo.fetch_recent_calibration_window(
        latitude=station["latitude"],
        longitude=station["longitude"],
        calibration_end_date=calibration_end_date,
        lookback_days=7,
        models=DEFAULT_MODELS,
    )
    calibration_errors = optimized_builder.extract_calibration_errors(forecast_history, actual_history)

    baseline_distribution = baseline_builder.build(
        market_id=representative_spec.market_id,
        model_max_temp_c=best_match,
        horizon_days=1,
    )
    baseline_distribution = apply_probability_temperature(baseline_distribution)
    optimized_distribution = optimized_builder.build(
        market_id=representative_spec.market_id,
        model_values_by_name=historical_models,
        calibration_errors_by_model=calibration_errors,
        horizon_days=1,
        ensemble_members_c=None,
    )
    optimized_distribution = apply_probability_temperature(optimized_distribution)

    winner = find_resolved_winner_spec(parsed_specs, payload.get("markets", []))
    if winner is None:
        return None, {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_resolved_market_winner",
        }

    baseline_rows = build_scored_rows(parsed_specs, baseline_distribution, pricing)
    optimized_rows = build_scored_rows(parsed_specs, optimized_distribution, pricing)
    adaptive_strategy = select_adaptive_forecast_strategy(1)
    adaptive_rows = baseline_rows if adaptive_strategy == "baseline_short_horizon" else optimized_rows

    baseline_mode = max(baseline_rows, key=lambda row: row[1])
    optimized_mode = max(optimized_rows, key=lambda row: row[1])
    adaptive_mode = max(adaptive_rows, key=lambda row: row[1])
    baseline_winner_prob = next(score for spec, score in baseline_rows if spec.market_id == winner.market_id)
    optimized_winner_prob = next(score for spec, score in optimized_rows if spec.market_id == winner.market_id)
    adaptive_winner_prob = next(score for spec, score in adaptive_rows if spec.market_id == winner.market_id)
    baseline_brier = multiclass_brier_score(baseline_rows, winner.market_id)
    optimized_brier = multiclass_brier_score(optimized_rows, winner.market_id)
    adaptive_brier = multiclass_brier_score(adaptive_rows, winner.market_id)

    return EventBacktestRow(
        event_slug=payload["event_slug"],
        event_title=payload["event_title"],
        event_date=representative_spec.local_date.isoformat(),
        station_code=representative_spec.station_code,
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
        baseline_brier=baseline_brier,
        optimized_brier=optimized_brier,
        adaptive_brier=adaptive_brier,
        adaptive_strategy=adaptive_strategy,
    ), None


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


def summarize(rows: list[EventBacktestRow]) -> dict:
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

    baseline_hit_rate = sum(row.baseline_mode_hit for row in rows) / len(rows)
    optimized_hit_rate = sum(row.optimized_mode_hit for row in rows) / len(rows)
    adaptive_hit_rate = sum(row.adaptive_mode_hit for row in rows) / len(rows)
    baseline_avg_winner_prob = sum(row.baseline_winner_probability for row in rows) / len(rows)
    optimized_avg_winner_prob = sum(row.optimized_winner_probability for row in rows) / len(rows)
    adaptive_avg_winner_prob = sum(row.adaptive_winner_probability for row in rows) / len(rows)
    baseline_log_loss = sum(-math.log(max(row.baseline_winner_probability, 1e-9)) for row in rows) / len(rows)
    optimized_log_loss = sum(-math.log(max(row.optimized_winner_probability, 1e-9)) for row in rows) / len(rows)
    adaptive_log_loss = sum(-math.log(max(row.adaptive_winner_probability, 1e-9)) for row in rows) / len(rows)
    baseline_brier = sum(row.baseline_brier for row in rows) / len(rows)
    optimized_brier = sum(row.optimized_brier for row in rows) / len(rows)
    adaptive_brier = sum(row.adaptive_brier for row in rows) / len(rows)
    winner_prob_improvement_rate = sum(
        row.optimized_winner_probability > row.baseline_winner_probability
        for row in rows
    ) / len(rows)
    adaptive_winner_prob_improvement_rate = sum(
        row.adaptive_winner_probability > row.baseline_winner_probability
        for row in rows
    ) / len(rows)

    return {
        "events": len(rows),
        "baseline_hit_rate": baseline_hit_rate,
        "optimized_hit_rate": optimized_hit_rate,
        "adaptive_hit_rate": adaptive_hit_rate,
        "baseline_avg_winner_prob": baseline_avg_winner_prob,
        "optimized_avg_winner_prob": optimized_avg_winner_prob,
        "adaptive_avg_winner_prob": adaptive_avg_winner_prob,
        "baseline_log_loss": baseline_log_loss,
        "optimized_log_loss": optimized_log_loss,
        "adaptive_log_loss": adaptive_log_loss,
        "baseline_brier": baseline_brier,
        "optimized_brier": optimized_brier,
        "adaptive_brier": adaptive_brier,
        "winner_prob_improvement_rate": winner_prob_improvement_rate,
        "adaptive_winner_prob_improvement_rate": adaptive_winner_prob_improvement_rate,
    }


def multiclass_brier_score(rows: list[tuple[MarketSpec, float]], winner_market_id: str) -> float:
    total = 0.0
    for spec, probability in rows:
        outcome = 1.0 if spec.market_id == winner_market_id else 0.0
        total += (probability - outcome) ** 2
    return total


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


def persist_snapshot(snapshot: dict, end_date: date) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{end_date.isoformat()}_historical_temperature_backtest.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    asyncio.run(main())
