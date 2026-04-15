from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from weather_trading.domain.models import MarketQuote
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.execution_engine.operational_evidence import OperationalEvidenceGate
from weather_trading.services.forecast_engine.calibrated_multimodel import (
    CalibratedMultiModelDistributionBuilder,
)
from weather_trading.services.forecast_engine.intraday_context import (
    resolve_intraday_max_so_far_context,
)
from weather_trading.services.forecast_engine.openmeteo_distribution import OpenMeteoDistributionBuilder
from weather_trading.services.forecast_engine.probability_temperature import (
    apply_probability_temperature,
    infer_temperature_unit,
)
from weather_trading.services.forecast_engine.station_temperature_bias import (
    apply_station_temperature_bias,
    apply_station_temperature_bias_to_models,
    get_station_temperature_bias_c,
)
from weather_trading.services.forecast_engine.strategy_selection import select_adaptive_forecast_strategy
from weather_trading.services.market_discovery.wallet_watchlist import WalletWatchlistService
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import DEFAULT_MODELS, OpenMeteoClient

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DB_PATH = PROJECT_ROOT / "weather_trading.db"


@dataclass(slots=True)
class MarketRow:
    market_id: str
    market_slug: str | None
    question: str
    bin_low_c: float | None
    bin_high_c: float | None
    best_bid: float | None
    best_ask: float | None
    fair_probability: float
    market_probability: float
    execution_price: float | None
    edge_net: float
    estimated_costs: float
    spread_width: float | None
    relative_spread_width: float | None
    quality_score: float | None
    quality_tier: str | None
    is_tradeable: bool
    blockers: tuple[str, ...]
    market_blockers: tuple[str, ...]
    event_blockers: tuple[str, ...]


WATCHLIST_OPPOSED_VETO_BLOCKER = "watchlist_opposed_veto"
EXPERIMENTAL_CELSIUS_RANGE_ACTIVE_UNCLASSIFIED_BLOCKER = (
    "experimental_celsius_range_bin_active_unclassified_veto"
)


async def evaluate_event(
    *,
    payload: dict,
    parser: DeterministicParser,
    mapper: StationMapperService,
    openmeteo: OpenMeteoClient,
    baseline_builder: OpenMeteoDistributionBuilder,
    optimized_builder: CalibratedMultiModelDistributionBuilder,
    pricing: PricingEngine,
    evidence_gate: OperationalEvidenceGate,
    wallet_watchlist: WalletWatchlistService,
    as_of_date: date,
) -> tuple[list[MarketRow] | None, dict]:
    parsed_specs = []
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
        spec = mapper.enrich(spec)
        parsed_specs.append((spec, market))

    if not parsed_specs:
        return None, {"event_slug": payload.get("event_slug"), "reason": "no_parseable_markets"}

    representative_spec = next(
        (spec for spec, _ in parsed_specs if not spec.requires_manual_review),
        parsed_specs[0][0],
    )
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

    horizon_days = (representative_spec.local_date - as_of_date).days
    temperature_unit = infer_temperature_unit(representative_spec.question)
    forecast_distribution, forecast_meta = await build_live_distribution(
        openmeteo=openmeteo,
        baseline_builder=baseline_builder,
        optimized_builder=optimized_builder,
        market_id=str(payload.get("event_slug")),
        station_code=representative_spec.station_code,
        station_timezone=station.get("timezone", representative_spec.timezone),
        latitude=station["latitude"],
        longitude=station["longitude"],
        local_date=representative_spec.local_date,
        as_of_date=as_of_date,
        horizon_days=horizon_days,
        temperature_unit=temperature_unit,
    )
    if forecast_distribution is None:
        return None, {
            "event_slug": payload.get("event_slug"),
            "reason": "missing_openmeteo_forecast",
            "station_code": representative_spec.station_code,
        }

    rows: list[MarketRow] = []
    evidence_assessment = evidence_gate.assess(
        parser_confidence_score=representative_spec.confidence_score,
        forecast_strategy=str(forecast_meta["strategy"]),
        horizon_days=horizon_days,
        calibration_days=int(forecast_meta["calibration_days"]),
        ensemble_members=int(forecast_meta["ensemble_members"]),
        forecast_std_dev_c=float(forecast_meta["std_dev_c"]),
        intraday_active=bool(forecast_meta.get("intraday_active", False)),
        intraday_source=(
            None if forecast_meta.get("intraday_source") in {None, ""} else str(forecast_meta["intraday_source"])
        ),
        intraday_remaining_hours=(
            None
            if forecast_meta.get("intraday_remaining_hours") is None
            else int(forecast_meta["intraday_remaining_hours"])
        ),
    )
    for spec, market in parsed_specs:
        quote = MarketQuote(
            market_id=spec.market_id,
            outcome="Yes",
            best_bid=to_float(market.get("bestBid")),
            best_ask=to_float(market.get("bestAsk")),
            last_price=extract_last_price(market),
            captured_at_utc=utc_now(),
        )
        signal = pricing.generate_signal(spec, forecast_distribution, quote)
        combined_blockers = tuple(dict.fromkeys(signal.blockers + evidence_assessment.blockers))
        rows.append(
            MarketRow(
                market_id=spec.market_id,
                market_slug=str(market.get("slug")) if market.get("slug") else None,
                question=spec.question,
                bin_low_c=spec.bin_low_c,
                bin_high_c=spec.bin_high_c,
                best_bid=quote.best_bid,
                best_ask=quote.best_ask,
                fair_probability=signal.fair_probability,
                market_probability=signal.market_probability,
                execution_price=signal.execution_price,
                edge_net=signal.edge_net,
                estimated_costs=signal.estimated_costs,
                spread_width=signal.spread_width,
                relative_spread_width=signal.relative_spread_width,
                quality_score=signal.quality_score,
                quality_tier=signal.quality_tier,
                is_tradeable=(not combined_blockers and signal.edge_net > 0),
                blockers=combined_blockers,
                market_blockers=signal.blockers,
                event_blockers=evidence_assessment.blockers,
            )
        )

    pre_watchlist_tradeable_rows = [row for row in rows if row.is_tradeable]
    pre_watchlist_top_edge = max(pre_watchlist_tradeable_rows or rows, key=lambda row: row.edge_net)
    watchlist_summary = wallet_watchlist.summarize_event_alignment(
        event_slug=str(payload.get("event_slug")),
        rows=rows,
        top_edge_market_id=pre_watchlist_top_edge.market_id,
    )
    rows, adjusted_event_blockers, adjusted_event_notes, adjusted_event_operable = apply_watchlist_opposed_veto(
        rows=rows,
        event_blockers=evidence_assessment.blockers,
        event_notes=evidence_assessment.notes,
        event_operable=evidence_assessment.is_operable,
        watchlist_summary=watchlist_summary,
    )
    tradeable_rows = [row for row in rows if row.is_tradeable]
    pre_experimental_top_edge = max(tradeable_rows or rows, key=lambda row: row.edge_net)
    rows, adjusted_event_blockers, adjusted_event_notes, adjusted_event_operable = (
        apply_experimental_celsius_range_bin_active_unclassified_filter(
            rows=rows,
            event_blockers=adjusted_event_blockers,
            event_notes=adjusted_event_notes,
            event_operable=adjusted_event_operable,
            watchlist_summary=watchlist_summary,
            temperature_unit=temperature_unit,
            top_edge=pre_experimental_top_edge,
        )
    )

    market_mode = max(rows, key=lambda row: row.market_probability)
    model_mode = max(rows, key=lambda row: row.fair_probability)
    tradeable_rows = [row for row in rows if row.is_tradeable]
    top_edge = max(tradeable_rows or rows, key=lambda row: row.edge_net)
    top_edge_market_family = build_market_family_label(temperature_unit=temperature_unit, row=top_edge)
    summary = {
        "station_code": representative_spec.station_code,
        "city": representative_spec.city,
        "forecast_center_c": forecast_meta["center_c"],
        "forecast_mode_c": forecast_distribution.most_likely_temperature(),
        "forecast_std_dev_c": forecast_meta["std_dev_c"],
        "forecast_model_name": forecast_distribution.model_name,
        "forecast_strategy": forecast_meta["strategy"],
        "ensemble_members": forecast_meta["ensemble_members"],
        "calibration_days": forecast_meta["calibration_days"],
        "market_mode_question": market_mode.question,
        "market_mode_probability": market_mode.market_probability,
        "model_mode_question": model_mode.question,
        "model_mode_probability": model_mode.fair_probability,
        "top_edge_question": top_edge.question,
        "top_edge_net": top_edge.edge_net,
        "top_edge_tradeable": top_edge.is_tradeable,
        "top_edge_quality_tier": top_edge.quality_tier,
        "top_edge_market_family": top_edge_market_family,
        "tradeable_markets": len(tradeable_rows),
        "event_operable": adjusted_event_operable,
        "event_evidence_score": evidence_assessment.score,
        "event_evidence_tier": evidence_assessment.tier,
        "event_blockers": adjusted_event_blockers,
        "event_evidence_notes": adjusted_event_notes,
        "parser_confidence_score": representative_spec.confidence_score,
        "station_region": station_region,
        "temperature_unit": temperature_unit,
        "intraday_active": forecast_meta.get("intraday_active", False),
        "intraday_source": forecast_meta.get("intraday_source"),
        "intraday_max_so_far_c": forecast_meta.get("intraday_max_so_far_c"),
        "intraday_remaining_hours": forecast_meta.get("intraday_remaining_hours"),
        "station_temperature_bias_c": get_station_temperature_bias_c(representative_spec.station_code),
        "watchlist_signal": watchlist_summary["signal"],
        "watchlist_alignment_score": watchlist_summary["alignment_score"],
        "watchlist_match_count": watchlist_summary["match_count"],
        "watchlist_active_traders": watchlist_summary["active_traders"],
        "watchlist_aligned_traders": watchlist_summary["aligned_traders"],
        "watchlist_opposed_traders": watchlist_summary["opposed_traders"],
        "watchlist_event_only_traders": watchlist_summary["event_only_traders"],
        "watchlist_trades": watchlist_summary["trades"],
        "watchlist_veto_applied": WATCHLIST_OPPOSED_VETO_BLOCKER in adjusted_event_blockers,
        "experimental_filter_applied": (
            EXPERIMENTAL_CELSIUS_RANGE_ACTIVE_UNCLASSIFIED_BLOCKER in adjusted_event_blockers
        ),
    }
    return rows, summary


def apply_watchlist_opposed_veto(
    *,
    rows: list[MarketRow],
    event_blockers: tuple[str, ...],
    event_notes: tuple[str, ...],
    event_operable: bool,
    watchlist_summary: dict[str, Any],
) -> tuple[list[MarketRow], tuple[str, ...], tuple[str, ...], bool]:
    if not should_veto_for_watchlist_opposition(watchlist_summary):
        return rows, event_blockers, event_notes, event_operable

    adjusted_event_blockers = tuple(dict.fromkeys(event_blockers + (WATCHLIST_OPPOSED_VETO_BLOCKER,)))
    adjusted_event_notes = tuple(
        dict.fromkeys(
            event_notes
            + (
                "watchlist_signal=opposed",
                "watchlist_veto_applied=true",
            )
        )
    )
    adjusted_rows = [
        MarketRow(
            market_id=row.market_id,
            market_slug=row.market_slug,
            question=row.question,
            bin_low_c=row.bin_low_c,
            bin_high_c=row.bin_high_c,
            best_bid=row.best_bid,
            best_ask=row.best_ask,
            fair_probability=row.fair_probability,
            market_probability=row.market_probability,
            execution_price=row.execution_price,
            edge_net=row.edge_net,
            estimated_costs=row.estimated_costs,
            spread_width=row.spread_width,
            relative_spread_width=row.relative_spread_width,
            quality_score=row.quality_score,
            quality_tier=row.quality_tier,
            is_tradeable=False,
            blockers=tuple(dict.fromkeys(row.blockers + (WATCHLIST_OPPOSED_VETO_BLOCKER,))),
            market_blockers=row.market_blockers,
            event_blockers=adjusted_event_blockers,
        )
        for row in rows
    ]
    return adjusted_rows, adjusted_event_blockers, adjusted_event_notes, False


def should_veto_for_watchlist_opposition(watchlist_summary: dict[str, Any]) -> bool:
    if not bool(ConfigLoader.get("watchlist_risk.opposed_signal_veto_enabled", False)):
        return False
    signal = str(watchlist_summary.get("signal") or "").strip().lower()
    if signal != "opposed":
        return False
    minimum_opposed = int(ConfigLoader.get("watchlist_risk.min_opposed_trader_count_for_veto", 1) or 1)
    opposed_count = len(watchlist_summary.get("opposed_traders") or [])
    match_count = int(watchlist_summary.get("match_count") or 0)
    return opposed_count >= minimum_opposed and match_count > 0


def apply_experimental_celsius_range_bin_active_unclassified_filter(
    *,
    rows: list[MarketRow],
    event_blockers: tuple[str, ...],
    event_notes: tuple[str, ...],
    event_operable: bool,
    watchlist_summary: dict[str, Any],
    temperature_unit: str,
    top_edge: MarketRow,
) -> tuple[list[MarketRow], tuple[str, ...], tuple[str, ...], bool]:
    if not should_apply_celsius_range_bin_active_unclassified_filter(
        watchlist_summary=watchlist_summary,
        temperature_unit=temperature_unit,
        top_edge=top_edge,
    ):
        return rows, event_blockers, event_notes, event_operable

    adjusted_event_blockers = tuple(
        dict.fromkeys(event_blockers + (EXPERIMENTAL_CELSIUS_RANGE_ACTIVE_UNCLASSIFIED_BLOCKER,))
    )
    adjusted_event_notes = tuple(
        dict.fromkeys(
            event_notes
            + (
                "experimental_filter=celsius_range_bin_active_unclassified",
                "experimental_filter_applied=true",
            )
        )
    )
    adjusted_rows = [
        MarketRow(
            market_id=row.market_id,
            market_slug=row.market_slug,
            question=row.question,
            bin_low_c=row.bin_low_c,
            bin_high_c=row.bin_high_c,
            best_bid=row.best_bid,
            best_ask=row.best_ask,
            fair_probability=row.fair_probability,
            market_probability=row.market_probability,
            execution_price=row.execution_price,
            edge_net=row.edge_net,
            estimated_costs=row.estimated_costs,
            spread_width=row.spread_width,
            relative_spread_width=row.relative_spread_width,
            quality_score=row.quality_score,
            quality_tier=row.quality_tier,
            is_tradeable=False,
            blockers=tuple(
                dict.fromkeys(row.blockers + (EXPERIMENTAL_CELSIUS_RANGE_ACTIVE_UNCLASSIFIED_BLOCKER,))
            ),
            market_blockers=row.market_blockers,
            event_blockers=adjusted_event_blockers,
        )
        for row in rows
    ]
    return adjusted_rows, adjusted_event_blockers, adjusted_event_notes, False


def should_apply_celsius_range_bin_active_unclassified_filter(
    *,
    watchlist_summary: dict[str, Any],
    temperature_unit: str,
    top_edge: MarketRow,
) -> bool:
    if not bool(
        ConfigLoader.get(
            "experimental_filters.celsius_range_bin_active_unclassified_veto_enabled",
            False,
        )
    ):
        return False
    if str(watchlist_summary.get("signal") or "").strip().lower() != "active_unclassified":
        return False
    return build_market_family_label(temperature_unit=temperature_unit, row=top_edge) == "celsius|range_bin"


def build_market_family_label(*, temperature_unit: str, row: MarketRow) -> str:
    return f"{temperature_unit}|{classify_market_shape(row)}"


def classify_market_shape(row: MarketRow) -> str:
    low_c = row.bin_low_c
    high_c = row.bin_high_c
    if low_c is None and high_c is not None:
        return "lower_tail"
    if low_c is not None and high_c is None:
        return "upper_tail"
    if low_c is not None and high_c is not None:
        if abs(low_c - high_c) < 1e-9:
            return "exact_point"
        return "range_bin"
    return "unknown"


async def build_live_distribution(
    *,
    openmeteo: OpenMeteoClient,
    baseline_builder: OpenMeteoDistributionBuilder,
    optimized_builder: CalibratedMultiModelDistributionBuilder,
    market_id: str,
    station_code: str,
    station_timezone: str,
    latitude: float,
    longitude: float,
    local_date: date,
    as_of_date: date,
    horizon_days: int,
    temperature_unit: str,
):
    baseline_payload = await openmeteo.fetch_forecast(
        latitude=latitude,
        longitude=longitude,
        local_date=local_date,
    )
    baseline_max_temp = None if not baseline_payload else apply_station_temperature_bias(
        baseline_payload.get("model_max_temp"),
        station_code,
    )
    if baseline_max_temp is None:
        return None, {}

    intraday_context = None
    if horizon_days == 0 and local_date == as_of_date == date.today():
        intraday_context = resolve_intraday_max_so_far_context(
            db_path=DB_PATH,
            station_code=station_code,
            station_timezone=station_timezone,
            local_date=local_date,
            hourly_temperatures_c=baseline_payload.get("model_hourly_temps"),
            hourly_times=baseline_payload.get("model_hourly_times"),
        )

    baseline_distribution = baseline_builder.build(
        market_id=market_id,
        model_max_temp_c=float(baseline_max_temp),
        horizon_days=horizon_days,
        hourly_temperatures_c=baseline_payload.get("model_hourly_temps"),
        cloud_cover_avg=baseline_payload.get("model_cloud_cover_avg"),
        intraday_max_so_far_c=None if intraday_context is None else intraday_context.max_so_far_c,
        intraday_hours_elapsed=None if intraday_context is None else intraday_context.hours_elapsed,
        intraday_last_local_hour=None if intraday_context is None else intraday_context.last_local_hour,
    )
    baseline_distribution = apply_probability_temperature(baseline_distribution, unit=temperature_unit)

    try:
        multimodel = await openmeteo.fetch_multimodel_forecast(
            latitude=latitude,
            longitude=longitude,
            local_date=local_date,
            models=DEFAULT_MODELS,
            historical=False,
        )
        multimodel = apply_station_temperature_bias_to_models(multimodel, station_code)
    except Exception:
        multimodel = {}
    if not multimodel:
        return baseline_distribution, {
            "center_c": extract_note_value(baseline_distribution.notes, "center", float(baseline_max_temp)),
            "std_dev_c": extract_note_value(baseline_distribution.notes, "std_dev", 0.0),
            "strategy": "baseline_fallback",
            "ensemble_members": 0,
            "calibration_days": 0,
            "intraday_active": intraday_context is not None,
            "intraday_source": None if intraday_context is None else intraday_context.source,
            "intraday_max_so_far_c": None if intraday_context is None else intraday_context.max_so_far_c,
            "intraday_remaining_hours": None if intraday_context is None else intraday_context.remaining_hours,
        }

    calibration_days = 0
    calibration_errors: dict[str, list[float]] = {}
    calibration_end_date = as_of_date
    if horizon_days > 0:
        calibration_end_date = as_of_date - timedelta(days=1)

    if calibration_end_date >= date(2024, 1, 1):
        try:
            forecast_history, actual_history = await openmeteo.fetch_horizon_calibration_window(
                latitude=latitude,
                longitude=longitude,
                as_of_date=as_of_date,
                horizon_days=max(horizon_days, 1),
                lookback_days=7,
                models=DEFAULT_MODELS,
            )
            calibration_errors = optimized_builder.extract_calibration_errors(forecast_history, actual_history)
            calibration_days = len(actual_history)
        except Exception:
            calibration_errors = {}
            calibration_days = 0

    try:
        ensemble_members = await openmeteo.fetch_ensemble_members(
            latitude=latitude,
            longitude=longitude,
            local_date=local_date,
        )
    except Exception:
        ensemble_members = []

    try:
        optimized_distribution = optimized_builder.build(
            market_id=market_id,
            model_values_by_name=multimodel,
            calibration_errors_by_model=calibration_errors,
            horizon_days=horizon_days,
            ensemble_members_c=ensemble_members,
        )
        optimized_distribution = apply_probability_temperature(optimized_distribution, unit=temperature_unit)
    except Exception:
        return baseline_distribution, {
            "center_c": extract_note_value(baseline_distribution.notes, "center", float(baseline_max_temp)),
            "std_dev_c": extract_note_value(baseline_distribution.notes, "std_dev", 0.0),
            "strategy": "baseline_fallback",
            "ensemble_members": 0,
            "calibration_days": calibration_days,
            "intraday_active": intraday_context is not None,
            "intraday_source": None if intraday_context is None else intraday_context.source,
            "intraday_max_so_far_c": None if intraday_context is None else intraday_context.max_so_far_c,
            "intraday_remaining_hours": None if intraday_context is None else intraday_context.remaining_hours,
        }

    adaptive_strategy = select_adaptive_forecast_strategy(horizon_days)
    if adaptive_strategy == "baseline_short_horizon":
        return baseline_distribution, {
            "center_c": extract_note_value(baseline_distribution.notes, "center", float(baseline_max_temp)),
            "std_dev_c": extract_note_value(baseline_distribution.notes, "std_dev", 0.0),
            "strategy": adaptive_strategy,
            "ensemble_members": len(ensemble_members),
            "calibration_days": calibration_days,
            "intraday_active": intraday_context is not None,
            "intraday_source": None if intraday_context is None else intraday_context.source,
            "intraday_max_so_far_c": None if intraday_context is None else intraday_context.max_so_far_c,
            "intraday_remaining_hours": None if intraday_context is None else intraday_context.remaining_hours,
        }

    return optimized_distribution, {
        "center_c": extract_note_value(
            optimized_distribution.notes,
            "center",
            float(baseline_max_temp),
        ),
        "std_dev_c": extract_note_value(optimized_distribution.notes, "std_dev", 0.0),
        "strategy": adaptive_strategy,
        "ensemble_members": len(ensemble_members),
        "calibration_days": calibration_days,
        "intraday_active": False,
        "intraday_source": None,
        "intraday_max_so_far_c": None,
        "intraday_remaining_hours": None,
    }


def extract_note_value(notes: tuple[str, ...], key: str, default: float) -> float:
    prefix = f"{key}="
    for note in notes:
        if note.startswith(prefix):
            try:
                return float(note.split("=", 1)[1])
            except ValueError:
                return default
    return default


def parse_event_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def to_float(value) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def extract_last_price(market: dict) -> float | None:
    direct = to_float(market.get("lastTradePrice"))
    if direct is not None:
        return direct

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        import json

        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None
    if isinstance(outcome_prices, list) and outcome_prices:
        return to_float(outcome_prices[0])
    return None
