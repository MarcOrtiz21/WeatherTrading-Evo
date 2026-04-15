from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BlindSnapshotEventEvaluation:
    snapshot_as_of_date: str
    event_slug: str
    event_title: str
    event_date: str
    station_code: str
    forecast_strategy: str
    horizon_days: int
    event_operable: bool
    event_evidence_score: float | None
    event_evidence_tier: str
    actual_temp_c: float
    actual_temperature_source: str | None
    winner_market_id: str
    winner_question: str
    model_mode_question: str
    market_mode_question: str
    top_edge_question: str
    top_edge_market_id: str
    top_edge_tradeable: bool
    top_edge_quality_tier: str
    model_mode_hit: bool
    market_mode_hit: bool
    top_edge_positive: bool
    top_edge_hit: bool | None
    winner_fair_probability: float
    winner_market_probability: float
    model_log_loss: float
    market_log_loss: float
    model_brier: float
    market_brier: float
    top_edge_net: float
    paper_trade_taken: bool
    paper_trade_stake: float
    paper_trade_pnl: float
    paper_trade_execution_price: float
    paper_trade_costs: float


def discover_blind_snapshot_paths(
    snapshots_dir: Path,
    *,
    start_as_of_date: date | None = None,
    end_as_of_date: date | None = None,
) -> list[Path]:
    paths = sorted(snapshots_dir.glob("*_polymarket_blind_live_validation.json"))
    selected: list[Path] = []
    for path in paths:
        as_of_date = extract_snapshot_as_of_date(path)
        if as_of_date is None:
            continue
        if start_as_of_date and as_of_date < start_as_of_date:
            continue
        if end_as_of_date and as_of_date > end_as_of_date:
            continue
        selected.append(path)
    return selected


def extract_snapshot_as_of_date(path: Path) -> date | None:
    try:
        prefix = path.name.split("_", 1)[0]
        return date.fromisoformat(prefix)
    except ValueError:
        return None


def is_event_eligible_for_resolution(event_date_value: str, reference_date: date) -> bool:
    event_day = date.fromisoformat(event_date_value)
    return event_day < reference_date


def select_realized_winner_market_row(markets: list[dict[str, Any]], actual_temp_c: float) -> dict[str, Any] | None:
    rounded_temp = round(float(actual_temp_c), 1)
    for market in markets:
        low = market.get("bin_low_c")
        high = market.get("bin_high_c")
        if _temperature_in_bin(rounded_temp, low, high):
            return market
    return None


def _temperature_in_bin(actual_temp_c: float, low: float | None, high: float | None) -> bool:
    epsilon = 1e-9
    if low is None and high is None:
        return False
    if low is None:
        return actual_temp_c <= float(high) + epsilon
    if high is None:
        return actual_temp_c >= float(low) - epsilon
    return float(low) - epsilon <= actual_temp_c <= float(high) + epsilon


def evaluate_blind_snapshot_event(
    snapshot_as_of_date: str,
    event: dict[str, Any],
    actual_temp_c: float,
    *,
    paper_edge_threshold: float = 0.0,
    actual_temperature_source: str | None = None,
) -> BlindSnapshotEventEvaluation | None:
    markets = list(event.get("markets", []))
    if not markets:
        return None

    winner_market = select_realized_winner_market_row(markets, actual_temp_c)
    if winner_market is None:
        return None

    market_mode = max(markets, key=lambda market: float(market["market_probability"]))
    model_mode = max(markets, key=lambda market: float(market["fair_probability"]))
    top_edge = max(markets, key=lambda market: float(market["edge_net"]))

    winner_market_id = str(winner_market["market_id"])
    winner_fair_probability = float(winner_market["fair_probability"])
    winner_market_probability = float(winner_market["market_probability"])
    model_brier = multiclass_brier_score(markets, winner_market_id, probability_key="fair_probability")
    market_brier = multiclass_brier_score(markets, winner_market_id, probability_key="market_probability")
    top_trade_candidate = select_top_trade_candidate(markets)
    top_edge_positive = (
        top_trade_candidate is not None and float(top_trade_candidate["edge_net"]) > paper_edge_threshold
    )
    top_edge_hit = None if not top_edge_positive else str(top_trade_candidate["market_id"]) == winner_market_id
    paper_trade_execution_price = (
        0.0
        if top_trade_candidate is None
        else float(
            top_trade_candidate.get(
                "execution_price",
                top_trade_candidate.get("market_probability", 0.0),
            )
        )
    )
    paper_trade_costs = 0.0 if top_trade_candidate is None else float(top_trade_candidate.get("estimated_costs", 0.0))
    paper_trade_stake = (paper_trade_execution_price + paper_trade_costs) if top_edge_positive else 0.0
    if top_edge_positive:
        paper_trade_pnl = (1.0 - paper_trade_stake) if top_edge_hit else -paper_trade_stake
    else:
        paper_trade_pnl = 0.0
    top_edge_market = top_trade_candidate or top_edge
    top_edge_quality_tier = str(top_edge_market.get("quality_tier") or "unknown")

    return BlindSnapshotEventEvaluation(
        snapshot_as_of_date=snapshot_as_of_date,
        event_slug=str(event["event_slug"]),
        event_title=str(event["event_title"]),
        event_date=str(event["event_date"]),
        station_code=str(event["station_code"]),
        forecast_strategy=str(event.get("forecast_strategy", "unknown")),
        horizon_days=(date.fromisoformat(str(event["event_date"])) - date.fromisoformat(snapshot_as_of_date)).days,
        event_operable=bool(event.get("event_operable", True)),
        event_evidence_score=(
            None if event.get("event_evidence_score") is None else float(event.get("event_evidence_score"))
        ),
        event_evidence_tier=str(event.get("event_evidence_tier") or "unknown"),
        actual_temp_c=round(float(actual_temp_c), 1),
        actual_temperature_source=actual_temperature_source,
        winner_market_id=winner_market_id,
        winner_question=str(winner_market["question"]),
        model_mode_question=str(event["model_mode_question"]),
        market_mode_question=str(event["market_mode_question"]),
        top_edge_question=str(top_edge_market["question"]),
        top_edge_market_id=str(top_edge_market["market_id"]),
        top_edge_tradeable=top_trade_candidate is not None,
        top_edge_quality_tier=top_edge_quality_tier,
        model_mode_hit=str(model_mode["market_id"]) == winner_market_id,
        market_mode_hit=str(market_mode["market_id"]) == winner_market_id,
        top_edge_positive=top_edge_positive,
        top_edge_hit=top_edge_hit,
        winner_fair_probability=winner_fair_probability,
        winner_market_probability=winner_market_probability,
        model_log_loss=-math.log(max(winner_fair_probability, 1e-9)),
        market_log_loss=-math.log(max(winner_market_probability, 1e-9)),
        model_brier=model_brier,
        market_brier=market_brier,
        top_edge_net=float(top_edge_market["edge_net"]),
        paper_trade_taken=top_edge_positive,
        paper_trade_stake=paper_trade_stake,
        paper_trade_pnl=paper_trade_pnl,
        paper_trade_execution_price=paper_trade_execution_price,
        paper_trade_costs=paper_trade_costs,
    )


def multiclass_brier_score(
    markets: list[dict[str, Any]],
    winner_market_id: str,
    *,
    probability_key: str,
) -> float:
    return sum(
        (float(market[probability_key]) - (1.0 if str(market["market_id"]) == winner_market_id else 0.0)) ** 2
        for market in markets
    )


def summarize_blind_snapshot_evaluations(
    evaluations: list[BlindSnapshotEventEvaluation],
    *,
    paper_edge_threshold: float,
) -> dict[str, Any]:
    summary = _summarize_evaluation_subset(evaluations, paper_edge_threshold=paper_edge_threshold)
    if not evaluations:
        summary["by_strategy"] = {}
        summary["by_horizon_days"] = {}
        summary["by_quality_tier"] = {}
        summary["by_event_evidence_tier"] = {}
        summary["by_actual_temperature_source"] = {}
        return summary

    summary["by_strategy"] = summarize_evaluation_groups(
        evaluations,
        key_fn=lambda evaluation: evaluation.forecast_strategy,
        paper_edge_threshold=paper_edge_threshold,
    )
    summary["by_horizon_days"] = summarize_evaluation_groups(
        evaluations,
        key_fn=lambda evaluation: str(evaluation.horizon_days),
        paper_edge_threshold=paper_edge_threshold,
    )
    summary["by_quality_tier"] = summarize_evaluation_groups(
        evaluations,
        key_fn=lambda evaluation: evaluation.top_edge_quality_tier,
        paper_edge_threshold=paper_edge_threshold,
    )
    summary["by_event_evidence_tier"] = summarize_evaluation_groups(
        evaluations,
        key_fn=lambda evaluation: evaluation.event_evidence_tier,
        paper_edge_threshold=paper_edge_threshold,
    )
    summary["by_actual_temperature_source"] = summarize_evaluation_groups(
        evaluations,
        key_fn=lambda evaluation: evaluation.actual_temperature_source,
        paper_edge_threshold=paper_edge_threshold,
    )
    return summary


def select_top_trade_candidate(markets: list[dict[str, Any]]) -> dict[str, Any] | None:
    tradeable_markets = [
        market
        for market in markets
        if bool(market.get("is_tradeable", not market.get("blockers")))
    ]
    if not tradeable_markets:
        return None
    return max(tradeable_markets, key=lambda market: float(market["edge_net"]))


def summarize_evaluation_groups(
    evaluations: list[BlindSnapshotEventEvaluation],
    *,
    key_fn,
    paper_edge_threshold: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[BlindSnapshotEventEvaluation]] = {}
    for evaluation in evaluations:
        key = str(key_fn(evaluation) or "unknown")
        grouped.setdefault(key, []).append(evaluation)

    return {
        key: _summarize_evaluation_subset(grouped_evaluations, paper_edge_threshold=paper_edge_threshold)
        for key, grouped_evaluations in sorted(grouped.items())
    }


def _summarize_evaluation_subset(
    evaluations: list[BlindSnapshotEventEvaluation],
    *,
    paper_edge_threshold: float,
) -> dict[str, Any]:
    if not evaluations:
        return {
            "events": 0,
            "operable_events": 0,
            "operable_rate": 0.0,
            "avg_event_evidence_score": 0.0,
            "model_mode_hit_rate": 0.0,
            "market_mode_hit_rate": 0.0,
            "avg_winner_fair_probability": 0.0,
            "avg_winner_market_probability": 0.0,
            "model_log_loss": 0.0,
            "market_log_loss": 0.0,
            "model_brier": 0.0,
            "market_brier": 0.0,
            "paper_edge_threshold": paper_edge_threshold,
            "paper_trades": 0,
            "paper_trade_hit_rate": 0.0,
            "paper_total_stake": 0.0,
            "paper_total_pnl": 0.0,
            "paper_roi_on_stake": 0.0,
        }

    paper_trades = [evaluation for evaluation in evaluations if evaluation.paper_trade_taken]
    operable_events = [evaluation for evaluation in evaluations if evaluation.event_operable]
    evidence_scores = [
        evaluation.event_evidence_score
        for evaluation in evaluations
        if evaluation.event_evidence_score is not None
    ]
    total_stake = sum(evaluation.paper_trade_stake for evaluation in paper_trades)
    total_pnl = sum(evaluation.paper_trade_pnl for evaluation in paper_trades)

    return {
        "events": len(evaluations),
        "operable_events": len(operable_events),
        "operable_rate": len(operable_events) / len(evaluations),
        "avg_event_evidence_score": (
            sum(evidence_scores) / len(evidence_scores)
            if evidence_scores
            else 0.0
        ),
        "model_mode_hit_rate": sum(evaluation.model_mode_hit for evaluation in evaluations) / len(evaluations),
        "market_mode_hit_rate": sum(evaluation.market_mode_hit for evaluation in evaluations) / len(evaluations),
        "avg_winner_fair_probability": sum(
            evaluation.winner_fair_probability for evaluation in evaluations
        ) / len(evaluations),
        "avg_winner_market_probability": sum(
            evaluation.winner_market_probability for evaluation in evaluations
        ) / len(evaluations),
        "model_log_loss": sum(evaluation.model_log_loss for evaluation in evaluations) / len(evaluations),
        "market_log_loss": sum(evaluation.market_log_loss for evaluation in evaluations) / len(evaluations),
        "model_brier": sum(evaluation.model_brier for evaluation in evaluations) / len(evaluations),
        "market_brier": sum(evaluation.market_brier for evaluation in evaluations) / len(evaluations),
        "paper_edge_threshold": paper_edge_threshold,
        "paper_trades": len(paper_trades),
        "paper_trade_hit_rate": (
            sum(bool(evaluation.top_edge_hit) for evaluation in paper_trades) / len(paper_trades)
            if paper_trades
            else 0.0
        ),
        "paper_total_stake": total_stake,
        "paper_total_pnl": total_pnl,
        "paper_roi_on_stake": (total_pnl / total_stake) if total_stake > 0 else 0.0,
    }
