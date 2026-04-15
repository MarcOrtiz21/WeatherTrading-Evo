from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

from weather_trading.domain.models import MetricKind
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def infer_temperature_unit(question: str) -> str:
    normalized = (question or "").lower()
    if "°f" in normalized or "fahrenheit" in normalized:
        return "fahrenheit"
    if "°c" in normalized or "celsius" in normalized:
        return "celsius"
    return "unknown"


def classify_temperature_market_shape(
    question: str,
    event_date: str | None,
    parser: DeterministicParser,
) -> str:
    spec = parser.parse(
        {
            "id": "bin-family-diagnostic",
            "question": question,
            "description": "",
            "rules": "",
            "event_date": event_date,
            "outcomes": ["Yes", "No"],
        }
    )
    if spec is None:
        return "unparsed"
    if spec.metric != MetricKind.TEMPERATURE_BIN:
        return "other_metric"

    low_c = spec.bin_low_c
    high_c = spec.bin_high_c
    if low_c is None and high_c is not None:
        return "lower_tail"
    if low_c is not None and high_c is None:
        return "upper_tail"
    if low_c is not None and high_c is not None:
        if abs(low_c - high_c) < 1e-9:
            return "exact_point"
        return "range_bin"
    return "unknown"


def build_temperature_market_family(
    question: str,
    event_date: str | None,
    parser: DeterministicParser,
) -> str:
    unit = infer_temperature_unit(question)
    shape = classify_temperature_market_shape(question, event_date, parser)
    return f"{unit}|{shape}"


def load_event_payloads_from_audit(audit_payload: dict, workspace_root: Path) -> dict[tuple[str, str], dict]:
    event_payloads: dict[tuple[str, str], dict] = {}
    for snapshot_file in audit_payload.get("snapshot_files", []):
        snapshot_path = workspace_root / snapshot_file
        if not snapshot_path.exists():
            continue
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot_as_of_date = str(snapshot.get("as_of_date"))
        for event in snapshot.get("evaluated_events", []):
            key = (snapshot_as_of_date, str(event.get("event_slug")))
            event_payloads[key] = event
    return event_payloads


def _summarize_evaluations(evaluations: list[dict]) -> dict:
    events = len(evaluations)
    if events == 0:
        return {
            "events": 0,
            "model_mode_hit_rate": 0.0,
            "market_mode_hit_rate": 0.0,
            "top_edge_hit_rate": 0.0,
            "paper_trade_rate": 0.0,
            "model_log_loss": 0.0,
            "market_log_loss": 0.0,
            "model_brier": 0.0,
            "market_brier": 0.0,
            "paper_total_pnl": 0.0,
            "paper_total_stake": 0.0,
            "paper_roi_on_stake": 0.0,
            "avg_top_edge_net": 0.0,
        }

    paper_total_stake = sum(float(evaluation.get("paper_trade_stake") or 0.0) for evaluation in evaluations)
    paper_total_pnl = sum(float(evaluation.get("paper_trade_pnl") or 0.0) for evaluation in evaluations)
    return {
        "events": events,
        "model_mode_hit_rate": sum(bool(evaluation.get("model_mode_hit")) for evaluation in evaluations) / events,
        "market_mode_hit_rate": sum(bool(evaluation.get("market_mode_hit")) for evaluation in evaluations) / events,
        "top_edge_hit_rate": sum(bool(evaluation.get("top_edge_hit")) for evaluation in evaluations) / events,
        "paper_trade_rate": sum(bool(evaluation.get("paper_trade_taken")) for evaluation in evaluations) / events,
        "model_log_loss": sum(float(evaluation.get("model_log_loss") or 0.0) for evaluation in evaluations) / events,
        "market_log_loss": sum(float(evaluation.get("market_log_loss") or 0.0) for evaluation in evaluations) / events,
        "model_brier": sum(float(evaluation.get("model_brier") or 0.0) for evaluation in evaluations) / events,
        "market_brier": sum(float(evaluation.get("market_brier") or 0.0) for evaluation in evaluations) / events,
        "paper_total_pnl": paper_total_pnl,
        "paper_total_stake": paper_total_stake,
        "paper_roi_on_stake": (paper_total_pnl / paper_total_stake) if paper_total_stake > 0 else 0.0,
        "avg_top_edge_net": sum(float(evaluation.get("top_edge_net") or 0.0) for evaluation in evaluations) / events,
    }


def _calibrate_market_probabilities(markets: list[dict], alpha: float) -> dict[str, float]:
    raw = {
        str(market["market_id"]): max(float(market.get("fair_probability", 0.0)), 1e-12)
        for market in markets
    }
    adjusted = {
        market_id: probability ** alpha
        for market_id, probability in raw.items()
    }
    total = sum(adjusted.values()) or 1.0
    return {
        market_id: probability / total
        for market_id, probability in adjusted.items()
    }


def _evaluate_alpha_config(
    audit_payload: dict,
    event_payloads: dict[tuple[str, str], dict],
    parser: DeterministicParser,
    *,
    global_alpha: float | None = None,
    unit_alpha_map: dict[str, float] | None = None,
) -> dict:
    winner_probabilities: list[float] = []
    briers: list[float] = []
    mode_hits: list[bool] = []

    for evaluation in audit_payload.get("evaluations", []):
        key = (str(evaluation.get("snapshot_as_of_date")), str(evaluation.get("event_slug")))
        event = event_payloads.get(key)
        if not event:
            continue
        markets = list(event.get("markets", []))
        if not markets:
            continue

        unit = infer_temperature_unit(str(evaluation.get("winner_question") or ""))
        alpha = global_alpha if global_alpha is not None else float((unit_alpha_map or {}).get(unit, 1.0))
        probabilities = _calibrate_market_probabilities(markets, alpha)
        winner_market_id = str(evaluation.get("winner_market_id"))
        winner_probability = probabilities.get(winner_market_id)
        if winner_probability is None:
            continue

        winner_probabilities.append(winner_probability)
        briers.append(
            sum(
                (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                for market_id, probability in probabilities.items()
            )
        )
        mode_hits.append(max(probabilities, key=probabilities.get) == winner_market_id)

    events = len(winner_probabilities)
    if events == 0:
        return {"events": 0, "log_loss": 0.0, "brier": 0.0, "mode_hit_rate": 0.0}
    return {
        "events": events,
        "log_loss": sum(-math.log(max(probability, 1e-9)) for probability in winner_probabilities) / events,
        "brier": sum(briers) / events,
        "mode_hit_rate": sum(mode_hits) / events,
    }


def build_bin_family_diagnostics(
    audit_payload: dict,
    event_payloads: dict[tuple[str, str], dict],
    *,
    current_alpha: float,
    parser: DeterministicParser | None = None,
) -> dict:
    diagnostic_parser = parser or DeterministicParser()

    winner_groups: dict[str, list[dict]] = defaultdict(list)
    top_edge_groups: dict[str, list[dict]] = defaultdict(list)
    cohort_groups: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

    for evaluation in audit_payload.get("evaluations", []):
        winner_family = build_temperature_market_family(
            str(evaluation.get("winner_question") or ""),
            str(evaluation.get("event_date") or ""),
            diagnostic_parser,
        )
        top_edge_family = build_temperature_market_family(
            str(evaluation.get("top_edge_question") or ""),
            str(evaluation.get("event_date") or ""),
            diagnostic_parser,
        )
        cohort = str(evaluation.get("snapshot_as_of_date"))

        winner_groups[winner_family].append(evaluation)
        top_edge_groups[top_edge_family].append(evaluation)
        cohort_groups[cohort][winner_family].append(evaluation)

    winner_family_summary = {
        family: _summarize_evaluations(evaluations)
        for family, evaluations in sorted(winner_groups.items())
    }
    top_edge_family_summary = {
        family: _summarize_evaluations(evaluations)
        for family, evaluations in sorted(top_edge_groups.items())
    }
    winner_family_by_cohort = {
        cohort: {
            family: _summarize_evaluations(evaluations)
            for family, evaluations in sorted(families.items())
        }
        for cohort, families in sorted(cohort_groups.items())
    }

    current_alpha_metrics = _evaluate_alpha_config(
        audit_payload,
        event_payloads,
        diagnostic_parser,
        global_alpha=current_alpha,
    )

    global_candidates = []
    for alpha in [round(0.45 + 0.05 * index, 2) for index in range(14)]:
        metrics = _evaluate_alpha_config(
            audit_payload,
            event_payloads,
            diagnostic_parser,
            global_alpha=alpha,
        )
        global_candidates.append({"alpha": alpha, **metrics})
    best_global_alpha = min(
        global_candidates,
        key=lambda item: (item["log_loss"], item["brier"], abs(item["alpha"] - current_alpha)),
    )

    unit_candidates = []
    grid = [0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8, 0.9, 1.0]
    for celsius_alpha in grid:
        for fahrenheit_alpha in grid:
            unit_alpha_map = {
                "celsius": celsius_alpha,
                "fahrenheit": fahrenheit_alpha,
                "unknown": current_alpha,
            }
            metrics = _evaluate_alpha_config(
                audit_payload,
                event_payloads,
                diagnostic_parser,
                unit_alpha_map=unit_alpha_map,
            )
            unit_candidates.append(
                {
                    "celsius_alpha": celsius_alpha,
                    "fahrenheit_alpha": fahrenheit_alpha,
                    **metrics,
                }
            )
    best_unit_alpha = min(
        unit_candidates,
        key=lambda item: (
            item["log_loss"],
            item["brier"],
            abs(item["celsius_alpha"] - current_alpha) + abs(item["fahrenheit_alpha"] - current_alpha),
        ),
    )

    dominating_unit_candidates = [
        candidate
        for candidate in unit_candidates
        if candidate["log_loss"] <= current_alpha_metrics["log_loss"]
        and candidate["brier"] <= current_alpha_metrics["brier"]
        and candidate["mode_hit_rate"] >= current_alpha_metrics["mode_hit_rate"]
        and (
            abs(candidate["celsius_alpha"] - current_alpha) > 1e-9
            or abs(candidate["fahrenheit_alpha"] - current_alpha) > 1e-9
        )
    ]

    weakest_groups = sorted(
        (
            {
                "family": family,
                "events": summary["events"],
                "log_loss_delta_vs_market": summary["model_log_loss"] - summary["market_log_loss"],
                "mode_hit_delta_vs_market": summary["model_mode_hit_rate"] - summary["market_mode_hit_rate"],
            }
            for family, summary in winner_family_summary.items()
            if summary["events"] >= 5
        ),
        key=lambda item: (item["log_loss_delta_vs_market"], -item["mode_hit_delta_vs_market"]),
        reverse=True,
    )[:5]
    strongest_groups = sorted(
        (
            {
                "family": family,
                "events": summary["events"],
                "log_loss_delta_vs_market": summary["model_log_loss"] - summary["market_log_loss"],
                "mode_hit_delta_vs_market": summary["model_mode_hit_rate"] - summary["market_mode_hit_rate"],
            }
            for family, summary in winner_family_summary.items()
            if summary["events"] >= 5
        ),
        key=lambda item: (
            item["log_loss_delta_vs_market"],
            item["mode_hit_delta_vs_market"],
        ),
    )[:5]

    return {
        "winner_family_summary": winner_family_summary,
        "top_edge_family_summary": top_edge_family_summary,
        "winner_family_by_cohort": winner_family_by_cohort,
        "calibration_probes": {
            "current_global_alpha": {"alpha": current_alpha, **current_alpha_metrics},
            "best_global_alpha": best_global_alpha,
            "best_unit_alpha": best_unit_alpha,
            "dominating_unit_candidates": dominating_unit_candidates[:10],
        },
        "recommendations": {
            "weakest_groups": weakest_groups,
            "strongest_groups": strongest_groups,
        },
    }
