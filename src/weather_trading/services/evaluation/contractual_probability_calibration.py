from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.evaluation.bin_family_diagnostics import (
    build_temperature_market_family,
    infer_temperature_unit,
    load_event_payloads_from_audit,
)
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def get_current_probability_temperature_config() -> dict[str, Any]:
    default_alpha = float(ConfigLoader.get("forecast_policy.probability_temperature_alpha", 1.0) or 1.0)
    raw_unit_mapping = ConfigLoader.get("forecast_policy.probability_temperature_alpha_by_unit", {}) or {}
    unit_alpha_map: dict[str, float] = {}
    if isinstance(raw_unit_mapping, dict):
        for key, value in raw_unit_mapping.items():
            try:
                unit_alpha_map[str(key).strip().lower()] = float(value)
            except (TypeError, ValueError):
                continue
    return {
        "default_alpha": default_alpha,
        "unit_alpha_map": unit_alpha_map,
    }


def load_contractual_event_payloads(
    contractual_audit_payload: dict[str, Any],
    workspace_root: Path,
) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    source_rel_path = contractual_audit_payload.get("source_audit_snapshot")
    if not source_rel_path:
        raise ValueError("El snapshot contractual no contiene source_audit_snapshot.")
    source_audit_path = workspace_root / str(source_rel_path)
    source_audit_payload = json.loads(source_audit_path.read_text(encoding="utf-8"))
    event_payloads = load_event_payloads_from_audit(source_audit_payload, workspace_root)
    return source_audit_payload, event_payloads


def normalize_market_probabilities(
    markets: list[dict[str, Any]],
    *,
    probability_key: str,
    alpha: float | None = None,
) -> dict[str, float]:
    raw = {
        str(market["market_id"]): max(float(market.get(probability_key, 0.0)), 1e-12)
        for market in markets
        if market.get("market_id") is not None
    }
    if alpha is not None:
        raw = {
            market_id: probability ** float(alpha)
            for market_id, probability in raw.items()
        }
    total = sum(raw.values()) or 1.0
    return {
        market_id: probability / total
        for market_id, probability in raw.items()
    }


def resolve_event_alpha(
    *,
    temperature_unit: str,
    default_alpha: float,
    unit_alpha_map: dict[str, float] | None,
) -> float:
    mapping = unit_alpha_map or {}
    normalized_unit = str(temperature_unit or "unknown").strip().lower()
    try:
        return float(mapping.get(normalized_unit, default_alpha))
    except (TypeError, ValueError):
        return float(default_alpha)


def evaluate_contractual_probability_config(
    contractual_audit_payload: dict[str, Any],
    event_payloads: dict[tuple[str, str], dict[str, Any]],
    *,
    default_alpha: float,
    unit_alpha_map: dict[str, float] | None = None,
) -> dict[str, Any]:
    winner_probabilities: list[float] = []
    market_winner_probabilities: list[float] = []
    model_briers: list[float] = []
    market_briers: list[float] = []
    model_mode_hits: list[bool] = []
    market_mode_hits: list[bool] = []

    for comparison in contractual_audit_payload.get("comparisons", []):
        key = (str(comparison.get("snapshot_as_of_date")), str(comparison.get("event_slug")))
        event = event_payloads.get(key)
        if not event:
            continue
        markets = list(event.get("markets", []))
        if not markets:
            continue

        winner_market_id = str(comparison.get("contractual_winner_market_id") or "")
        winner_question = str(comparison.get("contractual_winner_question") or "")
        temperature_unit = infer_temperature_unit(winner_question)
        alpha = resolve_event_alpha(
            temperature_unit=temperature_unit,
            default_alpha=default_alpha,
            unit_alpha_map=unit_alpha_map,
        )
        model_probabilities = normalize_market_probabilities(markets, probability_key="fair_probability", alpha=alpha)
        market_probabilities = normalize_market_probabilities(markets, probability_key="market_probability")
        if winner_market_id not in model_probabilities or winner_market_id not in market_probabilities:
            continue

        winner_probabilities.append(model_probabilities[winner_market_id])
        market_winner_probabilities.append(market_probabilities[winner_market_id])
        model_briers.append(
            sum(
                (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                for market_id, probability in model_probabilities.items()
            )
        )
        market_briers.append(
            sum(
                (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                for market_id, probability in market_probabilities.items()
            )
        )
        model_mode_hits.append(max(model_probabilities, key=model_probabilities.get) == winner_market_id)
        market_mode_hits.append(max(market_probabilities, key=market_probabilities.get) == winner_market_id)

    events = len(winner_probabilities)
    if events == 0:
        return {
            "events": 0,
            "model_log_loss": 0.0,
            "market_log_loss": 0.0,
            "model_brier": 0.0,
            "market_brier": 0.0,
            "model_mode_hit_rate": 0.0,
            "market_mode_hit_rate": 0.0,
        }

    return {
        "events": events,
        "model_log_loss": sum(-math.log(max(probability, 1e-9)) for probability in winner_probabilities) / events,
        "market_log_loss": sum(-math.log(max(probability, 1e-9)) for probability in market_winner_probabilities)
        / events,
        "model_brier": sum(model_briers) / events,
        "market_brier": sum(market_briers) / events,
        "model_mode_hit_rate": sum(model_mode_hits) / events,
        "market_mode_hit_rate": sum(market_mode_hits) / events,
    }


def build_contractual_family_summary(
    contractual_audit_payload: dict[str, Any],
    event_payloads: dict[tuple[str, str], dict[str, Any]],
    *,
    default_alpha: float,
    unit_alpha_map: dict[str, float] | None = None,
) -> dict[str, Any]:
    parser = DeterministicParser()
    winner_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cohort_groups: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

    for comparison in contractual_audit_payload.get("comparisons", []):
        key = (str(comparison.get("snapshot_as_of_date")), str(comparison.get("event_slug")))
        event = event_payloads.get(key)
        if not event:
            continue
        markets = list(event.get("markets", []))
        if not markets:
            continue
        winner_market_id = str(comparison.get("contractual_winner_market_id") or "")
        winner_question = str(comparison.get("contractual_winner_question") or "")
        family = build_temperature_market_family(
            winner_question,
            str(comparison.get("event_date") or ""),
            parser,
        )
        temperature_unit = infer_temperature_unit(winner_question)
        alpha = resolve_event_alpha(
            temperature_unit=temperature_unit,
            default_alpha=default_alpha,
            unit_alpha_map=unit_alpha_map,
        )
        model_probabilities = normalize_market_probabilities(markets, probability_key="fair_probability", alpha=alpha)
        market_probabilities = normalize_market_probabilities(markets, probability_key="market_probability")
        if winner_market_id not in model_probabilities or winner_market_id not in market_probabilities:
            continue

        row = {
            "winner_market_id": winner_market_id,
            "model_log_loss": -math.log(max(model_probabilities[winner_market_id], 1e-9)),
            "market_log_loss": -math.log(max(market_probabilities[winner_market_id], 1e-9)),
            "model_mode_hit": max(model_probabilities, key=model_probabilities.get) == winner_market_id,
            "market_mode_hit": max(market_probabilities, key=market_probabilities.get) == winner_market_id,
            "model_brier": sum(
                (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                for market_id, probability in model_probabilities.items()
            ),
            "market_brier": sum(
                (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                for market_id, probability in market_probabilities.items()
            ),
        }
        winner_groups[family].append(row)
        cohort_groups[str(comparison.get("snapshot_as_of_date"))][family].append(row)

    winner_family_summary = {
        family: summarize_contractual_rows(rows)
        for family, rows in sorted(winner_groups.items())
    }
    winner_family_by_cohort = {
        cohort: {
            family: summarize_contractual_rows(rows)
            for family, rows in sorted(groups.items())
        }
        for cohort, groups in sorted(cohort_groups.items())
    }

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
        "winner_family_by_cohort": winner_family_by_cohort,
        "recommendations": {
            "weakest_groups": weakest_groups,
            "strongest_groups": strongest_groups,
        },
    }


def summarize_contractual_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    events = len(rows)
    if events == 0:
        return {
            "events": 0,
            "model_log_loss": 0.0,
            "market_log_loss": 0.0,
            "model_brier": 0.0,
            "market_brier": 0.0,
            "model_mode_hit_rate": 0.0,
            "market_mode_hit_rate": 0.0,
        }
    return {
        "events": events,
        "model_log_loss": sum(float(row["model_log_loss"]) for row in rows) / events,
        "market_log_loss": sum(float(row["market_log_loss"]) for row in rows) / events,
        "model_brier": sum(float(row["model_brier"]) for row in rows) / events,
        "market_brier": sum(float(row["market_brier"]) for row in rows) / events,
        "model_mode_hit_rate": sum(bool(row["model_mode_hit"]) for row in rows) / events,
        "market_mode_hit_rate": sum(bool(row["market_mode_hit"]) for row in rows) / events,
    }


def evaluate_global_alpha_candidates(
    contractual_audit_payload: dict[str, Any],
    event_payloads: dict[tuple[str, str], dict[str, Any]],
    *,
    alpha_min: float,
    alpha_max: float,
    alpha_step: float,
) -> list[dict[str, Any]]:
    candidates = []
    value = alpha_min
    while value <= alpha_max + 1e-9:
        alpha = round(value, 4)
        metrics = evaluate_contractual_probability_config(
            contractual_audit_payload,
            event_payloads,
            default_alpha=alpha,
            unit_alpha_map={},
        )
        candidates.append({"kind": "global", "alpha": alpha, **metrics})
        value += alpha_step
    return candidates


def evaluate_unit_alpha_candidates(
    contractual_audit_payload: dict[str, Any],
    event_payloads: dict[tuple[str, str], dict[str, Any]],
    *,
    alpha_min: float,
    alpha_max: float,
    alpha_step: float,
    default_alpha: float,
) -> list[dict[str, Any]]:
    grid: list[float] = []
    value = alpha_min
    while value <= alpha_max + 1e-9:
        grid.append(round(value, 4))
        value += alpha_step

    candidates = []
    for celsius_alpha in grid:
        for fahrenheit_alpha in grid:
            unit_alpha_map = {
                "celsius": celsius_alpha,
                "fahrenheit": fahrenheit_alpha,
            }
            metrics = evaluate_contractual_probability_config(
                contractual_audit_payload,
                event_payloads,
                default_alpha=default_alpha,
                unit_alpha_map=unit_alpha_map,
            )
            candidates.append(
                {
                    "kind": "unit",
                    "default_alpha": default_alpha,
                    "celsius_alpha": celsius_alpha,
                    "fahrenheit_alpha": fahrenheit_alpha,
                    "unit_alpha_map": unit_alpha_map,
                    **metrics,
                }
            )
    return candidates


def select_contractual_calibration_candidate(
    *,
    current_config_metrics: dict[str, Any],
    best_global_candidate: dict[str, Any],
    best_unit_candidate: dict[str, Any],
    max_brier_degradation_ratio: float,
    max_mode_hit_drop: float,
) -> dict[str, Any]:
    baseline_brier = float(current_config_metrics["model_brier"])
    baseline_mode_hit = float(current_config_metrics["model_mode_hit_rate"])
    candidates = [
        {"kind": "current", **current_config_metrics},
        best_global_candidate,
        best_unit_candidate,
    ]

    def is_eligible(candidate: dict[str, Any]) -> bool:
        return (
            float(candidate["model_brier"]) <= baseline_brier * (1.0 + max_brier_degradation_ratio)
            and float(candidate["model_mode_hit_rate"]) >= baseline_mode_hit - max_mode_hit_drop
        )

    eligible = [candidate for candidate in candidates if is_eligible(candidate)]
    if not eligible:
        return {"kind": "current", **current_config_metrics}

    return min(
        eligible,
        key=lambda item: (
            float(item["model_log_loss"]),
            float(item["model_brier"]),
            -float(item["model_mode_hit_rate"]),
            0 if item["kind"] == "global" else 1 if item["kind"] == "unit" else 2,
        ),
    )
