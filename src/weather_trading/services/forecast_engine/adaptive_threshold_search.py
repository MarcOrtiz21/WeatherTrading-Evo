from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


def _normalize_objective_metric_name(objective: str) -> str:
    return objective[len("adaptive_"):] if objective.startswith("adaptive_") else objective


def _read_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row[key]
    return getattr(row, key)


def summarize_candidate_policy(rows: list[Any], baseline_max_horizon_days: int) -> dict[str, Any]:
    if not rows:
        return {
            "events": 0,
            "baseline_max_horizon_days": baseline_max_horizon_days,
            "adaptive_hit_rate": 0.0,
            "adaptive_avg_winner_prob": 0.0,
            "adaptive_log_loss": 0.0,
            "adaptive_brier": 0.0,
            "adaptive_winner_prob_improvement_rate": 0.0,
        }

    chosen_winner_probabilities: list[float] = []
    chosen_hits: list[bool] = []
    chosen_briers: list[float] = []
    improvements = 0

    for row in rows:
        horizon_days = int(_read_value(row, "horizon_days"))
        prefix = "baseline" if horizon_days <= baseline_max_horizon_days else "optimized"
        winner_probability = float(_read_value(row, f"{prefix}_winner_probability"))
        mode_hit = bool(_read_value(row, f"{prefix}_mode_hit"))
        brier = float(_read_value(row, f"{prefix}_brier"))
        baseline_winner_probability = float(_read_value(row, "baseline_winner_probability"))

        chosen_winner_probabilities.append(winner_probability)
        chosen_hits.append(mode_hit)
        chosen_briers.append(brier)
        if winner_probability > baseline_winner_probability:
            improvements += 1

    return {
        "events": len(rows),
        "baseline_max_horizon_days": baseline_max_horizon_days,
        "adaptive_hit_rate": sum(chosen_hits) / len(rows),
        "adaptive_avg_winner_prob": sum(chosen_winner_probabilities) / len(rows),
        "adaptive_log_loss": sum(-math.log(max(probability, 1e-9)) for probability in chosen_winner_probabilities) / len(rows),
        "adaptive_brier": sum(chosen_briers) / len(rows),
        "adaptive_winner_prob_improvement_rate": improvements / len(rows),
    }


def search_optimal_baseline_max_horizon_days(
    rows: list[Any],
    max_horizon_days: int,
    objective: str = "adaptive_log_loss",
) -> dict[str, Any]:
    candidates = [
        summarize_candidate_policy(rows, baseline_max_horizon_days=cutoff)
        for cutoff in range(0, max_horizon_days + 1)
    ]

    def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, int]:
        return (
            float(candidate.get(objective, 0.0)),
            float(candidate["adaptive_brier"]),
            -float(candidate["adaptive_hit_rate"]),
            -float(candidate["adaptive_avg_winner_prob"]),
            int(candidate["baseline_max_horizon_days"]),
        )

    selected_policy = min(candidates, key=ranking_key) if candidates else None
    return {
        "objective": objective,
        "selected_policy": selected_policy,
        "candidates": candidates,
    }


def aggregate_policy_searches(
    window_searches: list[dict[str, Any]],
    objective: str = "adaptive_log_loss",
) -> dict[str, Any]:
    if not window_searches:
        return {
            "objective": objective,
            "windows_evaluated": 0,
            "window_cutoffs": [],
            "selected_policy": None,
            "candidates": [],
        }

    aggregated: dict[int, dict[str, Any]] = {}
    window_cutoffs: list[int] = []

    for window_search in window_searches:
        lookback_days = int(window_search["lookback_days"])
        search_payload = window_search["policy_search"]
        selected_policy = search_payload.get("selected_policy")
        selected_cutoff = None if selected_policy is None else int(selected_policy["baseline_max_horizon_days"])
        if selected_cutoff is not None:
            window_cutoffs.append(selected_cutoff)

        for candidate in search_payload.get("candidates", []):
            cutoff = int(candidate["baseline_max_horizon_days"])
            entry = aggregated.setdefault(
                cutoff,
                {
                    "baseline_max_horizon_days": cutoff,
                    "windows": [],
                    "window_count": 0,
                    "selected_count": 0,
                    "mean_adaptive_hit_rate": 0.0,
                    "mean_adaptive_avg_winner_prob": 0.0,
                    "mean_adaptive_log_loss": 0.0,
                    "mean_adaptive_brier": 0.0,
                    "mean_adaptive_winner_prob_improvement_rate": 0.0,
                },
            )
            entry["windows"].append(
                {
                    "lookback_days": lookback_days,
                    "adaptive_hit_rate": candidate["adaptive_hit_rate"],
                    "adaptive_avg_winner_prob": candidate["adaptive_avg_winner_prob"],
                    "adaptive_log_loss": candidate["adaptive_log_loss"],
                    "adaptive_brier": candidate["adaptive_brier"],
                    "adaptive_winner_prob_improvement_rate": candidate["adaptive_winner_prob_improvement_rate"],
                }
            )
            entry["window_count"] += 1
            if selected_cutoff == cutoff:
                entry["selected_count"] += 1

    aggregated_candidates: list[dict[str, Any]] = []
    for cutoff in sorted(aggregated):
        entry = aggregated[cutoff]
        windows = entry["windows"]
        window_count = max(entry["window_count"], 1)
        aggregated_candidates.append(
            {
                "baseline_max_horizon_days": cutoff,
                "window_count": entry["window_count"],
                "selected_count": entry["selected_count"],
                "selection_frequency": entry["selected_count"] / window_count,
                "mean_adaptive_hit_rate": sum(window["adaptive_hit_rate"] for window in windows) / window_count,
                "mean_adaptive_avg_winner_prob": sum(
                    window["adaptive_avg_winner_prob"] for window in windows
                ) / window_count,
                "mean_adaptive_log_loss": sum(window["adaptive_log_loss"] for window in windows) / window_count,
                "mean_adaptive_brier": sum(window["adaptive_brier"] for window in windows) / window_count,
                "mean_adaptive_winner_prob_improvement_rate": sum(
                    window["adaptive_winner_prob_improvement_rate"] for window in windows
                ) / window_count,
                "windows": windows,
            }
        )

    objective_metric_name = _normalize_objective_metric_name(objective)

    def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, int]:
        return (
            float(candidate.get(f"mean_adaptive_{objective_metric_name}", candidate.get("mean_adaptive_log_loss", 0.0))),
            float(candidate["mean_adaptive_brier"]),
            -float(candidate["selection_frequency"]),
            -float(candidate["mean_adaptive_hit_rate"]),
            int(candidate["baseline_max_horizon_days"]),
        )

    selected_policy = min(aggregated_candidates, key=ranking_key) if aggregated_candidates else None
    return {
        "objective": objective,
        "windows_evaluated": len(window_searches),
        "window_cutoffs": window_cutoffs,
        "selected_policy": selected_policy,
        "candidates": aggregated_candidates,
    }


def aggregate_horizon_strategy_searches(
    window_searches: list[dict[str, Any]],
    max_horizon_days: int,
    objective: str = "adaptive_log_loss",
) -> dict[str, Any]:
    objective_metric_name = _normalize_objective_metric_name(objective)
    selected_strategy_by_horizon: dict[str, str] = {}
    per_horizon: list[dict[str, Any]] = []
    weighted_totals = {
        "events": 0,
        "adaptive_hit_rate": 0.0,
        "adaptive_avg_winner_prob": 0.0,
        "adaptive_log_loss": 0.0,
        "adaptive_brier": 0.0,
        "adaptive_winner_prob_improvement_rate": 0.0,
    }

    for horizon_days in range(1, max_horizon_days + 1):
        horizon_key = str(horizon_days)
        horizon_windows = []
        for window_search in window_searches:
            horizon_summary = window_search.get("by_horizon", {}).get(horizon_key)
            if horizon_summary and horizon_summary.get("events", 0) > 0:
                horizon_windows.append(
                    {
                        "lookback_days": int(window_search["lookback_days"]),
                        "summary": horizon_summary,
                    }
                )

        if not horizon_windows:
            continue

        total_events = sum(window["summary"]["events"] for window in horizon_windows)

        def build_strategy_candidate(strategy_prefix: str, strategy_name: str) -> dict[str, Any]:
            if strategy_prefix == "baseline":
                mean_improvement_rate = 0.0
            else:
                mean_improvement_rate = sum(
                    float(window["summary"].get("winner_prob_improvement_rate", 0.0)) * window["summary"]["events"]
                    for window in horizon_windows
                ) / total_events

            return {
                "strategy": strategy_name,
                "events": total_events,
                "mean_adaptive_hit_rate": sum(
                    window["summary"][f"{strategy_prefix}_hit_rate"] * window["summary"]["events"]
                    for window in horizon_windows
                ) / total_events,
                "mean_adaptive_avg_winner_prob": sum(
                    window["summary"][f"{strategy_prefix}_avg_winner_prob"] * window["summary"]["events"]
                    for window in horizon_windows
                ) / total_events,
                "mean_adaptive_log_loss": sum(
                    window["summary"][f"{strategy_prefix}_log_loss"] * window["summary"]["events"]
                    for window in horizon_windows
                ) / total_events,
                "mean_adaptive_brier": sum(
                    window["summary"][f"{strategy_prefix}_brier"] * window["summary"]["events"]
                    for window in horizon_windows
                ) / total_events,
                "mean_adaptive_winner_prob_improvement_rate": mean_improvement_rate,
            }

        candidates = [
            build_strategy_candidate("baseline", "baseline_short_horizon"),
            build_strategy_candidate("optimized", "calibrated_long_horizon"),
        ]

        def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, str]:
            return (
                float(candidate.get(f"mean_adaptive_{objective_metric_name}", candidate["mean_adaptive_log_loss"])),
                float(candidate["mean_adaptive_brier"]),
                -float(candidate["mean_adaptive_hit_rate"]),
                -float(candidate["mean_adaptive_avg_winner_prob"]),
                str(candidate["strategy"]),
            )

        selected_candidate = min(candidates, key=ranking_key)
        selected_strategy_by_horizon[horizon_key] = selected_candidate["strategy"]

        per_horizon.append(
            {
                "horizon_days": horizon_days,
                "events": total_events,
                "selected_strategy": selected_candidate["strategy"],
                "baseline_candidate": candidates[0],
                "optimized_candidate": candidates[1],
            }
        )

        weighted_totals["events"] += total_events
        weighted_totals["adaptive_hit_rate"] += selected_candidate["mean_adaptive_hit_rate"] * total_events
        weighted_totals["adaptive_avg_winner_prob"] += selected_candidate["mean_adaptive_avg_winner_prob"] * total_events
        weighted_totals["adaptive_log_loss"] += selected_candidate["mean_adaptive_log_loss"] * total_events
        weighted_totals["adaptive_brier"] += selected_candidate["mean_adaptive_brier"] * total_events
        weighted_totals["adaptive_winner_prob_improvement_rate"] += (
            selected_candidate["mean_adaptive_winner_prob_improvement_rate"] * total_events
        )

    total_events = max(weighted_totals["events"], 1)
    policy_summary = {
        "events": weighted_totals["events"],
        "adaptive_hit_rate": weighted_totals["adaptive_hit_rate"] / total_events,
        "adaptive_avg_winner_prob": weighted_totals["adaptive_avg_winner_prob"] / total_events,
        "adaptive_log_loss": weighted_totals["adaptive_log_loss"] / total_events,
        "adaptive_brier": weighted_totals["adaptive_brier"] / total_events,
        "adaptive_winner_prob_improvement_rate": weighted_totals["adaptive_winner_prob_improvement_rate"] / total_events,
    }

    return {
        "objective": objective,
        "selected_strategy_by_horizon": selected_strategy_by_horizon,
        "per_horizon": per_horizon,
        "policy_summary": policy_summary,
    }


def derive_fallback_cutoff_from_overrides(horizon_strategy_overrides: dict[str, str]) -> int:
    cutoff = 0
    for horizon_days in sorted(int(key) for key in horizon_strategy_overrides):
        if horizon_days != cutoff + 1:
            break
        if horizon_strategy_overrides[str(horizon_days)] != "baseline_short_horizon":
            break
        cutoff = horizon_days
    return cutoff


def select_applied_policy_candidate(
    aggregated_cutoff_search: dict[str, Any],
    aggregated_horizon_search: dict[str, Any],
    objective: str = "adaptive_log_loss",
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    objective_metric_name = _normalize_objective_metric_name(objective)

    cutoff_policy = aggregated_cutoff_search.get("selected_policy")
    if cutoff_policy:
        candidates.append(
            {
                "selection_mode": "cutoff",
                "baseline_max_horizon_days": int(cutoff_policy["baseline_max_horizon_days"]),
                "horizon_strategy_overrides": {},
                "objective_value": float(
                    cutoff_policy.get(
                        f"mean_adaptive_{objective_metric_name}",
                        cutoff_policy.get("mean_adaptive_log_loss", 0.0),
                    )
                ),
                "adaptive_hit_rate": float(cutoff_policy["mean_adaptive_hit_rate"]),
                "adaptive_avg_winner_prob": float(cutoff_policy["mean_adaptive_avg_winner_prob"]),
                "adaptive_log_loss": float(cutoff_policy["mean_adaptive_log_loss"]),
                "adaptive_brier": float(cutoff_policy["mean_adaptive_brier"]),
                "adaptive_winner_prob_improvement_rate": float(
                    cutoff_policy["mean_adaptive_winner_prob_improvement_rate"]
                ),
            }
        )

    horizon_policy = aggregated_horizon_search.get("policy_summary")
    horizon_overrides = aggregated_horizon_search.get("selected_strategy_by_horizon", {})
    if horizon_policy and horizon_overrides:
        candidates.append(
            {
                "selection_mode": "horizon_overrides",
                "baseline_max_horizon_days": derive_fallback_cutoff_from_overrides(horizon_overrides),
                "horizon_strategy_overrides": horizon_overrides,
                "objective_value": float(
                    horizon_policy.get(
                        objective,
                        horizon_policy.get("adaptive_log_loss", 0.0),
                    )
                ),
                "adaptive_hit_rate": float(horizon_policy["adaptive_hit_rate"]),
                "adaptive_avg_winner_prob": float(horizon_policy["adaptive_avg_winner_prob"]),
                "adaptive_log_loss": float(horizon_policy["adaptive_log_loss"]),
                "adaptive_brier": float(horizon_policy["adaptive_brier"]),
                "adaptive_winner_prob_improvement_rate": float(
                    horizon_policy["adaptive_winner_prob_improvement_rate"]
                ),
            }
        )

    if not candidates:
        return None

    def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, int]:
        return (
            float(candidate["objective_value"]),
            float(candidate["adaptive_brier"]),
            -float(candidate["adaptive_hit_rate"]),
            -float(candidate["adaptive_avg_winner_prob"]),
            0 if candidate["selection_mode"] == "cutoff" else 1,
        )

    return min(candidates, key=ranking_key)


def aggregate_multidate_cutoff_searches(
    date_results: list[dict[str, Any]],
    objective: str = "adaptive_log_loss",
) -> dict[str, Any]:
    if not date_results:
        return {
            "objective": objective,
            "dates_evaluated": 0,
            "selected_date_cutoffs": [],
            "selected_policy": None,
            "candidates": [],
        }

    aggregated: dict[int, dict[str, Any]] = {}
    selected_date_cutoffs: list[int] = []

    for date_result in date_results:
        as_of_date = str(date_result["as_of_date"])
        search_payload = date_result["aggregated_policy_search"]
        selected_policy = search_payload.get("selected_policy")
        selected_cutoff = None if selected_policy is None else int(selected_policy["baseline_max_horizon_days"])
        if selected_cutoff is not None:
            selected_date_cutoffs.append(selected_cutoff)

        for candidate in search_payload.get("candidates", []):
            cutoff = int(candidate["baseline_max_horizon_days"])
            entry = aggregated.setdefault(
                cutoff,
                {
                    "baseline_max_horizon_days": cutoff,
                    "dates": [],
                    "date_count": 0,
                    "selected_count": 0,
                },
            )
            entry["dates"].append(
                {
                    "as_of_date": as_of_date,
                    "mean_adaptive_hit_rate": candidate["mean_adaptive_hit_rate"],
                    "mean_adaptive_avg_winner_prob": candidate["mean_adaptive_avg_winner_prob"],
                    "mean_adaptive_log_loss": candidate["mean_adaptive_log_loss"],
                    "mean_adaptive_brier": candidate["mean_adaptive_brier"],
                    "mean_adaptive_winner_prob_improvement_rate": candidate[
                        "mean_adaptive_winner_prob_improvement_rate"
                    ],
                }
            )
            entry["date_count"] += 1
            if selected_cutoff == cutoff:
                entry["selected_count"] += 1

    aggregated_candidates: list[dict[str, Any]] = []
    for cutoff in sorted(aggregated):
        entry = aggregated[cutoff]
        dates = entry["dates"]
        date_count = max(entry["date_count"], 1)
        aggregated_candidates.append(
            {
                "baseline_max_horizon_days": cutoff,
                "date_count": entry["date_count"],
                "selected_count": entry["selected_count"],
                "selection_frequency": entry["selected_count"] / date_count,
                "mean_adaptive_hit_rate": sum(day["mean_adaptive_hit_rate"] for day in dates) / date_count,
                "mean_adaptive_avg_winner_prob": sum(
                    day["mean_adaptive_avg_winner_prob"] for day in dates
                ) / date_count,
                "mean_adaptive_log_loss": sum(day["mean_adaptive_log_loss"] for day in dates) / date_count,
                "mean_adaptive_brier": sum(day["mean_adaptive_brier"] for day in dates) / date_count,
                "mean_adaptive_winner_prob_improvement_rate": sum(
                    day["mean_adaptive_winner_prob_improvement_rate"] for day in dates
                ) / date_count,
                "dates": dates,
            }
        )

    objective_metric_name = _normalize_objective_metric_name(objective)

    def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, int]:
        return (
            float(candidate.get(f"mean_adaptive_{objective_metric_name}", candidate["mean_adaptive_log_loss"])),
            float(candidate["mean_adaptive_brier"]),
            -float(candidate["selection_frequency"]),
            -float(candidate["mean_adaptive_hit_rate"]),
            int(candidate["baseline_max_horizon_days"]),
        )

    selected_policy = min(aggregated_candidates, key=ranking_key) if aggregated_candidates else None
    return {
        "objective": objective,
        "dates_evaluated": len(date_results),
        "selected_date_cutoffs": selected_date_cutoffs,
        "selected_policy": selected_policy,
        "candidates": aggregated_candidates,
    }


def aggregate_multidate_horizon_strategy_searches(
    date_results: list[dict[str, Any]],
    max_horizon_days: int,
    objective: str = "adaptive_log_loss",
) -> dict[str, Any]:
    objective_metric_name = _normalize_objective_metric_name(objective)
    selected_strategy_by_horizon: dict[str, str] = {}
    per_horizon: list[dict[str, Any]] = []
    weighted_totals = {
        "events": 0,
        "adaptive_hit_rate": 0.0,
        "adaptive_avg_winner_prob": 0.0,
        "adaptive_log_loss": 0.0,
        "adaptive_brier": 0.0,
        "adaptive_winner_prob_improvement_rate": 0.0,
    }

    for horizon_days in range(1, max_horizon_days + 1):
        horizon_key = str(horizon_days)
        horizon_dates = []
        for date_result in date_results:
            search_payload = date_result.get("aggregated_horizon_strategy_search", {})
            per_horizon_rows = search_payload.get("per_horizon", [])
            horizon_row = next(
                (row for row in per_horizon_rows if int(row.get("horizon_days", 0)) == horizon_days),
                None,
            )
            if horizon_row and horizon_row.get("events", 0) > 0:
                horizon_dates.append(
                    {
                        "as_of_date": str(date_result["as_of_date"]),
                        "summary": horizon_row,
                    }
                )

        if not horizon_dates:
            continue

        total_events = sum(int(day["summary"]["events"]) for day in horizon_dates)

        def build_strategy_candidate(candidate_key: str, strategy_name: str) -> dict[str, Any]:
            return {
                "strategy": strategy_name,
                "events": total_events,
                "date_count": len(horizon_dates),
                "mean_adaptive_hit_rate": sum(
                    float(day["summary"][candidate_key]["mean_adaptive_hit_rate"]) * int(day["summary"]["events"])
                    for day in horizon_dates
                ) / total_events,
                "mean_adaptive_avg_winner_prob": sum(
                    float(day["summary"][candidate_key]["mean_adaptive_avg_winner_prob"])
                    * int(day["summary"]["events"])
                    for day in horizon_dates
                ) / total_events,
                "mean_adaptive_log_loss": sum(
                    float(day["summary"][candidate_key]["mean_adaptive_log_loss"]) * int(day["summary"]["events"])
                    for day in horizon_dates
                ) / total_events,
                "mean_adaptive_brier": sum(
                    float(day["summary"][candidate_key]["mean_adaptive_brier"]) * int(day["summary"]["events"])
                    for day in horizon_dates
                ) / total_events,
                "mean_adaptive_winner_prob_improvement_rate": sum(
                    float(day["summary"][candidate_key]["mean_adaptive_winner_prob_improvement_rate"])
                    * int(day["summary"]["events"])
                    for day in horizon_dates
                ) / total_events,
                "dates": [
                    {
                        "as_of_date": day["as_of_date"],
                        "events": day["summary"]["events"],
                    }
                    for day in horizon_dates
                ],
            }

        candidates = [
            build_strategy_candidate("baseline_candidate", "baseline_short_horizon"),
            build_strategy_candidate("optimized_candidate", "calibrated_long_horizon"),
        ]

        def ranking_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, str]:
            return (
                float(candidate.get(f"mean_adaptive_{objective_metric_name}", candidate["mean_adaptive_log_loss"])),
                float(candidate["mean_adaptive_brier"]),
                -float(candidate["mean_adaptive_hit_rate"]),
                -float(candidate["mean_adaptive_avg_winner_prob"]),
                str(candidate["strategy"]),
            )

        selected_candidate = min(candidates, key=ranking_key)
        selected_strategy_by_horizon[horizon_key] = selected_candidate["strategy"]
        per_horizon.append(
            {
                "horizon_days": horizon_days,
                "events": total_events,
                "selected_strategy": selected_candidate["strategy"],
                "baseline_candidate": candidates[0],
                "optimized_candidate": candidates[1],
            }
        )

        weighted_totals["events"] += total_events
        weighted_totals["adaptive_hit_rate"] += selected_candidate["mean_adaptive_hit_rate"] * total_events
        weighted_totals["adaptive_avg_winner_prob"] += (
            selected_candidate["mean_adaptive_avg_winner_prob"] * total_events
        )
        weighted_totals["adaptive_log_loss"] += selected_candidate["mean_adaptive_log_loss"] * total_events
        weighted_totals["adaptive_brier"] += selected_candidate["mean_adaptive_brier"] * total_events
        weighted_totals["adaptive_winner_prob_improvement_rate"] += (
            selected_candidate["mean_adaptive_winner_prob_improvement_rate"] * total_events
        )

    total_events = max(weighted_totals["events"], 1)
    policy_summary = {
        "events": weighted_totals["events"],
        "adaptive_hit_rate": weighted_totals["adaptive_hit_rate"] / total_events,
        "adaptive_avg_winner_prob": weighted_totals["adaptive_avg_winner_prob"] / total_events,
        "adaptive_log_loss": weighted_totals["adaptive_log_loss"] / total_events,
        "adaptive_brier": weighted_totals["adaptive_brier"] / total_events,
        "adaptive_winner_prob_improvement_rate": weighted_totals["adaptive_winner_prob_improvement_rate"]
        / total_events,
    }

    return {
        "objective": objective,
        "dates_evaluated": len(date_results),
        "selected_strategy_by_horizon": selected_strategy_by_horizon,
        "per_horizon": per_horizon,
        "policy_summary": policy_summary,
    }


def write_forecast_policy(
    path: Path,
    *,
    baseline_max_horizon_days: int,
    objective: str,
    as_of_date: str,
    lookback_days: int | None,
    max_events: int | None,
    max_horizon_days: int | None,
    learned_at_utc: datetime,
    source: str = "recent_horizon_backtest",
    extra_metadata: dict[str, Any] | None = None,
    selection_mode: str = "cutoff",
    horizon_strategy_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    existing_policy: dict[str, Any] = {}
    if path.exists():
        current_payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        existing_policy = dict(current_payload.get("forecast_policy", {}))

    learned_from = {
        "source": source,
        "as_of_date": as_of_date,
    }
    if lookback_days is not None:
        learned_from["lookback_days"] = lookback_days
    if max_events is not None:
        learned_from["max_events"] = max_events
    if max_horizon_days is not None:
        learned_from["max_horizon_days"] = max_horizon_days
    if extra_metadata:
        learned_from.update(extra_metadata)

    existing_policy.update(
        {
            "adaptive_baseline_max_horizon_days": baseline_max_horizon_days,
            "selection_mode": selection_mode,
            "horizon_strategy_overrides": horizon_strategy_overrides or {},
            "selection_objective": objective,
            "learned_at_utc": learned_at_utc.isoformat(),
            "learned_from": learned_from,
        }
    )
    payload = {"forecast_policy": existing_policy}
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return payload
