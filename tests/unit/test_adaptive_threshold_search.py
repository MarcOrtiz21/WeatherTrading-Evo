import json
from datetime import datetime
from pathlib import Path

import pytest

from weather_trading.services.forecast_engine.adaptive_threshold_search import (
    aggregate_multidate_cutoff_searches,
    aggregate_multidate_horizon_strategy_searches,
    aggregate_horizon_strategy_searches,
    aggregate_policy_searches,
    select_applied_policy_candidate,
    search_optimal_baseline_max_horizon_days,
    summarize_candidate_policy,
    write_forecast_policy,
)


ROOT = Path(__file__).resolve().parents[2]


def load_fixture_rows() -> list[dict]:
    fixture_path = ROOT / "tests" / "fixtures" / "snapshots" / "recent_horizon_temperature_backtest_fixture.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))["rows"]


def test_summarize_candidate_policy_prefers_baseline_until_selected_cutoff():
    rows = load_fixture_rows()

    summary = summarize_candidate_policy(rows, baseline_max_horizon_days=2)

    assert summary["events"] == 4
    assert summary["baseline_max_horizon_days"] == 2
    assert summary["adaptive_hit_rate"] == 1.0


def test_search_optimal_baseline_max_horizon_days_finds_cutoff_two_from_frozen_snapshot():
    rows = load_fixture_rows()

    result = search_optimal_baseline_max_horizon_days(rows, max_horizon_days=4)

    assert result["selected_policy"] is not None
    assert result["selected_policy"]["baseline_max_horizon_days"] == 2


def test_aggregate_policy_searches_selects_lowest_mean_log_loss_cutoff():
    window_searches = [
        {
            "lookback_days": 30,
            "policy_search": {
                "selected_policy": {"baseline_max_horizon_days": 0},
                "candidates": [
                    {
                        "baseline_max_horizon_days": 0,
                        "adaptive_hit_rate": 0.30,
                        "adaptive_avg_winner_prob": 0.28,
                        "adaptive_log_loss": 1.50,
                        "adaptive_brier": 0.70,
                        "adaptive_winner_prob_improvement_rate": 0.70,
                    },
                    {
                        "baseline_max_horizon_days": 1,
                        "adaptive_hit_rate": 0.31,
                        "adaptive_avg_winner_prob": 0.27,
                        "adaptive_log_loss": 1.55,
                        "adaptive_brier": 0.71,
                        "adaptive_winner_prob_improvement_rate": 0.60,
                    },
                ],
            },
        },
        {
            "lookback_days": 60,
            "policy_search": {
                "selected_policy": {"baseline_max_horizon_days": 1},
                "candidates": [
                    {
                        "baseline_max_horizon_days": 0,
                        "adaptive_hit_rate": 0.28,
                        "adaptive_avg_winner_prob": 0.29,
                        "adaptive_log_loss": 1.54,
                        "adaptive_brier": 0.72,
                        "adaptive_winner_prob_improvement_rate": 0.68,
                    },
                    {
                        "baseline_max_horizon_days": 1,
                        "adaptive_hit_rate": 0.32,
                        "adaptive_avg_winner_prob": 0.28,
                        "adaptive_log_loss": 1.58,
                        "adaptive_brier": 0.70,
                        "adaptive_winner_prob_improvement_rate": 0.62,
                    },
                ],
            },
        },
    ]

    result = aggregate_policy_searches(window_searches)

    assert result["windows_evaluated"] == 2
    assert result["window_cutoffs"] == [0, 1]
    assert result["selected_policy"] is not None
    assert result["selected_policy"]["baseline_max_horizon_days"] == 0
    assert result["selected_policy"]["mean_adaptive_log_loss"] == pytest.approx(1.52)


def test_aggregate_horizon_strategy_searches_builds_mixed_policy_by_horizon():
    window_searches = [
        {
            "lookback_days": 30,
            "by_horizon": {
                "1": {
                    "events": 10,
                    "baseline_hit_rate": 0.50,
                    "optimized_hit_rate": 0.40,
                    "baseline_avg_winner_prob": 0.35,
                    "optimized_avg_winner_prob": 0.32,
                    "baseline_log_loss": 1.00,
                    "optimized_log_loss": 1.10,
                    "baseline_brier": 0.50,
                    "optimized_brier": 0.54,
                    "baseline_winner_prob_improvement_rate": 0.0,
                    "optimized_winner_prob_improvement_rate": 0.60,
                    "adaptive_hit_rate": 0.0,
                    "adaptive_avg_winner_prob": 0.0,
                    "adaptive_log_loss": 0.0,
                    "adaptive_brier": 0.0,
                    "adaptive_winner_prob_improvement_rate": 0.0,
                },
                "2": {
                    "events": 8,
                    "baseline_hit_rate": 0.30,
                    "optimized_hit_rate": 0.55,
                    "baseline_avg_winner_prob": 0.22,
                    "optimized_avg_winner_prob": 0.31,
                    "baseline_log_loss": 1.60,
                    "optimized_log_loss": 1.20,
                    "baseline_brier": 0.72,
                    "optimized_brier": 0.58,
                    "baseline_winner_prob_improvement_rate": 0.0,
                    "optimized_winner_prob_improvement_rate": 0.75,
                    "adaptive_hit_rate": 0.0,
                    "adaptive_avg_winner_prob": 0.0,
                    "adaptive_log_loss": 0.0,
                    "adaptive_brier": 0.0,
                    "adaptive_winner_prob_improvement_rate": 0.0,
                },
            },
        },
        {
            "lookback_days": 60,
            "by_horizon": {
                "1": {
                    "events": 12,
                    "baseline_hit_rate": 0.48,
                    "optimized_hit_rate": 0.42,
                    "baseline_avg_winner_prob": 0.34,
                    "optimized_avg_winner_prob": 0.30,
                    "baseline_log_loss": 1.02,
                    "optimized_log_loss": 1.09,
                    "baseline_brier": 0.51,
                    "optimized_brier": 0.55,
                    "baseline_winner_prob_improvement_rate": 0.0,
                    "optimized_winner_prob_improvement_rate": 0.55,
                    "adaptive_hit_rate": 0.0,
                    "adaptive_avg_winner_prob": 0.0,
                    "adaptive_log_loss": 0.0,
                    "adaptive_brier": 0.0,
                    "adaptive_winner_prob_improvement_rate": 0.0,
                },
                "2": {
                    "events": 9,
                    "baseline_hit_rate": 0.28,
                    "optimized_hit_rate": 0.58,
                    "baseline_avg_winner_prob": 0.21,
                    "optimized_avg_winner_prob": 0.33,
                    "baseline_log_loss": 1.64,
                    "optimized_log_loss": 1.18,
                    "baseline_brier": 0.73,
                    "optimized_brier": 0.57,
                    "baseline_winner_prob_improvement_rate": 0.0,
                    "optimized_winner_prob_improvement_rate": 0.78,
                    "adaptive_hit_rate": 0.0,
                    "adaptive_avg_winner_prob": 0.0,
                    "adaptive_log_loss": 0.0,
                    "adaptive_brier": 0.0,
                    "adaptive_winner_prob_improvement_rate": 0.0,
                },
            },
        },
    ]

    result = aggregate_horizon_strategy_searches(window_searches, max_horizon_days=2)

    assert result["selected_strategy_by_horizon"] == {
        "1": "baseline_short_horizon",
        "2": "calibrated_long_horizon",
    }
    assert result["policy_summary"]["events"] == 39
    assert result["policy_summary"]["adaptive_log_loss"] == pytest.approx((1.0 * 10 + 1.02 * 12 + 1.2 * 8 + 1.18 * 9) / 39)


def test_write_forecast_policy_supports_stability_metadata(tmp_path: Path):
    output_path = tmp_path / "forecast_policy.yaml"

    payload = write_forecast_policy(
        output_path,
        baseline_max_horizon_days=1,
        objective="adaptive_log_loss",
        as_of_date="2026-04-05",
        lookback_days=None,
        max_events=30,
        max_horizon_days=4,
        learned_at_utc=datetime(2026, 4, 5, 12, 0, 0),
        source="adaptive_policy_stability",
        extra_metadata={"lookback_windows": [30, 45, 60]},
    )

    assert payload["forecast_policy"]["learned_from"]["source"] == "adaptive_policy_stability"
    assert payload["forecast_policy"]["learned_from"]["lookback_windows"] == [30, 45, 60]
    assert "lookback_days" not in payload["forecast_policy"]["learned_from"]


def test_write_forecast_policy_persists_horizon_overrides(tmp_path: Path):
    output_path = tmp_path / "forecast_policy.yaml"

    payload = write_forecast_policy(
        output_path,
        baseline_max_horizon_days=1,
        objective="adaptive_log_loss",
        as_of_date="2026-04-05",
        lookback_days=None,
        max_events=30,
        max_horizon_days=4,
        learned_at_utc=datetime(2026, 4, 5, 12, 0, 0),
        source="adaptive_policy_stability",
        selection_mode="horizon_overrides",
        horizon_strategy_overrides={
            "1": "baseline_short_horizon",
            "2": "calibrated_long_horizon",
        },
    )

    assert payload["forecast_policy"]["selection_mode"] == "horizon_overrides"
    assert payload["forecast_policy"]["horizon_strategy_overrides"]["1"] == "baseline_short_horizon"
    assert payload["forecast_policy"]["horizon_strategy_overrides"]["2"] == "calibrated_long_horizon"


def test_write_forecast_policy_preserves_non_threshold_fields(tmp_path: Path):
    output_path = tmp_path / "forecast_policy.yaml"
    output_path.write_text(
        """
forecast_policy:
  probability_temperature_alpha: 0.85
  station_temperature_bias_c:
    KLGA: 1.7
""".strip(),
        encoding="utf-8",
    )

    payload = write_forecast_policy(
        output_path,
        baseline_max_horizon_days=1,
        objective="adaptive_log_loss",
        as_of_date="2026-04-08",
        lookback_days=7,
        max_events=20,
        max_horizon_days=4,
        learned_at_utc=datetime(2026, 4, 8, 12, 0, 0),
    )

    assert payload["forecast_policy"]["probability_temperature_alpha"] == 0.85
    assert payload["forecast_policy"]["station_temperature_bias_c"]["KLGA"] == 1.7


def test_aggregate_multidate_cutoff_searches_selects_best_mean_cutoff():
    date_results = [
        {
            "as_of_date": "2026-04-03",
            "aggregated_policy_search": {
                "selected_policy": {"baseline_max_horizon_days": 1},
                "candidates": [
                    {
                        "baseline_max_horizon_days": 0,
                        "mean_adaptive_hit_rate": 0.34,
                        "mean_adaptive_avg_winner_prob": 0.28,
                        "mean_adaptive_log_loss": 1.55,
                        "mean_adaptive_brier": 0.70,
                        "mean_adaptive_winner_prob_improvement_rate": 0.50,
                    },
                    {
                        "baseline_max_horizon_days": 1,
                        "mean_adaptive_hit_rate": 0.38,
                        "mean_adaptive_avg_winner_prob": 0.30,
                        "mean_adaptive_log_loss": 1.50,
                        "mean_adaptive_brier": 0.68,
                        "mean_adaptive_winner_prob_improvement_rate": 0.55,
                    },
                ],
            },
        },
        {
            "as_of_date": "2026-04-04",
            "aggregated_policy_search": {
                "selected_policy": {"baseline_max_horizon_days": 0},
                "candidates": [
                    {
                        "baseline_max_horizon_days": 0,
                        "mean_adaptive_hit_rate": 0.36,
                        "mean_adaptive_avg_winner_prob": 0.29,
                        "mean_adaptive_log_loss": 1.54,
                        "mean_adaptive_brier": 0.69,
                        "mean_adaptive_winner_prob_improvement_rate": 0.48,
                    },
                    {
                        "baseline_max_horizon_days": 1,
                        "mean_adaptive_hit_rate": 0.35,
                        "mean_adaptive_avg_winner_prob": 0.28,
                        "mean_adaptive_log_loss": 1.57,
                        "mean_adaptive_brier": 0.70,
                        "mean_adaptive_winner_prob_improvement_rate": 0.45,
                    },
                ],
            },
        },
    ]

    result = aggregate_multidate_cutoff_searches(date_results)

    assert result["dates_evaluated"] == 2
    assert result["selected_date_cutoffs"] == [1, 0]
    assert result["selected_policy"] is not None
    assert result["selected_policy"]["baseline_max_horizon_days"] == 1
    assert result["selected_policy"]["mean_adaptive_log_loss"] == pytest.approx(1.535)


def test_aggregate_multidate_horizon_strategy_searches_builds_stable_policy():
    date_results = [
        {
            "as_of_date": "2026-04-03",
            "aggregated_horizon_strategy_search": {
                "per_horizon": [
                    {
                        "horizon_days": 1,
                        "events": 20,
                        "baseline_candidate": {
                            "mean_adaptive_hit_rate": 0.45,
                            "mean_adaptive_avg_winner_prob": 0.33,
                            "mean_adaptive_log_loss": 1.40,
                            "mean_adaptive_brier": 0.62,
                            "mean_adaptive_winner_prob_improvement_rate": 0.0,
                        },
                        "optimized_candidate": {
                            "mean_adaptive_hit_rate": 0.40,
                            "mean_adaptive_avg_winner_prob": 0.31,
                            "mean_adaptive_log_loss": 1.46,
                            "mean_adaptive_brier": 0.65,
                            "mean_adaptive_winner_prob_improvement_rate": 0.50,
                        },
                    },
                    {
                        "horizon_days": 2,
                        "events": 20,
                        "baseline_candidate": {
                            "mean_adaptive_hit_rate": 0.30,
                            "mean_adaptive_avg_winner_prob": 0.24,
                            "mean_adaptive_log_loss": 1.66,
                            "mean_adaptive_brier": 0.74,
                            "mean_adaptive_winner_prob_improvement_rate": 0.0,
                        },
                        "optimized_candidate": {
                            "mean_adaptive_hit_rate": 0.42,
                            "mean_adaptive_avg_winner_prob": 0.31,
                            "mean_adaptive_log_loss": 1.49,
                            "mean_adaptive_brier": 0.68,
                            "mean_adaptive_winner_prob_improvement_rate": 0.60,
                        },
                    },
                ]
            },
        },
        {
            "as_of_date": "2026-04-04",
            "aggregated_horizon_strategy_search": {
                "per_horizon": [
                    {
                        "horizon_days": 1,
                        "events": 18,
                        "baseline_candidate": {
                            "mean_adaptive_hit_rate": 0.44,
                            "mean_adaptive_avg_winner_prob": 0.34,
                            "mean_adaptive_log_loss": 1.39,
                            "mean_adaptive_brier": 0.61,
                            "mean_adaptive_winner_prob_improvement_rate": 0.0,
                        },
                        "optimized_candidate": {
                            "mean_adaptive_hit_rate": 0.39,
                            "mean_adaptive_avg_winner_prob": 0.30,
                            "mean_adaptive_log_loss": 1.45,
                            "mean_adaptive_brier": 0.64,
                            "mean_adaptive_winner_prob_improvement_rate": 0.48,
                        },
                    },
                    {
                        "horizon_days": 2,
                        "events": 18,
                        "baseline_candidate": {
                            "mean_adaptive_hit_rate": 0.32,
                            "mean_adaptive_avg_winner_prob": 0.25,
                            "mean_adaptive_log_loss": 1.64,
                            "mean_adaptive_brier": 0.73,
                            "mean_adaptive_winner_prob_improvement_rate": 0.0,
                        },
                        "optimized_candidate": {
                            "mean_adaptive_hit_rate": 0.41,
                            "mean_adaptive_avg_winner_prob": 0.30,
                            "mean_adaptive_log_loss": 1.50,
                            "mean_adaptive_brier": 0.67,
                            "mean_adaptive_winner_prob_improvement_rate": 0.58,
                        },
                    },
                ]
            },
        },
    ]

    result = aggregate_multidate_horizon_strategy_searches(date_results, max_horizon_days=2)

    assert result["dates_evaluated"] == 2
    assert result["selected_strategy_by_horizon"] == {
        "1": "baseline_short_horizon",
        "2": "calibrated_long_horizon",
    }
    assert result["policy_summary"]["events"] == 76


def test_select_applied_policy_candidate_prefers_cutoff_on_effective_tie():
    aggregated_cutoff_search = {
        "selected_policy": {
            "baseline_max_horizon_days": 1,
            "mean_adaptive_hit_rate": 0.40,
            "mean_adaptive_avg_winner_prob": 0.31,
            "mean_adaptive_log_loss": 1.55,
            "mean_adaptive_brier": 0.70,
            "mean_adaptive_winner_prob_improvement_rate": 0.50,
        }
    }
    aggregated_horizon_search = {
        "selected_strategy_by_horizon": {
            "1": "baseline_short_horizon",
            "2": "calibrated_long_horizon",
        },
        "policy_summary": {
            "adaptive_hit_rate": 0.40,
            "adaptive_avg_winner_prob": 0.31,
            "adaptive_log_loss": 1.55,
            "adaptive_brier": 0.70,
            "adaptive_winner_prob_improvement_rate": 0.50,
        },
    }

    result = select_applied_policy_candidate(aggregated_cutoff_search, aggregated_horizon_search)

    assert result is not None
    assert result["selection_mode"] == "cutoff"
    assert result["baseline_max_horizon_days"] == 1
