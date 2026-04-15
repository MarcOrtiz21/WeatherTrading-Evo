import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


def load_script_module(name: str, relative_path: str):
    script_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_historical_backtest_summary_includes_adaptive_metrics():
    module = load_script_module("historical_backtest_module", "scripts/run_historical_temperature_backtest.py")

    rows = [
        module.EventBacktestRow(
            event_slug="e1",
            event_title="Event 1",
            event_date="2026-04-01",
            station_code="LEMD",
            actual_temp_c=25.0,
            actual_winner_question="Q1",
            baseline_winner_probability=0.6,
            optimized_winner_probability=0.4,
            adaptive_winner_probability=0.6,
            baseline_mode_question="Q1",
            optimized_mode_question="Q2",
            adaptive_mode_question="Q1",
            baseline_mode_hit=True,
            optimized_mode_hit=False,
            adaptive_mode_hit=True,
            baseline_brier=0.2,
            optimized_brier=0.4,
            adaptive_brier=0.2,
            adaptive_strategy="baseline_short_horizon",
        ),
        module.EventBacktestRow(
            event_slug="e2",
            event_title="Event 2",
            event_date="2026-04-02",
            station_code="LEMD",
            actual_temp_c=26.0,
            actual_winner_question="Q2",
            baseline_winner_probability=0.3,
            optimized_winner_probability=0.5,
            adaptive_winner_probability=0.3,
            baseline_mode_question="Q1",
            optimized_mode_question="Q2",
            adaptive_mode_question="Q1",
            baseline_mode_hit=False,
            optimized_mode_hit=True,
            adaptive_mode_hit=False,
            baseline_brier=0.5,
            optimized_brier=0.3,
            adaptive_brier=0.5,
            adaptive_strategy="baseline_short_horizon",
        ),
    ]

    summary = module.summarize(rows)

    assert summary["events"] == 2
    assert summary["baseline_hit_rate"] == 0.5
    assert summary["optimized_hit_rate"] == 0.5
    assert summary["adaptive_hit_rate"] == 0.5
    assert summary["adaptive_avg_winner_prob"] == pytest.approx(0.45)
    assert summary["adaptive_brier"] == pytest.approx(0.35)


def test_recent_horizon_summary_rewards_adaptive_mix():
    module = load_script_module("recent_horizon_backtest_module", "scripts/run_recent_horizon_temperature_backtest.py")

    rows = [
        module.HorizonBacktestRow(
            event_slug="e1",
            event_title="Event 1",
            event_date="2026-04-01",
            station_code="LEMD",
            horizon_days=1,
            actual_temp_c=25.0,
            actual_winner_question="Q1",
            baseline_winner_probability=0.55,
            optimized_winner_probability=0.40,
            adaptive_winner_probability=0.55,
            baseline_mode_question="Q1",
            optimized_mode_question="Q2",
            adaptive_mode_question="Q1",
            baseline_mode_hit=True,
            optimized_mode_hit=False,
            adaptive_mode_hit=True,
            baseline_brier=0.25,
            optimized_brier=0.45,
            adaptive_brier=0.25,
            adaptive_strategy="baseline_short_horizon",
        ),
        module.HorizonBacktestRow(
            event_slug="e2",
            event_title="Event 2",
            event_date="2026-04-02",
            station_code="LEMD",
            horizon_days=4,
            actual_temp_c=28.0,
            actual_winner_question="Q2",
            baseline_winner_probability=0.20,
            optimized_winner_probability=0.45,
            adaptive_winner_probability=0.45,
            baseline_mode_question="Q1",
            optimized_mode_question="Q2",
            adaptive_mode_question="Q2",
            baseline_mode_hit=False,
            optimized_mode_hit=True,
            adaptive_mode_hit=True,
            baseline_brier=0.60,
            optimized_brier=0.35,
            adaptive_brier=0.35,
            adaptive_strategy="calibrated_long_horizon",
        ),
    ]

    summary = module.summarize(rows)

    assert summary["events"] == 2
    assert summary["adaptive_hit_rate"] == 1.0
    assert summary["baseline_hit_rate"] == 0.5
    assert summary["optimized_hit_rate"] == 0.5
    assert summary["adaptive_avg_winner_prob"] == pytest.approx(0.5)
    assert summary["adaptive_brier"] == pytest.approx(0.3)
