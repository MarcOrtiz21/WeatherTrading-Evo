from datetime import date
from pathlib import Path

import pytest

from weather_trading.services.evaluation.blind_snapshot_resolution import (
    BlindSnapshotEventEvaluation,
    discover_blind_snapshot_paths,
    evaluate_blind_snapshot_event,
    extract_snapshot_as_of_date,
    is_event_eligible_for_resolution,
    select_realized_winner_market_row,
    summarize_blind_snapshot_evaluations,
)


def build_event(markets: list[dict]) -> dict:
    return {
        "event_slug": "highest-temperature-in-test-on-april-6-2026",
        "event_title": "Highest temperature in Test on April 6?",
        "event_date": "2026-04-06",
        "station_code": "TEST",
        "model_mode_question": markets[-1]["question"],
        "market_mode_question": markets[0]["question"],
        "top_edge_question": markets[-1]["question"],
        "markets": markets,
    }


def test_select_realized_winner_market_row_matches_temperature_bin():
    markets = [
        {"market_id": "1", "question": "<=12", "bin_low_c": None, "bin_high_c": 12.9},
        {"market_id": "2", "question": "13", "bin_low_c": 13.0, "bin_high_c": 13.9},
        {"market_id": "3", "question": ">=14", "bin_low_c": 14.0, "bin_high_c": None},
    ]

    winner = select_realized_winner_market_row(markets, 13.4)

    assert winner is not None
    assert winner["market_id"] == "2"


def test_evaluate_blind_snapshot_event_computes_hits_and_paper_trade():
    markets = [
        {
            "market_id": "1",
            "question": "<=12",
            "bin_low_c": None,
            "bin_high_c": 12.9,
            "fair_probability": 0.10,
            "market_probability": 0.60,
            "edge_net": -0.05,
        },
        {
            "market_id": "2",
            "question": "13",
            "bin_low_c": 13.0,
            "bin_high_c": 13.9,
            "fair_probability": 0.70,
            "market_probability": 0.20,
            "edge_net": 0.25,
        },
        {
            "market_id": "3",
            "question": ">=14",
            "bin_low_c": 14.0,
            "bin_high_c": None,
            "fair_probability": 0.20,
            "market_probability": 0.20,
            "edge_net": -0.02,
        },
    ]

    evaluation = evaluate_blind_snapshot_event(
        "2026-04-05",
        build_event(markets),
        13.4,
        paper_edge_threshold=0.0,
    )

    assert evaluation is not None
    assert evaluation.winner_market_id == "2"
    assert evaluation.forecast_strategy == "unknown"
    assert evaluation.horizon_days == 1
    assert evaluation.event_operable is True
    assert evaluation.event_evidence_tier == "unknown"
    assert evaluation.actual_temperature_source is None
    assert evaluation.model_mode_hit is True
    assert evaluation.market_mode_hit is False
    assert evaluation.top_edge_market_id == "2"
    assert evaluation.top_edge_tradeable is True
    assert evaluation.paper_trade_taken is True
    assert evaluation.top_edge_hit is True
    assert evaluation.paper_trade_stake == pytest.approx(0.20)
    assert evaluation.paper_trade_pnl == pytest.approx(0.80)


def test_summarize_blind_snapshot_evaluations_aggregates_metrics():
    evaluations = [
        BlindSnapshotEventEvaluation(
            snapshot_as_of_date="2026-04-05",
            event_slug="a",
            event_title="A",
            event_date="2026-04-06",
            station_code="A",
            forecast_strategy="baseline_short_horizon",
            horizon_days=1,
            event_operable=True,
            event_evidence_score=0.88,
            event_evidence_tier="A",
            actual_temp_c=13.4,
            actual_temperature_source="local_weather_observations",
            winner_market_id="2",
            winner_question="13",
            model_mode_question="13",
            market_mode_question="<=12",
            top_edge_question="13",
            top_edge_market_id="2",
            top_edge_tradeable=True,
            top_edge_quality_tier="A",
            model_mode_hit=True,
            market_mode_hit=False,
            top_edge_positive=True,
            top_edge_hit=True,
            winner_fair_probability=0.70,
            winner_market_probability=0.20,
            model_log_loss=0.3566749439,
            market_log_loss=1.6094379124,
            model_brier=0.14,
            market_brier=1.04,
            top_edge_net=0.25,
            paper_trade_taken=True,
            paper_trade_stake=0.20,
            paper_trade_pnl=0.80,
            paper_trade_execution_price=0.20,
            paper_trade_costs=0.0,
        ),
        BlindSnapshotEventEvaluation(
            snapshot_as_of_date="2026-04-05",
            event_slug="b",
            event_title="B",
            event_date="2026-04-06",
            station_code="B",
            forecast_strategy="optimized_multimodel",
            horizon_days=1,
            event_operable=False,
            event_evidence_score=0.54,
            event_evidence_tier="D",
            actual_temp_c=18.1,
            actual_temperature_source="openmeteo_archive",
            winner_market_id="9",
            winner_question="18",
            model_mode_question="17",
            market_mode_question="18",
            top_edge_question="17",
            top_edge_market_id="8",
            top_edge_tradeable=False,
            top_edge_quality_tier="D",
            model_mode_hit=False,
            market_mode_hit=True,
            top_edge_positive=False,
            top_edge_hit=None,
            winner_fair_probability=0.30,
            winner_market_probability=0.40,
            model_log_loss=1.2039728043,
            market_log_loss=0.9162907319,
            model_brier=0.88,
            market_brier=0.62,
            top_edge_net=-0.01,
            paper_trade_taken=False,
            paper_trade_stake=0.0,
            paper_trade_pnl=0.0,
            paper_trade_execution_price=0.0,
            paper_trade_costs=0.0,
        ),
    ]

    summary = summarize_blind_snapshot_evaluations(evaluations, paper_edge_threshold=0.0)

    assert summary["events"] == 2
    assert summary["operable_events"] == 1
    assert summary["operable_rate"] == pytest.approx(0.5)
    assert summary["avg_event_evidence_score"] == pytest.approx(0.71)
    assert summary["model_mode_hit_rate"] == pytest.approx(0.5)
    assert summary["market_mode_hit_rate"] == pytest.approx(0.5)
    assert summary["paper_trades"] == 1
    assert summary["paper_total_stake"] == pytest.approx(0.20)
    assert summary["paper_total_pnl"] == pytest.approx(0.80)
    assert summary["paper_roi_on_stake"] == pytest.approx(4.0)
    assert summary["by_strategy"]["baseline_short_horizon"]["paper_trades"] == 1
    assert summary["by_horizon_days"]["1"]["events"] == 2
    assert summary["by_quality_tier"]["A"]["paper_total_pnl"] == pytest.approx(0.80)
    assert summary["by_event_evidence_tier"]["A"]["events"] == 1
    assert summary["by_actual_temperature_source"]["local_weather_observations"]["events"] == 1


def test_evaluate_blind_snapshot_event_prefers_tradeable_candidate_over_blocked_raw_edge():
    markets = [
        {
            "market_id": "1",
            "question": "13",
            "bin_low_c": 13.0,
            "bin_high_c": 13.9,
            "fair_probability": 0.55,
            "market_probability": 0.20,
            "execution_price": 0.22,
            "estimated_costs": 0.03,
            "edge_net": 0.30,
            "is_tradeable": False,
            "blockers": ["spread_too_wide"],
        },
        {
            "market_id": "2",
            "question": "14",
            "bin_low_c": 14.0,
            "bin_high_c": 14.9,
            "fair_probability": 0.35,
            "market_probability": 0.18,
            "execution_price": 0.19,
            "estimated_costs": 0.02,
            "edge_net": 0.11,
            "is_tradeable": True,
            "blockers": [],
        },
        {
            "market_id": "3",
            "question": "15+",
            "bin_low_c": 15.0,
            "bin_high_c": None,
            "fair_probability": 0.10,
            "market_probability": 0.62,
            "execution_price": 0.64,
            "estimated_costs": 0.02,
            "edge_net": -0.56,
            "is_tradeable": False,
            "blockers": ["spread_too_wide"],
        },
    ]

    evaluation = evaluate_blind_snapshot_event(
        "2026-04-05",
        build_event(markets),
        14.2,
        paper_edge_threshold=0.0,
    )

    assert evaluation is not None
    assert evaluation.winner_market_id == "2"
    assert evaluation.top_edge_market_id == "2"
    assert evaluation.top_edge_tradeable is True
    assert evaluation.paper_trade_taken is True
    assert evaluation.paper_trade_stake == pytest.approx(0.21)


def test_discover_blind_snapshot_paths_filters_by_date(tmp_path: Path):
    first = tmp_path / "2026-04-05_polymarket_blind_live_validation.json"
    second = tmp_path / "2026-04-06_polymarket_blind_live_validation.json"
    ignored = tmp_path / "2026-04-06_recent_horizon_temperature_backtest.json"
    for path in (first, second, ignored):
        path.write_text("{}", encoding="utf-8")

    paths = discover_blind_snapshot_paths(
        tmp_path,
        start_as_of_date=date(2026, 4, 6),
        end_as_of_date=date(2026, 4, 6),
    )

    assert [path.name for path in paths] == [second.name]
    assert extract_snapshot_as_of_date(second) == date(2026, 4, 6)


def test_is_event_eligible_for_resolution_requires_event_to_be_past_reference_date():
    assert is_event_eligible_for_resolution("2026-04-05", date(2026, 4, 6)) is True
    assert is_event_eligible_for_resolution("2026-04-06", date(2026, 4, 6)) is False
