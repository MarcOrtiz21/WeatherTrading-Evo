import pytest

from scripts.run_watchlist_strategy_simulation import (
    build_strategy_comparison_digest,
    build_alignment_from_snapshot_event,
    build_trader_candidates,
    build_missing_alignment,
    evaluate_candidate_trade,
    infer_yes_bias,
    should_skip_celsius_active_unclassified,
)
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def test_infer_yes_bias_handles_yes_and_no_sides():
    assert infer_yes_bias({"outcome": "Yes", "side": "BUY"}) == 1
    assert infer_yes_bias({"outcome": "Yes", "side": "SELL"}) == -1
    assert infer_yes_bias({"outcome": "No", "side": "BUY"}) == -1
    assert infer_yes_bias({"outcome": "No", "side": "SELL"}) == 1


def test_build_trader_candidates_aggregates_positive_yes_conviction():
    event = {
        "markets": [
            {
                "market_id": "m1",
                "market_slug": "madrid-10-11c-apr-12",
                "question": "Will the highest temperature in Madrid be 10-11°C on April 12?",
                "is_tradeable": True,
                "execution_price": 0.2,
                "estimated_costs": 0.02,
            },
            {
                "market_id": "m2",
                "market_slug": "madrid-12-13c-apr-12",
                "question": "Will the highest temperature in Madrid be 12-13°C on April 12?",
                "is_tradeable": True,
                "execution_price": 0.3,
                "estimated_costs": 0.02,
            },
        ]
    }
    trades = [
        {
            "label": "ColdMath",
            "market_slug": "madrid-10-11c-apr-12",
            "market_title": None,
            "outcome": "yes",
            "side": "buy",
            "size": 10,
        },
        {
            "label": "ColdMath",
            "market_slug": "madrid-10-11c-apr-12",
            "market_title": None,
            "outcome": "yes",
            "side": "buy",
            "size": 5,
        },
        {
            "label": "Poligarch",
            "market_slug": "madrid-12-13c-apr-12",
            "market_title": None,
            "outcome": "yes",
            "side": "buy",
            "size": 12,
        },
    ]

    candidates = build_trader_candidates(event, trades)

    assert candidates["by_trader"]["ColdMath"]["market_id"] == "m1"
    assert candidates["by_trader"]["ColdMath"]["conviction"] == 15
    assert candidates["consensus"]["market_id"] == "m1"
    assert candidates["directional_by_trader"]["ColdMath"]["yes_bias"] == 1


def test_build_trader_candidates_tracks_directional_no_conviction():
    event = {
        "markets": [
            {
                "market_id": "m1",
                "market_slug": "madrid-10-11c-apr-12",
                "question": "Will the highest temperature in Madrid be 10-11°C on April 12?",
                "is_tradeable": True,
                "execution_price": 0.2,
                "estimated_costs": 0.02,
            },
        ]
    }
    trades = [
        {
            "label": "ColdMath",
            "market_slug": "madrid-10-11c-apr-12",
            "market_title": None,
            "outcome": "no",
            "side": "buy",
            "size": 10,
        },
    ]

    candidates = build_trader_candidates(event, trades)

    assert "ColdMath" not in candidates["by_trader"]
    directional = candidates["directional_by_trader"]["ColdMath"]
    assert directional["market_id"] == "m1"
    assert directional["yes_bias"] == -1
    assert directional["conviction"] == 10
    assert directional["signed_conviction"] == -10


def test_evaluate_candidate_trade_scores_hit_and_pnl():
    candidate = {
        "market_id": "m1",
        "question": "Q",
        "conviction": 10,
        "execution_price": 0.2,
        "costs": 0.02,
    }
    evaluation = {"event_slug": "e1", "winner_market_id": "m1"}

    trade = evaluate_candidate_trade(candidate, evaluation)

    assert trade["hit"] is True
    assert trade["stake"] == 0.22
    assert trade["pnl"] == 0.78
    assert trade["side"] == "YES"


def test_evaluate_candidate_trade_scores_no_side_against_selected_market():
    candidate = {
        "market_id": "m1",
        "question": "Q",
        "conviction": 10,
        "signed_conviction": -10,
        "execution_price": 0.2,
        "costs": 0.02,
        "yes_bias": -1,
    }
    evaluation = {"event_slug": "e1", "winner_market_id": "m2"}

    trade = evaluate_candidate_trade(candidate, evaluation)

    assert trade["hit"] is True
    assert trade["stake"] == pytest.approx(0.82)
    assert trade["pnl"] == pytest.approx(0.18)
    assert trade["execution_price"] == pytest.approx(0.8)
    assert trade["yes_market_price"] == 0.2
    assert trade["side"] == "NO"


def test_build_alignment_from_snapshot_event_reads_frozen_watchlist_fields():
    event = {
        "watchlist_signal": "aligned",
        "watchlist_alignment_score": 1.0,
        "watchlist_match_count": 2,
        "watchlist_active_traders": ["ColdMath"],
        "watchlist_aligned_traders": ["ColdMath"],
        "watchlist_opposed_traders": [],
        "watchlist_event_only_traders": [],
        "watchlist_trades": [{"label": "ColdMath"}],
    }

    alignment = build_alignment_from_snapshot_event(event)

    assert alignment["signal"] == "aligned"
    assert alignment["alignment_score"] == 1.0
    assert alignment["active_traders"] == ["ColdMath"]
    assert alignment["trades"] == [{"label": "ColdMath"}]


def test_build_missing_alignment_marks_overlay_unavailable():
    alignment = build_missing_alignment()

    assert alignment["signal"] == "unavailable"
    assert alignment["match_count"] == 0
    assert alignment["trades"] == []


def test_should_skip_celsius_active_unclassified_matches_target_pattern():
    parser = DeterministicParser()
    evaluation = {
        "top_edge_question": "Will the highest temperature in Madrid be between 15-16°C on April 12?",
        "event_date": "2026-04-12",
    }
    alignment = {"signal": "active_unclassified"}

    assert should_skip_celsius_active_unclassified(
        evaluation=evaluation,
        alignment=alignment,
        parser=parser,
    ) is True


def test_should_skip_celsius_active_unclassified_ignores_other_signals():
    parser = DeterministicParser()
    evaluation = {
        "top_edge_question": "Will the highest temperature in Madrid be between 15-16°C on April 12?",
        "event_date": "2026-04-12",
    }
    alignment = {"signal": "silent"}

    assert should_skip_celsius_active_unclassified(
        evaluation=evaluation,
        alignment=alignment,
        parser=parser,
    ) is False


def test_build_strategy_comparison_digest_reports_best_strategies_and_deltas():
    summary = {
        "strategies": {
            "model_current": {
                "trades": 10,
                "selected_market_hit_rate": 0.4,
                "total_pnl": 1.0,
                "roi_on_stake": 0.1,
            },
            "model_skip_opposed": {
                "trades": 9,
                "selected_market_hit_rate": 0.5,
                "total_pnl": 1.4,
                "roi_on_stake": 0.2,
            },
            "model_skip_celsius_active_unclassified": {
                "trades": 8,
                "selected_market_hit_rate": 0.45,
                "total_pnl": 0.9,
                "roi_on_stake": 0.11,
            },
            "model_skip_opposed_and_celsius_active_unclassified": {
                "trades": 7,
                "selected_market_hit_rate": 0.57,
                "total_pnl": 1.3,
                "roi_on_stake": 0.25,
            },
        }
    }

    digest = build_strategy_comparison_digest(summary)

    assert digest["best_strategy_by_pnl"] == "model_skip_opposed"
    assert digest["best_strategy_by_roi"] == "model_skip_opposed_and_celsius_active_unclassified"
    assert digest["deltas_vs_model_current"]["model_skip_opposed"]["pnl_delta"] == pytest.approx(0.4)
    assert digest["deltas_vs_model_current"]["model_skip_opposed"]["trades_delta"] == -1
