from scripts.run_cohort_overlay_diagnostics import (
    enrich_row,
    select_worst_groups,
    summarize_groups,
    summarize_rows,
)


def test_enrich_row_uses_live_watchlist_overlay_when_present():
    row = {"event_slug": "madrid-apr-14"}
    live_event = {
        "watchlist_signal": "mixed",
        "watchlist_alignment_score": 0.25,
        "watchlist_match_count": 3,
        "watchlist_active_traders": ["ColdMath", "Poligarch"],
        "watchlist_aligned_traders": ["ColdMath"],
        "watchlist_opposed_traders": ["Poligarch"],
    }

    enriched = enrich_row(row, live_event)

    assert enriched["watchlist_signal"] == "mixed"
    assert enriched["watchlist_match_count"] == 3
    assert enriched["watchlist_aligned_traders"] == ["ColdMath"]


def test_summarize_groups_aggregates_quality_tiers():
    rows = [
        {
            "top_edge_quality_tier": "A",
            "model_mode_hit": True,
            "market_mode_hit": False,
            "model_log_loss": 0.8,
            "market_log_loss": 1.1,
            "model_brier": 0.4,
            "market_brier": 0.5,
            "paper_trade_taken": True,
            "paper_trade_stake": 0.2,
            "paper_trade_pnl": 0.8,
        },
        {
            "top_edge_quality_tier": "B",
            "model_mode_hit": False,
            "market_mode_hit": True,
            "model_log_loss": 1.6,
            "market_log_loss": 0.9,
            "model_brier": 0.7,
            "market_brier": 0.5,
            "paper_trade_taken": True,
            "paper_trade_stake": 0.3,
            "paper_trade_pnl": -0.3,
        },
    ]

    summary = summarize_groups(rows, key="top_edge_quality_tier")

    assert summary["A"]["paper_total_pnl"] == 0.8
    assert summary["B"]["paper_total_pnl"] == -0.3


def test_select_worst_groups_sorts_by_pnl_then_log_loss():
    groups = {
        "A": summarize_rows(
            [
                {
                    "model_mode_hit": True,
                    "market_mode_hit": True,
                    "model_log_loss": 0.9,
                    "market_log_loss": 0.8,
                    "model_brier": 0.4,
                    "market_brier": 0.4,
                    "paper_trade_taken": True,
                    "paper_trade_stake": 0.2,
                    "paper_trade_pnl": 0.1,
                }
            ]
        ),
        "B": summarize_rows(
            [
                {
                    "model_mode_hit": False,
                    "market_mode_hit": True,
                    "model_log_loss": 1.4,
                    "market_log_loss": 0.9,
                    "model_brier": 0.7,
                    "market_brier": 0.5,
                    "paper_trade_taken": True,
                    "paper_trade_stake": 0.2,
                    "paper_trade_pnl": -0.2,
                }
            ]
        ),
    }

    ranked = select_worst_groups(groups, min_events=1)

    assert ranked[0]["group"] == "B"
