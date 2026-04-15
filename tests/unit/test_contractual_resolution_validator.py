from weather_trading.services.evaluation.contractual_resolution_validator import (
    compare_contractual_resolution,
    summarize_contractual_comparisons,
)


def test_compare_contractual_resolution_detects_match_and_preserves_pnl():
    evaluation = {
        "snapshot_as_of_date": "2026-04-10",
        "event_slug": "highest-temperature-in-madrid-on-april-11-2026",
        "event_date": "2026-04-11",
        "station_code": "LEMD",
        "actual_temperature_source": "local_open_meteo_archive_backfill",
        "winner_market_id": "m2",
        "winner_question": "Will the highest temperature in Madrid be 15°C on April 11?",
        "top_edge_market_id": "m2",
        "top_edge_question": "Will the highest temperature in Madrid be 15°C on April 11?",
        "paper_trade_taken": True,
        "paper_trade_pnl": 0.72,
        "paper_trade_stake": 0.28,
    }
    contractual_event = {
        "closed": True,
        "active": False,
        "archived": True,
        "resolution_source_url": "https://wunderground.com/history/daily/es/madrid/LEMD",
        "markets": [
            {"id": "m1", "question": "14°C", "outcomePrices": "[\"0\", \"1\"]"},
            {
                "id": "m2",
                "question": "Will the highest temperature in Madrid be 15°C on April 11?",
                "outcomePrices": "[\"1\", \"0\"]",
            },
        ],
    }

    comparison = compare_contractual_resolution(evaluation, contractual_event)

    assert comparison is not None
    assert comparison.market_id_match is True
    assert comparison.question_match is True
    assert comparison.contractual_paper_trade_pnl == 0.72
    assert comparison.contractual_paper_pnl_delta == 0.0


def test_compare_contractual_resolution_detects_mismatch_and_adjusts_pnl():
    evaluation = {
        "snapshot_as_of_date": "2026-04-10",
        "event_slug": "highest-temperature-in-madrid-on-april-11-2026",
        "event_date": "2026-04-11",
        "station_code": "LEMD",
        "actual_temperature_source": "local_open_meteo_archive_backfill",
        "winner_market_id": "m2",
        "winner_question": "15°C",
        "top_edge_market_id": "m2",
        "top_edge_question": "15°C",
        "paper_trade_taken": True,
        "paper_trade_pnl": 0.72,
        "paper_trade_stake": 0.28,
    }
    contractual_event = {
        "closed": True,
        "active": False,
        "archived": True,
        "resolution_source_url": "https://wunderground.com/history/daily/es/madrid/LEMD",
        "markets": [
            {"id": "m1", "question": "14°C", "outcomePrices": "[\"1\", \"0\"]"},
            {"id": "m2", "question": "15°C", "outcomePrices": "[\"0\", \"1\"]"},
        ],
    }

    comparison = compare_contractual_resolution(evaluation, contractual_event)

    assert comparison is not None
    assert comparison.market_id_match is False
    assert comparison.question_match is False
    assert comparison.contractual_top_edge_hit is False
    assert comparison.contractual_paper_trade_pnl == -0.28
    assert comparison.contractual_paper_pnl_delta == -1.0


def test_summarize_contractual_comparisons_aggregates_delta():
    comparisons = [
        compare_contractual_resolution(
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "e1",
                "event_date": "2026-04-11",
                "station_code": "LEMD",
                "actual_temperature_source": "local_open_meteo_archive_backfill",
                "winner_market_id": "m1",
                "winner_question": "14°C",
                "top_edge_market_id": "m1",
                "top_edge_question": "14°C",
                "paper_trade_taken": True,
                "paper_trade_pnl": 0.8,
                "paper_trade_stake": 0.2,
            },
            {
                "closed": True,
                "active": False,
                "archived": True,
                "markets": [{"id": "m1", "question": "14°C", "outcomePrices": "[\"1\", \"0\"]"}],
            },
        ),
        compare_contractual_resolution(
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "e2",
                "event_date": "2026-04-11",
                "station_code": "LEMD",
                "actual_temperature_source": "local_open_meteo_archive_backfill",
                "winner_market_id": "m2",
                "winner_question": "15°C",
                "top_edge_market_id": "m2",
                "top_edge_question": "15°C",
                "paper_trade_taken": True,
                "paper_trade_pnl": 0.75,
                "paper_trade_stake": 0.25,
            },
            {
                "closed": True,
                "active": False,
                "archived": True,
                "markets": [{"id": "m1", "question": "14°C", "outcomePrices": "[\"1\", \"0\"]"}],
            },
        ),
    ]

    summary = summarize_contractual_comparisons([comparison for comparison in comparisons if comparison is not None])

    assert summary["events"] == 2
    assert summary["question_match_rate"] == 0.5
    assert summary["discrepancies"] == 1
    assert summary["contractual_paper_pnl_delta"] == -1.0
