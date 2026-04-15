from weather_trading.services.evaluation.bin_family_diagnostics import (
    build_bin_family_diagnostics,
    build_temperature_market_family,
    classify_temperature_market_shape,
    infer_temperature_unit,
)
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def test_infer_temperature_unit_detects_celsius_and_fahrenheit():
    assert infer_temperature_unit("Will the highest temperature be 14°C?") == "celsius"
    assert infer_temperature_unit("Will the highest temperature be 60°F or higher?") == "fahrenheit"
    assert infer_temperature_unit("Highest temperature tomorrow?") == "unknown"


def test_classify_temperature_market_shape_recognizes_tail_and_range_bins():
    parser = DeterministicParser()

    assert (
        classify_temperature_market_shape(
            "Will the highest temperature in Chicago be 46°F or higher on April 11?",
            "2026-04-11",
            parser,
        )
        == "upper_tail"
    )
    assert (
        classify_temperature_market_shape(
            "Will the highest temperature in Amsterdam be 14°C or below on April 11?",
            "2026-04-11",
            parser,
        )
        == "lower_tail"
    )
    assert (
        classify_temperature_market_shape(
            "Will the highest temperature in Madrid be 14°C on April 11?",
            "2026-04-11",
            parser,
        )
        == "range_bin"
    )
    assert (
        build_temperature_market_family(
            "Will the highest temperature in Atlanta be between 64-65°F on April 11?",
            "2026-04-11",
            parser,
        )
        == "fahrenheit|range_bin"
    )


def test_build_bin_family_diagnostics_groups_and_scores_synthetic_payload():
    audit_payload = {
        "evaluations": [
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "highest-temperature-in-chicago-on-april-11-2026",
                "event_date": "2026-04-11",
                "winner_market_id": "m1",
                "winner_question": "Will the highest temperature in Chicago be 46°F or higher on April 11?",
                "top_edge_question": "Will the highest temperature in Chicago be 46°F or higher on April 11?",
                "model_mode_hit": True,
                "market_mode_hit": False,
                "top_edge_hit": True,
                "paper_trade_taken": True,
                "paper_trade_stake": 1.0,
                "paper_trade_pnl": 0.5,
                "model_log_loss": 0.2,
                "market_log_loss": 0.4,
                "model_brier": 0.1,
                "market_brier": 0.2,
                "top_edge_net": 0.12,
            }
        ]
    }
    event_payloads = {
        ("2026-04-10", "highest-temperature-in-chicago-on-april-11-2026"): {
            "markets": [
                {"market_id": "m1", "fair_probability": 0.8},
                {"market_id": "m2", "fair_probability": 0.2},
            ]
        }
    }

    diagnostics = build_bin_family_diagnostics(
        audit_payload,
        event_payloads,
        current_alpha=0.55,
    )

    family_summary = diagnostics["winner_family_summary"]["fahrenheit|upper_tail"]
    assert family_summary["events"] == 1
    assert family_summary["model_mode_hit_rate"] == 1.0
    assert diagnostics["calibration_probes"]["current_global_alpha"]["events"] == 1
    assert diagnostics["recommendations"]["strongest_groups"] == []
    assert diagnostics["recommendations"]["weakest_groups"] == []
