from weather_trading.services.evaluation.contractual_probability_calibration import (
    build_contractual_family_summary,
    evaluate_contractual_probability_config,
    select_contractual_calibration_candidate,
)


def test_evaluate_contractual_probability_config_prefers_flatter_alpha_for_overconfident_sample():
    contractual_audit_payload = {
        "comparisons": [
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "atlanta-apr-11",
                "event_date": "2026-04-11",
                "contractual_winner_market_id": "m2",
                "contractual_winner_question": "Will the highest temperature in Atlanta be between 66-67°F on April 11?",
            }
        ]
    }
    event_payloads = {
        ("2026-04-10", "atlanta-apr-11"): {
            "markets": [
                {"market_id": "m1", "fair_probability": 0.9, "market_probability": 0.7},
                {"market_id": "m2", "fair_probability": 0.1, "market_probability": 0.3},
            ]
        }
    }

    sharp = evaluate_contractual_probability_config(
        contractual_audit_payload,
        event_payloads,
        default_alpha=1.0,
    )
    flat = evaluate_contractual_probability_config(
        contractual_audit_payload,
        event_payloads,
        default_alpha=0.5,
    )

    assert flat["events"] == 1
    assert flat["model_log_loss"] < sharp["model_log_loss"]


def test_select_contractual_calibration_candidate_prefers_unit_candidate_when_it_improves_log_loss():
    current = {
        "default_alpha": 0.55,
        "unit_alpha_map": {},
        "events": 100,
        "model_log_loss": 1.60,
        "market_log_loss": 1.25,
        "model_brier": 0.64,
        "market_brier": 0.63,
        "model_mode_hit_rate": 0.57,
        "market_mode_hit_rate": 0.46,
    }
    best_global = {
        "kind": "global",
        "alpha": 0.50,
        "events": 100,
        "model_log_loss": 1.56,
        "market_log_loss": 1.25,
        "model_brier": 0.645,
        "market_brier": 0.63,
        "model_mode_hit_rate": 0.57,
        "market_mode_hit_rate": 0.46,
    }
    best_unit = {
        "kind": "unit",
        "default_alpha": 0.55,
        "unit_alpha_map": {"celsius": 0.55, "fahrenheit": 0.40},
        "celsius_alpha": 0.55,
        "fahrenheit_alpha": 0.40,
        "events": 100,
        "model_log_loss": 1.52,
        "market_log_loss": 1.25,
        "model_brier": 0.646,
        "market_brier": 0.63,
        "model_mode_hit_rate": 0.56,
        "market_mode_hit_rate": 0.46,
    }

    selected = select_contractual_calibration_candidate(
        current_config_metrics=current,
        best_global_candidate=best_global,
        best_unit_candidate=best_unit,
        max_brier_degradation_ratio=0.05,
        max_mode_hit_drop=0.02,
    )

    assert selected["kind"] == "unit"


def test_build_contractual_family_summary_groups_events_by_contractual_family():
    contractual_audit_payload = {
        "comparisons": [
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "madrid-apr-11",
                "event_date": "2026-04-11",
                "contractual_winner_market_id": "m1",
                "contractual_winner_question": "Will the highest temperature in Madrid be 15°C on April 11?",
            },
            {
                "snapshot_as_of_date": "2026-04-10",
                "event_slug": "atlanta-apr-11",
                "event_date": "2026-04-11",
                "contractual_winner_market_id": "m2",
                "contractual_winner_question": "Will the highest temperature in Atlanta be between 66-67°F on April 11?",
            },
        ]
    }
    event_payloads = {
        ("2026-04-10", "madrid-apr-11"): {
            "markets": [
                {"market_id": "m1", "fair_probability": 0.6, "market_probability": 0.4},
                {"market_id": "m2", "fair_probability": 0.4, "market_probability": 0.6},
            ]
        },
        ("2026-04-10", "atlanta-apr-11"): {
            "markets": [
                {"market_id": "m1", "fair_probability": 0.9, "market_probability": 0.7},
                {"market_id": "m2", "fair_probability": 0.1, "market_probability": 0.3},
            ]
        },
    }

    summary = build_contractual_family_summary(
        contractual_audit_payload,
        event_payloads,
        default_alpha=0.5,
    )

    assert "celsius|range_bin" in summary["winner_family_summary"]
    assert "fahrenheit|range_bin" in summary["winner_family_summary"]
