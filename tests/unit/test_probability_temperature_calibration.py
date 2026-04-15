import importlib.util
import json
import sys
from pathlib import Path


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


def test_probability_temperature_calibration_prefers_flatter_alpha_for_overconfident_sample(tmp_path):
    module = load_script_module(
        "probability_temperature_calibration_module",
        "scripts/run_probability_temperature_calibration.py",
    )

    audit_payload = {
        "evaluations": [
            {
                "snapshot_as_of_date": "2026-04-05",
                "event_slug": "event-1",
                "winner_market_id": "2",
            }
        ]
    }
    event_payloads = {
        ("2026-04-05", "event-1"): {
            "markets": [
                {"market_id": "1", "fair_probability": 0.90},
                {"market_id": "2", "fair_probability": 0.10},
            ]
        }
    }

    candidates = module.evaluate_alpha_candidates(
        audit_payload=audit_payload,
        event_payloads=event_payloads,
        alpha_min=0.5,
        alpha_max=1.0,
        alpha_step=0.25,
    )

    best = min(candidates, key=lambda item: (item["log_loss"], item["brier"], abs(item["alpha"] - 1.0)))

    assert best["alpha"] == 0.5
    assert best["events"] == 1


def test_calibrate_market_probabilities_normalizes_probabilities():
    module = load_script_module(
        "probability_temperature_calibration_module_2",
        "scripts/run_probability_temperature_calibration.py",
    )

    calibrated = module.calibrate_market_probabilities(
        [
            {"market_id": "1", "fair_probability": 0.7},
            {"market_id": "2", "fair_probability": 0.3},
        ],
        alpha=0.8,
    )

    assert set(calibrated) == {"1", "2"}
    assert abs(sum(calibrated.values()) - 1.0) < 1e-9
