import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.contractual_probability_calibration import (
    build_contractual_family_summary,
    evaluate_contractual_probability_config,
    evaluate_global_alpha_candidates,
    evaluate_unit_alpha_candidates,
    get_current_probability_temperature_config,
    load_contractual_event_payloads,
    select_contractual_calibration_candidate,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aprende una calibracion de probability temperature usando el settlement contractual real de Polymarket."
    )
    parser.add_argument(
        "--contractual-audit-snapshot-path",
        default="logs/snapshots/2026-04-11_contractual_resolution_audit.json",
        help="Ruta al snapshot de auditoria contractual.",
    )
    parser.add_argument("--alpha-min", type=float, default=0.35)
    parser.add_argument("--alpha-max", type=float, default=1.0)
    parser.add_argument("--alpha-step", type=float, default=0.05)
    parser.add_argument(
        "--max-brier-degradation-ratio",
        type=float,
        default=0.05,
        help="Maximo deterioro relativo permitido en Brier respecto a la config actual.",
    )
    parser.add_argument(
        "--max-mode-hit-drop",
        type=float,
        default=0.02,
        help="Maxima caida absoluta permitida en mode hit-rate respecto a la config actual.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Aplica la calibracion seleccionada en config/forecast_policy.yaml.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workspace_root = Path(ROOT)
    contractual_audit_path = workspace_root / args.contractual_audit_snapshot_path
    contractual_audit_payload = json.loads(contractual_audit_path.read_text(encoding="utf-8"))
    _, event_payloads = load_contractual_event_payloads(contractual_audit_payload, workspace_root)

    current_config = get_current_probability_temperature_config()
    current_metrics = evaluate_contractual_probability_config(
        contractual_audit_payload,
        event_payloads,
        default_alpha=float(current_config["default_alpha"]),
        unit_alpha_map=dict(current_config["unit_alpha_map"]),
    )

    global_candidates = evaluate_global_alpha_candidates(
        contractual_audit_payload,
        event_payloads,
        alpha_min=args.alpha_min,
        alpha_max=args.alpha_max,
        alpha_step=args.alpha_step,
    )
    unit_candidates = evaluate_unit_alpha_candidates(
        contractual_audit_payload,
        event_payloads,
        alpha_min=args.alpha_min,
        alpha_max=args.alpha_max,
        alpha_step=args.alpha_step,
        default_alpha=float(current_config["default_alpha"]),
    )

    best_global_candidate = min(
        global_candidates,
        key=lambda item: (
            float(item["model_log_loss"]),
            float(item["model_brier"]),
            -float(item["model_mode_hit_rate"]),
            abs(float(item["alpha"]) - float(current_config["default_alpha"])),
        ),
    )
    best_unit_candidate = min(
        unit_candidates,
        key=lambda item: (
            float(item["model_log_loss"]),
            float(item["model_brier"]),
            -float(item["model_mode_hit_rate"]),
            abs(float(item["celsius_alpha"]) - float(current_config["default_alpha"]))
            + abs(float(item["fahrenheit_alpha"]) - float(current_config["default_alpha"])),
        ),
    )

    selected_candidate = select_contractual_calibration_candidate(
        current_config_metrics={
            "kind": "current",
            "default_alpha": float(current_config["default_alpha"]),
            "unit_alpha_map": dict(current_config["unit_alpha_map"]),
            **current_metrics,
        },
        best_global_candidate=best_global_candidate,
        best_unit_candidate=best_unit_candidate,
        max_brier_degradation_ratio=args.max_brier_degradation_ratio,
        max_mode_hit_drop=args.max_mode_hit_drop,
    )

    family_summary = build_contractual_family_summary(
        contractual_audit_payload,
        event_payloads,
        default_alpha=(
            float(selected_candidate["default_alpha"])
            if selected_candidate["kind"] in {"current", "unit"}
            else float(selected_candidate["alpha"])
        ),
        unit_alpha_map=(
            dict(selected_candidate.get("unit_alpha_map", {}))
            if selected_candidate["kind"] in {"current", "unit"}
            else {}
        ),
    )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "contractual_audit_snapshot_path": contractual_audit_path.relative_to(workspace_root).as_posix(),
        "source_audit_snapshot": contractual_audit_payload.get("source_audit_snapshot"),
        "search_space": {
            "alpha_min": args.alpha_min,
            "alpha_max": args.alpha_max,
            "alpha_step": args.alpha_step,
            "max_brier_degradation_ratio": args.max_brier_degradation_ratio,
            "max_mode_hit_drop": args.max_mode_hit_drop,
        },
        "current_config": {
            "default_alpha": float(current_config["default_alpha"]),
            "unit_alpha_map": dict(current_config["unit_alpha_map"]),
            **current_metrics,
        },
        "best_global_candidate": best_global_candidate,
        "best_unit_candidate": best_unit_candidate,
        "selected_candidate": selected_candidate,
        "market_baseline": {
            "events": int(current_metrics["events"]),
            "market_log_loss": float(current_metrics["market_log_loss"]),
            "market_brier": float(current_metrics["market_brier"]),
            "market_mode_hit_rate": float(current_metrics["market_mode_hit_rate"]),
        },
        "family_summary": family_summary,
    }

    output_path = persist_snapshot(snapshot, reference_date=date.today())
    print(f"Calibracion contractual guardada en: {output_path}")
    print("")
    print("=== RESUMEN CALIBRACION CONTRACTUAL ===")
    print(
        f"Config actual | alpha={current_config['default_alpha']:.2f} | "
        f"log_loss={current_metrics['model_log_loss']:.3f} | "
        f"brier={current_metrics['model_brier']:.3f} | "
        f"mode_hit={current_metrics['model_mode_hit_rate']:.1%}"
    )
    print(
        f"Mejor global | alpha={best_global_candidate['alpha']:.2f} | "
        f"log_loss={best_global_candidate['model_log_loss']:.3f} | "
        f"brier={best_global_candidate['model_brier']:.3f} | "
        f"mode_hit={best_global_candidate['model_mode_hit_rate']:.1%}"
    )
    print(
        f"Mejor por unidad | C={best_unit_candidate['celsius_alpha']:.2f} | "
        f"F={best_unit_candidate['fahrenheit_alpha']:.2f} | "
        f"log_loss={best_unit_candidate['model_log_loss']:.3f} | "
        f"brier={best_unit_candidate['model_brier']:.3f} | "
        f"mode_hit={best_unit_candidate['model_mode_hit_rate']:.1%}"
    )
    print(
        f"Seleccionado | kind={selected_candidate['kind']} | "
        f"log_loss={selected_candidate['model_log_loss']:.3f} | "
        f"brier={selected_candidate['model_brier']:.3f} | "
        f"mode_hit={selected_candidate['model_mode_hit_rate']:.1%}"
    )
    print(
        f"Mercado contractual | log_loss={current_metrics['market_log_loss']:.3f} | "
        f"brier={current_metrics['market_brier']:.3f} | "
        f"mode_hit={current_metrics['market_mode_hit_rate']:.1%}"
    )

    if args.apply:
        policy_path = workspace_root / "config" / "forecast_policy.yaml"
        apply_selected_candidate(
            policy_path=policy_path,
            selected_candidate=selected_candidate,
            snapshot_path=output_path,
        )
        print("Calibracion contractual aplicada en forecast_policy.yaml")


def apply_selected_candidate(*, policy_path: Path, selected_candidate: dict, snapshot_path: Path) -> None:
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
    forecast_policy = dict(payload.get("forecast_policy", {}))

    if selected_candidate["kind"] == "global":
        forecast_policy["probability_temperature_alpha"] = float(selected_candidate["alpha"])
        forecast_policy["probability_temperature_alpha_by_unit"] = {}
    else:
        forecast_policy["probability_temperature_alpha"] = float(selected_candidate["default_alpha"])
        forecast_policy["probability_temperature_alpha_by_unit"] = dict(selected_candidate.get("unit_alpha_map", {}))

    forecast_policy["probability_temperature_learned_at_utc"] = utc_now().isoformat()
    forecast_policy["probability_temperature_learned_from"] = {
        "source": "contractual_resolution_audit",
        "snapshot": snapshot_path.name,
        "events": int(selected_candidate["events"]),
        "selection_kind": str(selected_candidate["kind"]),
        "model_log_loss": float(selected_candidate["model_log_loss"]),
        "market_log_loss": float(selected_candidate["market_log_loss"]),
        "model_brier": float(selected_candidate["model_brier"]),
        "market_brier": float(selected_candidate["market_brier"]),
        "model_mode_hit_rate": float(selected_candidate["model_mode_hit_rate"]),
        "market_mode_hit_rate": float(selected_candidate["market_mode_hit_rate"]),
    }
    payload["forecast_policy"] = forecast_policy
    policy_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def persist_snapshot(snapshot: dict, reference_date: date) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_contractual_probability_temperature_calibration.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    main()
