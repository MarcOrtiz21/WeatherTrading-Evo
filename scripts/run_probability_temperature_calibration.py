import argparse
import json
import math
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aprende un alpha global para calibrar probabilidades del forecast desde auditorias resueltas."
    )
    parser.add_argument(
        "--audit-snapshot-path",
        default="logs/snapshots/2026-04-08_blind_snapshot_resolution_audit.json",
        help="Ruta al snapshot de auditoria resuelta.",
    )
    parser.add_argument("--alpha-min", type=float, default=0.5)
    parser.add_argument("--alpha-max", type=float, default=1.5)
    parser.add_argument("--alpha-step", type=float, default=0.05)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Aplica el alpha seleccionado en config/forecast_policy.yaml.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit_path = Path(args.audit_snapshot_path)
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))

    event_payloads = load_resolved_event_payloads(audit_payload)
    candidates = evaluate_alpha_candidates(
        audit_payload=audit_payload,
        event_payloads=event_payloads,
        alpha_min=args.alpha_min,
        alpha_max=args.alpha_max,
        alpha_step=args.alpha_step,
    )
    selected_candidate = min(
        candidates,
        key=lambda item: (item["log_loss"], item["brier"], abs(item["alpha"] - 1.0)),
    )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "audit_snapshot_path": audit_path.as_posix(),
        "events": selected_candidate["events"],
        "selected_candidate": selected_candidate,
        "candidates": candidates,
    }
    output_path = persist_snapshot(snapshot, reference_date=date.today())
    print(f"Calibracion guardada en: {output_path}")
    print("")
    print("=== RESUMEN CALIBRACION PROBABILITY TEMPERATURE ===")
    print(f"Eventos usados: {selected_candidate['events']}")
    print(
        f"Alpha seleccionado: {selected_candidate['alpha']:.2f} | "
        f"log_loss={selected_candidate['log_loss']:.3f} | "
        f"brier={selected_candidate['brier']:.3f} | "
        f"mode_hit={selected_candidate['mode_hit_rate']:.1%}"
    )
    baseline_candidate = next(candidate for candidate in candidates if abs(candidate["alpha"] - 1.0) < 1e-9)
    print(
        f"Baseline alpha=1.00 | log_loss={baseline_candidate['log_loss']:.3f} | "
        f"brier={baseline_candidate['brier']:.3f} | "
        f"mode_hit={baseline_candidate['mode_hit_rate']:.1%}"
    )

    if args.apply:
        policy_path = Path(ROOT) / "config" / "forecast_policy.yaml"
        update_forecast_policy_alpha(
            policy_path,
            alpha=selected_candidate["alpha"],
            source_snapshot=audit_path.name,
            selected_candidate=selected_candidate,
        )
        print(f"Alpha aplicado en config: {selected_candidate['alpha']:.2f}")


def load_resolved_event_payloads(audit_payload: dict) -> dict[tuple[str, str], dict]:
    event_payloads: dict[tuple[str, str], dict] = {}
    for snapshot_file in audit_payload.get("snapshot_files", []):
        snapshot_path = Path(ROOT) / snapshot_file
        if not snapshot_path.exists():
            continue
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot_as_of_date = str(snapshot.get("as_of_date"))
        for event in snapshot.get("evaluated_events", []):
            event_payloads[(snapshot_as_of_date, str(event.get("event_slug")))] = event
    return event_payloads


def evaluate_alpha_candidates(
    *,
    audit_payload: dict,
    event_payloads: dict[tuple[str, str], dict],
    alpha_min: float,
    alpha_max: float,
    alpha_step: float,
) -> list[dict]:
    alphas: list[float] = []
    value = alpha_min
    while value <= alpha_max + 1e-9:
        alphas.append(round(value, 4))
        value += alpha_step

    candidates = []
    for alpha in alphas:
        winner_probabilities: list[float] = []
        briers: list[float] = []
        mode_hits: list[bool] = []
        for evaluation in audit_payload.get("evaluations", []):
            key = (str(evaluation.get("snapshot_as_of_date")), str(evaluation.get("event_slug")))
            event = event_payloads.get(key)
            if not event:
                continue
            markets = list(event.get("markets", []))
            if not markets:
                continue
            calibrated_probs = calibrate_market_probabilities(markets, alpha)
            winner_market_id = str(evaluation.get("winner_market_id"))
            winner_probability = calibrated_probs.get(winner_market_id)
            if winner_probability is None:
                continue
            winner_probabilities.append(winner_probability)
            briers.append(
                sum(
                    (probability - (1.0 if market_id == winner_market_id else 0.0)) ** 2
                    for market_id, probability in calibrated_probs.items()
                )
            )
            mode_hits.append(max(calibrated_probs, key=calibrated_probs.get) == winner_market_id)

        events = len(winner_probabilities)
        candidates.append(
            {
                "alpha": alpha,
                "events": events,
                "log_loss": (
                    sum(-math.log(max(probability, 1e-9)) for probability in winner_probabilities) / events
                    if events
                    else 0.0
                ),
                "brier": (sum(briers) / events) if events else 0.0,
                "mode_hit_rate": (sum(mode_hits) / events) if events else 0.0,
            }
        )
    return candidates


def calibrate_market_probabilities(markets: list[dict], alpha: float) -> dict[str, float]:
    raw = {
        str(market["market_id"]): max(float(market.get("fair_probability", 0.0)), 1e-12)
        for market in markets
    }
    adjusted = {
        market_id: probability ** alpha
        for market_id, probability in raw.items()
    }
    total = sum(adjusted.values()) or 1.0
    return {
        market_id: probability / total
        for market_id, probability in adjusted.items()
    }


def update_forecast_policy_alpha(
    path: Path,
    *,
    alpha: float,
    source_snapshot: str,
    selected_candidate: dict,
) -> None:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    forecast_policy = dict(payload.get("forecast_policy", {}))
    forecast_policy["probability_temperature_alpha"] = float(alpha)
    forecast_policy["probability_temperature_learned_at_utc"] = utc_now().isoformat()
    forecast_policy["probability_temperature_learned_from"] = {
        "source": "resolved_audit_snapshot",
        "snapshot": source_snapshot,
        "events": int(selected_candidate["events"]),
        "log_loss": float(selected_candidate["log_loss"]),
        "brier": float(selected_candidate["brier"]),
        "mode_hit_rate": float(selected_candidate["mode_hit_rate"]),
    }
    payload["forecast_policy"] = forecast_policy
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def persist_snapshot(snapshot: dict, reference_date: date) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_probability_temperature_calibration.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    main()
