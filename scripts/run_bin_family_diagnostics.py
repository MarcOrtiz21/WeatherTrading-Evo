import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.bin_family_diagnostics import (
    build_bin_family_diagnostics,
    load_event_payloads_from_audit,
)
from weather_trading.services.forecast_engine.probability_temperature import (
    get_probability_temperature_alpha,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnostica auditorias resueltas por familia de mercado (unidad, shape y cohorte)."
    )
    parser.add_argument(
        "--audit-snapshot-path",
        default="logs/snapshots/2026-04-10_blind_snapshot_resolution_audit.json",
        help="Ruta al snapshot de auditoria resuelta.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit_path = Path(ROOT) / args.audit_snapshot_path
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    event_payloads = load_event_payloads_from_audit(audit_payload, Path(ROOT))
    current_alpha = get_probability_temperature_alpha()

    diagnostics = build_bin_family_diagnostics(
        audit_payload,
        event_payloads,
        current_alpha=current_alpha,
    )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "audit_snapshot_path": audit_path.relative_to(ROOT).as_posix(),
        "current_probability_temperature_alpha": current_alpha,
        **diagnostics,
    }
    output_path = persist_snapshot(snapshot, reference_date=date.today())
    print(f"Diagnostico guardado en: {output_path}")
    print("")
    print("=== RESUMEN BIN FAMILY DIAGNOSTICS ===")
    print(
        f"Alpha actual: {current_alpha:.2f} | "
        f"mejor alpha global: {snapshot['calibration_probes']['best_global_alpha']['alpha']:.2f} | "
        f"log_loss={snapshot['calibration_probes']['best_global_alpha']['log_loss']:.3f}"
    )
    best_unit = snapshot["calibration_probes"]["best_unit_alpha"]
    print(
        f"Mejor alpha por unidad: C={best_unit['celsius_alpha']:.2f}, "
        f"F={best_unit['fahrenheit_alpha']:.2f} | "
        f"log_loss={best_unit['log_loss']:.3f}"
    )
    print(
        f"Candidatos por unidad que dominan al alpha actual: "
        f"{len(snapshot['calibration_probes']['dominating_unit_candidates'])}"
    )
    weakest = snapshot["recommendations"]["weakest_groups"]
    if weakest:
        first = weakest[0]
        print(
            f"Grupo mas debil: {first['family']} | events={first['events']} | "
            f"log_loss_delta={first['log_loss_delta_vs_market']:.3f} | "
            f"mode_hit_delta={first['mode_hit_delta_vs_market']:.3f}"
        )


def persist_snapshot(snapshot: dict, reference_date: date) -> Path:
    output_dir = Path(ROOT) / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_bin_family_diagnostics.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    main()
