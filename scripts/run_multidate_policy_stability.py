import argparse
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.adaptive_threshold_search import (
    aggregate_multidate_cutoff_searches,
    aggregate_multidate_horizon_strategy_searches,
    select_applied_policy_candidate,
    write_forecast_policy,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evalua estabilidad de la politica adaptive sobre varias fechas de referencia."
    )
    parser.add_argument("--start-as-of-date", help="Primera fecha YYYY-MM-DD incluida.")
    parser.add_argument("--end-as-of-date", default=date.today().isoformat(), help="Ultima fecha YYYY-MM-DD incluida.")
    parser.add_argument("--lookback-windows", default="30,45,60", help="Ventanas separadas por coma.")
    parser.add_argument("--max-events", type=int, default=30, help="Maximo de eventos por fecha y ventana.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Horizonte maximo a evaluar.")
    parser.add_argument(
        "--apply-learned-policy",
        action="store_true",
        help="Aplica en config/forecast_policy.yaml la politica agregada multifecha.",
    )
    return parser.parse_args()


def resolve_start_date(raw_start_date: str | None, end_date: date) -> date:
    if raw_start_date:
        return date.fromisoformat(raw_start_date)
    return end_date - timedelta(days=2)


def iter_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        raise ValueError("La fecha inicial no puede ser posterior a la final.")

    current = start_date
    dates: list[date] = []
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def run_date_stability(as_of_date: str, lookback_windows: str, max_events: int, max_horizon_days: int) -> dict:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "run_adaptive_policy_stability.py"),
        "--as-of-date",
        as_of_date,
        "--lookback-windows",
        lookback_windows,
        "--max-events",
        str(max_events),
        "--max-horizon-days",
        str(max_horizon_days),
    ]
    subprocess.run(command, cwd=ROOT, check=True)

    snapshot_path = ROOT / "logs" / "snapshots" / f"{as_of_date}_adaptive_policy_stability.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return {
        "as_of_date": as_of_date,
        "snapshot_path": snapshot_path.relative_to(ROOT).as_posix(),
        "selected_applied_policy": snapshot.get("selected_applied_policy"),
        "aggregated_policy_search": snapshot["aggregated_policy_search"],
        "aggregated_horizon_strategy_search": snapshot["aggregated_horizon_strategy_search"],
    }


def persist_snapshot(snapshot: dict, end_as_of_date: str) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{end_as_of_date}_multidate_policy_stability.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    end_date = date.fromisoformat(args.end_as_of_date)
    start_date = resolve_start_date(args.start_as_of_date, end_date)
    as_of_dates = [day.isoformat() for day in iter_dates(start_date, end_date)]
    objective = "adaptive_log_loss"

    date_results = [
        run_date_stability(
            as_of_date=as_of_date,
            lookback_windows=args.lookback_windows,
            max_events=args.max_events,
            max_horizon_days=args.max_horizon_days,
        )
        for as_of_date in as_of_dates
    ]

    aggregated_cutoff_search = aggregate_multidate_cutoff_searches(date_results, objective=objective)
    aggregated_horizon_search = aggregate_multidate_horizon_strategy_searches(
        date_results,
        max_horizon_days=args.max_horizon_days,
        objective=objective,
    )
    selected_applied_policy = select_applied_policy_candidate(
        aggregated_cutoff_search=aggregated_cutoff_search,
        aggregated_horizon_search=aggregated_horizon_search,
        objective=objective,
    )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "start_as_of_date": start_date.isoformat(),
        "end_as_of_date": end_date.isoformat(),
        "as_of_dates": as_of_dates,
        "lookback_windows": [int(chunk) for chunk in args.lookback_windows.split(",") if chunk.strip()],
        "max_events": args.max_events,
        "max_horizon_days": args.max_horizon_days,
        "date_results": date_results,
        "aggregated_cutoff_search": aggregated_cutoff_search,
        "aggregated_horizon_strategy_search": aggregated_horizon_search,
        "selected_applied_policy": selected_applied_policy,
    }
    output_path = persist_snapshot(snapshot, end_date.isoformat())

    print(f"Estabilidad multifecha guardada en: {output_path}")
    print("")
    print("=== RESUMEN ESTABILIDAD MULTIFECHA ===")
    for date_result in date_results:
        selected = date_result.get("selected_applied_policy")
        if not selected:
            continue
        print(
            f"{date_result['as_of_date']} -> {selected['selection_mode']} | "
            f"H<={selected['baseline_max_horizon_days']} | "
            f"log_loss={selected['adaptive_log_loss']:.3f} | "
            f"brier={selected['adaptive_brier']:.3f}"
        )

    cutoff_policy = aggregated_cutoff_search.get("selected_policy")
    if cutoff_policy:
        print(
            "Agregado por cutoff -> "
            f"H<={cutoff_policy['baseline_max_horizon_days']} | "
            f"log_loss medio={cutoff_policy['mean_adaptive_log_loss']:.3f} | "
            f"brier medio={cutoff_policy['mean_adaptive_brier']:.3f} | "
            f"frecuencia={cutoff_policy['selection_frequency']:.1%}"
        )

    horizon_policy = aggregated_horizon_search.get("policy_summary")
    if horizon_policy:
        print(
            "Agregado por horizonte -> "
            f"log_loss medio={horizon_policy['adaptive_log_loss']:.3f} | "
            f"brier medio={horizon_policy['adaptive_brier']:.3f} | "
            f"hit-rate medio={horizon_policy['adaptive_hit_rate']:.1%} | "
            f"overrides={aggregated_horizon_search.get('selected_strategy_by_horizon', {})}"
        )

    if selected_applied_policy:
        print(
            "Politica elegida -> "
            f"{selected_applied_policy['selection_mode']} | "
            f"H<={selected_applied_policy['baseline_max_horizon_days']} | "
            f"log_loss={selected_applied_policy['adaptive_log_loss']:.3f} | "
            f"brier={selected_applied_policy['adaptive_brier']:.3f}"
        )

    if args.apply_learned_policy and selected_applied_policy:
        policy_path = ROOT / "config" / "forecast_policy.yaml"
        write_forecast_policy(
            policy_path,
            baseline_max_horizon_days=int(selected_applied_policy["baseline_max_horizon_days"]),
            objective=objective,
            as_of_date=end_date.isoformat(),
            lookback_days=None,
            max_events=args.max_events,
            max_horizon_days=args.max_horizon_days,
            learned_at_utc=utc_now(),
            source="multidate_policy_stability",
            extra_metadata={
                "start_as_of_date": start_date.isoformat(),
                "end_as_of_date": end_date.isoformat(),
                "as_of_dates": as_of_dates,
                "lookback_windows": [int(chunk) for chunk in args.lookback_windows.split(",") if chunk.strip()],
                "selected_date_cutoffs": aggregated_cutoff_search.get("selected_date_cutoffs", []),
                "dates_evaluated": aggregated_cutoff_search.get("dates_evaluated", 0),
            },
            selection_mode=selected_applied_policy["selection_mode"],
            horizon_strategy_overrides=selected_applied_policy["horizon_strategy_overrides"],
        )
        print(
            "Politica multifecha aplicada en config: "
            f"{selected_applied_policy['selection_mode']} | "
            f"fallback H<={selected_applied_policy['baseline_max_horizon_days']}"
        )


if __name__ == "__main__":
    main()
