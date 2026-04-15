import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.adaptive_threshold_search import (
    aggregate_horizon_strategy_searches,
    aggregate_policy_searches,
    select_applied_policy_candidate,
    write_forecast_policy,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evalua estabilidad del corte adaptive en varias ventanas y aprende un umbral comun."
    )
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Fecha de referencia YYYY-MM-DD.")
    parser.add_argument(
        "--lookback-windows",
        default="30,45,60",
        help="Ventanas separadas por coma para comparar estabilidad.",
    )
    parser.add_argument("--max-events", type=int, default=30, help="Maximo de eventos por ventana.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Horizonte maximo a evaluar.")
    parser.add_argument(
        "--apply-learned-policy",
        action="store_true",
        help="Aplica en config/forecast_policy.yaml el corte agregado estable.",
    )
    return parser.parse_args()


def parse_lookback_windows(raw_value: str) -> tuple[int, ...]:
    windows: list[int] = []
    for chunk in raw_value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        value = int(chunk)
        if value <= 0:
            raise ValueError("Todas las ventanas deben ser enteros positivos.")
        if value not in windows:
            windows.append(value)
    if not windows:
        raise ValueError("Debes indicar al menos una ventana de lookback.")
    return tuple(windows)


def run_recent_horizon_backtest(as_of_date: str, lookback_days: int, max_events: int, max_horizon_days: int) -> dict:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "run_recent_horizon_temperature_backtest.py"),
        "--as-of-date",
        as_of_date,
        "--lookback-days",
        str(lookback_days),
        "--max-events",
        str(max_events),
        "--max-horizon-days",
        str(max_horizon_days),
    ]
    subprocess.run(command, cwd=ROOT, check=True)

    snapshot_path = ROOT / "logs" / "snapshots" / f"{as_of_date}_recent_horizon_temperature_backtest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return {
        "lookback_days": lookback_days,
        "summary": snapshot["summary"],
        "by_horizon": snapshot["by_horizon"],
        "policy_search": snapshot["policy_search"],
        "snapshot_path": snapshot_path.relative_to(ROOT).as_posix(),
    }


def persist_snapshot(snapshot: dict, as_of_date: str) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{as_of_date}_adaptive_policy_stability.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    windows = parse_lookback_windows(args.lookback_windows)

    window_results = [
        run_recent_horizon_backtest(
            as_of_date=args.as_of_date,
            lookback_days=lookback_days,
            max_events=args.max_events,
            max_horizon_days=args.max_horizon_days,
        )
        for lookback_days in windows
    ]

    objective = "adaptive_log_loss"
    aggregated_search = aggregate_policy_searches(window_results, objective=objective)
    horizon_strategy_search = aggregate_horizon_strategy_searches(
        window_results,
        max_horizon_days=args.max_horizon_days,
        objective=objective,
    )
    selected_applied_policy = select_applied_policy_candidate(
        aggregated_cutoff_search=aggregated_search,
        aggregated_horizon_search=horizon_strategy_search,
        objective=objective,
    )
    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "as_of_date": args.as_of_date,
        "lookback_windows": list(windows),
        "max_events": args.max_events,
        "max_horizon_days": args.max_horizon_days,
        "window_results": window_results,
        "aggregated_policy_search": aggregated_search,
        "aggregated_horizon_strategy_search": horizon_strategy_search,
        "selected_applied_policy": selected_applied_policy,
    }
    output_path = persist_snapshot(snapshot, args.as_of_date)

    print(f"Estabilidad guardada en: {output_path}")
    print("")
    print("=== RESUMEN ESTABILIDAD ADAPTIVE ===")
    for window_result in window_results:
        selected = window_result["policy_search"].get("selected_policy")
        if not selected:
            continue
        print(
            f"Ventana {window_result['lookback_days']}d -> "
            f"H<={selected['baseline_max_horizon_days']} | "
            f"log_loss={selected['adaptive_log_loss']:.3f} | "
            f"brier={selected['adaptive_brier']:.3f}"
        )

    selected_policy = aggregated_search.get("selected_policy")
    if selected_policy:
        print(
            f"Estable agregado -> H<={selected_policy['baseline_max_horizon_days']} | "
            f"log_loss medio={selected_policy['mean_adaptive_log_loss']:.3f} | "
            f"brier medio={selected_policy['mean_adaptive_brier']:.3f} | "
            f"frecuencia={selected_policy['selection_frequency']:.1%}"
        )

    horizon_policy = horizon_strategy_search.get("policy_summary")
    if horizon_policy:
        overrides = horizon_strategy_search.get("selected_strategy_by_horizon", {})
        print(
            "Politica por horizonte -> "
            f"log_loss medio={horizon_policy['adaptive_log_loss']:.3f} | "
            f"brier medio={horizon_policy['adaptive_brier']:.3f} | "
            f"hit-rate medio={horizon_policy['adaptive_hit_rate']:.1%} | "
            f"overrides={overrides}"
        )

    if selected_applied_policy:
        print(
            "Politica elegida para config -> "
            f"{selected_applied_policy['selection_mode']} | "
            f"log_loss={selected_applied_policy['adaptive_log_loss']:.3f} | "
            f"brier={selected_applied_policy['adaptive_brier']:.3f}"
        )

    if args.apply_learned_policy and selected_applied_policy:
        policy_path = ROOT / "config" / "forecast_policy.yaml"
        write_forecast_policy(
            policy_path,
            baseline_max_horizon_days=int(selected_applied_policy["baseline_max_horizon_days"]),
            objective=aggregated_search["objective"],
            as_of_date=args.as_of_date,
            lookback_days=None,
            max_events=args.max_events,
            max_horizon_days=args.max_horizon_days,
            learned_at_utc=utc_now(),
            source="adaptive_policy_stability",
            extra_metadata={
                "lookback_windows": list(windows),
                "window_cutoffs": aggregated_search["window_cutoffs"],
                "windows_evaluated": aggregated_search["windows_evaluated"],
            },
            selection_mode=selected_applied_policy["selection_mode"],
            horizon_strategy_overrides=selected_applied_policy["horizon_strategy_overrides"],
        )
        print(
            "Politica aplicada en config: "
            f"{selected_applied_policy['selection_mode']} | "
            f"fallback H<={selected_applied_policy['baseline_max_horizon_days']}"
        )


if __name__ == "__main__":
    main()
