import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnostica sesgo térmico por estación a partir de snapshots resueltos de Polymarket."
    )
    parser.add_argument(
        "--snapshot-path",
        default="logs/snapshots/2026-04-06_recent_horizon_temperature_backtest.json",
        help="Ruta al snapshot de backtest reciente.",
    )
    parser.add_argument("--min-samples", type=int, default=4, help="Mínimo de filas con centro finito por estación.")
    parser.add_argument("--min-abs-bias-c", type=float, default=0.5, help="Sesgo absoluto mínimo para recomendar ajuste.")
    parser.add_argument("--max-abs-bias-c", type=float, default=2.0, help="Límite absoluto del ajuste recomendado.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.snapshot_path, encoding="utf-8") as handle:
        payload = json.load(handle)

    parser = DeterministicParser()
    stats: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {
            "rows": 0,
            "finite_rows": 0,
            "adaptive_hits": 0,
            "sum_delta_c": 0.0,
            "sum_abs_delta_c": 0.0,
        }
    )

    for row in iter_diagnostic_rows(payload):
        station_code = str(row.get("station_code", "UNKNOWN"))
        actual_center = question_center_c(parser, str(row.get("actual_winner_question", "")), str(row.get("event_date", "")))
        predicted_center = question_center_c(parser, str(row.get("adaptive_mode_question", "")), str(row.get("event_date", "")))

        bucket = stats[station_code]
        bucket["rows"] += 1
        bucket["adaptive_hits"] += 1 if row.get("adaptive_mode_hit") else 0

        if actual_center is None or predicted_center is None:
            continue

        delta_c = actual_center - predicted_center
        bucket["finite_rows"] += 1
        bucket["sum_delta_c"] += delta_c
        bucket["sum_abs_delta_c"] += abs(delta_c)

    diagnostics = []
    recommended_biases: dict[str, float] = {}
    for station_code, station_stats in sorted(stats.items()):
        finite_rows = int(station_stats["finite_rows"])
        rows = int(station_stats["rows"])
        avg_delta_c = None if finite_rows == 0 else float(station_stats["sum_delta_c"]) / finite_rows
        mean_abs_delta_c = None if finite_rows == 0 else float(station_stats["sum_abs_delta_c"]) / finite_rows
        hit_rate = 0.0 if rows == 0 else float(station_stats["adaptive_hits"]) / rows
        recommended_bias_c = None

        if (
            avg_delta_c is not None
            and finite_rows >= args.min_samples
            and abs(avg_delta_c) >= args.min_abs_bias_c
        ):
            capped_bias = max(-args.max_abs_bias_c, min(args.max_abs_bias_c, avg_delta_c))
            recommended_bias_c = round(capped_bias, 1)
            recommended_biases[station_code] = recommended_bias_c

        diagnostics.append(
            {
                "station_code": station_code,
                "rows": rows,
                "finite_rows": finite_rows,
                "adaptive_hit_rate": round(hit_rate, 4),
                "avg_delta_c": None if avg_delta_c is None else round(avg_delta_c, 3),
                "mean_abs_delta_c": None if mean_abs_delta_c is None else round(mean_abs_delta_c, 3),
                "recommended_bias_c": recommended_bias_c,
            }
        )

    output = {
        "snapshot_path": args.snapshot_path,
        "diagnostics_generated_for_date": date.today().isoformat(),
        "station_diagnostics": sorted(
            diagnostics,
            key=lambda item: (
                item["recommended_bias_c"] is None,
                -(item["finite_rows"] or 0),
                -(abs(item["avg_delta_c"]) if item["avg_delta_c"] is not None else 0.0),
            ),
        ),
        "recommended_station_temperature_bias_c": recommended_biases,
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))


def iter_diagnostic_rows(payload: dict) -> list[dict]:
    if payload.get("rows"):
        return list(payload["rows"])
    if payload.get("evaluations"):
        rows = []
        for evaluation in payload["evaluations"]:
            rows.append(
                {
                    "station_code": evaluation.get("station_code"),
                    "event_date": evaluation.get("event_date"),
                    "actual_winner_question": evaluation.get("winner_question", ""),
                    "adaptive_mode_question": evaluation.get("model_mode_question", ""),
                    "adaptive_mode_hit": evaluation.get("model_mode_hit", False),
                }
            )
        return rows
    return []


def question_center_c(parser: DeterministicParser, question: str, event_date: str) -> float | None:
    spec = parser.parse(
        {
            "id": "diagnostic",
            "question": question,
            "description": "",
            "rules": "",
            "event_date": event_date,
            "outcomes": ["Yes", "No"],
        }
    )
    if spec is None:
        return None
    if spec.bin_low_c is None or spec.bin_high_c is None:
        return None
    return (float(spec.bin_low_c) + float(spec.bin_high_c)) / 2


if __name__ == "__main__":
    main()
