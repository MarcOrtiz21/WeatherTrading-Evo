import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.services.evaluation.watchlist_strategy_analysis import (
    build_strategy_comparison_digest,
    build_watchlist_strategy_summary,
    build_alignment_from_snapshot_event,
    build_missing_alignment,
    build_trader_candidates,
    evaluate_candidate_trade,
    infer_yes_bias,
    persist_watchlist_strategy_snapshot,
    should_skip_celsius_active_unclassified,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compara el book actual del modelo contra overlays y estrategias de copytrading basadas en la watchlist."
    )
    parser.add_argument(
        "--reference-date",
        default=date.today().isoformat(),
        help="Fecha de referencia YYYY-MM-DD para elegir la auditoria resuelta.",
    )
    parser.add_argument(
        "--allow-remote-reconstruction",
        action="store_true",
        help="Permite reconstruir watchlist historica via Data API cuando el snapshot no la trae congelada.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    audit_path = ROOT / "logs" / "snapshots" / f"{reference_date.isoformat()}_blind_snapshot_resolution_audit.json"
    if not audit_path.exists():
        raise SystemExit(f"No existe auditoria para {reference_date}: {audit_path}")

    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    summary = await build_watchlist_strategy_summary(
        reference_date=reference_date,
        audit_snapshot=audit,
        root=ROOT,
        allow_remote_reconstruction=bool(args.allow_remote_reconstruction),
    )
    summary["strategy_comparison_digest"] = build_strategy_comparison_digest(summary)

    output_path = persist_watchlist_strategy_snapshot(root=ROOT, reference_date=reference_date, payload=summary)
    print(f"Snapshot guardado en: {output_path}")
    print("")
    print("=== COMPARATIVA WATCHLIST VS MODELO ===")
    for name, metrics in summary["strategies"].items():
        print(
            f"- {name}: trades={metrics['trades']} | "
            f"hit_rate={metrics['selected_market_hit_rate']:.1%} | "
            f"pnl={metrics['total_pnl']:+.3f} | roi={metrics['roi_on_stake']:.1%}"
        )


if __name__ == "__main__":
    asyncio.run(main())
