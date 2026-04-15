import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.contractual_resolution_validator import (
    compare_contractual_resolution,
    summarize_contractual_comparisons,
)
from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Contrasta la auditoria resuelta basada en Open-Meteo contra el settlement real de Polymarket."
    )
    parser.add_argument(
        "--reference-date",
        default=date.today().isoformat(),
        help="Fecha de referencia YYYY-MM-DD para localizar la auditoria resuelta.",
    )
    parser.add_argument(
        "--audit-snapshot-path",
        help="Ruta explicita a un snapshot de auditoria resuelta. Si se omite, usa logs/snapshots/<reference-date>_blind_snapshot_resolution_audit.json.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    audit_path = (
        Path(args.audit_snapshot_path)
        if args.audit_snapshot_path
        else ROOT / "logs" / "snapshots" / f"{reference_date.isoformat()}_blind_snapshot_resolution_audit.json"
    )
    if not audit_path.exists():
        raise SystemExit(f"No existe la auditoria resuelta: {audit_path}")

    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    gamma = PolymarketGammaClient()

    comparisons = []
    skipped_events = []
    event_cache: dict[str, dict] = {}

    for evaluation in audit_payload.get("evaluations", []):
        event_slug = str(evaluation.get("event_slug") or "")
        if not event_slug:
            skipped_events.append({"event_slug": event_slug, "reason": "missing_event_slug"})
            continue

        if event_slug not in event_cache:
            try:
                event_cache[event_slug] = await gamma.fetch_event_by_slug(event_slug)
            except Exception as exc:
                skipped_events.append(
                    {
                        "event_slug": event_slug,
                        "reason": "gamma_fetch_failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue

        comparison = compare_contractual_resolution(evaluation, event_cache[event_slug])
        if comparison is None:
            skipped_events.append(
                {
                    "event_slug": event_slug,
                    "reason": "contractual_winner_unavailable",
                }
            )
            continue
        comparisons.append(comparison)

    summary = summarize_contractual_comparisons(comparisons)
    payload = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date.isoformat(),
        "source_audit_snapshot": str(audit_path.relative_to(ROOT)),
        "summary": summary,
        "comparisons": [asdict(comparison) for comparison in comparisons],
        "skipped_events": skipped_events,
    }
    output_path = persist_snapshot(reference_date, payload)

    print(f"Snapshot guardado en: {output_path}")
    print("")
    print("=== AUDITORIA CONTRACTUAL POLYMARKET ===")
    print(f"Eventos comparados: {summary['events']}")
    print(f"Question match rate: {summary['question_match_rate']:.1%}")
    print(f"Market id match rate: {summary['market_id_match_rate']:.1%}")
    print(f"Discrepancias: {summary['discrepancies']}")
    print(
        f"Paper PnL Open-Meteo: {summary['openmeteo_paper_total_pnl']:+.3f} | "
        f"Paper PnL contractual: {summary['contractual_paper_total_pnl']:+.3f} | "
        f"delta={summary['contractual_paper_pnl_delta']:+.3f}"
    )


def persist_snapshot(reference_date: date, payload: dict) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_contractual_resolution_audit.json"
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    asyncio.run(main())
