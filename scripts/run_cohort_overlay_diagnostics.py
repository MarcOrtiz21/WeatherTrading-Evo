import argparse
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un diagnostico por cohorte con cortes por calidad, estacion y watchlist overlay."
    )
    parser.add_argument(
        "--reference-date",
        default=date.today().isoformat(),
        help="Fecha del snapshot de auditoria YYYY-MM-DD.",
    )
    parser.add_argument(
        "--cohort-as-of-date",
        default=None,
        help="Cohorte snapshot_as_of_date a analizar. Si no se indica, usa la ultima cohorte madura.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    audit_path = ROOT / "logs" / "snapshots" / f"{reference_date.isoformat()}_blind_snapshot_resolution_audit.json"
    if not audit_path.exists():
        raise SystemExit(f"No existe auditoria para {reference_date}: {audit_path}")

    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    cohort_as_of_date = args.cohort_as_of_date or infer_latest_mature_cohort(audit)
    cohort_rows = [
        dict(row)
        for row in audit.get("evaluations", [])
        if str(row.get("snapshot_as_of_date")) == str(cohort_as_of_date)
    ]
    if not cohort_rows:
        raise SystemExit(f"No hay eventos evaluados para cohort_as_of_date={cohort_as_of_date}")

    live_snapshot_path = ROOT / "logs" / "snapshots" / f"{cohort_as_of_date}_polymarket_blind_live_validation.json"
    live_snapshot = (
        json.loads(live_snapshot_path.read_text(encoding="utf-8"))
        if live_snapshot_path.exists()
        else {"evaluated_events": []}
    )
    live_events_by_slug = {
        str(event["event_slug"]): event
        for event in live_snapshot.get("evaluated_events", [])
        if event.get("event_slug")
    }

    enriched_rows = [enrich_row(row, live_events_by_slug.get(str(row["event_slug"]))) for row in cohort_rows]

    summary = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date.isoformat(),
        "cohort_as_of_date": str(cohort_as_of_date),
        "events": len(enriched_rows),
        "live_snapshot_path": (
            live_snapshot_path.relative_to(ROOT).as_posix() if live_snapshot_path.exists() else None
        ),
        "cohort_summary": summarize_rows(enriched_rows),
        "by_quality_tier": summarize_groups(enriched_rows, key="top_edge_quality_tier"),
        "by_watchlist_signal": summarize_groups(enriched_rows, key="watchlist_signal"),
        "stations": build_station_rows(enriched_rows),
        "recommendations": {
            "weakest_quality_tiers": select_worst_groups(
                summarize_groups(enriched_rows, key="top_edge_quality_tier"),
                min_events=1,
            ),
            "weakest_watchlist_signals": select_worst_groups(
                summarize_groups(enriched_rows, key="watchlist_signal"),
                min_events=1,
            ),
            "worst_stations_by_pnl": build_station_rows(enriched_rows)[:5],
        },
    }

    output_path = persist_snapshot(reference_date, cohort_as_of_date, summary)
    print(f"Diagnostico guardado en: {output_path}")
    print("")
    print("=== DIAGNOSTICO DE COHORTE ===")
    print(
        f"Cohorte {cohort_as_of_date} | eventos={summary['events']} | "
        f"model_hit={summary['cohort_summary']['model_mode_hit_rate']:.1%} | "
        f"market_hit={summary['cohort_summary']['market_mode_hit_rate']:.1%} | "
        f"model_log_loss={summary['cohort_summary']['model_log_loss']:.3f} | "
        f"market_log_loss={summary['cohort_summary']['market_log_loss']:.3f} | "
        f"paper_pnl={summary['cohort_summary']['paper_total_pnl']:+.3f}"
    )
    print("Peores tiers de calidad:")
    for item in summary["recommendations"]["weakest_quality_tiers"]:
        print(
            f"- {item['group']}: events={item['events']} | "
            f"log_loss_delta={item['log_loss_delta_vs_market']:+.3f} | "
            f"paper_pnl={item['paper_total_pnl']:+.3f} | roi={item['paper_roi_on_stake']:.1%}"
        )
    print("Peores señales de watchlist:")
    for item in summary["recommendations"]["weakest_watchlist_signals"]:
        print(
            f"- {item['group']}: events={item['events']} | "
            f"log_loss_delta={item['log_loss_delta_vs_market']:+.3f} | "
            f"paper_pnl={item['paper_total_pnl']:+.3f} | roi={item['paper_roi_on_stake']:.1%}"
        )


def infer_latest_mature_cohort(audit_snapshot: dict) -> str:
    matured = sorted(
        {
            str(row.get("snapshot_as_of_date"))
            for row in audit_snapshot.get("evaluations", [])
            if row.get("snapshot_as_of_date")
        }
    )
    if not matured:
        raise ValueError("La auditoria no contiene cohortes maduras.")
    return matured[-1]


def enrich_row(row: dict, live_event: dict | None) -> dict:
    enriched = dict(row)
    if live_event:
        enriched["watchlist_signal"] = str(live_event.get("watchlist_signal") or "silent")
        enriched["watchlist_alignment_score"] = float(live_event.get("watchlist_alignment_score") or 0.0)
        enriched["watchlist_match_count"] = int(live_event.get("watchlist_match_count") or 0)
        enriched["watchlist_active_traders"] = list(live_event.get("watchlist_active_traders") or [])
        enriched["watchlist_aligned_traders"] = list(live_event.get("watchlist_aligned_traders") or [])
        enriched["watchlist_opposed_traders"] = list(live_event.get("watchlist_opposed_traders") or [])
    else:
        enriched["watchlist_signal"] = "unavailable"
        enriched["watchlist_alignment_score"] = 0.0
        enriched["watchlist_match_count"] = 0
        enriched["watchlist_active_traders"] = []
        enriched["watchlist_aligned_traders"] = []
        enriched["watchlist_opposed_traders"] = []
    return enriched


def summarize_rows(rows: list[dict]) -> dict:
    events = len(rows)
    if events == 0:
        return {
            "events": 0,
            "model_mode_hit_rate": 0.0,
            "market_mode_hit_rate": 0.0,
            "model_log_loss": 0.0,
            "market_log_loss": 0.0,
            "model_brier": 0.0,
            "market_brier": 0.0,
            "paper_trades": 0,
            "paper_total_stake": 0.0,
            "paper_total_pnl": 0.0,
            "paper_roi_on_stake": 0.0,
        }
    paper_trades = [row for row in rows if bool(row.get("paper_trade_taken"))]
    total_stake = sum(float(row.get("paper_trade_stake", 0.0)) for row in paper_trades)
    total_pnl = sum(float(row.get("paper_trade_pnl", 0.0)) for row in paper_trades)
    return {
        "events": events,
        "model_mode_hit_rate": sum(bool(row.get("model_mode_hit")) for row in rows) / events,
        "market_mode_hit_rate": sum(bool(row.get("market_mode_hit")) for row in rows) / events,
        "model_log_loss": sum(float(row.get("model_log_loss", 0.0)) for row in rows) / events,
        "market_log_loss": sum(float(row.get("market_log_loss", 0.0)) for row in rows) / events,
        "model_brier": sum(float(row.get("model_brier", 0.0)) for row in rows) / events,
        "market_brier": sum(float(row.get("market_brier", 0.0)) for row in rows) / events,
        "paper_trades": len(paper_trades),
        "paper_total_stake": total_stake,
        "paper_total_pnl": total_pnl,
        "paper_roi_on_stake": (total_pnl / total_stake) if total_stake else 0.0,
    }


def summarize_groups(rows: list[dict], *, key: str) -> dict[str, dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(key) or "unknown")].append(row)
    return {
        group: summarize_rows(group_rows)
        for group, group_rows in sorted(groups.items())
    }


def build_station_rows(rows: list[dict]) -> list[dict]:
    station_rows = []
    for row in rows:
        station_rows.append(
            {
                "station_code": str(row.get("station_code")),
                "event_slug": str(row.get("event_slug")),
                "top_edge_quality_tier": str(row.get("top_edge_quality_tier") or "unknown"),
                "watchlist_signal": str(row.get("watchlist_signal") or "unknown"),
                "paper_trade_taken": bool(row.get("paper_trade_taken")),
                "paper_trade_pnl": float(row.get("paper_trade_pnl", 0.0)),
                "model_mode_hit": bool(row.get("model_mode_hit")),
                "market_mode_hit": bool(row.get("market_mode_hit")),
                "model_log_loss": float(row.get("model_log_loss", 0.0)),
                "market_log_loss": float(row.get("market_log_loss", 0.0)),
                "log_loss_delta_vs_market": float(row.get("model_log_loss", 0.0)) - float(row.get("market_log_loss", 0.0)),
                "top_edge_question": row.get("top_edge_question"),
                "winner_question": row.get("winner_question"),
            }
        )
    return sorted(
        station_rows,
        key=lambda item: (
            item["paper_trade_pnl"],
            item["log_loss_delta_vs_market"],
        ),
    )


def select_worst_groups(group_summaries: dict[str, dict], *, min_events: int) -> list[dict]:
    ranked = []
    for group, summary in group_summaries.items():
        if int(summary["events"]) < min_events:
            continue
        ranked.append(
            {
                "group": group,
                "events": int(summary["events"]),
                "log_loss_delta_vs_market": float(summary["model_log_loss"]) - float(summary["market_log_loss"]),
                "paper_total_pnl": float(summary["paper_total_pnl"]),
                "paper_roi_on_stake": float(summary["paper_roi_on_stake"]),
            }
        )
    return sorted(
        ranked,
        key=lambda item: (
            item["paper_total_pnl"],
            item["log_loss_delta_vs_market"],
        ),
    )[:5]


def persist_snapshot(reference_date: date, cohort_as_of_date: str, payload: dict) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date.isoformat()}_cohort_overlay_diagnostics_{cohort_as_of_date}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


if __name__ == "__main__":
    main()
