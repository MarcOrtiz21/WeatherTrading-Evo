import argparse
import asyncio
from collections import Counter
import json
import sqlite3
import sys
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.observation_backfill import local_date_utc_bounds
from weather_trading.services.evaluation.blind_snapshot_resolution import (
    BlindSnapshotEventEvaluation,
    discover_blind_snapshot_paths,
    evaluate_blind_snapshot_event,
    is_event_eligible_for_resolution,
    summarize_blind_snapshot_evaluations,
)
from weather_trading.services.evaluation.watchlist_strategy_analysis import (
    build_strategy_comparison_digest,
    build_watchlist_strategy_summary,
    persist_watchlist_strategy_snapshot,
)
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import OpenMeteoClient


class ActualTemperatureResolver:
    def __init__(self, db_path: Path, openmeteo: OpenMeteoClient):
        self.db_path = db_path
        self.openmeteo = openmeteo
        self.remote_archive_available = True
        self.remote_archive_error: str | None = None
        self.source_counts = {
            "openmeteo_archive": 0,
        }
        self._local_cache: dict[tuple[str, str, str], tuple[float | None, str | None]] = {}

    async def resolve(
        self,
        *,
        station_code: str,
        station_timezone: str,
        latitude: float,
        longitude: float,
        local_date: date,
    ) -> tuple[float | None, str | None, str | None]:
        local_temp, local_source = self.lookup_local_daily_max(
            station_code=station_code,
            station_timezone=station_timezone,
            local_date=local_date,
        )
        if local_temp is not None:
            source_label = local_source or "local_weather_observations"
            self.source_counts[source_label] = self.source_counts.get(source_label, 0) + 1
            return local_temp, source_label, None

        if not self.remote_archive_available:
            return None, None, "archive_fetch_unavailable"

        try:
            remote_temp = await self.openmeteo.fetch_archive_daily_max(
                latitude=latitude,
                longitude=longitude,
                local_date=local_date,
            )
        except Exception as exc:
            self.remote_archive_available = False
            self.remote_archive_error = f"{type(exc).__name__}: {exc}"
            return None, None, "archive_fetch_error"

        if remote_temp is None:
            return None, None, "missing_archive_observation"

        self.source_counts["openmeteo_archive"] += 1
        return round(float(remote_temp), 1), "openmeteo_archive", None

    def lookup_local_daily_max(
        self,
        *,
        station_code: str,
        station_timezone: str,
        local_date: date,
    ) -> tuple[float | None, str | None]:
        cache_key = (station_code, station_timezone, local_date.isoformat())
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        if not self.db_path.exists():
            self._local_cache[cache_key] = (None, None)
            return None, None

        start_utc, end_utc = local_date_utc_bounds(local_date, station_timezone)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT temp_c, provider, raw_reference
                FROM weather_observations
                WHERE station_code = ?
                  AND observed_at_utc >= ?
                  AND observed_at_utc < ?
                ORDER BY temp_c DESC, observed_at_utc DESC
                LIMIT 1
                """,
                (
                    station_code,
                    start_utc.isoformat(sep=" "),
                    end_utc.isoformat(sep=" "),
                ),
            ).fetchone()

        if row is None or row[0] is None:
            self._local_cache[cache_key] = (None, None)
            return None, None

        result = round(float(row[0]), 1)
        provider = "" if row[1] is None else str(row[1])
        raw_reference = "" if row[2] is None else str(row[2])
        source_label = self._local_source_label(provider=provider, raw_reference=raw_reference)
        self._local_cache[cache_key] = (result, source_label)
        return result, source_label

    @staticmethod
    def _local_source_label(*, provider: str, raw_reference: str) -> str:
        if provider == "open_meteo" and raw_reference.startswith("archive_daily_max_backfill:"):
            return "local_open_meteo_archive_backfill"
        return "local_weather_observations"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evalua snapshots ciegos live ya maduros contra la observacion meteorologica real."
    )
    parser.add_argument("--reference-date", default=date.today().isoformat(), help="Fecha de corte YYYY-MM-DD.")
    parser.add_argument("--start-as-of-date", help="Primera fecha de snapshot YYYY-MM-DD incluida.")
    parser.add_argument("--end-as-of-date", help="Ultima fecha de snapshot YYYY-MM-DD incluida.")
    parser.add_argument(
        "--paper-edge-threshold",
        type=float,
        default=0.0,
        help="Solo toma paper trades si el top edge supera este umbral.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    start_as_of_date = None if not args.start_as_of_date else date.fromisoformat(args.start_as_of_date)
    end_as_of_date = None if not args.end_as_of_date else date.fromisoformat(args.end_as_of_date)

    mapper = StationMapperService()
    openmeteo = OpenMeteoClient()
    actual_temp_resolver = ActualTemperatureResolver(ROOT / "weather_trading.db", openmeteo)
    snapshot_paths = discover_blind_snapshot_paths(
        ROOT / "logs" / "snapshots",
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
    )

    evaluations: list[BlindSnapshotEventEvaluation] = []
    evaluation_rows = []
    pending_events = []
    skipped_events = []

    for snapshot_path in snapshot_paths:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot_as_of_date = str(snapshot["as_of_date"])

        for event in snapshot.get("evaluated_events", []):
            event_date = str(event["event_date"])
            if not is_event_eligible_for_resolution(event_date, reference_date):
                pending_events.append(
                    {
                        "snapshot_as_of_date": snapshot_as_of_date,
                        "event_slug": event["event_slug"],
                        "event_date": event_date,
                        "reason": "event_not_finished_yet",
                    }
                )
                continue

            station = mapper.get_station(str(event["station_code"]))
            if not station:
                skipped_events.append(
                    {
                        "snapshot_as_of_date": snapshot_as_of_date,
                        "event_slug": event["event_slug"],
                        "event_date": event_date,
                        "reason": "missing_station_catalog",
                        "station_code": event["station_code"],
                    }
                )
                continue

            actual_temp_c, actual_temp_source, actual_temp_error = await actual_temp_resolver.resolve(
                station_code=str(event["station_code"]),
                station_timezone=str(station.get("timezone") or "UTC"),
                latitude=float(station["latitude"]),
                longitude=float(station["longitude"]),
                local_date=date.fromisoformat(event_date),
            )
            if actual_temp_c is None:
                skipped_event = {
                    "snapshot_as_of_date": snapshot_as_of_date,
                    "event_slug": event["event_slug"],
                    "event_date": event_date,
                    "reason": actual_temp_error or "missing_archive_observation",
                }
                if actual_temp_resolver.remote_archive_error and actual_temp_error in {
                    "archive_fetch_error",
                    "archive_fetch_unavailable",
                }:
                    skipped_event["archive_fetch_error"] = actual_temp_resolver.remote_archive_error
                skipped_events.append(
                    skipped_event
                )
                continue

            evaluation = evaluate_blind_snapshot_event(
                snapshot_as_of_date,
                event,
                actual_temp_c,
                paper_edge_threshold=args.paper_edge_threshold,
                actual_temperature_source=actual_temp_source,
            )
            if evaluation is None:
                skipped_events.append(
                    {
                        "snapshot_as_of_date": snapshot_as_of_date,
                        "event_slug": event["event_slug"],
                        "event_date": event_date,
                        "reason": "unable_to_match_winner_bin",
                        "actual_temp_c": round(float(actual_temp_c), 1),
                    }
                )
                continue

            evaluations.append(evaluation)
            evaluation_rows.append(asdict(evaluation))

    summary = summarize_blind_snapshot_evaluations(
        evaluations,
        paper_edge_threshold=args.paper_edge_threshold,
    )
    watchlist_strategy_summary = await build_watchlist_strategy_summary(
        reference_date=reference_date,
        audit_snapshot={
            "evaluations": evaluation_rows,
            "snapshot_files": [path.relative_to(ROOT).as_posix() for path in snapshot_paths],
        },
        root=ROOT,
        allow_remote_reconstruction=False,
    )
    watchlist_strategy_summary["strategy_comparison_digest"] = build_strategy_comparison_digest(
        watchlist_strategy_summary
    )
    watchlist_strategy_snapshot_path = persist_watchlist_strategy_snapshot(
        root=ROOT,
        reference_date=reference_date,
        payload=watchlist_strategy_summary,
    )
    coverage = summarize_resolution_coverage(
        snapshot_paths=snapshot_paths,
        evaluations=evaluations,
        pending_events=pending_events,
        skipped_events=skipped_events,
    )

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date.isoformat(),
        "start_as_of_date": None if start_as_of_date is None else start_as_of_date.isoformat(),
        "end_as_of_date": None if end_as_of_date is None else end_as_of_date.isoformat(),
        "paper_edge_threshold": args.paper_edge_threshold,
        "snapshot_files": [path.relative_to(ROOT).as_posix() for path in snapshot_paths],
        "actual_temperature_sources": actual_temp_resolver.source_counts,
        "archive_fetch_status": {
            "remote_archive_available": actual_temp_resolver.remote_archive_available,
            "remote_archive_error": actual_temp_resolver.remote_archive_error,
        },
        "watchlist_strategy_snapshot": watchlist_strategy_snapshot_path.relative_to(ROOT).as_posix(),
        "watchlist_strategy_comparison": watchlist_strategy_summary["strategy_comparison_digest"],
        "coverage": coverage,
        "summary": summary,
        "evaluations": evaluation_rows,
        "pending_events": pending_events,
        "skipped_events": skipped_events,
    }
    output_path = persist_snapshot(snapshot, reference_date)

    print(f"Auditoria guardada en: {output_path}")
    print("")
    print("=== RESUMEN AUDITORIA SNAPSHOTS CIEGOS ===")
    print(f"Snapshots revisados: {len(snapshot_paths)}")
    print(
        f"Cobertura madura: {coverage['mature_resolution_coverage']:.1%} | "
        f"Eventos maduros: {coverage['mature_events']}"
    )
    print(f"Eventos evaluados: {summary['events']}")
    print(f"Eventos pendientes: {len(pending_events)}")
    print(f"Eventos omitidos: {len(skipped_events)}")
    if coverage["skip_reason_counts"]:
        print("Motivos de omision:")
        for reason, count in coverage["skip_reason_counts"].items():
            print(f"  {reason}: {count}")
    if summary["events"] > 0:
        print(
            f"Eventos operables: {summary['operable_events']} ({summary['operable_rate']:.1%}) | "
            f"Evidence score medio: {summary['avg_event_evidence_score']:.3f}"
        )
        print(
            f"Model mode hit-rate: {summary['model_mode_hit_rate']:.1%} | "
            f"Market mode hit-rate: {summary['market_mode_hit_rate']:.1%}"
        )
        print(
            f"Model log loss: {summary['model_log_loss']:.3f} | "
            f"Market log loss: {summary['market_log_loss']:.3f}"
        )
        print(
            f"Model Brier: {summary['model_brier']:.3f} | "
            f"Market Brier: {summary['market_brier']:.3f}"
        )
        print(
            f"Paper trades: {summary['paper_trades']} | "
            f"Paper PnL: {summary['paper_total_pnl']:.3f} | "
            f"ROI sobre stake: {summary['paper_roi_on_stake']:.1%}"
        )
        print("")
        print("Comparativa filtros watchlist:")
        for strategy_name, metrics in summary_top_strategies(
            watchlist_strategy_summary["strategy_comparison_digest"]
        ):
            print(
                f"  {strategy_name}: trades={metrics['trades']} | "
                f"hit_rate={metrics['selected_market_hit_rate']:.1%} | "
                f"pnl={metrics['total_pnl']:+.3f} | roi={metrics['roi_on_stake']:.1%}"
            )
        print("")
        print("Por estrategia:")
        for strategy, strategy_summary in summary["by_strategy"].items():
            print(
                f"  {strategy}: events={strategy_summary['events']} | "
                f"paper_trades={strategy_summary['paper_trades']} | "
                f"log_loss={strategy_summary['model_log_loss']:.3f} | "
                f"pnl={strategy_summary['paper_total_pnl']:.3f}"
            )
        print("Por horizonte:")
        for horizon_days, horizon_summary in summary["by_horizon_days"].items():
            print(
                f"  H{horizon_days}: events={horizon_summary['events']} | "
                f"paper_trades={horizon_summary['paper_trades']} | "
                f"log_loss={horizon_summary['model_log_loss']:.3f} | "
                f"pnl={horizon_summary['paper_total_pnl']:.3f}"
            )
        print("Por calidad:")
        for quality_tier, quality_summary in summary["by_quality_tier"].items():
            print(
                f"  {quality_tier}: events={quality_summary['events']} | "
                f"paper_trades={quality_summary['paper_trades']} | "
                f"log_loss={quality_summary['model_log_loss']:.3f} | "
                f"pnl={quality_summary['paper_total_pnl']:.3f}"
            )
        print("Por evidencia:")
        for evidence_tier, evidence_summary in summary["by_event_evidence_tier"].items():
            print(
                f"  {evidence_tier}: events={evidence_summary['events']} | "
                f"operable={evidence_summary['operable_events']} | "
                f"log_loss={evidence_summary['model_log_loss']:.3f} | "
                f"pnl={evidence_summary['paper_total_pnl']:.3f}"
            )
        print("Por fuente de temperatura real:")
        for source_name, source_summary in summary["by_actual_temperature_source"].items():
            print(
                f"  {source_name}: events={source_summary['events']} | "
                f"log_loss={source_summary['model_log_loss']:.3f} | "
                f"pnl={source_summary['paper_total_pnl']:.3f}"
            )


def persist_snapshot(snapshot: dict, reference_date: date) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_blind_snapshot_resolution_audit.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def summarize_resolution_coverage(
    *,
    snapshot_paths: list[Path],
    evaluations: list[BlindSnapshotEventEvaluation],
    pending_events: list[dict],
    skipped_events: list[dict],
) -> dict:
    mature_events = len(evaluations) + len(skipped_events)
    reviewed_events = mature_events + len(pending_events)
    skip_reason_counts = Counter(str(event.get("reason") or "unknown") for event in skipped_events)
    pending_reason_counts = Counter(str(event.get("reason") or "unknown") for event in pending_events)
    return {
        "snapshots_reviewed": len(snapshot_paths),
        "reviewed_events": reviewed_events,
        "mature_events": mature_events,
        "evaluated_events": len(evaluations),
        "pending_events": len(pending_events),
        "skipped_events": len(skipped_events),
        "mature_resolution_coverage": (len(evaluations) / mature_events) if mature_events > 0 else 0.0,
        "mature_skip_rate": (len(skipped_events) / mature_events) if mature_events > 0 else 0.0,
        "pending_rate_over_reviewed": (len(pending_events) / reviewed_events) if reviewed_events > 0 else 0.0,
        "skip_reason_counts": dict(sorted(skip_reason_counts.items())),
        "pending_reason_counts": dict(sorted(pending_reason_counts.items())),
    }


def summary_top_strategies(strategy_digest: dict) -> list[tuple[str, dict]]:
    selected = strategy_digest.get("selected_strategies", {})
    order = [
        "model_current",
        "model_skip_opposed",
        "model_skip_celsius_active_unclassified",
        "model_skip_opposed_and_celsius_active_unclassified",
    ]
    return [(name, selected.get(name, {})) for name in order if name in selected]


if __name__ == "__main__":
    asyncio.run(main())
