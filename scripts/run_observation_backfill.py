import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.database import AsyncSessionLocal, init_db
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.observation_backfill import (
    build_archive_daily_max_observation,
    discover_mature_snapshot_targets,
    group_backfill_targets_by_station,
    partition_targets_by_local_observation_coverage,
)
from weather_trading.services.persistence.repository import WeatherRepository
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import OpenMeteoClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Puebla weather_trading.db con maximas diarias observadas para snapshots ya maduros."
    )
    parser.add_argument("--reference-date", default=date.today().isoformat(), help="Fecha de corte YYYY-MM-DD.")
    parser.add_argument("--start-as-of-date", help="Primera fecha de snapshot YYYY-MM-DD incluida.")
    parser.add_argument("--end-as-of-date", help="Ultima fecha de snapshot YYYY-MM-DD incluida.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    reference_date = date.fromisoformat(args.reference_date)
    start_as_of_date = None if not args.start_as_of_date else date.fromisoformat(args.start_as_of_date)
    end_as_of_date = None if not args.end_as_of_date else date.fromisoformat(args.end_as_of_date)

    await init_db()
    mapper = StationMapperService()
    openmeteo = OpenMeteoClient()

    targets, discovery_skipped = discover_mature_snapshot_targets(
        ROOT / "logs" / "snapshots",
        reference_date=reference_date,
        mapper=mapper,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
    )
    locally_covered_targets, missing_targets = partition_targets_by_local_observation_coverage(
        ROOT / "weather_trading.db",
        targets,
    )
    grouped_targets = group_backfill_targets_by_station(missing_targets)

    created_rows = 0
    updated_rows = 0
    resolved_targets = 0
    missing_archive_targets: list[dict] = []
    fetch_failures: list[dict] = []
    remote_archive_available = True
    remote_archive_error: str | None = None

    async with AsyncSessionLocal() as session:
        repo = WeatherRepository(session)
        for station_code, station_targets in grouped_targets.items():
            if not remote_archive_available:
                fetch_failures.append(
                    {
                        "station_code": station_code,
                        "start_date": station_targets[0].event_date.isoformat(),
                        "end_date": station_targets[-1].event_date.isoformat(),
                        "error": f"remote_archive_unavailable: {remote_archive_error}",
                    }
                )
                continue
            first_target = station_targets[0]
            try:
                history = await openmeteo.fetch_archive_daily_max_history(
                    latitude=first_target.latitude,
                    longitude=first_target.longitude,
                    start_date=station_targets[0].event_date,
                    end_date=station_targets[-1].event_date,
                )
            except Exception as exc:
                remote_archive_available = False
                remote_archive_error = f"{type(exc).__name__}: {exc}"
                fetch_failures.append(
                    {
                        "station_code": station_code,
                        "start_date": station_targets[0].event_date.isoformat(),
                        "end_date": station_targets[-1].event_date.isoformat(),
                        "error": remote_archive_error,
                    }
                )
                continue

            for target in station_targets:
                local_date_key = target.event_date.isoformat()
                temp_c = history.get(local_date_key)
                if temp_c is None:
                    missing_archive_targets.append(
                        {
                            "station_code": target.station_code,
                            "event_slug": target.event_slug,
                            "event_date": local_date_key,
                            "reason": "missing_archive_daily_max",
                        }
                    )
                    continue

                created = await repo.upsert_observation(
                    build_archive_daily_max_observation(target, temp_c)
                )
                resolved_targets += 1
                if created:
                    created_rows += 1
                else:
                    updated_rows += 1

    snapshot = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date.isoformat(),
        "start_as_of_date": None if start_as_of_date is None else start_as_of_date.isoformat(),
        "end_as_of_date": None if end_as_of_date is None else end_as_of_date.isoformat(),
        "targets_discovered": len(targets),
        "targets_already_resolved_locally": len(locally_covered_targets),
        "targets_missing_locally": len(missing_targets),
        "stations_covered": len(grouped_targets),
        "resolved_targets": resolved_targets,
        "created_rows": created_rows,
        "updated_rows": updated_rows,
        "missing_archive_targets": missing_archive_targets,
        "fetch_failures": fetch_failures,
        "archive_fetch_status": {
            "remote_archive_available": remote_archive_available,
            "remote_archive_error": remote_archive_error,
        },
        "discovery_skipped_events": discovery_skipped,
        "missing_archive_count": len(missing_archive_targets),
        "fetch_failure_count": len(fetch_failures),
        "discovery_skip_reason_counts": dict(
            sorted(Counter(str(item.get("reason") or "unknown") for item in discovery_skipped).items())
        ),
    }

    output_path = persist_snapshot(snapshot, reference_date)
    print(f"Backfill guardado en: {output_path}")
    print("")
    print("=== RESUMEN BACKFILL OBSERVACIONES ===")
    print(f"Targets descubiertos: {len(targets)}")
    print(f"Targets ya resueltos localmente: {len(locally_covered_targets)}")
    print(f"Targets pendientes de fetch remoto: {len(missing_targets)}")
    print(f"Estaciones cubiertas: {len(grouped_targets)}")
    print(f"Targets resueltos: {resolved_targets}")
    print(f"Filas creadas: {created_rows}")
    print(f"Filas actualizadas: {updated_rows}")
    print(f"Targets sin archivo: {len(missing_archive_targets)}")
    print(f"Fallos de fetch: {len(fetch_failures)}")


def persist_snapshot(snapshot: dict, reference_date: date) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{reference_date.isoformat()}_observation_backfill.json"
    output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    asyncio.run(main())
