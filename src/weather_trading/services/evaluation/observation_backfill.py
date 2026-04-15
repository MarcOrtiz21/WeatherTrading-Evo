from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from weather_trading.domain.models import ResolutionSource, WeatherObservation
from weather_trading.services.evaluation.blind_snapshot_resolution import (
    discover_blind_snapshot_paths,
    is_event_eligible_for_resolution,
)


@dataclass(frozen=True, slots=True)
class ObservationBackfillTarget:
    snapshot_as_of_date: str
    event_slug: str
    event_date: date
    station_code: str
    timezone: str
    latitude: float
    longitude: float


def discover_mature_snapshot_targets(
    snapshots_dir: Path,
    *,
    reference_date: date,
    mapper,
    start_as_of_date: date | None = None,
    end_as_of_date: date | None = None,
) -> tuple[list[ObservationBackfillTarget], list[dict]]:
    snapshot_paths = discover_blind_snapshot_paths(
        snapshots_dir,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
    )
    targets_by_key: dict[tuple[str, date], ObservationBackfillTarget] = {}
    skipped_events: list[dict] = []

    for snapshot_path in snapshot_paths:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot_as_of_date = str(snapshot.get("as_of_date"))
        for event in snapshot.get("evaluated_events", []):
            event_date_value = str(event.get("event_date"))
            if not is_event_eligible_for_resolution(event_date_value, reference_date):
                continue

            station_code = str(event.get("station_code"))
            station = mapper.get_station(station_code)
            if not station:
                skipped_events.append(
                    {
                        "snapshot_as_of_date": snapshot_as_of_date,
                        "event_slug": event.get("event_slug"),
                        "event_date": event_date_value,
                        "station_code": station_code,
                        "reason": "missing_station_catalog",
                    }
                )
                continue

            local_date = date.fromisoformat(event_date_value)
            key = (station_code, local_date)
            targets_by_key.setdefault(
                key,
                ObservationBackfillTarget(
                    snapshot_as_of_date=snapshot_as_of_date,
                    event_slug=str(event.get("event_slug")),
                    event_date=local_date,
                    station_code=station_code,
                    timezone=str(station.get("timezone") or "UTC"),
                    latitude=float(station["latitude"]),
                    longitude=float(station["longitude"]),
                ),
            )

    return sorted(
        targets_by_key.values(),
        key=lambda item: (item.event_date.isoformat(), item.station_code),
    ), skipped_events


def build_archive_daily_max_observation(
    target: ObservationBackfillTarget,
    temp_c: float,
) -> WeatherObservation:
    return WeatherObservation(
        station_code=target.station_code,
        provider=ResolutionSource.OPEN_METEO,
        observed_at_utc=archive_daily_max_timestamp_utc(target.event_date, target.timezone),
        temp_c=round(float(temp_c), 1),
        raw_reference=f"archive_daily_max_backfill:{target.event_date.isoformat()}",
    )


def archive_daily_max_timestamp_utc(local_date: date, timezone_name: str) -> datetime:
    station_tz = ZoneInfo(timezone_name or "UTC")
    end_local = datetime.combine(local_date, time.max, tzinfo=station_tz).replace(second=0, microsecond=0)
    return end_local.astimezone(timezone.utc).replace(tzinfo=None)


def local_date_utc_bounds(local_date: date, timezone_name: str) -> tuple[datetime, datetime]:
    station_tz = ZoneInfo(timezone_name or "UTC")
    start_local = datetime.combine(local_date, time.min, tzinfo=station_tz)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
    )


def partition_targets_by_local_observation_coverage(
    db_path: Path,
    targets: list[ObservationBackfillTarget],
) -> tuple[list[ObservationBackfillTarget], list[ObservationBackfillTarget]]:
    if not db_path.exists():
        return [], list(targets)

    covered_targets: list[ObservationBackfillTarget] = []
    missing_targets: list[ObservationBackfillTarget] = []

    with sqlite3.connect(db_path) as conn:
        for target in targets:
            start_utc, end_utc = local_date_utc_bounds(target.event_date, target.timezone)
            row = conn.execute(
                """
                SELECT 1
                FROM weather_observations
                WHERE station_code = ?
                  AND observed_at_utc >= ?
                  AND observed_at_utc < ?
                LIMIT 1
                """,
                (
                    target.station_code,
                    start_utc.isoformat(sep=" "),
                    end_utc.isoformat(sep=" "),
                ),
            ).fetchone()
            if row:
                covered_targets.append(target)
            else:
                missing_targets.append(target)

    return covered_targets, missing_targets


def group_backfill_targets_by_station(
    targets: list[ObservationBackfillTarget],
) -> dict[str, list[ObservationBackfillTarget]]:
    grouped: dict[str, list[ObservationBackfillTarget]] = {}
    for target in targets:
        grouped.setdefault(target.station_code, []).append(target)
    for station_targets in grouped.values():
        station_targets.sort(key=lambda item: item.event_date)
    return grouped
