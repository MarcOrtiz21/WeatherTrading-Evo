import json
import sqlite3
from datetime import date
from pathlib import Path

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.evaluation.observation_backfill import (
    ObservationBackfillTarget,
    archive_daily_max_timestamp_utc,
    build_archive_daily_max_observation,
    discover_mature_snapshot_targets,
    group_backfill_targets_by_station,
    partition_targets_by_local_observation_coverage,
)
from weather_trading.services.station_mapper.service import StationMapperService


def test_discover_mature_snapshot_targets_dedupes_station_dates_and_skips_missing_catalog(tmp_path):
    ConfigLoader._config = {
        "stations": {
            "LEMD": {
                "city": "Madrid",
                "country": "Spain",
                "timezone": "Europe/Madrid",
                "latitude": 40.47,
                "longitude": -3.56,
            }
        }
    }
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    snapshot_path = snapshots_dir / "2026-04-05_polymarket_blind_live_validation.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "as_of_date": "2026-04-05",
                "evaluated_events": [
                    {
                        "event_slug": "madrid-a",
                        "event_date": "2026-04-06",
                        "station_code": "LEMD",
                    },
                    {
                        "event_slug": "madrid-b",
                        "event_date": "2026-04-06",
                        "station_code": "LEMD",
                    },
                    {
                        "event_slug": "unknown-station",
                        "event_date": "2026-04-06",
                        "station_code": "XXXX",
                    },
                    {
                        "event_slug": "not-mature-yet",
                        "event_date": "2026-04-07",
                        "station_code": "LEMD",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    targets, skipped = discover_mature_snapshot_targets(
        snapshots_dir,
        reference_date=date(2026, 4, 7),
        mapper=StationMapperService(),
    )

    assert len(targets) == 1
    assert targets[0].station_code == "LEMD"
    assert targets[0].event_date == date(2026, 4, 6)
    assert skipped == [
        {
            "snapshot_as_of_date": "2026-04-05",
            "event_slug": "unknown-station",
            "event_date": "2026-04-06",
            "station_code": "XXXX",
            "reason": "missing_station_catalog",
        }
    ]


def test_build_archive_daily_max_observation_uses_deterministic_local_end_of_day_timestamp():
    target = ObservationBackfillTarget(
        snapshot_as_of_date="2026-04-05",
        event_slug="madrid",
        event_date=date(2026, 4, 6),
        station_code="LEMD",
        timezone="Europe/Madrid",
        latitude=40.47,
        longitude=-3.56,
    )

    observation = build_archive_daily_max_observation(target, 24.37)

    assert observation.station_code == "LEMD"
    assert observation.temp_c == 24.4
    assert observation.raw_reference == "archive_daily_max_backfill:2026-04-06"
    assert observation.observed_at_utc == archive_daily_max_timestamp_utc(date(2026, 4, 6), "Europe/Madrid")


def test_group_backfill_targets_by_station_groups_targets_and_preserves_date_order():
    grouped = group_backfill_targets_by_station(
        [
            ObservationBackfillTarget(
                snapshot_as_of_date="2026-04-05",
                event_slug="b",
                event_date=date(2026, 4, 7),
                station_code="LEMD",
                timezone="Europe/Madrid",
                latitude=40.47,
                longitude=-3.56,
            ),
            ObservationBackfillTarget(
                snapshot_as_of_date="2026-04-05",
                event_slug="a",
                event_date=date(2026, 4, 6),
                station_code="LEMD",
                timezone="Europe/Madrid",
                latitude=40.47,
                longitude=-3.56,
            ),
        ]
    )

    assert list(grouped.keys()) == ["LEMD"]
    assert [target.event_date.isoformat() for target in grouped["LEMD"]] == [
        "2026-04-06",
        "2026-04-07",
    ]


def test_partition_targets_by_local_observation_coverage_reuses_existing_rows(tmp_path):
    db_path = tmp_path / "weather_trading.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE weather_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code TEXT,
            provider TEXT,
            observed_at_utc TEXT,
            temp_c REAL,
            raw_reference TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO weather_observations (station_code, provider, observed_at_utc, temp_c, raw_reference)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "LEMD",
            "open_meteo",
            archive_daily_max_timestamp_utc(date(2026, 4, 6), "Europe/Madrid").isoformat(sep=" "),
            24.4,
            "archive_daily_max_backfill:2026-04-06",
        ),
    )
    conn.commit()
    conn.close()

    covered, missing = partition_targets_by_local_observation_coverage(
        db_path,
        [
            ObservationBackfillTarget(
                snapshot_as_of_date="2026-04-05",
                event_slug="madrid-covered",
                event_date=date(2026, 4, 6),
                station_code="LEMD",
                timezone="Europe/Madrid",
                latitude=40.47,
                longitude=-3.56,
            ),
            ObservationBackfillTarget(
                snapshot_as_of_date="2026-04-05",
                event_slug="madrid-missing",
                event_date=date(2026, 4, 7),
                station_code="LEMD",
                timezone="Europe/Madrid",
                latitude=40.47,
                longitude=-3.56,
            ),
        ],
    )

    assert [target.event_slug for target in covered] == ["madrid-covered"]
    assert [target.event_slug for target in missing] == ["madrid-missing"]
