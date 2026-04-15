import sqlite3
from datetime import date, datetime
from pathlib import Path

from weather_trading.services.forecast_engine.intraday_context import (
    build_intraday_context_from_hourly_forecast,
    lookup_local_intraday_max_so_far,
    resolve_intraday_max_so_far_context,
)


def create_weather_observations_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE weather_observations (
                id INTEGER PRIMARY KEY,
                station_code VARCHAR NOT NULL,
                provider VARCHAR NOT NULL,
                observed_at_utc DATETIME NOT NULL,
                temp_c FLOAT NOT NULL,
                dewpoint_c FLOAT,
                pressure_hpa FLOAT,
                raw_reference VARCHAR,
                created_at DATETIME NOT NULL
            )
            """
        )


def test_build_intraday_context_from_hourly_forecast_uses_elapsed_local_hours():
    context = build_intraday_context_from_hourly_forecast(
        local_date=date(2026, 4, 7),
        station_timezone="Asia/Shanghai",
        hourly_temperatures_c=[
            12.0, 12.3, 12.5, 12.8, 13.0, 13.2, 13.1, 13.0,
            13.4, 14.1, 14.7, 14.8, 14.6, 14.3, 14.0, 13.7,
            13.3, 13.0, 12.8, 12.6, 12.5, 12.4, 12.3, 12.2,
        ],
        hourly_times=[f"2026-04-07T{hour:02d}:00" for hour in range(24)],
        as_of_utc=datetime(2026, 4, 7, 10, 30, 0),
    )

    assert context is not None
    assert context.source == "hourly_forecast_proxy"
    assert context.hours_elapsed == 19
    assert context.remaining_hours == 5
    assert context.max_so_far_c == 14.8


def test_lookup_local_intraday_max_so_far_prefers_local_observations(tmp_path: Path):
    db_path = tmp_path / "weather_trading.db"
    create_weather_observations_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO weather_observations (
                station_code,
                provider,
                observed_at_utc,
                temp_c,
                dewpoint_c,
                pressure_hpa,
                raw_reference,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("ZSPD", "metar", "2026-04-07 02:00:00.000000", 13.6, None, None, None, "2026-04-07 02:01:00.000000"),
                ("ZSPD", "metar", "2026-04-07 06:00:00.000000", 16.1, None, None, None, "2026-04-07 06:01:00.000000"),
            ],
        )

    context = lookup_local_intraday_max_so_far(
        db_path=db_path,
        station_code="ZSPD",
        station_timezone="Asia/Shanghai",
        local_date=date(2026, 4, 7),
        as_of_utc=datetime(2026, 4, 7, 10, 30, 0),
    )

    assert context is not None
    assert context.source == "local_weather_observations"
    assert context.max_so_far_c == 16.1
    assert context.hours_elapsed == 19


def test_resolve_intraday_context_falls_back_to_forecast_when_db_has_no_data(tmp_path: Path):
    db_path = tmp_path / "weather_trading.db"
    create_weather_observations_db(db_path)

    context = resolve_intraday_max_so_far_context(
        db_path=db_path,
        station_code="ZSPD",
        station_timezone="Asia/Shanghai",
        local_date=date(2026, 4, 7),
        hourly_temperatures_c=[12.0, 13.0, 14.0, 15.0],
        hourly_times=[f"2026-04-07T{hour:02d}:00" for hour in range(4)],
        as_of_utc=datetime(2026, 4, 6, 16, 30, 0),
    )

    assert context is not None
    assert context.source == "hourly_forecast_proxy"
    assert context.max_so_far_c == 12.0
