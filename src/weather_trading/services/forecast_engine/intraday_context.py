from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from weather_trading.infrastructure.utils import utc_now


@dataclass(slots=True)
class IntradayMaxSoFarContext:
    max_so_far_c: float
    hours_elapsed: int
    last_local_hour: int
    remaining_hours: int
    source: str


def resolve_intraday_max_so_far_context(
    *,
    db_path: Path,
    station_code: str,
    station_timezone: str,
    local_date: date,
    hourly_temperatures_c: list[float] | None,
    hourly_times: list[str] | None = None,
    as_of_utc: datetime | None = None,
) -> IntradayMaxSoFarContext | None:
    local_context = lookup_local_intraday_max_so_far(
        db_path=db_path,
        station_code=station_code,
        station_timezone=station_timezone,
        local_date=local_date,
        as_of_utc=as_of_utc,
    )
    if local_context is not None:
        return local_context

    return build_intraday_context_from_hourly_forecast(
        local_date=local_date,
        station_timezone=station_timezone,
        hourly_temperatures_c=hourly_temperatures_c or [],
        hourly_times=hourly_times or [],
        as_of_utc=as_of_utc,
    )


def lookup_local_intraday_max_so_far(
    *,
    db_path: Path,
    station_code: str,
    station_timezone: str,
    local_date: date,
    as_of_utc: datetime | None = None,
) -> IntradayMaxSoFarContext | None:
    if not db_path.exists():
        return None

    as_of_utc = utc_now() if as_of_utc is None else as_of_utc
    start_utc, end_utc = intraday_local_date_utc_bounds(
        local_date=local_date,
        timezone_name=station_timezone,
        as_of_utc=as_of_utc,
    )
    if end_utc <= start_utc:
        return None

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT MAX(temp_c), MAX(observed_at_utc)
            FROM weather_observations
            WHERE station_code = ?
              AND observed_at_utc >= ?
              AND observed_at_utc < ?
            """,
            (
                station_code,
                start_utc.isoformat(sep=" "),
                end_utc.isoformat(sep=" "),
            ),
        ).fetchone()

    if row is None or row[0] is None:
        return None

    station_tz = ZoneInfo(station_timezone or "UTC")
    local_now = as_of_utc.replace(tzinfo=timezone.utc).astimezone(station_tz)
    hours_elapsed = min(max(local_now.hour + 1, 1), 24)
    return IntradayMaxSoFarContext(
        max_so_far_c=round(float(row[0]), 1),
        hours_elapsed=hours_elapsed,
        last_local_hour=min(local_now.hour, 23),
        remaining_hours=max(0, 24 - hours_elapsed),
        source="local_weather_observations",
    )


def build_intraday_context_from_hourly_forecast(
    *,
    local_date: date,
    station_timezone: str,
    hourly_temperatures_c: list[float],
    hourly_times: list[str] | None = None,
    as_of_utc: datetime | None = None,
) -> IntradayMaxSoFarContext | None:
    cleaned_hourly_temps = [
        float(value)
        for value in hourly_temperatures_c
        if value is not None
    ]
    if not cleaned_hourly_temps:
        return None

    local_now = _resolve_local_now(
        station_timezone=station_timezone,
        as_of_utc=as_of_utc,
    )
    if local_now.date() < local_date:
        return None

    if local_now.date() > local_date:
        hours_elapsed = len(cleaned_hourly_temps)
    else:
        hours_elapsed = _infer_hours_elapsed(
            local_date=local_date,
            local_now=local_now,
            hourly_times=hourly_times or [],
            fallback_hours=len(cleaned_hourly_temps),
        )

    hours_elapsed = max(1, min(hours_elapsed, len(cleaned_hourly_temps)))
    realized_profile = cleaned_hourly_temps[:hours_elapsed]

    return IntradayMaxSoFarContext(
        max_so_far_c=max(realized_profile),
        hours_elapsed=hours_elapsed,
        last_local_hour=min(local_now.hour, hours_elapsed - 1),
        remaining_hours=max(0, len(cleaned_hourly_temps) - hours_elapsed),
        source="hourly_forecast_proxy",
    )


def intraday_local_date_utc_bounds(
    local_date: date,
    timezone_name: str,
    *,
    as_of_utc: datetime,
) -> tuple[datetime, datetime]:
    station_tz = ZoneInfo(timezone_name or "UTC")
    start_local = datetime.combine(local_date, time.min, tzinfo=station_tz)
    current_utc = as_of_utc.replace(tzinfo=timezone.utc)
    end_local = min(start_local + timedelta(days=1), current_utc.astimezone(station_tz) + timedelta(hours=1))
    return (
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
    )


def _resolve_local_now(*, station_timezone: str, as_of_utc: datetime | None) -> datetime:
    utc_value = utc_now() if as_of_utc is None else as_of_utc
    return utc_value.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(station_timezone or "UTC"))


def _infer_hours_elapsed(
    *,
    local_date: date,
    local_now: datetime,
    hourly_times: list[str],
    fallback_hours: int,
) -> int:
    if hourly_times:
        eligible = [
            hourly_time
            for hourly_time in hourly_times
            if hourly_time and hourly_time.startswith(local_date.isoformat())
        ]
        if eligible:
            current_prefix = local_now.strftime("%Y-%m-%dT%H:")
            elapsed = sum(hourly_time[:14] <= current_prefix for hourly_time in eligible)
            if elapsed > 0:
                return elapsed

    return min(local_now.hour + 1, fallback_hours)
