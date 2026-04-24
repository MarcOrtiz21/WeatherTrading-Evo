import sqlite3
from datetime import date

import pytest

from scripts.run_blind_snapshot_resolution_audit import (
    ActualTemperatureResolver,
    summarize_coverage_debt,
    summarize_audit_quality,
    summarize_resolution_coverage,
)
from weather_trading.services.evaluation.blind_snapshot_resolution import BlindSnapshotEventEvaluation


def create_weather_observations_db(db_path):
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


@pytest.mark.asyncio
async def test_actual_temperature_resolver_prefers_local_weather_observations(tmp_path):
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
                ("TEST", "metar", "2026-04-05 23:30:00.000000", 17.2, None, None, None, "2026-04-05 23:31:00.000000"),
                ("TEST", "metar", "2026-04-06 01:30:00.000000", 18.6, None, None, None, "2026-04-06 01:31:00.000000"),
            ],
        )

    class UnexpectedRemoteClient:
        async def fetch_archive_daily_max(self, **kwargs):
            raise AssertionError("remote archive should not be used when local observations exist")

    resolver = ActualTemperatureResolver(db_path, UnexpectedRemoteClient())

    actual_temp_c, source, reason = await resolver.resolve(
        station_code="TEST",
        station_timezone="Europe/Madrid",
        latitude=0.0,
        longitude=0.0,
        local_date=date(2026, 4, 6),
    )

    assert actual_temp_c == pytest.approx(18.6)
    assert source == "local_weather_observations"
    assert reason is None


@pytest.mark.asyncio
async def test_actual_temperature_resolver_disables_remote_after_first_fetch_error(tmp_path):
    db_path = tmp_path / "weather_trading.db"
    create_weather_observations_db(db_path)

    class FailingRemoteClient:
        def __init__(self):
            self.calls = 0

        async def fetch_archive_daily_max(self, **kwargs):
            self.calls += 1
            raise RuntimeError("dns unavailable")

    remote = FailingRemoteClient()
    resolver = ActualTemperatureResolver(db_path, remote)

    first = await resolver.resolve(
        station_code="AAA",
        station_timezone="UTC",
        latitude=0.0,
        longitude=0.0,
        local_date=date(2026, 4, 6),
    )
    second = await resolver.resolve(
        station_code="BBB",
        station_timezone="UTC",
        latitude=1.0,
        longitude=1.0,
        local_date=date(2026, 4, 6),
    )

    assert first == (None, None, "archive_fetch_error")
    assert second == (None, None, "archive_fetch_unavailable")
    assert remote.calls == 1
    assert resolver.remote_archive_error == "RuntimeError: dns unavailable"


@pytest.mark.asyncio
async def test_actual_temperature_resolver_local_only_skips_remote_fetch(tmp_path):
    db_path = tmp_path / "weather_trading.db"
    create_weather_observations_db(db_path)

    class UnexpectedRemoteClient:
        async def fetch_archive_daily_max(self, **kwargs):
            raise AssertionError("remote archive should not be used in local-only mode")

    resolver = ActualTemperatureResolver(
        db_path,
        UnexpectedRemoteClient(),
        allow_remote_archive=False,
    )

    result = await resolver.resolve(
        station_code="AAA",
        station_timezone="UTC",
        latitude=0.0,
        longitude=0.0,
        local_date=date(2026, 4, 6),
    )

    assert result == (None, None, "archive_fetch_skipped_local_only")


@pytest.mark.asyncio
async def test_actual_temperature_resolver_labels_local_archive_backfill_source(tmp_path):
    db_path = tmp_path / "weather_trading.db"
    create_weather_observations_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
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
            (
                "LEMD",
                "open_meteo",
                "2026-04-06 21:59:00.000000",
                24.6,
                None,
                None,
                "archive_daily_max_backfill:2026-04-06",
                "2026-04-08 09:00:00.000000",
            ),
        )

    class UnexpectedRemoteClient:
        async def fetch_archive_daily_max(self, **kwargs):
            raise AssertionError("remote archive should not be used when local backfill exists")

    resolver = ActualTemperatureResolver(db_path, UnexpectedRemoteClient())

    actual_temp_c, source, reason = await resolver.resolve(
        station_code="LEMD",
        station_timezone="Europe/Madrid",
        latitude=40.47,
        longitude=-3.56,
        local_date=date(2026, 4, 6),
    )

    assert actual_temp_c == pytest.approx(24.6)
    assert source == "local_open_meteo_archive_backfill"
    assert reason is None


def test_summarize_resolution_coverage_counts_mature_and_pending_events(tmp_path):
    snapshot_paths = [
        tmp_path / "2026-04-05_polymarket_blind_live_validation.json",
        tmp_path / "2026-04-06_polymarket_blind_live_validation.json",
    ]
    evaluations = [
        BlindSnapshotEventEvaluation(
            snapshot_as_of_date="2026-04-05",
            event_slug="a",
            event_title="A",
            event_date="2026-04-06",
            station_code="A",
            forecast_strategy="baseline_short_horizon",
            horizon_days=1,
            event_operable=True,
            event_evidence_score=0.80,
            event_evidence_tier="A",
            actual_temp_c=18.1,
            actual_temperature_source="local_weather_observations",
            winner_market_id="1",
            winner_question="18",
            model_mode_question="18",
            market_mode_question="18",
            top_edge_question="18",
            top_edge_market_id="1",
            top_edge_tradeable=True,
            top_edge_quality_tier="A",
            model_mode_hit=True,
            market_mode_hit=True,
            top_edge_positive=True,
            top_edge_hit=True,
            winner_fair_probability=0.60,
            winner_market_probability=0.55,
            model_log_loss=0.5,
            market_log_loss=0.6,
            model_brier=0.4,
            market_brier=0.5,
            top_edge_net=0.10,
            paper_trade_taken=True,
            paper_trade_stake=0.22,
            paper_trade_pnl=0.78,
            paper_trade_execution_price=0.20,
            paper_trade_costs=0.02,
        )
    ]
    pending_events = [{"reason": "event_not_finished_yet"}]
    skipped_events = [
        {"reason": "archive_fetch_unavailable"},
        {"reason": "missing_station_catalog"},
    ]

    coverage = summarize_resolution_coverage(
        snapshot_paths=snapshot_paths,
        evaluations=evaluations,
        pending_events=pending_events,
        skipped_events=skipped_events,
    )

    assert coverage["snapshots_reviewed"] == 2
    assert coverage["reviewed_events"] == 4
    assert coverage["mature_events"] == 3
    assert coverage["evaluated_events"] == 1
    assert coverage["mature_resolution_coverage"] == pytest.approx(1 / 3)
    assert coverage["skip_reason_counts"] == {
        "archive_fetch_unavailable": 1,
        "missing_station_catalog": 1,
    }


def test_summarize_audit_quality_marks_partial_and_non_conclusive(monkeypatch):
    monkeypatch.setattr(
        "scripts.run_blind_snapshot_resolution_audit.ConfigLoader.get",
        lambda key, default=None: {
            "audit_quality.min_mature_coverage_for_complete": 0.95,
            "audit_quality.min_mature_coverage_for_actionable": 0.85,
        }.get(key, default),
    )

    partial = summarize_audit_quality(
        {
            "mature_events": 100,
            "evaluated_events": 90,
            "mature_resolution_coverage": 0.90,
        }
    )
    non_conclusive = summarize_audit_quality(
        {
            "mature_events": 100,
            "evaluated_events": 80,
            "mature_resolution_coverage": 0.80,
        }
    )

    assert partial["classification"] == "partial"
    assert partial["is_actionable"] is True
    assert partial["is_comparable_to_full_coverage"] is False

    assert non_conclusive["classification"] == "non_conclusive"
    assert non_conclusive["is_actionable"] is False


def test_summarize_coverage_debt_groups_skipped_events():
    debt = summarize_coverage_debt(
        [
            {"reason": "archive_fetch_unavailable", "station_code": "LEMD", "event_date": "2026-04-15"},
            {"reason": "archive_fetch_unavailable", "station_code": "LEMD", "event_date": "2026-04-16"},
            {"reason": "missing_station_catalog", "station_code": "XXXX", "event_date": "2026-04-16"},
        ]
    )

    assert debt["unresolved_mature_events"] == 3
    assert debt["by_reason"] == {
        "archive_fetch_unavailable": 2,
        "missing_station_catalog": 1,
    }
    assert debt["by_station"]["LEMD"] == 2
    assert debt["by_event_date"]["2026-04-16"] == 2
