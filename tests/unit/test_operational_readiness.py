import json
import sqlite3
from pathlib import Path

from scripts import run_operational_readiness


def create_weather_db(root: Path, *, observed_at: str = "2026-04-23 12:00:00.000000") -> None:
    with sqlite3.connect(root / "weather_trading.db") as conn:
        conn.execute(
            """
            create table weather_observations (
                id integer primary key autoincrement,
                station_code varchar not null,
                provider varchar not null,
                observed_at_utc datetime not null,
                temp_c float not null,
                dewpoint_c float,
                pressure_hpa float,
                raw_reference varchar,
                created_at datetime not null
            )
            """
        )
        conn.execute(
            """
            insert into weather_observations (
                station_code,
                provider,
                observed_at_utc,
                temp_c,
                created_at
            )
            values ('KNYC', 'fixture', ?, 20.0, '2026-04-23 12:00:00.000000')
            """,
            (observed_at,),
        )


def test_build_readiness_blocks_when_live_snapshot_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_operational_readiness, "ROOT", tmp_path)
    create_weather_db(tmp_path)
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    (snapshots / "2026-04-23_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "degraded"}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_blind_snapshot_resolution_audit.json").write_text(
        json.dumps({"audit_quality": {"classification": "non_conclusive"}, "coverage_debt": {"unresolved_mature_events": 40}}),
        encoding="utf-8",
    )

    payload = run_operational_readiness.build_readiness("2026-04-23")

    assert payload["status"] == "blocked"
    assert "live_snapshot_missing" in payload["blockers"]
    assert "audit_non_conclusive" in payload["blockers"]


def test_build_readiness_ready_when_artifacts_are_clean(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_operational_readiness, "ROOT", tmp_path)
    create_weather_db(tmp_path)
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    (snapshots / "2026-04-23_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_blind_snapshot_resolution_audit.json").write_text(
        json.dumps({"audit_quality": {"classification": "complete"}, "coverage_debt": {"unresolved_mature_events": 0}}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_polymarket_blind_live_validation.json").write_text("{}", encoding="utf-8")

    payload = run_operational_readiness.build_readiness("2026-04-23")

    assert payload["status"] == "ready"
    assert payload["recommended_mode"] == "eligible_for_supervised_execution"


def test_build_readiness_blocks_when_weather_db_is_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_operational_readiness, "ROOT", tmp_path)
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    (snapshots / "2026-04-23_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_blind_snapshot_resolution_audit.json").write_text(
        json.dumps({"audit_quality": {"classification": "complete"}, "coverage_debt": {"unresolved_mature_events": 0}}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_polymarket_blind_live_validation.json").write_text("{}", encoding="utf-8")

    payload = run_operational_readiness.build_readiness("2026-04-23")

    assert payload["status"] == "blocked"
    assert "weather_db_unavailable" in payload["blockers"]
    assert payload["database_health"]["status"] == "missing"


def test_build_readiness_warns_when_observations_are_stale(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_operational_readiness, "ROOT", tmp_path)
    create_weather_db(tmp_path, observed_at="2026-04-15 12:00:00.000000")
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    (snapshots / "2026-04-23_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_blind_snapshot_resolution_audit.json").write_text(
        json.dumps({"audit_quality": {"classification": "complete"}, "coverage_debt": {"unresolved_mature_events": 0}}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-23_polymarket_blind_live_validation.json").write_text("{}", encoding="utf-8")

    payload = run_operational_readiness.build_readiness("2026-04-23")

    assert payload["status"] == "paper_only"
    assert "weather_observations_stale" in payload["warnings"]
    assert payload["database_health"]["observation_lag_days"] == 8
