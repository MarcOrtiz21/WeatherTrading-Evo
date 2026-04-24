import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import utc_now


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evalua si el sistema esta en condiciones de operar, seguir en paper o quedar bloqueado."
    )
    parser.add_argument("--reference-date", default=date.today().isoformat(), help="Fecha de corte YYYY-MM-DD.")
    return parser.parse_args()


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def inspect_weather_database(reference_date: str) -> dict:
    db_path = ROOT / "weather_trading.db"
    health = {
        "path": db_path.relative_to(ROOT).as_posix(),
        "exists": db_path.exists(),
        "status": "missing",
        "observation_count": 0,
        "latest_observed_at_utc": None,
        "latest_observed_date": None,
        "observation_lag_days": None,
        "error": None,
    }
    if not db_path.exists():
        return health

    try:
        with sqlite3.connect(db_path) as conn:
            count, latest_observed_at = conn.execute(
                "select count(*), max(observed_at_utc) from weather_observations"
            ).fetchone()
    except sqlite3.Error as exc:
        health["status"] = "error"
        health["error"] = str(exc)
        return health

    observation_count = int(count or 0)
    health["observation_count"] = observation_count
    if observation_count == 0:
        health["status"] = "empty"
        return health

    health["latest_observed_at_utc"] = str(latest_observed_at)
    latest_observed_date = parse_sqlite_datetime(str(latest_observed_at)).date()
    health["latest_observed_date"] = latest_observed_date.isoformat()
    health["observation_lag_days"] = (date.fromisoformat(reference_date) - latest_observed_date).days
    health["status"] = "ok"
    return health


def parse_sqlite_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def build_readiness(reference_date: str) -> dict:
    snapshots_dir = ROOT / "logs" / "snapshots"
    pipeline_path = snapshots_dir / f"{reference_date}_daily_pipeline_report.json"
    live_path = snapshots_dir / f"{reference_date}_polymarket_blind_live_validation.json"
    audit_path = snapshots_dir / f"{reference_date}_blind_snapshot_resolution_audit.json"

    pipeline = load_json(pipeline_path)
    audit = load_json(audit_path)
    live_snapshot_present = live_path.exists()

    blockers: list[str] = []
    warnings: list[str] = []
    database_health = inspect_weather_database(reference_date)

    if database_health["status"] in {"missing", "error"} and ConfigLoader.get(
        "operational_readiness.block_if_weather_db_unavailable",
        True,
    ):
        blockers.append("weather_db_unavailable")
    elif database_health["status"] == "empty" and ConfigLoader.get(
        "operational_readiness.block_if_weather_db_empty",
        True,
    ):
        blockers.append("weather_observations_empty")

    max_observation_lag_days = ConfigLoader.get("operational_readiness.warn_if_observation_lag_days_exceeds", 3)
    observation_lag_days = database_health.get("observation_lag_days")
    if observation_lag_days is not None and int(observation_lag_days) > int(max_observation_lag_days):
        warnings.append("weather_observations_stale")

    if ConfigLoader.get("operational_readiness.block_if_live_snapshot_missing", True) and not live_snapshot_present:
        blockers.append("live_snapshot_missing")

    audit_quality = {}
    if audit is None:
        blockers.append("audit_snapshot_missing")
    else:
        audit_quality = audit.get("audit_quality") or {}
        classification = str(audit_quality.get("classification") or "unknown")
        if classification == "non_conclusive" and ConfigLoader.get(
            "operational_readiness.block_if_audit_non_conclusive",
            True,
        ):
            blockers.append("audit_non_conclusive")
        elif classification == "partial" and ConfigLoader.get(
            "operational_readiness.warn_if_audit_partial",
            True,
        ):
            warnings.append("audit_partial")

    pipeline_status = None if pipeline is None else pipeline.get("overall_status")
    if pipeline is None:
        blockers.append("daily_pipeline_report_missing")
    elif pipeline_status == "degraded":
        warnings.append("pipeline_degraded")
    elif pipeline_status == "warning":
        warnings.append("pipeline_warning")

    if blockers:
        status = "blocked"
        recommended_mode = "do_not_trade"
    elif warnings:
        status = "paper_only"
        recommended_mode = "paper_only"
    else:
        status = "ready"
        recommended_mode = "eligible_for_supervised_execution"

    coverage_debt = {} if audit is None else (audit.get("coverage_debt") or {})
    payload = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date,
        "status": status,
        "recommended_mode": recommended_mode,
        "pipeline_report_path": pipeline_path.relative_to(ROOT).as_posix() if pipeline_path.exists() else None,
        "audit_snapshot_path": audit_path.relative_to(ROOT).as_posix() if audit_path.exists() else None,
        "live_snapshot_path": live_path.relative_to(ROOT).as_posix() if live_snapshot_present else None,
        "pipeline_status": pipeline_status,
        "audit_quality": audit_quality,
        "coverage_debt": coverage_debt,
        "database_health": database_health,
        "blockers": blockers,
        "warnings": warnings,
    }
    return payload


def persist_snapshot(reference_date: str, payload: dict) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date}_operational_readiness.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def main() -> None:
    args = parse_args()
    payload = build_readiness(str(args.reference_date))
    path = persist_snapshot(str(args.reference_date), payload)

    print(f"Readiness guardado en: {path}")
    print("")
    print("=== OPERATIONAL READINESS ===")
    print(
        f"reference_date={payload['reference_date']} | "
        f"status={payload['status']} | "
        f"recommended_mode={payload['recommended_mode']}"
    )
    if payload["blockers"]:
        print("Blockers:")
        for blocker in payload["blockers"]:
            print(f"- {blocker}")
    if payload["warnings"]:
        print("Warnings:")
        for warning in payload["warnings"]:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
