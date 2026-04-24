import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.utils import utc_now


@dataclass(frozen=True)
class PipelineStep:
    name: str
    command: list[str]
    expected_artifacts: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orquesta el pipeline diario live -> backfill -> audit -> cohort diagnostics."
    )
    parser.add_argument("--reference-date", default=date.today().isoformat(), help="Fecha de corte YYYY-MM-DD.")
    parser.add_argument("--max-events", type=int, default=20, help="Numero maximo de eventos live a evaluar.")
    parser.add_argument("--min-horizon-days", type=int, default=0, help="Horizonte minimo para validacion live.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Horizonte maximo para validacion live.")
    parser.add_argument(
        "--skip-cohort-diagnostics",
        action="store_true",
        help="No ejecutar el diagnostico de cohorte tras la auditoria.",
    )
    parser.add_argument(
        "--with-health-check",
        action="store_true",
        help="Ejecuta pytest al final del pipeline.",
    )
    return parser.parse_args()


def build_steps(args: argparse.Namespace) -> list[PipelineStep]:
    reference_date = str(args.reference_date)
    steps = [
        PipelineStep(
            name="blind_live_validation",
            command=[
                sys.executable,
                str(ROOT / "scripts" / "run_blind_live_validation.py"),
                "--as-of-date",
                reference_date,
                "--max-events",
                str(args.max_events),
                "--min-horizon-days",
                str(args.min_horizon_days),
                "--max-horizon-days",
                str(args.max_horizon_days),
            ],
            expected_artifacts=[f"logs/snapshots/{reference_date}_polymarket_blind_live_validation.json"],
        ),
        PipelineStep(
            name="observation_backfill",
            command=[
                sys.executable,
                str(ROOT / "scripts" / "run_observation_backfill.py"),
                "--reference-date",
                reference_date,
            ],
            expected_artifacts=[f"logs/snapshots/{reference_date}_observation_backfill.json"],
        ),
        PipelineStep(
            name="blind_snapshot_resolution_audit",
            command=[
                sys.executable,
                str(ROOT / "scripts" / "run_blind_snapshot_resolution_audit.py"),
                "--reference-date",
                reference_date,
            ],
            expected_artifacts=[
                f"logs/snapshots/{reference_date}_blind_snapshot_resolution_audit.json",
                f"logs/snapshots/{reference_date}_watchlist_strategy_simulation.json",
            ],
        ),
        PipelineStep(
            name="operational_readiness",
            command=[
                sys.executable,
                str(ROOT / "scripts" / "run_operational_readiness.py"),
                "--reference-date",
                reference_date,
            ],
            expected_artifacts=[
                f"logs/snapshots/{reference_date}_operational_readiness.json",
            ],
        ),
    ]
    if not args.skip_cohort_diagnostics:
        steps.append(
            PipelineStep(
                name="cohort_overlay_diagnostics",
                command=[
                    sys.executable,
                    str(ROOT / "scripts" / "run_cohort_overlay_diagnostics.py"),
                    "--reference-date",
                    reference_date,
                ],
                expected_artifacts=[],
            )
        )
    if args.with_health_check:
        steps.append(
            PipelineStep(
                name="health_check",
                command=[sys.executable, "-m", "pytest", "-q"],
                expected_artifacts=[],
            )
        )
    return steps


def prepare_step_for_context(step: PipelineStep, *, reference_date: str) -> PipelineStep:
    if step.name != "blind_snapshot_resolution_audit":
        return step
    if "--local-only" in step.command:
        return step
    if not should_run_audit_local_only(reference_date):
        return step
    return replace(step, command=[*step.command, "--local-only"])


def should_run_audit_local_only(reference_date: str) -> bool:
    backfill_path = ROOT / "logs" / "snapshots" / f"{reference_date}_observation_backfill.json"
    if not backfill_path.exists():
        return False
    try:
        payload = json.loads(backfill_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False

    archive_status = payload.get("archive_fetch_status") or {}
    remote_available = bool(archive_status.get("remote_archive_available", True))
    missing_locally = int(payload.get("targets_missing_locally") or 0)
    fetch_failure_count = int(payload.get("fetch_failure_count") or 0)
    return missing_locally > 0 and (not remote_available or fetch_failure_count > 0)


def run_step(step: PipelineStep) -> dict:
    started_at = utc_now().isoformat()
    completed = subprocess.run(
        step.command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    finished_at = utc_now().isoformat()
    artifacts = {
        artifact: (ROOT / artifact).exists()
        for artifact in step.expected_artifacts
    }
    status = "ok" if completed.returncode == 0 else "failed"
    notes: list[str] = []
    if completed.returncode == 0 and artifacts and not all(artifacts.values()):
        status = "degraded"
        notes.append("missing_expected_artifacts")
    audit_quality = None
    if completed.returncode == 0 and step.name == "blind_snapshot_resolution_audit":
        audit_quality = extract_audit_quality(artifacts)
        if audit_quality:
            classification = audit_quality.get("classification")
            if classification == "partial" and status == "ok":
                status = "warning"
                notes.append("audit_partial")
            elif classification == "non_conclusive":
                status = "degraded"
                notes.append("audit_non_conclusive")
    stdout_tail = tail_text(completed.stdout)
    stderr_tail = tail_text(completed.stderr)
    return {
        "name": step.name,
        "command": step.command,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "exit_code": completed.returncode,
        "status": status,
        "expected_artifacts": artifacts,
        "notes": notes,
        "audit_quality": audit_quality,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


def tail_text(value: str, *, max_lines: int = 20) -> list[str]:
    lines = [line.rstrip() for line in value.splitlines() if line.strip()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def compute_overall_status(step_results: list[dict]) -> str:
    statuses = {result["status"] for result in step_results}
    if "failed" in statuses or "degraded" in statuses:
        return "degraded"
    if "warning" in statuses:
        return "warning"
    return "ok"


def extract_audit_quality(expected_artifacts: dict[str, bool]) -> dict | None:
    for artifact, exists in expected_artifacts.items():
        if not exists or not artifact.endswith("_blind_snapshot_resolution_audit.json"):
            continue
        artifact_path = ROOT / artifact
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        audit_quality = payload.get("audit_quality")
        return audit_quality if isinstance(audit_quality, dict) else None
    return None


def persist_report(reference_date: str, report: dict) -> Path:
    output_dir = ROOT / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date}_daily_pipeline_report.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def build_report(reference_date: str, step_results: list[dict]) -> dict:
    return {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date,
        "overall_status": compute_overall_status(step_results),
        "steps": step_results,
    }


def main() -> None:
    args = parse_args()
    reference_date = str(args.reference_date)
    step_results = []
    deferred_readiness_steps = []
    for step in build_steps(args):
        if step.name == "operational_readiness":
            deferred_readiness_steps.append(step)
            continue
        prepared_step = prepare_step_for_context(step, reference_date=reference_date)
        step_results.append(run_step(prepared_step))

    if deferred_readiness_steps:
        persist_report(reference_date, build_report(reference_date, step_results))
        for step in deferred_readiness_steps:
            prepared_step = prepare_step_for_context(step, reference_date=reference_date)
            step_results.append(run_step(prepared_step))

    report = build_report(reference_date, step_results)
    output_path = persist_report(reference_date, report)

    print(f"Pipeline diario guardado en: {output_path.relative_to(ROOT)}")
    print("")
    print("=== DAILY PIPELINE ===")
    print(f"reference_date={reference_date} | overall_status={report['overall_status']}")
    for result in step_results:
        artifact_summary = ", ".join(
            f"{Path(path).name}={'ok' if exists else 'missing'}"
            for path, exists in result["expected_artifacts"].items()
        )
        artifact_line = f" | artifacts: {artifact_summary}" if artifact_summary else ""
        print(
            f"- {result['name']}: status={result['status']} exit_code={result['exit_code']}{artifact_line}"
        )


if __name__ == "__main__":
    main()
