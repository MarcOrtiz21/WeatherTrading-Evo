import json
from pathlib import Path
from types import SimpleNamespace

from scripts import run_daily_pipeline


def test_build_steps_includes_core_pipeline() -> None:
    args = SimpleNamespace(
        reference_date="2026-04-17",
        max_events=20,
        min_horizon_days=0,
        max_horizon_days=4,
        skip_cohort_diagnostics=False,
        with_health_check=False,
    )

    steps = run_daily_pipeline.build_steps(args)

    assert [step.name for step in steps] == [
        "blind_live_validation",
        "observation_backfill",
        "blind_snapshot_resolution_audit",
        "operational_readiness",
        "cohort_overlay_diagnostics",
    ]
    assert steps[0].expected_artifacts == [
        "logs/snapshots/2026-04-17_polymarket_blind_live_validation.json"
    ]


def test_run_step_marks_degraded_when_artifact_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    monkeypatch.setattr(
        run_daily_pipeline,
        "utc_now",
        lambda: SimpleNamespace(isoformat=lambda: "2026-04-17T09:00:00"),
    )

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="ok\n", stderr="")

    monkeypatch.setattr(run_daily_pipeline.subprocess, "run", fake_run)

    result = run_daily_pipeline.run_step(
        run_daily_pipeline.PipelineStep(
            name="blind_live_validation",
            command=["python", "dummy.py"],
            expected_artifacts=["logs/snapshots/2026-04-17_polymarket_blind_live_validation.json"],
        )
    )

    assert result["status"] == "degraded"
    assert result["expected_artifacts"] == {
        "logs/snapshots/2026-04-17_polymarket_blind_live_validation.json": False
    }


def test_persist_report_writes_json(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    report = {
        "reference_date": "2026-04-17",
        "overall_status": "ok",
        "steps": [{"name": "blind_live_validation", "status": "ok"}],
    }

    path = run_daily_pipeline.persist_report("2026-04-17", report)

    assert path == tmp_path / "logs" / "snapshots" / "2026-04-17_daily_pipeline_report.json"
    assert json.loads(path.read_text(encoding="utf-8"))["overall_status"] == "ok"


def test_main_persists_preliminary_report_before_operational_readiness(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    monkeypatch.setattr(
        run_daily_pipeline,
        "utc_now",
        lambda: SimpleNamespace(isoformat=lambda: "2026-04-17T09:00:00"),
    )
    monkeypatch.setattr(
        run_daily_pipeline,
        "parse_args",
        lambda: SimpleNamespace(
            reference_date="2026-04-17",
            max_events=20,
            min_horizon_days=0,
            max_horizon_days=4,
            skip_cohort_diagnostics=True,
            with_health_check=False,
        ),
    )

    def fake_run_step(step):
        if step.name == "operational_readiness":
            preliminary_path = tmp_path / "logs" / "snapshots" / "2026-04-17_daily_pipeline_report.json"
            assert preliminary_path.exists()
            preliminary = json.loads(preliminary_path.read_text(encoding="utf-8"))
            assert [item["name"] for item in preliminary["steps"]] == [
                "blind_live_validation",
                "observation_backfill",
                "blind_snapshot_resolution_audit",
            ]
        return {
            "name": step.name,
            "status": "ok",
            "exit_code": 0,
            "expected_artifacts": {},
            "notes": [],
        }

    monkeypatch.setattr(run_daily_pipeline, "run_step", fake_run_step)

    run_daily_pipeline.main()

    final_report = json.loads(
        (tmp_path / "logs" / "snapshots" / "2026-04-17_daily_pipeline_report.json").read_text(encoding="utf-8")
    )
    assert [item["name"] for item in final_report["steps"]] == [
        "blind_live_validation",
        "observation_backfill",
        "blind_snapshot_resolution_audit",
        "operational_readiness",
    ]


def test_compute_overall_status_prioritizes_failures() -> None:
    assert run_daily_pipeline.compute_overall_status(
        [{"status": "ok"}, {"status": "warning"}]
    ) == "warning"
    assert run_daily_pipeline.compute_overall_status(
        [{"status": "ok"}, {"status": "degraded"}]
    ) == "degraded"
    assert run_daily_pipeline.compute_overall_status(
        [{"status": "ok"}, {"status": "failed"}]
    ) == "degraded"


def test_extract_audit_quality_reads_snapshot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    audit_path = tmp_path / "logs" / "snapshots" / "2026-04-18_blind_snapshot_resolution_audit.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps({"audit_quality": {"classification": "partial", "is_actionable": True}}),
        encoding="utf-8",
    )

    audit_quality = run_daily_pipeline.extract_audit_quality(
        {"logs/snapshots/2026-04-18_blind_snapshot_resolution_audit.json": True}
    )

    assert audit_quality == {"classification": "partial", "is_actionable": True}


def test_run_step_marks_warning_for_partial_audit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    monkeypatch.setattr(
        run_daily_pipeline,
        "utc_now",
        lambda: SimpleNamespace(isoformat=lambda: "2026-04-18T09:00:00"),
    )

    audit_path = tmp_path / "logs" / "snapshots" / "2026-04-18_blind_snapshot_resolution_audit.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps({"audit_quality": {"classification": "partial", "is_actionable": True}}),
        encoding="utf-8",
    )

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="ok\n", stderr="")

    monkeypatch.setattr(run_daily_pipeline.subprocess, "run", fake_run)

    result = run_daily_pipeline.run_step(
        run_daily_pipeline.PipelineStep(
            name="blind_snapshot_resolution_audit",
            command=["python", "dummy.py"],
            expected_artifacts=["logs/snapshots/2026-04-18_blind_snapshot_resolution_audit.json"],
        )
    )

    assert result["status"] == "warning"
    assert result["notes"] == ["audit_partial"]


def test_should_run_audit_local_only_when_backfill_remote_failed(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    backfill_path = tmp_path / "logs" / "snapshots" / "2026-04-20_observation_backfill.json"
    backfill_path.parent.mkdir(parents=True, exist_ok=True)
    backfill_path.write_text(
        json.dumps(
            {
                "targets_missing_locally": 40,
                "fetch_failure_count": 20,
                "archive_fetch_status": {
                    "remote_archive_available": False,
                    "remote_archive_error": "ConnectError",
                },
            }
        ),
        encoding="utf-8",
    )

    assert run_daily_pipeline.should_run_audit_local_only("2026-04-20") is True


def test_prepare_step_for_context_adds_local_only_to_audit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_daily_pipeline, "ROOT", tmp_path)
    backfill_path = tmp_path / "logs" / "snapshots" / "2026-04-20_observation_backfill.json"
    backfill_path.parent.mkdir(parents=True, exist_ok=True)
    backfill_path.write_text(
        json.dumps(
            {
                "targets_missing_locally": 40,
                "fetch_failure_count": 20,
                "archive_fetch_status": {"remote_archive_available": False},
            }
        ),
        encoding="utf-8",
    )
    step = run_daily_pipeline.PipelineStep(
        name="blind_snapshot_resolution_audit",
        command=["python", "scripts/run_blind_snapshot_resolution_audit.py", "--reference-date", "2026-04-20"],
        expected_artifacts=[],
    )

    prepared = run_daily_pipeline.prepare_step_for_context(step, reference_date="2026-04-20")

    assert prepared.command[-1] == "--local-only"
