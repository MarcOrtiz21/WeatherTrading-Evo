import plistlib

from scripts import install_launchd_scheduler


def test_build_launch_agent_plist_uses_runner_and_schedule() -> None:
    job = install_launchd_scheduler.LaunchdJob(
        label="com.weathertrading.test",
        mode="daily",
        hour=8,
        minute=20,
    )

    payload = install_launchd_scheduler.build_launch_agent_plist(job)

    assert payload["Label"] == "com.weathertrading.test"
    assert payload["ProgramArguments"][-1] == "daily"
    assert payload["StartCalendarInterval"] == {"Hour": 8, "Minute": 20}
    assert payload["RunAtLoad"] is False
    assert payload["EnvironmentVariables"]["TZ"] == "Europe/Madrid"
    assert payload["SoftResourceLimits"]["NumberOfFiles"] == 4096
    assert payload["HardResourceLimits"]["NumberOfFiles"] == 4096


def test_write_launch_agent_creates_plist(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(install_launchd_scheduler, "LAUNCH_AGENTS_DIR", tmp_path)
    job = install_launchd_scheduler.LaunchdJob(
        label="com.weathertrading.test",
        mode="watchdog",
        hour=9,
        minute=10,
    )

    path = install_launchd_scheduler.write_launch_agent(job, run_at_load=True)

    assert path == tmp_path / "com.weathertrading.test.plist"
    with path.open("rb") as handle:
        payload = plistlib.load(handle)
    assert payload["ProgramArguments"][-1] == "watchdog"
    assert payload["RunAtLoad"] is True
