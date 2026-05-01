from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
RUNNER = ROOT / "scripts" / "run_daily_pipeline_launchd.sh"
MACOS_PROTECTED_USER_DIR_NAMES = {"Desktop", "Documents", "Downloads"}


@dataclass(frozen=True)
class LaunchdJob:
    label: str
    mode: str
    hour: int
    minute: int


JOBS = (
    LaunchdJob(
        label="com.weathertrading.evo.daily-pipeline",
        mode="daily",
        hour=8,
        minute=20,
    ),
    LaunchdJob(
        label="com.weathertrading.evo.pipeline-watchdog",
        mode="watchdog",
        hour=9,
        minute=10,
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Instala LaunchAgents locales para WeatherTrading Evo.")
    parser.add_argument("--uninstall", action="store_true", help="Descarga y elimina los LaunchAgents.")
    parser.add_argument("--no-load", action="store_true", help="Solo escribe plist; no llama a launchctl.")
    parser.add_argument("--run-at-load", action="store_true", help="Ejecuta al cargar el agente. Por defecto queda desactivado.")
    return parser.parse_args()


def build_launch_agent_plist(job: LaunchdJob, *, run_at_load: bool = False) -> dict:
    log_dir = ROOT / "logs" / "launchd"
    return {
        "Label": job.label,
        "ProgramArguments": [
            "/bin/zsh",
            RUNNER.as_posix(),
            job.mode,
        ],
        "WorkingDirectory": ROOT.as_posix(),
        "StartCalendarInterval": {
            "Hour": job.hour,
            "Minute": job.minute,
        },
        "RunAtLoad": bool(run_at_load),
        "StandardOutPath": (log_dir / f"{job.label}.out.log").as_posix(),
        "StandardErrorPath": (log_dir / f"{job.label}.err.log").as_posix(),
        "SoftResourceLimits": {
            "NumberOfFiles": 4096,
        },
        "HardResourceLimits": {
            "NumberOfFiles": 4096,
        },
        "EnvironmentVariables": {
            "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "TZ": "Europe/Madrid",
        },
    }


def write_launch_agent(job: LaunchdJob, *, run_at_load: bool = False) -> Path:
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    (ROOT / "logs" / "launchd").mkdir(parents=True, exist_ok=True)
    path = LAUNCH_AGENTS_DIR / f"{job.label}.plist"
    with path.open("wb") as handle:
        plistlib.dump(build_launch_agent_plist(job, run_at_load=run_at_load), handle)
    return path


def load_launch_agent(path: Path, label: str) -> dict:
    domain = f"gui/{os.getuid()}"
    bootout = subprocess.run(
        ["launchctl", "bootout", domain, path.as_posix()],
        capture_output=True,
        text=True,
        check=False,
    )
    bootstrap = subprocess.run(
        ["launchctl", "bootstrap", domain, path.as_posix()],
        capture_output=True,
        text=True,
        check=False,
    )
    enable = subprocess.run(
        ["launchctl", "enable", f"{domain}/{label}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "label": label,
        "path": path.as_posix(),
        "bootout_exit_code": bootout.returncode,
        "bootstrap_exit_code": bootstrap.returncode,
        "enable_exit_code": enable.returncode,
        "bootstrap_stderr": bootstrap.stderr.strip(),
        "enable_stderr": enable.stderr.strip(),
    }


def unload_launch_agent(path: Path, label: str) -> dict:
    domain = f"gui/{os.getuid()}"
    bootout = subprocess.run(
        ["launchctl", "bootout", domain, path.as_posix()],
        capture_output=True,
        text=True,
        check=False,
    )
    if path.exists():
        path.unlink()
    return {
        "label": label,
        "path": path.as_posix(),
        "bootout_exit_code": bootout.returncode,
        "bootout_stderr": bootout.stderr.strip(),
    }


def install_launch_agents(*, load: bool = True, run_at_load: bool = False) -> list[dict]:
    results: list[dict] = []
    for job in JOBS:
        path = write_launch_agent(job, run_at_load=run_at_load)
        result = {"label": job.label, "path": path.as_posix(), "written": True}
        if load:
            result |= load_launch_agent(path, job.label)
        results.append(result)
    return results


def uninstall_launch_agents() -> list[dict]:
    results: list[dict] = []
    for job in JOBS:
        path = LAUNCH_AGENTS_DIR / f"{job.label}.plist"
        results.append(unload_launch_agent(path, job.label))
    return results


def is_inside_macos_protected_user_dir(path: Path) -> bool:
    try:
        relative = path.resolve().relative_to(Path.home().resolve())
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in MACOS_PROTECTED_USER_DIR_NAMES


def main() -> None:
    args = parse_args()
    if args.uninstall:
        results = uninstall_launch_agents()
    else:
        results = install_launch_agents(load=not args.no_load, run_at_load=bool(args.run_at_load))

    print("=== WEATHERTRADING LAUNCHD SCHEDULER ===")
    if is_inside_macos_protected_user_dir(ROOT):
        print(
            "WARNING: el proyecto esta dentro de Desktop/Documents/Downloads. "
            "macOS puede bloquear LaunchAgents con permisos de privacidad; "
            "si ves exit code 127, mueve el repo a ~/Developer o concede Full Disk Access."
        )
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
