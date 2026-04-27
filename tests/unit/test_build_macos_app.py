import plistlib
from pathlib import Path

from scripts import build_macos_app


def test_build_info_plist_sets_bundle_executable() -> None:
    info = build_macos_app.build_info_plist(app_name="WeatherTrading Evo", executable_name="launcher")

    assert info["CFBundleName"] == "WeatherTrading Evo"
    assert info["CFBundleExecutable"] == "launcher"
    assert info["CFBundlePackageType"] == "APPL"


def test_build_launcher_script_contains_preflight_and_ui_command(tmp_path: Path) -> None:
    script = build_macos_app.build_launcher_script(
        project_root=tmp_path,
        port=8787,
        budget_usd=10.0,
        max_tickets=12,
    )

    assert "build_operator_dashboard" in script
    assert "run_operator_ui.py" in script
    assert "Preflight bloqueado" in script
    assert tmp_path.as_posix() in script


def test_build_app_bundle_creates_executable_bundle(tmp_path: Path) -> None:
    app_path = build_macos_app.build_app_bundle(
        output_dir=tmp_path,
        project_root=tmp_path,
        port=8787,
        budget_usd=10.0,
        max_tickets=12,
    )

    launcher = app_path / "Contents" / "MacOS" / "weathertrading-evo"
    info_path = app_path / "Contents" / "Info.plist"

    assert launcher.exists()
    assert launcher.stat().st_mode & 0o111
    assert info_path.exists()
    with info_path.open("rb") as handle:
        info = plistlib.load(handle)
    assert info["CFBundleExecutable"] == "weathertrading-evo"
