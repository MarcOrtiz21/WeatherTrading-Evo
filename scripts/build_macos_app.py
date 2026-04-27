from __future__ import annotations

import argparse
import os
import plistlib
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_NAME = "WeatherTrading Evo"
DEFAULT_OUTPUT_DIR = ROOT / "build" / "macos"
APP_VERSION = "0.4.56"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye un lanzador .app local para WeatherTrading Evo.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--budget-usd", type=float, default=10.0)
    parser.add_argument("--max-tickets", type=int, default=12)
    return parser.parse_args()


def build_info_plist(*, app_name: str, executable_name: str) -> dict:
    return {
        "CFBundleName": app_name,
        "CFBundleDisplayName": app_name,
        "CFBundleIdentifier": "com.weathertrading.evo.operator",
        "CFBundleVersion": APP_VERSION,
        "CFBundleShortVersionString": APP_VERSION,
        "CFBundleExecutable": executable_name,
        "CFBundlePackageType": "APPL",
        "LSMinimumSystemVersion": "12.0",
        "NSHighResolutionCapable": True,
    }


def build_launcher_script(
    *,
    project_root: Path,
    port: int,
    budget_usd: float,
    max_tickets: int,
) -> str:
    project_root_text = project_root.as_posix()
    return f"""#!/bin/zsh
set -u

PROJECT_ROOT={shell_quote(project_root_text)}
PYTHON_BIN="$PROJECT_ROOT/venv/bin/python"
PORT="${{WEATHERTRADING_OPERATOR_PORT:-{port}}}"
BUDGET_USD="${{WEATHERTRADING_BUDGET_USD:-{budget_usd}}}"
MAX_TICKETS="${{WEATHERTRADING_MAX_TICKETS:-{max_tickets}}}"

alert() {{
  /usr/bin/osascript -e "display alert \\"WeatherTrading Evo\\" message \\"$1\\" as critical" >/dev/null 2>&1 || true
}}

if [ ! -d "$PROJECT_ROOT" ]; then
  alert "No encuentro el proyecto en $PROJECT_ROOT."
  exit 1
fi

cd "$PROJECT_ROOT" || exit 1

if [ ! -x "$PYTHON_BIN" ]; then
  alert "No encuentro el Python virtualenv en venv/bin/python. Ejecuta la instalacion del proyecto antes de abrir la app."
  exit 1
fi

"$PYTHON_BIN" - "$PROJECT_ROOT" "$BUDGET_USD" "$MAX_TICKETS" <<'PY'
import sys
from pathlib import Path

project_root = Path(sys.argv[1])
budget_usd = float(sys.argv[2])
max_tickets = int(float(sys.argv[3]))

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from scripts.run_operator_console import build_operator_dashboard

payload = build_operator_dashboard(
    root=project_root,
    reference_date="latest",
    budget_usd=budget_usd,
    max_tickets=max_tickets,
)
preflight = payload.get("preflight", {{}})
print("Preflight:", preflight.get("status"))
print("Approval allowed:", preflight.get("approval_allowed"))
if preflight.get("blockers"):
    print("Blockers:", ", ".join(preflight.get("blockers", [])))
if preflight.get("warnings"):
    print("Warnings:", ", ".join(preflight.get("warnings", [])))
if preflight.get("status") == "blocked":
    sys.exit(20)
PY

PREFLIGHT_CODE=$?
if [ "$PREFLIGHT_CODE" -eq 20 ]; then
  alert "Preflight bloqueado. La UI se abrira en modo lectura y no permitira aprobaciones paper hasta corregir los blockers."
elif [ "$PREFLIGHT_CODE" -ne 0 ]; then
  alert "Preflight no ha podido completarse. Revisa el proyecto desde terminal."
  exit "$PREFLIGHT_CODE"
fi

exec "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_operator_ui.py" \\
  --host 127.0.0.1 \\
  --port "$PORT" \\
  --reference-date latest \\
  --budget-usd "$BUDGET_USD" \\
  --max-tickets "$MAX_TICKETS"
"""


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def build_app_bundle(
    *,
    output_dir: Path,
    app_name: str = APP_NAME,
    project_root: Path = ROOT,
    port: int = 8787,
    budget_usd: float = 10.0,
    max_tickets: int = 12,
) -> Path:
    app_path = output_dir / f"{app_name}.app"
    contents = app_path / "Contents"
    macos = contents / "MacOS"
    resources = contents / "Resources"
    executable_name = "weathertrading-evo"

    if app_path.exists():
        shutil.rmtree(app_path)
    macos.mkdir(parents=True, exist_ok=True)
    resources.mkdir(parents=True, exist_ok=True)

    info = build_info_plist(app_name=app_name, executable_name=executable_name)
    with (contents / "Info.plist").open("wb") as handle:
        plistlib.dump(info, handle)

    launcher = macos / executable_name
    launcher.write_text(
        build_launcher_script(
            project_root=project_root,
            port=port,
            budget_usd=budget_usd,
            max_tickets=max_tickets,
        ),
        encoding="utf-8",
    )
    launcher.chmod(0o755)

    readme = resources / "README.txt"
    readme.write_text(
        "WeatherTrading Evo local operator launcher. "
        "This app opens a local dashboard and never executes real trades by itself.\n",
        encoding="utf-8",
    )
    return app_path


def main() -> None:
    args = parse_args()
    app_path = build_app_bundle(
        output_dir=Path(args.output_dir),
        port=args.port,
        budget_usd=args.budget_usd,
        max_tickets=args.max_tickets,
    )
    print(f"macOS app creada en: {app_path}")
    print("Abrir con:")
    print(f"open {shell_quote(app_path.as_posix())}")


if __name__ == "__main__":
    main()
