#!/bin/zsh
set -uo pipefail

MODE="${1:-daily}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/venv/bin/python"
REFERENCE_DATE="$(TZ=Europe/Madrid /bin/date +%F)"
LOG_DIR="$PROJECT_ROOT/logs/launchd"
REPORT_PATH="$PROJECT_ROOT/logs/snapshots/${REFERENCE_DATE}_daily_pipeline_report.json"
LOCK_DIR="$LOG_DIR/.daily_pipeline.lock"
BUDGET_USD="${WEATHERTRADING_BUDGET_USD:-10}"
MAX_TICKETS="${WEATHERTRADING_MAX_TICKETS:-12}"

/bin/mkdir -p "$LOG_DIR"

{
  echo "=== WeatherTrading launchd ${MODE} ${REFERENCE_DATE} ==="
  /bin/date -u "+started_at_utc=%Y-%m-%dT%H:%M:%SZ"
  echo "project_root=$PROJECT_ROOT"

  if [ ! -x "$PYTHON_BIN" ]; then
    echo "missing_python=$PYTHON_BIN"
    exit 20
  fi

  if ! /bin/mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "another_pipeline_run_is_active=true"
    exit 0
  fi
  trap '/bin/rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

  cd "$PROJECT_ROOT" || exit 21

  if [ "$MODE" = "watchdog" ] && [ -f "$REPORT_PATH" ]; then
    echo "daily_report_already_present=$REPORT_PATH"
    "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_operator_console.py" \
      --reference-date "$REFERENCE_DATE" \
      --budget-usd "$BUDGET_USD" \
      --max-tickets "$MAX_TICKETS" \
      --export
    STATUS=$?
    /bin/date -u "+finished_at_utc=%Y-%m-%dT%H:%M:%SZ"
    echo "exit_code=$STATUS"
    exit "$STATUS"
  fi

  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_daily_pipeline.py" \
    --reference-date "$REFERENCE_DATE"
  PIPELINE_STATUS=$?

  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_operator_console.py" \
    --reference-date "$REFERENCE_DATE" \
    --budget-usd "$BUDGET_USD" \
    --max-tickets "$MAX_TICKETS" \
    --export || true

  /bin/date -u "+finished_at_utc=%Y-%m-%dT%H:%M:%SZ"
  echo "exit_code=$PIPELINE_STATUS"
  exit "$PIPELINE_STATUS"
} >> "$LOG_DIR/${REFERENCE_DATE}_${MODE}.log" 2>&1
