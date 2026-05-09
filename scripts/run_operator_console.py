from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.watchlist_strategy_analysis import infer_yes_bias

AUTOMATIONS_ROOT = Path.home() / ".codex" / "automations"
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
LAUNCHD_LABELS = (
    "com.weathertrading.evo.daily-pipeline",
    "com.weathertrading.evo.pipeline-watchdog",
)
MACOS_PROTECTED_USER_DIR_NAMES = {"Desktop", "Documents", "Downloads"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consola operativa local: resume pipeline, audit, live snapshot, copytrading y tickets supervisados."
    )
    parser.add_argument("--reference-date", default="latest", help="Fecha YYYY-MM-DD o latest.")
    parser.add_argument("--budget-usd", type=float, default=10.0, help="Cap maximo por ticket supervisado.")
    parser.add_argument("--max-tickets", type=int, default=8, help="Numero maximo de tickets a mostrar.")
    parser.add_argument(
        "--export",
        action="store_true",
        help="Persistir el dashboard en logs/snapshots/<fecha>_operator_console.json.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Permite aprobar un ticket y registrarlo localmente como paper/dry-run. No ejecuta ordenes.",
    )
    parser.add_argument("--json", action="store_true", help="Imprime el payload completo en JSON.")
    return parser.parse_args()


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def snapshot_path(root: Path, reference_date: str, suffix: str) -> Path:
    return root / "logs" / "snapshots" / f"{reference_date}_{suffix}.json"


def find_latest_reference_date(root: Path) -> str:
    snapshots_dir = root / "logs" / "snapshots"
    candidates = sorted(snapshots_dir.glob("*_daily_pipeline_report.json"), reverse=True)
    if not candidates:
        candidates = sorted(snapshots_dir.glob("*_polymarket_blind_live_validation.json"), reverse=True)
    if not candidates:
        raise SystemExit("No hay snapshots suficientes para construir la consola operativa.")
    return candidates[0].name[:10]


def latest_trader_profile(root: Path, username: str, reference_date: str) -> tuple[Path | None, dict]:
    safe_username = "".join(ch.lower() if ch.isalnum() else "-" for ch in username).strip("-")
    same_day = snapshot_path(root, reference_date, f"{safe_username}_behavior_profile")
    if same_day.exists():
        return same_day, load_json(same_day)
    candidates = sorted((root / "logs" / "snapshots").glob(f"*_{safe_username}_behavior_profile.json"), reverse=True)
    if not candidates:
        return None, {}
    return candidates[0], load_json(candidates[0])


def build_operator_dashboard(
    *,
    root: Path,
    reference_date: str,
    budget_usd: float,
    max_tickets: int,
) -> dict:
    if reference_date == "latest":
        reference_date = find_latest_reference_date(root)

    pipeline = load_json(snapshot_path(root, reference_date, "daily_pipeline_report"))
    readiness = load_json(snapshot_path(root, reference_date, "operational_readiness"))
    audit = load_json(snapshot_path(root, reference_date, "blind_snapshot_resolution_audit"))
    live = load_json(snapshot_path(root, reference_date, "polymarket_blind_live_validation"))
    watchlist = load_json(snapshot_path(root, reference_date, "watchlist_strategy_simulation"))

    profile_paths: dict[str, str | None] = {}
    profiles: dict[str, dict] = {}
    for username in ("ColdMath", "Poligarch"):
        profile_path, profile = latest_trader_profile(root, username, reference_date)
        profile_paths[username] = None if profile_path is None else profile_path.relative_to(root).as_posix()
        profiles[username] = profile

    payload = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date,
        "budget_usd": budget_usd,
        "artifact_paths": build_artifact_paths(root, reference_date, profile_paths),
        "system_health": build_system_health(root, reference_date=reference_date),
        "pipeline": summarize_pipeline(pipeline),
        "readiness": summarize_readiness(readiness),
        "audit": summarize_audit(audit),
        "live": summarize_live(live),
        "watchlist_strategy": summarize_watchlist_strategy(watchlist),
        "copytrading_size_guidance": {
            username: summarize_trader_sizing(profile, budget_usd=budget_usd)
            for username, profile in profiles.items()
        },
        "tickets": build_trade_tickets(live, budget_usd=budget_usd, max_tickets=max_tickets),
        "execution_policy": build_execution_policy_summary(),
    }
    payload["preflight"] = build_preflight_summary(payload)
    return payload


def build_execution_policy_summary() -> dict:
    min_trade_horizon_days = get_min_trade_horizon_days()
    horizon_label = f"H{min_trade_horizon_days}+" if min_trade_horizon_days > 0 else "H0+"
    return {
        "mode": "supervised_paper_ticket",
        "live_execution_enabled": bool(ConfigLoader.get("operator_policy.live_execution_enabled", False)),
        "approval_text": "OK <ticket_id>",
        "min_trade_horizon_days": min_trade_horizon_days,
        "trade_horizon_label": horizon_label,
        "horizon0_mode": str(ConfigLoader.get("operator_policy.horizon0_mode", "quarantined")),
        "copytrading_mode": str(ConfigLoader.get("operator_policy.copytrading_mode", "veto_only")),
        "notes": [
            "La consola no envia ordenes al CLOB.",
            f"Solo se revisan tickets {horizon_label}; H0 queda en cuarentena hasta que mejore su auditoria.",
            "Copytrading se usa como veto/contexto, no como entrada directa.",
            "Un OK solo registra una decision paper/dry-run local.",
        ],
    }


def get_min_trade_horizon_days(default: int = 1) -> int:
    value = ConfigLoader.get("operator_policy.min_trade_horizon_days", default)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def build_preflight_summary(payload: dict) -> dict:
    blockers: list[str] = []
    warnings: list[str] = []

    pipeline_status = payload.get("pipeline", {}).get("overall_status")
    if pipeline_status != "ok":
        blockers.append("pipeline_not_ok")

    readiness = payload.get("readiness", {})
    readiness_status = readiness.get("status")
    if readiness_status not in {"ready", "paper_only"}:
        blockers.append("readiness_not_operable")
    blockers.extend(str(item) for item in readiness.get("blockers", []))
    warnings.extend(str(item) for item in readiness.get("warnings", []))

    db = readiness.get("database_health") or {}
    if db.get("status") != "ok":
        blockers.append("database_not_ok")
    observation_lag_days = db.get("observation_lag_days")
    if observation_lag_days is not None and int(observation_lag_days) > 3:
        add_unique(warnings, "weather_observations_stale")

    audit_quality = payload.get("audit", {}).get("quality") or {}
    if not bool(audit_quality.get("is_actionable")):
        blockers.append("audit_not_actionable")
    elif audit_quality.get("classification") != "complete":
        warnings.append("audit_not_complete")

    if int(payload.get("live", {}).get("evaluated_events") or 0) <= 0:
        blockers.append("live_snapshot_empty")

    system_health = payload.get("system_health") or {}
    if system_health and system_health.get("status") != "ok":
        warnings.append("system_health_warning")
    if system_health.get("reference_pipeline_status") == "missing":
        add_unique(blockers, "daily_pipeline_report_missing")

    audit = payload.get("audit", {})
    log_loss_delta = audit.get("model_market_log_loss_delta")
    warn_delta = to_float(ConfigLoader.get("operator_risk.warn_if_log_loss_delta_exceeds", 0.25))
    if log_loss_delta is not None and float(log_loss_delta) > warn_delta:
        warnings.append("model_log_loss_underperforms_market")
    horizon0_delta = audit.get("horizon0_model_market_log_loss_delta")
    if horizon0_delta is not None and float(horizon0_delta) > warn_delta:
        warnings.append("horizon0_log_loss_underperforms_market")
    horizon0_roi = audit.get("horizon0_paper_roi_on_stake")
    if horizon0_roi is not None and float(horizon0_roi) < 0:
        warnings.append("horizon0_negative_paper_roi")

    reviewable_tickets = [
        ticket
        for ticket in payload.get("tickets", [])
        if ticket.get("action") == "REVIEW" and float(ticket.get("stake_suggestion_usd") or 0.0) > 0
    ]
    if not reviewable_tickets:
        warnings.append("no_reviewable_tickets")

    status = "blocked" if blockers else ("warning" if warnings else "ok")
    return {
        "status": status,
        "approval_allowed": not blockers and bool(reviewable_tickets),
        "reviewable_ticket_count": len(reviewable_tickets),
        "blockers": unique_sorted(blockers),
        "warnings": unique_sorted(warnings),
        "live_execution_enabled": False,
        "notes": [
            "Preflight bloquea aprobaciones paper si falta evidencia critica.",
            "La UI nunca ejecuta ordenes reales; solo registra decisiones locales.",
        ],
    }


def build_system_health(root: Path, *, reference_date: str) -> dict:
    daily_reports = load_daily_pipeline_reports(root)
    reference_report = next((item for item in daily_reports if item["date"] == reference_date), None)
    latest_report = daily_reports[-1] if daily_reports else None
    latest_ok = next((item for item in reversed(daily_reports) if item["overall_status"] == "ok"), None)
    automation_status = discover_automation_status()
    launchd_status = discover_launchd_status(root)
    missing_dates = find_missing_pipeline_dates(daily_reports)

    reference_date_value = parse_date_safe(reference_date)
    latest_ok_lag_days = None
    if reference_date_value and latest_ok:
        latest_ok_lag_days = (reference_date_value - date.fromisoformat(latest_ok["date"])).days

    warnings: list[str] = []
    blockers: list[str] = []
    reference_status = "missing" if reference_report is None else str(reference_report.get("overall_status") or "unknown")
    if reference_status == "missing":
        blockers.append("reference_daily_pipeline_missing")
    elif reference_status != "ok":
        warnings.append("reference_daily_pipeline_not_ok")
    if latest_ok_lag_days is not None and latest_ok_lag_days > 1:
        warnings.append("latest_ok_pipeline_stale")
    if missing_dates:
        warnings.append("pipeline_date_gap_detected")
    active_daily = [
        automation
        for automation in automation_status
        if automation.get("status") == "ACTIVE" and "daily" in str(automation.get("id", "")).lower()
    ]
    if not active_daily:
        warnings.append("no_active_daily_automation_detected")
    failed_launchd = [
        item
        for item in launchd_status
        if item.get("status") in {"failed", "unloaded", "missing_plist", "unknown"}
    ]
    if failed_launchd:
        warnings.append("launchd_scheduler_not_healthy")
    if is_inside_macos_protected_user_dir(root):
        warnings.append("project_root_in_macos_protected_directory")

    status = "blocked" if blockers else ("warning" if warnings else "ok")
    return {
        "status": status,
        "reference_pipeline_status": reference_status,
        "latest_report_date": None if latest_report is None else latest_report["date"],
        "latest_ok_pipeline_date": None if latest_ok is None else latest_ok["date"],
        "latest_ok_pipeline_lag_days": latest_ok_lag_days,
        "missing_pipeline_dates": missing_dates,
        "automation_status": automation_status,
        "launchd_status": launchd_status,
        "warnings": unique_sorted(warnings),
        "blockers": unique_sorted(blockers),
        "recovery_command": f"venv/bin/python scripts/run_daily_pipeline.py --reference-date {reference_date}",
        "scheduler_reinstall_command": "venv/bin/python scripts/install_launchd_scheduler.py",
    }


def load_daily_pipeline_reports(root: Path) -> list[dict]:
    reports: list[dict] = []
    for path in sorted((root / "logs" / "snapshots").glob("*_daily_pipeline_report.json")):
        report_date = path.name[:10]
        if parse_date_safe(report_date) is None:
            continue
        payload = load_json(path)
        reports.append(
            {
                "date": report_date,
                "overall_status": payload.get("overall_status"),
                "path": path.relative_to(root).as_posix(),
            }
        )
    return reports


def discover_automation_status(automations_root: Path = AUTOMATIONS_ROOT) -> list[dict]:
    if not automations_root.exists():
        return []
    automations: list[dict] = []
    for path in sorted(automations_root.glob("*/automation.toml")):
        try:
            data = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        automations.append(
            {
                "id": data.get("id") or path.parent.name,
                "name": data.get("name") or path.parent.name,
                "status": data.get("status") or "UNKNOWN",
                "rrule": data.get("rrule"),
                "cwd_matches": str(ROOT) in [str(item) for item in data.get("cwds", [])],
            }
        )
    return automations


def discover_launchd_status(root: Path = ROOT) -> list[dict]:
    statuses: list[dict] = []
    domain = f"gui/{os.getuid()}"
    for label in LAUNCHD_LABELS:
        plist_path = LAUNCH_AGENTS_DIR / f"{label}.plist"
        stderr_path = root / "logs" / "launchd" / f"{label}.err.log"
        if not plist_path.exists():
            statuses.append(
                {
                    "label": label,
                    "status": "missing_plist",
                    "plist_path": plist_path.as_posix(),
                    "last_exit_code": None,
                    "runs": None,
                    "stderr_tail": read_text_tail(stderr_path),
                }
            )
            continue

        result = subprocess.run(
            ["launchctl", "print", f"{domain}/{label}"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = "\n".join(part for part in (result.stdout, result.stderr) if part)
        last_exit_code = parse_launchctl_int(output, "last exit code")
        runs = parse_launchctl_int(output, "runs")
        state = parse_launchctl_value(output, "state")
        if result.returncode != 0:
            status = "unloaded"
        elif last_exit_code not in (None, 0):
            status = "failed"
        elif runs == 0:
            status = "scheduled"
        else:
            status = "ok"
        statuses.append(
            {
                "label": label,
                "status": status,
                "plist_path": plist_path.as_posix(),
                "state": state,
                "last_exit_code": last_exit_code,
                "runs": runs,
                "stderr_tail": read_text_tail(stderr_path),
            }
        )
    return statuses


def parse_launchctl_int(output: str, key: str) -> int | None:
    match = re.search(rf"{re.escape(key)}\s*=\s*(-?\d+)", output)
    return None if match is None else int(match.group(1))


def parse_launchctl_value(output: str, key: str) -> str | None:
    match = re.search(rf"{re.escape(key)}\s*=\s*([^\n]+)", output)
    return None if match is None else match.group(1).strip()


def read_text_tail(path: Path, *, lines: int = 3) -> list[str]:
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    except OSError:
        return []


def is_inside_macos_protected_user_dir(path: Path) -> bool:
    try:
        relative = path.resolve().relative_to(Path.home().resolve())
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in MACOS_PROTECTED_USER_DIR_NAMES


def find_missing_pipeline_dates(
    reports: list[dict],
    *,
    max_gap_days: int = 10,
    recent_window_days: int = 4,
) -> list[str]:
    dates = sorted(date.fromisoformat(item["date"]) for item in reports if parse_date_safe(item["date"]))
    if len(dates) < 2:
        return []
    missing: list[str] = []
    for previous, current in zip(dates, dates[1:]):
        gap_days = (current - previous).days
        if gap_days <= 1 or gap_days > max_gap_days:
            continue
        probe = previous + timedelta(days=1)
        while probe < current:
            missing.append(probe.isoformat())
            probe += timedelta(days=1)
    latest = dates[-1]
    recent_cutoff = latest - timedelta(days=recent_window_days)
    return [item for item in missing if date.fromisoformat(item) >= recent_cutoff][-14:]


def parse_date_safe(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def add_unique(items: list[str], item: str) -> None:
    if item not in items:
        items.append(item)


def unique_sorted(items: list[str]) -> list[str]:
    return sorted({item for item in items if item})


def build_artifact_paths(root: Path, reference_date: str, profile_paths: dict[str, str | None]) -> dict:
    paths = {
        "pipeline": snapshot_path(root, reference_date, "daily_pipeline_report"),
        "readiness": snapshot_path(root, reference_date, "operational_readiness"),
        "audit": snapshot_path(root, reference_date, "blind_snapshot_resolution_audit"),
        "live": snapshot_path(root, reference_date, "polymarket_blind_live_validation"),
        "watchlist_strategy": snapshot_path(root, reference_date, "watchlist_strategy_simulation"),
    }
    return {
        key: path.relative_to(root).as_posix() if path.exists() else None
        for key, path in paths.items()
    } | {"trader_profiles": profile_paths}


def summarize_pipeline(pipeline: dict) -> dict:
    return {
        "overall_status": pipeline.get("overall_status"),
        "steps": [
            {
                "name": step.get("name"),
                "status": step.get("status"),
                "exit_code": step.get("exit_code"),
                "notes": step.get("notes", []),
            }
            for step in pipeline.get("steps", [])
        ],
    }


def summarize_readiness(readiness: dict) -> dict:
    return {
        "status": readiness.get("status"),
        "recommended_mode": readiness.get("recommended_mode"),
        "blockers": readiness.get("blockers", []),
        "warnings": readiness.get("warnings", []),
        "database_health": readiness.get("database_health", {}),
    }


def summarize_audit(audit: dict) -> dict:
    summary = audit.get("summary", {})
    by_horizon_days = summary.get("by_horizon_days") or {}
    horizon0 = by_horizon_days.get("0") or {}
    horizon1 = by_horizon_days.get("1") or {}
    return {
        "quality": audit.get("audit_quality", {}),
        "events": summary.get("events"),
        "model_mode_hit_rate": summary.get("model_mode_hit_rate"),
        "market_mode_hit_rate": summary.get("market_mode_hit_rate"),
        "model_log_loss": summary.get("model_log_loss"),
        "market_log_loss": summary.get("market_log_loss"),
        "model_market_log_loss_delta": (
            to_float(summary.get("model_log_loss")) - to_float(summary.get("market_log_loss"))
            if summary.get("model_log_loss") is not None and summary.get("market_log_loss") is not None
            else None
        ),
        "model_brier": summary.get("model_brier"),
        "market_brier": summary.get("market_brier"),
        "paper_trades": summary.get("paper_trades"),
        "paper_total_pnl": summary.get("paper_total_pnl"),
        "paper_roi_on_stake": summary.get("paper_roi_on_stake"),
        "by_horizon_days": by_horizon_days,
        "horizon0_model_market_log_loss_delta": (
            to_float(horizon0.get("model_log_loss")) - to_float(horizon0.get("market_log_loss"))
            if horizon0.get("model_log_loss") is not None and horizon0.get("market_log_loss") is not None
            else None
        ),
        "horizon0_paper_roi_on_stake": horizon0.get("paper_roi_on_stake"),
        "horizon1_model_market_log_loss_delta": (
            to_float(horizon1.get("model_log_loss")) - to_float(horizon1.get("market_log_loss"))
            if horizon1.get("model_log_loss") is not None and horizon1.get("market_log_loss") is not None
            else None
        ),
        "horizon1_paper_roi_on_stake": horizon1.get("paper_roi_on_stake"),
    }


def summarize_live(live: dict) -> dict:
    events = list(live.get("evaluated_events", []))
    signal_counts = Counter(str(event.get("watchlist_signal") or "unknown") for event in events)
    blocker_counts: Counter[str] = Counter()
    tradeable_count = 0
    for event in events:
        if bool(event.get("event_operable")) and bool(event.get("top_edge_tradeable")):
            tradeable_count += 1
        blocker_counts.update(str(item) for item in event.get("event_blockers", []))
    return {
        "as_of_date": live.get("as_of_date"),
        "evaluated_events": len(events),
        "tradeable_events": tradeable_count,
        "watchlist_signals": dict(sorted(signal_counts.items())),
        "event_blockers": dict(sorted(blocker_counts.items())),
        "watchlist_error": live.get("wallet_watchlist_error"),
        "tracked_traders": live.get("wallet_watchlist_tracked_traders", []),
    }


def summarize_watchlist_strategy(watchlist: dict) -> dict:
    strategies = watchlist.get("strategies", {})
    selected = {
        name: strategies.get(name, {})
        for name in (
            "model_current",
            "model_skip_opposed",
            "model_skip_silent",
            "model_skip_opposed_and_silent",
            "model_skip_weak_watchlist",
            "copy_coldmath_directional",
            "copy_poligarch_directional",
            "copy_watchlist_consensus_directional",
        )
    }
    return {
        "evaluated_events": watchlist.get("evaluated_events"),
        "overlay_breakdown": watchlist.get("watchlist_overlay_breakdown", {}),
        "selected_strategies": selected,
        "best_strategy_by_pnl": watchlist.get("strategy_comparison_digest", {}).get("best_strategy_by_pnl"),
        "best_strategy_by_roi": watchlist.get("strategy_comparison_digest", {}).get("best_strategy_by_roi"),
    }


def summarize_trader_sizing(profile: dict, *, budget_usd: float) -> dict:
    summary = profile.get("recent_trades_summary", {})
    timing = profile.get("timing_summary", {})
    direction = profile.get("trade_direction_summary", {})
    avg_notional = to_float(summary.get("avg_notional_usd"))
    median_notional = to_float(summary.get("median_notional_usd"))
    return {
        "username": profile.get("username"),
        "profile_captured_at_utc": profile.get("captured_at_utc"),
        "trade_count": summary.get("trade_count"),
        "unique_event_count": summary.get("unique_event_count"),
        "avg_notional_usd": avg_notional,
        "median_notional_usd": median_notional,
        "budget_vs_avg_notional": safe_ratio(budget_usd, avg_notional),
        "budget_vs_median_notional": safe_ratio(budget_usd, median_notional),
        "avg_hours_before_event": timing.get("avg_hours_before_event"),
        "median_hours_before_event": timing.get("median_hours_before_event"),
        "same_day_share": timing.get("same_day_share"),
        "yes_buy_share": direction.get("yes_buy_share"),
        "no_buy_share": direction.get("no_buy_share"),
        "sell_share": direction.get("sell_share"),
        "sizing_note": build_sizing_note(avg_notional=avg_notional, median_notional=median_notional, budget_usd=budget_usd),
    }


def build_sizing_note(*, avg_notional: float, median_notional: float, budget_usd: float) -> str:
    if median_notional <= 0:
        return "sin mediana fiable; usar solo cap manual"
    if avg_notional / median_notional >= 8:
        return "media muy sesgada por trades grandes; usar mediana/cap, no copiar tamano absoluto"
    if budget_usd > median_notional * 2:
        return "el presupuesto supera bastante la mediana del trader; reducir si es live"
    return "presupuesto comparable a la mediana historica del trader"


def build_trade_tickets(live: dict, *, budget_usd: float, max_tickets: int) -> list[dict]:
    candidates = []
    for event in live.get("evaluated_events", []):
        market = find_top_edge_market(event)
        if market is None:
            continue
        ticket = build_trade_ticket(
            event,
            market=market,
            budget_usd=budget_usd,
            reference_date=str(live.get("as_of_date") or ""),
        )
        candidates.append(ticket)
    candidates.sort(
        key=lambda item: (
            item["action"] == "REVIEW",
            item["stake_suggestion_usd"],
            item["edge_net"],
        ),
        reverse=True,
    )
    return [
        {
            **ticket,
            "ticket_id": f"T{idx:02d}",
        }
        for idx, ticket in enumerate(candidates[:max_tickets], start=1)
    ]


def find_top_edge_market(event: dict) -> dict | None:
    target_question = str(event.get("top_edge_question") or "")
    markets = list(event.get("markets", []))
    for market in markets:
        if str(market.get("question") or "") == target_question:
            return market
    if not markets:
        return None
    return max(markets, key=lambda market: float(market.get("edge_net") or -999.0))


def build_trade_ticket(event: dict, *, market: dict, budget_usd: float, reference_date: str | None = None) -> dict:
    blockers = list(event.get("event_blockers", [])) + list(market.get("blockers", []))
    blockers.extend(build_operator_risk_blockers(event, reference_date=reference_date))
    is_tradeable = bool(event.get("event_operable")) and bool(event.get("top_edge_tradeable")) and bool(market.get("is_tradeable"))
    if bool(event.get("watchlist_veto_applied")) and "watchlist_opposed_veto" not in blockers:
        blockers.append("watchlist_opposed_veto")
    blockers = list(dict.fromkeys(blockers))
    action = "REVIEW" if is_tradeable and not blockers else "NO_TRADE"
    copy_confirmation = classify_copy_confirmation(event)
    risk_controls = build_ticket_risk_controls(
        event,
        market=market,
        copy_confirmation=copy_confirmation,
        reference_date=reference_date,
    )
    return {
        "action": action,
        "event_slug": event.get("event_slug"),
        "event_title": event.get("event_title"),
        "event_date": event.get("event_date"),
        "city": event.get("city"),
        "station_code": event.get("station_code"),
        "question": market.get("question") or event.get("top_edge_question"),
        "market_id": market.get("market_id"),
        "market_slug": market.get("market_slug"),
        "execution_price": to_float(market.get("execution_price")),
        "fair_probability": to_float(market.get("fair_probability")),
        "market_probability": to_float(market.get("market_probability")),
        "edge_net": to_float(market.get("edge_net") if market.get("edge_net") is not None else event.get("top_edge_net")),
        "quality_tier": market.get("quality_tier") or event.get("top_edge_quality_tier"),
        "temperature_unit": event.get("temperature_unit"),
        "market_family": infer_ticket_market_family(event, market),
        "horizon_days": infer_ticket_horizon_days(event, reference_date=reference_date),
        "forecast": {
            "center_c": event.get("forecast_center_c"),
            "mode_c": event.get("forecast_mode_c"),
            "std_dev_c": event.get("forecast_std_dev_c"),
            "model_name": event.get("forecast_model_name"),
        },
        "watchlist": {
            "signal": event.get("watchlist_signal"),
            "copy_confirmation": copy_confirmation,
            "alignment_score": event.get("watchlist_alignment_score"),
            "active_traders": event.get("watchlist_active_traders", []),
            "aligned_traders": event.get("watchlist_aligned_traders", []),
            "opposed_traders": event.get("watchlist_opposed_traders", []),
            "copy_flow": summarize_event_copy_flow(event),
        },
        "risk_controls": risk_controls,
        "blockers": blockers,
        "stake_suggestion_usd": compute_stake_suggestion(
            event,
            market=market,
            budget_usd=budget_usd,
            blockers=blockers,
            copy_confirmation=copy_confirmation,
            risk_controls=risk_controls,
        ),
        "approval_command": "OK {ticket_id}",
    }


def classify_copy_confirmation(event: dict) -> str:
    aligned = bool(event.get("watchlist_aligned_traders"))
    opposed = bool(event.get("watchlist_opposed_traders"))
    active = bool(event.get("watchlist_active_traders"))
    if aligned and opposed:
        return "conflicted"
    if aligned:
        return "confirmed"
    if opposed:
        return "opposed"
    if active:
        return "active_unclassified"
    return "silent"


def summarize_event_copy_flow(event: dict) -> list[dict]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "trade_count": 0,
            "gross_notional_usd": 0.0,
            "net_yes_notional_usd": 0.0,
            "classifications": Counter(),
            "latest_timestamp": None,
        }
    )
    for trade in event.get("watchlist_trades", []):
        label = str(trade.get("label") or trade.get("username") or trade.get("proxy_wallet") or "unknown")
        price = to_float(trade.get("price"))
        size = abs(to_float(trade.get("size")))
        notional = price * size if price and size else size
        bias = infer_yes_bias(trade)
        row = grouped[label]
        row["trade_count"] += 1
        row["gross_notional_usd"] += notional
        row["net_yes_notional_usd"] += notional * bias
        row["classifications"].update([str(trade.get("classification") or "unknown")])
        timestamp = trade.get("timestamp")
        if timestamp is not None and (row["latest_timestamp"] is None or int(timestamp) > int(row["latest_timestamp"])):
            row["latest_timestamp"] = int(timestamp)

    output = []
    for label, row in grouped.items():
        net_yes = float(row["net_yes_notional_usd"])
        output.append(
            {
                "trader": label,
                "trade_count": row["trade_count"],
                "gross_notional_usd": round(float(row["gross_notional_usd"]), 4),
                "net_yes_notional_usd": round(net_yes, 4),
                "direction": "YES" if net_yes > 0 else ("NO" if net_yes < 0 else "FLAT"),
                "classifications": dict(row["classifications"]),
                "latest_timestamp": row["latest_timestamp"],
            }
        )
    output.sort(key=lambda item: abs(float(item["net_yes_notional_usd"])), reverse=True)
    return output


def compute_stake_suggestion(
    event: dict,
    *,
    market: dict,
    budget_usd: float,
    blockers: list[str],
    copy_confirmation: str | None = None,
    risk_controls: dict | None = None,
) -> float:
    if blockers or not bool(market.get("is_tradeable")) or not bool(event.get("event_operable")):
        return 0.0
    edge = max(0.0, to_float(market.get("edge_net") if market.get("edge_net") is not None else event.get("top_edge_net")))
    quality_multiplier = {"A": 1.0, "B": 0.65, "C": 0.35, "D": 0.15}.get(str(market.get("quality_tier") or ""), 0.25)
    confirmation = copy_confirmation or classify_copy_confirmation(event)
    watchlist_multiplier = {
        "confirmed": 1.0,
        "conflicted": 0.50,
        "silent": 0.50,
        "active_unclassified": 0.35,
        "opposed": 0.0,
    }.get(confirmation, 0.35)
    edge_multiplier = min(1.0, max(0.25, edge / 0.20))
    controls = risk_controls or build_ticket_risk_controls(
        event,
        market=market,
        copy_confirmation=confirmation,
    )
    risk_multiplier = to_float(controls.get("combined_multiplier"))
    max_stake = to_float(controls.get("max_stake_usd")) or budget_usd
    raw_stake = budget_usd * quality_multiplier * watchlist_multiplier * edge_multiplier * risk_multiplier
    return round(max(0.0, min(budget_usd, max_stake, raw_stake)), 2)


def build_operator_risk_blockers(event: dict, *, reference_date: str | None = None) -> list[str]:
    blockers: list[str] = []
    if should_block_silent_watchlist_signal(event):
        blockers.append("watchlist_silent_blocked")
    horizon_days = infer_ticket_horizon_days(event, reference_date=reference_date)
    if horizon_days is None:
        return blockers
    if horizon_days < get_min_trade_horizon_days():
        blockers.append("below_min_trade_horizon")
    if horizon_days != 0:
        return blockers

    policy = str(ConfigLoader.get("operator_risk.horizon0_intraday_policy", "cap") or "cap").lower()
    if policy == "block":
        blockers.append("same_day_intraday_blocked")
        return blockers

    max_remaining = ConfigLoader.get("operator_risk.horizon0_intraday_block_if_remaining_hours_exceeds", None)
    remaining_hours = event.get("intraday_remaining_hours")
    if max_remaining is not None and remaining_hours is not None and int(remaining_hours) > int(max_remaining):
        blockers.append("same_day_intraday_too_early")

    if bool(ConfigLoader.get("operator_risk.horizon0_block_without_local_observed_max_when_late", True)):
        late_threshold = int(ConfigLoader.get("operator_risk.horizon0_late_remaining_hours_threshold", 6) or 6)
        intraday_source = str(event.get("intraday_source") or "")
        if remaining_hours is not None and int(remaining_hours) <= late_threshold and intraday_source != "local_weather_observations":
            blockers.append("same_day_intraday_without_local_observed_max")

    return blockers


def should_block_silent_watchlist_signal(event: dict) -> bool:
    if not bool(ConfigLoader.get("operator_risk.block_silent_watchlist_signal", False)):
        return False
    return str(event.get("watchlist_signal") or "").strip().lower() == "silent"


def build_ticket_risk_controls(
    event: dict,
    *,
    market: dict,
    copy_confirmation: str,
    reference_date: str | None = None,
) -> dict:
    market_family = infer_ticket_market_family(event, market)
    fair_probability = to_float(market.get("fair_probability"))
    execution_price = to_float(market.get("execution_price"))
    horizon_days = infer_ticket_horizon_days(event, reference_date=reference_date)
    multipliers: dict[str, float] = {}
    notes: list[str] = []
    max_stake_usd = float(ConfigLoader.get("operator_risk.default_max_stake_usd", 999999.0) or 999999.0)

    if horizon_days == 0:
        multipliers["same_day_intraday"] = float(
            ConfigLoader.get("operator_risk.horizon0_intraday_stake_multiplier", 0.20) or 0.20
        )
        max_stake_usd = min(
            max_stake_usd,
            float(ConfigLoader.get("operator_risk.horizon0_intraday_max_stake_usd", 1.0) or 1.0),
        )
        notes.append("same_day_intraday_exposure_capped")

    if market_family == "fahrenheit|range_bin":
        multipliers["fahrenheit_range_bin"] = float(
            ConfigLoader.get("operator_risk.fahrenheit_range_bin_stake_multiplier", 0.55) or 0.55
        )
        max_stake_usd = min(
            max_stake_usd,
            float(ConfigLoader.get("operator_risk.fahrenheit_range_bin_max_stake_usd", 5.0) or 5.0),
        )
        notes.append("fahrenheit_range_bin_exposure_reduced")
    elif str(market_family).startswith("fahrenheit|"):
        multipliers["fahrenheit_other"] = float(
            ConfigLoader.get("operator_risk.fahrenheit_other_stake_multiplier", 0.75) or 0.75
        )
        notes.append("fahrenheit_exposure_reduced")

    extreme_threshold = float(ConfigLoader.get("operator_risk.extreme_fair_probability_threshold", 0.85) or 0.85)
    if fair_probability >= extreme_threshold or (0 < fair_probability <= (1.0 - extreme_threshold)):
        multipliers["extreme_fair_probability"] = float(
            ConfigLoader.get("operator_risk.extreme_probability_stake_multiplier", 0.65) or 0.65
        )
        notes.append("overconfidence_guardrail")

    tail_price_threshold = float(ConfigLoader.get("operator_risk.tail_price_threshold", 0.03) or 0.03)
    high_price_threshold = 1.0 - tail_price_threshold
    if 0 < execution_price <= tail_price_threshold or execution_price >= high_price_threshold:
        multipliers["tail_price"] = float(ConfigLoader.get("operator_risk.tail_price_stake_multiplier", 0.75) or 0.75)
        notes.append("tail_price_exposure_reduced")

    if copy_confirmation == "conflicted":
        notes.append("copytrading_conflict_keeps_half_size")
    elif copy_confirmation == "opposed":
        notes.append("opposed_copy_signal_blocks_ticket")

    combined = 1.0
    for multiplier in multipliers.values():
        combined *= multiplier

    return {
        "market_family": market_family,
        "horizon_days": horizon_days,
        "fair_probability": fair_probability,
        "execution_price": execution_price,
        "combined_multiplier": round(combined, 6),
        "max_stake_usd": max_stake_usd,
        "multipliers": multipliers,
        "notes": notes,
    }


def infer_ticket_market_family(event: dict, market: dict) -> str:
    explicit = event.get("top_edge_market_family")
    if explicit:
        return str(explicit)
    unit = str(event.get("temperature_unit") or infer_temperature_unit_from_question(str(market.get("question") or "")))
    shape = infer_bin_shape_from_question(str(market.get("question") or ""))
    return f"{unit}|{shape}"


def infer_ticket_horizon_days(event: dict, *, reference_date: str | None = None) -> int | None:
    if event.get("horizon_days") is not None:
        try:
            return int(event["horizon_days"])
        except (TypeError, ValueError):
            return None
    if not reference_date or not event.get("event_date"):
        return None
    event_date = parse_date_safe(str(event.get("event_date")))
    as_of_date = parse_date_safe(str(reference_date))
    if event_date is None or as_of_date is None:
        return None
    return (event_date - as_of_date).days


def infer_temperature_unit_from_question(question: str) -> str:
    normalized = question.lower()
    if "°f" in normalized or " fahrenheit" in normalized or " f " in normalized:
        return "fahrenheit"
    if "°c" in normalized or " celsius" in normalized or " c " in normalized:
        return "celsius"
    return "unknown"


def infer_bin_shape_from_question(question: str) -> str:
    normalized = question.lower()
    if "between" in normalized or "-" in normalized:
        return "range_bin"
    if "or higher" in normalized or "or above" in normalized or "or more" in normalized:
        return "upper_tail"
    if "or below" in normalized or "or lower" in normalized or "or less" in normalized:
        return "lower_tail"
    return "range_bin"


def persist_dashboard(root: Path, reference_date: str, payload: dict) -> Path:
    output_dir = root / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date}_operator_console.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def append_decision(root: Path, reference_date: str, ticket: dict, *, budget_usd: float) -> Path:
    output_dir = root / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date}_operator_decisions.jsonl"
    record = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "reference_date": reference_date,
        "budget_usd": budget_usd,
        "decision": "approved_paper_ticket",
        "ticket": ticket,
        "live_execution_enabled": False,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def render_text(payload: dict) -> str:
    lines = []
    lines.append("=== WEATHERTRADING OPERATOR CONSOLE ===")
    lines.append(f"Fecha: {payload['reference_date']} | budget/ticket: ${payload['budget_usd']:.2f}")
    readiness = payload["readiness"]
    pipeline = payload["pipeline"]
    lines.append(
        f"Pipeline: {pipeline.get('overall_status') or 'missing'} | "
        f"Readiness: {readiness.get('status') or 'missing'} / {readiness.get('recommended_mode') or 'unknown'}"
    )
    preflight = payload.get("preflight", {})
    lines.append(
        f"Preflight: {preflight.get('status') or 'missing'} | "
        f"approval_allowed={preflight.get('approval_allowed')}"
    )
    execution_policy = payload.get("execution_policy") or {}
    lines.append(
        f"Policy: {execution_policy.get('trade_horizon_label') or 'unknown'} only | "
        f"H0={execution_policy.get('horizon0_mode') or 'unknown'} | "
        f"copytrading={execution_policy.get('copytrading_mode') or 'unknown'} | "
        f"live={execution_policy.get('live_execution_enabled')}"
    )
    if preflight.get("warnings"):
        lines.append(f"Preflight warnings: {', '.join(preflight['warnings'])}")
    if preflight.get("blockers"):
        lines.append(f"Preflight blockers: {', '.join(preflight['blockers'])}")
    blockers = readiness.get("blockers") or []
    warnings = readiness.get("warnings") or []
    lines.append(f"Blockers: {', '.join(blockers) if blockers else 'none'}")
    lines.append(f"Warnings: {', '.join(warnings) if warnings else 'none'}")
    db = readiness.get("database_health") or {}
    if db:
        lines.append(
            f"DB: {db.get('status')} | obs={db.get('observation_count')} | "
            f"latest={db.get('latest_observed_at_utc')} | lag_days={db.get('observation_lag_days')}"
        )
    system_health = payload.get("system_health", {})
    if system_health:
        lines.append(
            f"Scheduler: {system_health.get('status')} | "
            f"latest_ok={system_health.get('latest_ok_pipeline_date') or 'n/a'} | "
            f"missing_dates={', '.join(system_health.get('missing_pipeline_dates') or []) or 'none'}"
        )
        if system_health.get("warnings"):
            lines.append(f"Scheduler warnings: {', '.join(system_health['warnings'])}")
        launchd_status = system_health.get("launchd_status") or []
        if launchd_status:
            launchd_parts = []
            for item in launchd_status:
                label = str(item.get("label") or "").replace("com.weathertrading.evo.", "")
                exit_code = item.get("last_exit_code")
                exit_text = "n/a" if exit_code is None else str(exit_code)
                launchd_parts.append(f"{label}:{item.get('status')}({exit_text})")
            lines.append(f"Launchd: {', '.join(launchd_parts)}")

    audit = payload["audit"]
    if audit.get("events") is not None:
        lines.append("")
        lines.append(
            "Audit: "
            f"events={audit.get('events')} | "
            f"mode_hit model={format_pct(audit.get('model_mode_hit_rate'))} vs market={format_pct(audit.get('market_mode_hit_rate'))} | "
            f"log_loss model={format_num(audit.get('model_log_loss'))} vs market={format_num(audit.get('market_log_loss'))} | "
            f"paper_pnl={format_signed(audit.get('paper_total_pnl'))}"
        )
        horizon_summaries = audit.get("by_horizon_days") or {}
        if horizon_summaries:
            horizon_parts = []
            for horizon in ("0", "1"):
                horizon_summary = horizon_summaries.get(horizon)
                if not horizon_summary:
                    continue
                horizon_parts.append(
                    f"H{horizon}: roi={format_pct(horizon_summary.get('paper_roi_on_stake'))} "
                    f"LL={format_num(horizon_summary.get('model_log_loss'))}/{format_num(horizon_summary.get('market_log_loss'))}"
                )
            if horizon_parts:
                lines.append("Horizon split: " + " | ".join(horizon_parts))

    lines.append("")
    lines.append("Copytrading sizing:")
    for username, sizing in payload["copytrading_size_guidance"].items():
        lines.append(
            f"- {username}: median=${format_num(sizing.get('median_notional_usd'))}, "
            f"avg=${format_num(sizing.get('avg_notional_usd'))}, "
            f"$budget/median={format_num(sizing.get('budget_vs_median_notional'))}x, "
            f"$budget/avg={format_num(sizing.get('budget_vs_avg_notional'))}x, "
            f"same_day={format_pct(sizing.get('same_day_share'))}"
        )
        lines.append(f"  note: {sizing.get('sizing_note')}")

    watchlist = payload["watchlist_strategy"]
    selected = watchlist.get("selected_strategies", {})
    lines.append("")
    lines.append("Strategy comparison:")
    for name, metrics in selected.items():
        if not metrics:
            continue
        lines.append(
            f"- {name}: trades={metrics.get('trades')} | "
            f"hit={format_pct(metrics.get('selected_market_hit_rate'))} | "
            f"pnl={format_signed(metrics.get('total_pnl'))} | "
            f"roi={format_pct(metrics.get('roi_on_stake'))}"
        )

    lines.append("")
    lines.append("Tickets:")
    if not payload["tickets"]:
        lines.append("- No hay tickets candidatos.")
    for ticket in payload["tickets"]:
        lines.append(
            f"[{ticket['ticket_id']}] {ticket['action']} | {ticket.get('city')} | "
            f"stake=${ticket['stake_suggestion_usd']:.2f} | edge={format_pct(ticket.get('edge_net'))} | "
            f"price={format_num(ticket.get('execution_price'))} | q={ticket.get('quality_tier')} | "
            f"h={ticket.get('horizon_days')} | family={ticket.get('market_family')} | "
            f"watch={ticket['watchlist'].get('signal')} / copy={ticket['watchlist'].get('copy_confirmation')}"
        )
        lines.append(f"  {ticket.get('question')}")
        risk_notes = ticket.get("risk_controls", {}).get("notes", [])
        if risk_notes:
            lines.append(
                f"  risk: x{format_num(ticket.get('risk_controls', {}).get('combined_multiplier'))} | "
                f"{', '.join(risk_notes)}"
            )
        copy_flow = ticket["watchlist"].get("copy_flow", [])
        if copy_flow:
            flow_text = "; ".join(
                f"{flow['trader']} {flow['direction']} net=${flow['net_yes_notional_usd']:.2f} gross=${flow['gross_notional_usd']:.2f}"
                for flow in copy_flow[:3]
            )
            lines.append(f"  copy_flow: {flow_text}")
        if ticket.get("blockers"):
            lines.append(f"  blockers: {', '.join(ticket['blockers'])}")
    lines.append("")
    lines.append("Para registrar un OK local: venv/bin/python scripts/run_operator_console.py --interactive --budget-usd 10")
    return "\n".join(lines)


def run_interactive(payload: dict) -> Path | None:
    reviewable = [ticket for ticket in payload["tickets"] if ticket["action"] == "REVIEW" and ticket["stake_suggestion_usd"] > 0]
    if not reviewable:
        print("No hay tickets REVIEW aprobables en esta consola.")
        return None
    print("")
    print("Para aprobar en paper/dry-run, escribe exactamente: OK <ticket_id>. Enter cancela.")
    command = input("> ").strip()
    if not command:
        print("Sin aprobacion registrada.")
        return None
    parts = command.split()
    if len(parts) != 2 or parts[0].upper() != "OK":
        print("Comando no reconocido. No se registra nada.")
        return None
    selected_id = parts[1].upper()
    ticket = next((item for item in reviewable if item["ticket_id"].upper() == selected_id), None)
    if ticket is None:
        print(f"Ticket {selected_id} no es aprobable.")
        return None
    path = append_decision(ROOT, payload["reference_date"], ticket, budget_usd=float(payload["budget_usd"]))
    print(f"Decision paper registrada en: {path.relative_to(ROOT)}")
    return path


def to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def format_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{to_float(value) * 100:.1f}%"


def format_num(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{to_float(value):.3f}"


def format_signed(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{to_float(value):+.3f}"


def main() -> None:
    args = parse_args()
    payload = build_operator_dashboard(
        root=ROOT,
        reference_date=str(args.reference_date),
        budget_usd=float(args.budget_usd),
        max_tickets=int(args.max_tickets),
    )

    output_path = persist_dashboard(ROOT, payload["reference_date"], payload) if args.export else None
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(render_text(payload))
    if output_path is not None:
        print("")
        print(f"Dashboard guardado en: {output_path.relative_to(ROOT)}")
    if args.interactive:
        run_interactive(payload)


if __name__ == "__main__":
    main()
