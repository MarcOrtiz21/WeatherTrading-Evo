from __future__ import annotations

import argparse
import errno
import json
import subprocess
import sys
import urllib.parse
import webbrowser
from datetime import date
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from scripts.run_operator_console import (
    append_decision,
    build_operator_dashboard,
    persist_dashboard,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="UI local rapida para la consola operativa de WeatherTrading.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--reference-date", default="latest")
    parser.add_argument("--budget-usd", type=float, default=10.0)
    parser.add_argument("--max-tickets", type=int, default=12)
    parser.add_argument("--no-open", action="store_true", help="No abrir navegador automaticamente.")
    parser.add_argument("--print-html", action="store_true", help="Imprime el HTML y sale, util para tests/manual review.")
    return parser.parse_args()


def build_operator_ui_html() -> str:
    return r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WeatherTrading Operator</title>
  <style>
    :root {
      --bg: #f4efe3;
      --ink: #17212b;
      --muted: #65707d;
      --panel: rgba(255, 252, 243, 0.92);
      --panel-strong: #fffaf0;
      --line: rgba(23, 33, 43, 0.12);
      --good: #1f7a4d;
      --warn: #a86413;
      --bad: #a93b32;
      --blue: #285c7d;
      --shadow: 0 18px 50px rgba(33, 31, 24, 0.12);
      --radius: 22px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Trebuchet MS", Verdana, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(40, 92, 125, 0.16), transparent 34rem),
        radial-gradient(circle at 82% 8%, rgba(168, 100, 19, 0.12), transparent 28rem),
        linear-gradient(135deg, #f7f1e4 0%, #eee6d3 100%);
      min-height: 100vh;
    }

    button, input, select {
      font: inherit;
    }

    .app {
      width: min(1480px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0 40px;
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 18px;
      align-items: end;
      margin-bottom: 18px;
    }

    .eyebrow {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 12px;
      font-weight: 800;
      margin-bottom: 8px;
    }

    h1 {
      font-size: clamp(34px, 4vw, 68px);
      line-height: 0.94;
      margin: 0;
      letter-spacing: -0.055em;
    }

    .subtitle {
      color: var(--muted);
      max-width: 760px;
      margin-top: 12px;
      font-size: 16px;
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 10px;
    }

    .control {
      display: grid;
      gap: 5px;
      min-width: 112px;
    }

    .control span {
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    input, select {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 252, 243, 0.78);
      padding: 10px 11px;
      color: var(--ink);
      outline: none;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.45);
    }

    .btn {
      border: 0;
      border-radius: 16px;
      padding: 11px 14px;
      background: var(--ink);
      color: #fffaf0;
      font-weight: 900;
      cursor: pointer;
      transition: transform 140ms ease, opacity 140ms ease;
    }

    .btn:hover { transform: translateY(-1px); }
    .btn:disabled { opacity: 0.45; cursor: not-allowed; transform: none; }
    .btn.secondary { color: var(--ink); background: #e5d8bd; }
    .btn.good { background: var(--good); }
    .btn.bad { background: var(--bad); }

    .grid {
      display: grid;
      grid-template-columns: 1.15fr 1.85fr 1fr;
      gap: 16px;
      align-items: start;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
      overflow: hidden;
    }

    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 17px 18px 11px;
      border-bottom: 1px solid var(--line);
    }

    .panel-title {
      font-size: 15px;
      font-weight: 950;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .panel-body { padding: 16px 18px 18px; }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }

    .kpi {
      border-radius: 18px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
      padding: 14px;
      min-height: 92px;
    }

    .kpi .label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: 0.09em;
    }

    .kpi .value {
      margin-top: 8px;
      font-size: 28px;
      letter-spacing: -0.045em;
      font-weight: 950;
    }

    .kpi .hint {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
    }

    .chip-row { display: flex; flex-wrap: wrap; gap: 7px; }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 6px 9px;
      font-size: 12px;
      font-weight: 850;
      background: rgba(23, 33, 43, 0.08);
      color: var(--ink);
      border: 1px solid var(--line);
    }
    .chip.good { background: rgba(31, 122, 77, 0.12); color: var(--good); }
    .chip.warn { background: rgba(168, 100, 19, 0.14); color: var(--warn); }
    .chip.bad { background: rgba(169, 59, 50, 0.13); color: var(--bad); }
    .chip.blue { background: rgba(40, 92, 125, 0.13); color: var(--blue); }

    .ticket-tools {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto auto;
      gap: 8px;
      margin-bottom: 12px;
    }

    .tickets {
      display: grid;
      gap: 10px;
      max-height: calc(100vh - 292px);
      overflow: auto;
      padding-right: 4px;
    }

    .ticket {
      border: 1px solid var(--line);
      background: rgba(255, 252, 243, 0.78);
      border-radius: 20px;
      padding: 14px;
      display: grid;
      gap: 10px;
    }

    .ticket-top {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      gap: 10px;
      align-items: start;
    }

    .ticket-id {
      border-radius: 13px;
      background: var(--ink);
      color: #fffaf0;
      padding: 8px 9px;
      font-weight: 950;
      letter-spacing: -0.04em;
    }

    .ticket h3 {
      margin: 0;
      font-size: 17px;
      line-height: 1.18;
      letter-spacing: -0.02em;
    }

    .ticket .meta {
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
    }

    .stake {
      text-align: right;
      font-weight: 950;
      font-size: 24px;
      letter-spacing: -0.05em;
    }

    .stake small {
      display: block;
      color: var(--muted);
      font-size: 10px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .ticket-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .copy-flow {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
    }

    .strategy, .trader {
      display: grid;
      gap: 7px;
      padding: 13px 0;
      border-bottom: 1px solid var(--line);
    }

    .strategy:last-child, .trader:last-child { border-bottom: 0; }
    .row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
    }

    .row strong { font-weight: 950; }
    .muted { color: var(--muted); }
    .mono { font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; }
    .danger { color: var(--bad); }
    .ok { color: var(--good); }
    .warn-text { color: var(--warn); }
    .hidden { display: none !important; }

    .toast {
      position: fixed;
      right: 20px;
      bottom: 20px;
      max-width: min(420px, calc(100vw - 40px));
      background: var(--ink);
      color: #fffaf0;
      border-radius: 18px;
      padding: 14px 16px;
      box-shadow: var(--shadow);
      transform: translateY(20px);
      opacity: 0;
      pointer-events: none;
      transition: all 180ms ease;
      z-index: 20;
    }
    .toast.show { opacity: 1; transform: translateY(0); }

    @media (max-width: 1120px) {
      .grid { grid-template-columns: 1fr; }
      .tickets { max-height: none; }
      .hero { grid-template-columns: 1fr; }
      .toolbar { justify-content: flex-start; }
    }

    @media (max-width: 720px) {
      .app { width: min(100vw - 18px, 1480px); padding-top: 12px; }
      .toolbar, .ticket-tools { grid-template-columns: 1fr; display: grid; }
      .ticket-top { grid-template-columns: 1fr; }
      .stake { text-align: left; }
      .kpi-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="app">
    <section class="hero">
      <div>
        <div class="eyebrow">WeatherTrading Evo</div>
        <h1>Operator Desk</h1>
        <div class="subtitle">Una pantalla local para decidir rapido: estado del sistema, evidencia, copytrading y tickets paper. No ejecuta ordenes reales.</div>
      </div>
      <div class="toolbar">
        <label class="control"><span>Fecha</span><input id="referenceDate" value="latest" /></label>
        <label class="control"><span>Budget</span><input id="budgetUsd" type="number" min="1" step="1" value="10" /></label>
        <label class="control"><span>Tickets</span><input id="maxTickets" type="number" min="1" max="40" step="1" value="12" /></label>
        <button id="refreshBtn" class="btn">Refresh</button>
        <button id="recoverBtn" class="btn secondary">Recover</button>
        <button id="exportBtn" class="btn secondary">Export</button>
      </div>
    </section>

    <section class="grid">
      <aside class="panel">
        <div class="panel-head">
          <div class="panel-title">System</div>
          <span id="modeChip" class="chip">loading</span>
        </div>
        <div class="panel-body">
          <div class="kpi-grid">
            <div class="kpi"><div class="label">Preflight</div><div id="preflightValue" class="value">-</div><div id="preflightHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">Readiness</div><div id="readinessValue" class="value">-</div><div id="readinessHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">Pipeline</div><div id="pipelineValue" class="value">-</div><div id="pipelineHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">Scheduler</div><div id="schedulerValue" class="value">-</div><div id="schedulerHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">Policy</div><div id="policyValue" class="value">-</div><div id="policyHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">DB Lag</div><div id="dbLagValue" class="value">-</div><div id="dbHint" class="hint">-</div></div>
            <div class="kpi"><div class="label">Paper PnL</div><div id="pnlValue" class="value">-</div><div id="auditHint" class="hint">-</div></div>
          </div>
          <div style="height: 14px"></div>
          <div id="blockerChips" class="chip-row"></div>
        </div>
      </aside>

      <section class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title">Tickets</div>
            <div id="ticketCount" class="muted" style="font-size: 12px; margin-top: 4px;">-</div>
          </div>
          <span id="lastUpdated" class="chip blue">-</span>
        </div>
        <div class="panel-body">
          <div class="ticket-tools">
            <input id="searchBox" placeholder="Filtrar por ciudad, pregunta, trader..." />
            <select id="actionFilter">
              <option value="all">Todos</option>
              <option value="REVIEW">Review</option>
              <option value="NO_TRADE">No trade</option>
            </select>
            <select id="sortBy">
              <option value="stake">Stake</option>
              <option value="edge">Edge</option>
              <option value="copy">Copy</option>
            </select>
          </div>
          <div id="tickets" class="tickets"></div>
        </div>
      </section>

      <aside class="panel">
        <div class="panel-head">
          <div class="panel-title">Copy & Edge</div>
          <span id="copyChip" class="chip warn">supervised</span>
        </div>
        <div class="panel-body">
          <div id="traders"></div>
          <div style="height: 12px"></div>
          <div class="panel-title" style="font-size: 12px;">Strategies</div>
          <div id="strategies"></div>
        </div>
      </aside>
    </section>
  </main>

  <div id="toast" class="toast"></div>

  <script>
    let dashboard = null;

    const $ = (id) => document.getElementById(id);
    const pct = (v) => v === null || v === undefined ? "n/a" : `${(Number(v) * 100).toFixed(1)}%`;
    const num = (v, digits = 2) => v === null || v === undefined ? "n/a" : Number(v).toFixed(digits);
    const signed = (v) => v === null || v === undefined ? "n/a" : `${Number(v) >= 0 ? "+" : ""}${Number(v).toFixed(3)}`;

    function chipClass(value) {
      if (!value) return "chip";
      const lower = String(value).toLowerCase();
      if (["ok", "ready", "complete", "review", "confirmed"].includes(lower)) return "chip good";
      if (["blocked", "failed", "no_trade", "opposed"].includes(lower)) return "chip bad";
      if (["paper_only", "warning", "degraded", "conflicted", "mixed"].includes(lower)) return "chip warn";
      return "chip blue";
    }

    function showToast(text) {
      const node = $("toast");
      node.textContent = text;
      node.classList.add("show");
      setTimeout(() => node.classList.remove("show"), 2600);
    }

    function queryParams() {
      return new URLSearchParams({
        reference_date: $("referenceDate").value || "latest",
        budget_usd: $("budgetUsd").value || "10",
        max_tickets: $("maxTickets").value || "12",
      });
    }

    async function refreshDashboard() {
      $("refreshBtn").disabled = true;
      try {
        const res = await fetch(`/api/dashboard?${queryParams().toString()}`);
        if (!res.ok) throw new Error(await res.text());
        dashboard = await res.json();
        $("referenceDate").value = dashboard.reference_date;
        renderDashboard();
        showToast("Dashboard actualizado");
      } catch (err) {
        showToast(`Error: ${err.message}`);
      } finally {
        $("refreshBtn").disabled = false;
      }
    }

    async function exportDashboard() {
      try {
        const res = await fetch("/api/export", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            reference_date: $("referenceDate").value || "latest",
            budget_usd: Number($("budgetUsd").value || 10),
            max_tickets: Number($("maxTickets").value || 12),
          }),
        });
        const payload = await res.json();
        if (!res.ok) throw new Error(payload.error || "export failed");
        showToast(`Exportado: ${payload.path}`);
      } catch (err) {
        showToast(`Error: ${err.message}`);
      }
    }

    async function recoverPipeline() {
      $("recoverBtn").disabled = true;
      showToast("Ejecutando recovery del pipeline...");
      try {
        const res = await fetch("/api/recover", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            reference_date: $("referenceDate").value || "latest",
            budget_usd: Number($("budgetUsd").value || 10),
            max_tickets: Number($("maxTickets").value || 12),
          }),
        });
        const payload = await res.json();
        if (!res.ok) throw new Error(payload.error || "recovery failed");
        dashboard = payload.dashboard;
        $("referenceDate").value = dashboard.reference_date;
        renderDashboard();
        showToast(`Recovery ${payload.recovery.status}: ${payload.recovery.reference_date}`);
      } catch (err) {
        showToast(`Error: ${err.message}`);
      } finally {
        $("recoverBtn").disabled = false;
      }
    }

    async function approveTicket(ticketId) {
      const ticket = dashboard.tickets.find((item) => item.ticket_id === ticketId);
      if (!ticket) return;
      const ok = window.confirm(`Registrar aprobacion paper para ${ticketId} por $${ticket.stake_suggestion_usd.toFixed(2)}?`);
      if (!ok) return;
      try {
        const res = await fetch("/api/decision", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            ticket_id: ticketId,
            reference_date: dashboard.reference_date,
            budget_usd: dashboard.budget_usd,
            max_tickets: dashboard.tickets.length,
          }),
        });
        const payload = await res.json();
        if (!res.ok) throw new Error(payload.error || "decision failed");
        showToast(`Decision registrada: ${payload.path}`);
      } catch (err) {
        showToast(`Error: ${err.message}`);
      }
    }

    function renderDashboard() {
      renderSystem();
      renderTraders();
      renderStrategies();
      renderTickets();
    }

    function renderSystem() {
      const readiness = dashboard.readiness || {};
      const pipeline = dashboard.pipeline || {};
      const audit = dashboard.audit || {};
      const preflight = dashboard.preflight || {};
      const health = dashboard.system_health || {};
      const executionPolicy = dashboard.execution_policy || {};
      const db = readiness.database_health || {};
      $("modeChip").className = chipClass(preflight.status || readiness.status);
      $("modeChip").textContent = preflight.approval_allowed ? "approval ready" : (preflight.status || readiness.recommended_mode || "unknown");
      $("preflightValue").textContent = preflight.status || "missing";
      $("preflightHint").textContent = preflight.approval_allowed ? `${preflight.reviewable_ticket_count || 0} aprobables` : "aprobacion bloqueada";
      $("readinessValue").textContent = readiness.status || "missing";
      $("readinessHint").textContent = (readiness.blockers || []).length ? "bloqueado" : "sin blockers";
      $("pipelineValue").textContent = pipeline.overall_status || "missing";
      $("pipelineHint").textContent = `${(pipeline.steps || []).length} pasos`;
      $("schedulerValue").textContent = health.status || "missing";
      const launchdFailed = (health.launchd_status || []).filter(item => ["failed", "unloaded", "missing_plist", "unknown"].includes(item.status));
      $("schedulerHint").textContent = launchdFailed.length ? `${launchdFailed.length} launchd con fallo` : `latest ok ${health.latest_ok_pipeline_date || "n/a"}`;
      $("policyValue").textContent = executionPolicy.trade_horizon_label || "H1+";
      $("policyHint").textContent = `H0 ${executionPolicy.horizon0_mode || "quarantined"} | copy ${executionPolicy.copytrading_mode || "veto_only"}`;
      $("dbLagValue").textContent = db.observation_lag_days === null || db.observation_lag_days === undefined ? "-" : `${db.observation_lag_days}d`;
      $("dbHint").textContent = `${db.status || "unknown"} | obs=${db.observation_count || 0}`;
      $("pnlValue").textContent = signed(audit.paper_total_pnl);
      $("auditHint").textContent = `LL ${num(audit.model_log_loss, 3)} vs ${num(audit.market_log_loss, 3)}`;
      $("lastUpdated").textContent = dashboard.captured_at_utc ? dashboard.captured_at_utc.slice(11, 19) + " UTC" : "-";

      const chips = [];
      for (const blocker of preflight.blockers || []) chips.push(`<span class="chip bad">preflight:${escapeHtml(blocker)}</span>`);
      for (const warning of preflight.warnings || []) chips.push(`<span class="chip warn">preflight:${escapeHtml(warning)}</span>`);
      for (const blocker of readiness.blockers || []) chips.push(`<span class="chip bad">${escapeHtml(blocker)}</span>`);
      for (const warning of readiness.warnings || []) chips.push(`<span class="chip warn">${escapeHtml(warning)}</span>`);
      for (const blocker of health.blockers || []) chips.push(`<span class="chip bad">scheduler:${escapeHtml(blocker)}</span>`);
      for (const warning of health.warnings || []) chips.push(`<span class="chip warn">scheduler:${escapeHtml(warning)}</span>`);
      for (const item of health.launchd_status || []) {
        if (["ok", "scheduled"].includes(item.status)) continue;
        chips.push(`<span class="chip bad">launchd:${escapeHtml((item.label || "").replace("com.weathertrading.evo.", ""))}:${escapeHtml(item.status || "unknown")}</span>`);
      }
      for (const missing of health.missing_pipeline_dates || []) chips.push(`<span class="chip warn">missing ${escapeHtml(missing)}</span>`);
      if (!chips.length) chips.push(`<span class="chip good">sin blockers</span>`);
      $("blockerChips").innerHTML = chips.join("");
    }

    function renderTraders() {
      const sizing = dashboard.copytrading_size_guidance || {};
      $("traders").innerHTML = Object.entries(sizing).map(([name, item]) => `
        <div class="trader">
          <div class="row"><strong>${escapeHtml(name)}</strong><span class="chip blue">${item.trade_count || 0} trades</span></div>
          <div class="row"><span class="muted">Mediana</span><strong>$${num(item.median_notional_usd, 2)}</strong></div>
          <div class="row"><span class="muted">Media</span><strong>$${num(item.avg_notional_usd, 2)}</strong></div>
          <div class="row"><span class="muted">Budget/median</span><strong>${num(item.budget_vs_median_notional, 2)}x</strong></div>
          <div class="row"><span class="muted">Same day</span><strong>${pct(item.same_day_share)}</strong></div>
          <div class="muted" style="font-size:12px">${escapeHtml(item.sizing_note || "")}</div>
        </div>
      `).join("");
    }

    function renderStrategies() {
      const strategies = (dashboard.watchlist_strategy || {}).selected_strategies || {};
      $("strategies").innerHTML = Object.entries(strategies).filter(([, item]) => item && item.trades !== undefined).map(([name, item]) => `
        <div class="strategy">
          <div class="row"><strong>${escapeHtml(name)}</strong><span>${item.trades} trades</span></div>
          <div class="row"><span class="muted">PnL / ROI</span><strong class="${Number(item.total_pnl) >= 0 ? "ok" : "danger"}">${signed(item.total_pnl)} / ${pct(item.roi_on_stake)}</strong></div>
          <div class="row"><span class="muted">Hit</span><strong>${pct(item.selected_market_hit_rate)}</strong></div>
        </div>
      `).join("");
    }

    function renderTickets() {
      const q = ($("searchBox").value || "").toLowerCase();
      const action = $("actionFilter").value;
      const sortBy = $("sortBy").value;
      let tickets = [...(dashboard.tickets || [])];
      if (action !== "all") tickets = tickets.filter((ticket) => ticket.action === action);
      if (q) {
        tickets = tickets.filter((ticket) => JSON.stringify(ticket).toLowerCase().includes(q));
      }
      tickets.sort((a, b) => {
        if (sortBy === "edge") return Number(b.edge_net || 0) - Number(a.edge_net || 0);
        if (sortBy === "copy") return copyMagnitude(b) - copyMagnitude(a);
        return Number(b.stake_suggestion_usd || 0) - Number(a.stake_suggestion_usd || 0);
      });
      $("ticketCount").textContent = `${tickets.length} visibles / ${(dashboard.tickets || []).length} total`;
      $("tickets").innerHTML = tickets.map(renderTicket).join("") || `<div class="muted">No hay tickets para este filtro.</div>`;
    }

    function renderTicket(ticket) {
      const flow = (((ticket.watchlist || {}).copy_flow) || []).slice(0, 3);
      const blockers = ticket.blockers || [];
      const canApprove = Boolean((dashboard.preflight || {}).approval_allowed) && ticket.action === "REVIEW" && Number(ticket.stake_suggestion_usd || 0) > 0;
      return `
        <article class="ticket">
          <div class="ticket-top">
            <div class="ticket-id">${ticket.ticket_id}</div>
            <div>
              <h3>${escapeHtml(ticket.question || ticket.event_title || ticket.event_slug)}</h3>
              <div class="meta">${escapeHtml(ticket.city || "-")} · ${escapeHtml(ticket.station_code || "-")} · ${escapeHtml(ticket.event_date || "-")}</div>
            </div>
            <div class="stake"><small>stake</small>$${num(ticket.stake_suggestion_usd, 2)}</div>
          </div>
          <div class="chip-row">
            <span class="${chipClass(ticket.action)}">${ticket.action}</span>
            <span class="chip blue">edge ${pct(ticket.edge_net)}</span>
            <span class="chip">price ${num(ticket.execution_price, 3)}</span>
            <span class="chip">Q ${escapeHtml(ticket.quality_tier || "-")}</span>
            <span class="chip">h${ticket.horizon_days === null || ticket.horizon_days === undefined ? "-" : ticket.horizon_days}</span>
            <span class="chip">family ${escapeHtml(ticket.market_family || "-")}</span>
            <span class="chip warn">risk x${num((ticket.risk_controls || {}).combined_multiplier, 2)}</span>
            <span class="${chipClass((ticket.watchlist || {}).copy_confirmation)}">copy ${(ticket.watchlist || {}).copy_confirmation || "-"}</span>
          </div>
          ${((ticket.risk_controls || {}).notes || []).length ? `<div class="chip-row">${(ticket.risk_controls.notes || []).map((item) => `<span class="chip warn">${escapeHtml(item)}</span>`).join("")}</div>` : ""}
          ${flow.length ? `<div class="copy-flow">${flow.map((item) => `<div>${escapeHtml(item.trader)} <strong>${item.direction}</strong> net $${num(item.net_yes_notional_usd, 2)} · gross $${num(item.gross_notional_usd, 2)}</div>`).join("")}</div>` : ""}
          ${blockers.length ? `<div class="chip-row">${blockers.map((item) => `<span class="chip bad">${escapeHtml(item)}</span>`).join("")}</div>` : ""}
          <div class="ticket-actions">
            <button class="btn secondary" onclick='copyTicket("${ticket.ticket_id}")'>Copiar</button>
            <button class="btn good" ${canApprove ? "" : "disabled"} onclick='approveTicket("${ticket.ticket_id}")'>Aprobar paper</button>
          </div>
        </article>
      `;
    }

    async function copyTicket(ticketId) {
      const ticket = dashboard.tickets.find((item) => item.ticket_id === ticketId);
      await navigator.clipboard.writeText(JSON.stringify(ticket, null, 2));
      showToast(`${ticketId} copiado`);
    }

    function copyMagnitude(ticket) {
      return (((ticket.watchlist || {}).copy_flow) || []).reduce((acc, item) => acc + Math.abs(Number(item.net_yes_notional_usd || 0)), 0);
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;",
      })[char]);
    }

    $("refreshBtn").addEventListener("click", refreshDashboard);
    $("recoverBtn").addEventListener("click", recoverPipeline);
    $("exportBtn").addEventListener("click", exportDashboard);
    $("searchBox").addEventListener("input", renderTickets);
    $("actionFilter").addEventListener("change", renderTickets);
    $("sortBy").addEventListener("change", renderTickets);
    refreshDashboard();
  </script>
</body>
</html>"""


def build_dashboard(root: Path, *, reference_date: str, budget_usd: float, max_tickets: int) -> dict:
    return build_operator_dashboard(
        root=root,
        reference_date=reference_date,
        budget_usd=budget_usd,
        max_tickets=max_tickets,
    )


def normalize_recovery_reference_date(value: str) -> str:
    if str(value).strip().lower() == "latest":
        return date.today().isoformat()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return date.today().isoformat()


def run_pipeline_recovery(root: Path, *, reference_date: str, timeout_seconds: int = 180) -> dict:
    normalized_date = normalize_recovery_reference_date(reference_date)
    command = [
        sys.executable,
        str(root / "scripts" / "run_daily_pipeline.py"),
        "--reference-date",
        normalized_date,
    ]
    completed = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    status = "ok" if completed.returncode == 0 else "failed"
    return {
        "status": status,
        "reference_date": normalized_date,
        "exit_code": completed.returncode,
        "command": command,
        "stdout_tail": tail_text(completed.stdout),
        "stderr_tail": tail_text(completed.stderr),
    }


def tail_text(value: str, *, max_lines: int = 20) -> list[str]:
    lines = [line.rstrip() for line in value.splitlines() if line.strip()]
    return lines[-max_lines:]


def find_reviewable_ticket(payload: dict, ticket_id: str) -> dict | None:
    if not bool(payload.get("preflight", {}).get("approval_allowed")):
        return None
    ticket_id = ticket_id.strip().upper()
    for ticket in payload.get("tickets", []):
        if str(ticket.get("ticket_id", "")).upper() != ticket_id:
            continue
        if ticket.get("action") != "REVIEW" or float(ticket.get("stake_suggestion_usd") or 0.0) <= 0:
            return None
        return ticket
    return None


def parse_query_options(query: str, defaults: argparse.Namespace) -> dict:
    params = urllib.parse.parse_qs(query)
    return {
        "reference_date": first_param(params, "reference_date", defaults.reference_date),
        "budget_usd": parse_float(first_param(params, "budget_usd", str(defaults.budget_usd)), defaults.budget_usd),
        "max_tickets": parse_int(first_param(params, "max_tickets", str(defaults.max_tickets)), defaults.max_tickets),
    }


def first_param(params: dict[str, list[str]], key: str, default: str) -> str:
    values = params.get(key)
    if not values:
        return default
    return values[0]


def parse_float(value: str, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def parse_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def create_handler(root: Path, defaults: argparse.Namespace):
    html = build_operator_ui_html().encode("utf-8")

    class OperatorUIHandler(BaseHTTPRequestHandler):
        server_version = "WeatherTradingOperatorUI/0.1"

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/":
                self.send_bytes(html, content_type="text/html; charset=utf-8")
                return
            if parsed.path == "/api/dashboard":
                options = parse_query_options(parsed.query, defaults)
                payload = build_dashboard(root, **options)
                self.send_json(payload)
                return
            if parsed.path == "/api/health":
                self.send_json({"status": "ok"})
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def do_HEAD(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/":
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return
            if parsed.path.startswith("/api/"):
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            body = self.read_json_body()
            if parsed.path == "/api/export":
                payload = build_dashboard(
                    root,
                    reference_date=str(body.get("reference_date") or defaults.reference_date),
                    budget_usd=parse_float(str(body.get("budget_usd") or defaults.budget_usd), defaults.budget_usd),
                    max_tickets=parse_int(str(body.get("max_tickets") or defaults.max_tickets), defaults.max_tickets),
                )
                path = persist_dashboard(root, payload["reference_date"], payload)
                self.send_json({"status": "ok", "path": path.relative_to(root).as_posix()})
                return
            if parsed.path == "/api/recover":
                recovery = run_pipeline_recovery(
                    root,
                    reference_date=str(body.get("reference_date") or defaults.reference_date),
                )
                payload = build_dashboard(
                    root,
                    reference_date=recovery["reference_date"],
                    budget_usd=parse_float(str(body.get("budget_usd") or defaults.budget_usd), defaults.budget_usd),
                    max_tickets=parse_int(str(body.get("max_tickets") or defaults.max_tickets), defaults.max_tickets),
                )
                status = HTTPStatus.OK if recovery["status"] == "ok" else HTTPStatus.INTERNAL_SERVER_ERROR
                self.send_json({"status": recovery["status"], "recovery": recovery, "dashboard": payload}, status=status)
                return
            if parsed.path == "/api/decision":
                payload = build_dashboard(
                    root,
                    reference_date=str(body.get("reference_date") or defaults.reference_date),
                    budget_usd=parse_float(str(body.get("budget_usd") or defaults.budget_usd), defaults.budget_usd),
                    max_tickets=parse_int(str(body.get("max_tickets") or defaults.max_tickets), defaults.max_tickets),
                )
                ticket = find_reviewable_ticket(payload, str(body.get("ticket_id") or ""))
                if ticket is None:
                    self.send_json({"status": "error", "error": "ticket_not_reviewable"}, status=HTTPStatus.BAD_REQUEST)
                    return
                path = append_decision(root, payload["reference_date"], ticket, budget_usd=float(payload["budget_usd"]))
                self.send_json(
                    {
                        "status": "ok",
                        "path": path.relative_to(root).as_posix(),
                        "ticket_id": ticket["ticket_id"],
                    }
                )
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def read_json_body(self) -> dict:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return {}
            return payload if isinstance(payload, dict) else {}

        def send_json(self, payload: dict, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_bytes(data, content_type="application/json; charset=utf-8", status=status)

        def send_bytes(
            self,
            data: bytes,
            *,
            content_type: str,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return OperatorUIHandler


def main() -> None:
    args = parse_args()
    if args.print_html:
        print(build_operator_ui_html())
        return

    url = f"http://{args.host}:{args.port}"
    handler = create_handler(ROOT, args)
    try:
        server = ThreadingHTTPServer((args.host, args.port), handler)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            print(f"WeatherTrading Operator UI ya parece estar activa en: {url}")
            if not args.no_open:
                webbrowser.open(url)
            return
        raise
    print(f"WeatherTrading Operator UI: {url}")
    print("Ctrl+C para cerrar. La UI no ejecuta ordenes reales.")
    if not args.no_open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCerrando Operator UI...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
