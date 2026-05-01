import json
from pathlib import Path

from scripts import run_operator_console


def test_summarize_trader_sizing_uses_budget_ratios() -> None:
    profile = {
        "username": "ColdMath",
        "captured_at_utc": "2026-04-24T10:00:00+00:00",
        "recent_trades_summary": {
            "trade_count": 2000,
            "unique_event_count": 231,
            "avg_notional_usd": 137.94,
            "median_notional_usd": 8.05,
        },
        "timing_summary": {"same_day_share": 0.45},
        "trade_direction_summary": {"yes_buy_share": 0.36, "no_buy_share": 0.52},
    }

    sizing = run_operator_console.summarize_trader_sizing(profile, budget_usd=10.0)

    assert sizing["username"] == "ColdMath"
    assert sizing["budget_vs_avg_notional"] == 10.0 / 137.94
    assert sizing["budget_vs_median_notional"] == 10.0 / 8.05
    assert "sesgada" in sizing["sizing_note"]


def test_summarize_event_copy_flow_tracks_yes_and_no_notional() -> None:
    event = {
        "watchlist_trades": [
            {
                "label": "ColdMath",
                "outcome": "yes",
                "side": "buy",
                "price": 0.4,
                "size": 10,
                "classification": "aligned",
                "timestamp": 1,
            },
            {
                "label": "ColdMath",
                "outcome": "no",
                "side": "buy",
                "price": 0.3,
                "size": 20,
                "classification": "opposed",
                "timestamp": 2,
            },
        ]
    }

    flow = run_operator_console.summarize_event_copy_flow(event)

    assert flow == [
        {
            "trader": "ColdMath",
            "trade_count": 2,
            "gross_notional_usd": 10.0,
            "net_yes_notional_usd": -2.0,
            "direction": "NO",
            "classifications": {"aligned": 1, "opposed": 1},
            "latest_timestamp": 2,
        }
    ]


def test_build_trade_ticket_blocks_opposed_veto() -> None:
    event = {
        "event_slug": "e1",
        "event_operable": False,
        "top_edge_tradeable": False,
        "watchlist_veto_applied": True,
        "event_blockers": ["watchlist_opposed_veto"],
        "watchlist_signal": "opposed",
        "watchlist_trades": [],
    }
    market = {
        "market_id": "m1",
        "question": "Q",
        "is_tradeable": True,
        "execution_price": 0.2,
        "fair_probability": 0.5,
        "market_probability": 0.2,
        "edge_net": 0.25,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(event, market=market, budget_usd=10.0)

    assert ticket["action"] == "NO_TRADE"
    assert ticket["stake_suggestion_usd"] == 0.0
    assert ticket["blockers"] == ["watchlist_opposed_veto"]


def test_build_trade_ticket_suggests_scaled_review_stake() -> None:
    event = {
        "event_slug": "e1",
        "event_operable": True,
        "top_edge_tradeable": True,
        "watchlist_veto_applied": False,
        "event_blockers": [],
        "watchlist_signal": "mixed",
        "watchlist_aligned_traders": ["Poligarch"],
        "watchlist_opposed_traders": ["ColdMath"],
        "watchlist_trades": [],
    }
    market = {
        "market_id": "m1",
        "question": "Q",
        "is_tradeable": True,
        "execution_price": 0.2,
        "fair_probability": 0.5,
        "market_probability": 0.2,
        "edge_net": 0.1,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(event, market=market, budget_usd=10.0)

    assert ticket["action"] == "REVIEW"
    assert ticket["watchlist"]["copy_confirmation"] == "conflicted"
    assert ticket["stake_suggestion_usd"] == 2.5


def test_build_trade_ticket_reduces_fahrenheit_range_bin_exposure() -> None:
    event = {
        "event_slug": "e1",
        "event_operable": True,
        "top_edge_tradeable": True,
        "event_blockers": [],
        "watchlist_signal": "confirmed",
        "watchlist_aligned_traders": ["Poligarch"],
        "watchlist_opposed_traders": [],
        "top_edge_market_family": "fahrenheit|range_bin",
        "watchlist_trades": [],
    }
    market = {
        "market_id": "m1",
        "question": "Will the highest temperature be between 82-83°F?",
        "is_tradeable": True,
        "execution_price": 0.52,
        "fair_probability": 0.83,
        "market_probability": 0.52,
        "edge_net": 0.31,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(event, market=market, budget_usd=10.0)

    assert ticket["action"] == "REVIEW"
    assert ticket["market_family"] == "fahrenheit|range_bin"
    assert ticket["stake_suggestion_usd"] == 5.0
    assert ticket["risk_controls"]["multipliers"]["fahrenheit_range_bin"] == 0.55
    assert "fahrenheit_range_bin_exposure_reduced" in ticket["risk_controls"]["notes"]


def test_build_trade_ticket_reduces_extreme_probability_and_tail_price() -> None:
    event = {
        "event_slug": "e1",
        "event_operable": True,
        "top_edge_tradeable": True,
        "event_blockers": [],
        "watchlist_signal": "silent",
        "watchlist_trades": [],
        "temperature_unit": "celsius",
    }
    market = {
        "market_id": "m1",
        "question": "Will the highest temperature be 18°C?",
        "is_tradeable": True,
        "execution_price": 0.02,
        "fair_probability": 0.91,
        "market_probability": 0.02,
        "edge_net": 0.7,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(event, market=market, budget_usd=10.0)

    assert ticket["stake_suggestion_usd"] == 2.44
    assert ticket["risk_controls"]["combined_multiplier"] == 0.4875
    assert "overconfidence_guardrail" in ticket["risk_controls"]["notes"]
    assert "tail_price_exposure_reduced" in ticket["risk_controls"]["notes"]


def test_build_trade_ticket_blocks_early_same_day_intraday() -> None:
    event = {
        "event_slug": "e1",
        "event_date": "2026-04-27",
        "event_operable": True,
        "top_edge_tradeable": True,
        "event_blockers": [],
        "watchlist_signal": "confirmed",
        "watchlist_aligned_traders": ["Poligarch"],
        "watchlist_trades": [],
        "intraday_remaining_hours": 8,
        "intraday_source": "hourly_forecast_proxy",
    }
    market = {
        "market_id": "m1",
        "question": "Will the highest temperature be 20°C?",
        "is_tradeable": True,
        "execution_price": 0.4,
        "fair_probability": 0.7,
        "market_probability": 0.4,
        "edge_net": 0.2,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(
        event,
        market=market,
        budget_usd=10.0,
        reference_date="2026-04-27",
    )

    assert ticket["action"] == "NO_TRADE"
    assert ticket["stake_suggestion_usd"] == 0.0
    assert "below_min_trade_horizon" in ticket["blockers"]
    assert "same_day_intraday_blocked" in ticket["blockers"]


def test_build_trade_ticket_blocks_same_day_intraday_even_with_local_observation() -> None:
    event = {
        "event_slug": "e1",
        "event_date": "2026-04-27",
        "event_operable": True,
        "top_edge_tradeable": True,
        "event_blockers": [],
        "watchlist_signal": "confirmed",
        "watchlist_aligned_traders": ["Poligarch"],
        "watchlist_trades": [],
        "intraday_remaining_hours": 3,
        "intraday_source": "local_weather_observations",
    }
    market = {
        "market_id": "m1",
        "question": "Will the highest temperature be 20°C?",
        "is_tradeable": True,
        "execution_price": 0.4,
        "fair_probability": 0.7,
        "market_probability": 0.4,
        "edge_net": 0.25,
        "quality_tier": "A",
        "blockers": [],
    }

    ticket = run_operator_console.build_trade_ticket(
        event,
        market=market,
        budget_usd=10.0,
        reference_date="2026-04-27",
    )

    assert ticket["action"] == "NO_TRADE"
    assert ticket["horizon_days"] == 0
    assert ticket["stake_suggestion_usd"] == 0.0
    assert "below_min_trade_horizon" in ticket["blockers"]
    assert "same_day_intraday_blocked" in ticket["blockers"]
    assert ticket["risk_controls"]["multipliers"]["same_day_intraday"] == 0.2
    assert "same_day_intraday_exposure_capped" in ticket["risk_controls"]["notes"]


def test_build_execution_policy_summary_defaults_to_h1_only_veto_mode() -> None:
    policy = run_operator_console.build_execution_policy_summary()

    assert policy["min_trade_horizon_days"] == 1
    assert policy["trade_horizon_label"] == "H1+"
    assert policy["horizon0_mode"] == "quarantined"
    assert policy["copytrading_mode"] == "veto_only"
    assert policy["live_execution_enabled"] is False


def test_classify_copy_confirmation_prefers_conflicted_when_both_sides_present() -> None:
    assert (
        run_operator_console.classify_copy_confirmation(
            {"watchlist_aligned_traders": ["A"], "watchlist_opposed_traders": ["B"]}
        )
        == "conflicted"
    )
    assert run_operator_console.classify_copy_confirmation({"watchlist_aligned_traders": ["A"]}) == "confirmed"
    assert run_operator_console.classify_copy_confirmation({"watchlist_opposed_traders": ["B"]}) == "opposed"


def test_build_operator_dashboard_reads_fixture_snapshots(tmp_path: Path) -> None:
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True)
    (snapshots / "2026-04-24_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok", "steps": []}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-24_operational_readiness.json").write_text(
        json.dumps({"status": "paper_only", "recommended_mode": "paper_only", "blockers": [], "warnings": []}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-24_blind_snapshot_resolution_audit.json").write_text(
        json.dumps({"summary": {"events": 10, "paper_total_pnl": 1.2}}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-24_polymarket_blind_live_validation.json").write_text(
        json.dumps(
            {
                "as_of_date": "2026-04-24",
                "evaluated_events": [
                    {
                        "event_slug": "e1",
                        "event_operable": True,
                        "top_edge_tradeable": True,
                        "event_blockers": [],
                        "watchlist_signal": "silent",
                        "watchlist_trades": [],
                        "markets": [
                            {
                                "market_id": "m1",
                                "question": "Q",
                                "is_tradeable": True,
                                "execution_price": 0.2,
                                "edge_net": 0.2,
                                "quality_tier": "A",
                                "blockers": [],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = run_operator_console.build_operator_dashboard(
        root=tmp_path,
        reference_date="latest",
        budget_usd=10.0,
        max_tickets=3,
    )

    assert payload["reference_date"] == "2026-04-24"
    assert payload["pipeline"]["overall_status"] == "ok"
    assert payload["tickets"][0]["action"] == "REVIEW"
    assert payload["preflight"]["approval_allowed"] is False
    assert "audit_not_actionable" in payload["preflight"]["blockers"]


def test_build_preflight_allows_approval_when_core_evidence_is_clean() -> None:
    payload = {
        "pipeline": {"overall_status": "ok"},
        "readiness": {
            "status": "paper_only",
            "blockers": [],
            "warnings": [],
            "database_health": {"status": "ok", "observation_lag_days": 1},
        },
        "audit": {"quality": {"is_actionable": True, "classification": "complete"}},
        "live": {"evaluated_events": 3},
        "tickets": [{"action": "REVIEW", "stake_suggestion_usd": 5.0}],
    }

    preflight = run_operator_console.build_preflight_summary(payload)

    assert preflight["status"] == "ok"
    assert preflight["approval_allowed"] is True


def test_build_preflight_blocks_when_pipeline_is_not_ok() -> None:
    payload = {
        "pipeline": {"overall_status": "degraded"},
        "readiness": {
            "status": "paper_only",
            "blockers": [],
            "warnings": [],
            "database_health": {"status": "ok", "observation_lag_days": 1},
        },
        "audit": {"quality": {"is_actionable": True, "classification": "complete"}},
        "live": {"evaluated_events": 3},
        "tickets": [{"action": "REVIEW", "stake_suggestion_usd": 5.0}],
    }

    preflight = run_operator_console.build_preflight_summary(payload)

    assert preflight["status"] == "blocked"
    assert preflight["approval_allowed"] is False
    assert "pipeline_not_ok" in preflight["blockers"]


def test_build_preflight_warns_when_model_log_loss_underperforms_market() -> None:
    payload = {
        "pipeline": {"overall_status": "ok"},
        "system_health": {"status": "ok"},
        "readiness": {
            "status": "ready",
            "blockers": [],
            "warnings": [],
            "database_health": {"status": "ok", "observation_lag_days": 1},
        },
        "audit": {
            "quality": {"is_actionable": True, "classification": "complete"},
            "model_market_log_loss_delta": 0.40,
        },
        "live": {"evaluated_events": 3},
        "tickets": [{"action": "REVIEW", "stake_suggestion_usd": 5.0}],
    }

    preflight = run_operator_console.build_preflight_summary(payload)

    assert preflight["approval_allowed"] is True
    assert preflight["status"] == "warning"
    assert "model_log_loss_underperforms_market" in preflight["warnings"]


def test_build_system_health_detects_pipeline_gaps(tmp_path: Path, monkeypatch) -> None:
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True)
    (snapshots / "2026-04-24_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    (snapshots / "2026-04-26_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(run_operator_console, "discover_automation_status", lambda: [
        {"id": "daily-live-snapshot", "status": "ACTIVE"}
    ])
    monkeypatch.setattr(run_operator_console, "discover_launchd_status", lambda root: [])

    health = run_operator_console.build_system_health(tmp_path, reference_date="2026-04-26")

    assert health["status"] == "warning"
    assert health["latest_ok_pipeline_date"] == "2026-04-26"
    assert health["missing_pipeline_dates"] == ["2026-04-25"]
    assert "pipeline_date_gap_detected" in health["warnings"]


def test_build_system_health_warns_on_failed_launchd(tmp_path: Path, monkeypatch) -> None:
    snapshots = tmp_path / "logs" / "snapshots"
    snapshots.mkdir(parents=True)
    (snapshots / "2026-04-26_daily_pipeline_report.json").write_text(
        json.dumps({"overall_status": "ok"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(run_operator_console, "discover_automation_status", lambda: [
        {"id": "daily-live-snapshot", "status": "ACTIVE"}
    ])
    monkeypatch.setattr(run_operator_console, "discover_launchd_status", lambda root: [
        {"label": "com.weathertrading.evo.daily-pipeline", "status": "failed", "last_exit_code": 127}
    ])

    health = run_operator_console.build_system_health(tmp_path, reference_date="2026-04-26")

    assert health["status"] == "warning"
    assert "launchd_scheduler_not_healthy" in health["warnings"]
    assert health["launchd_status"][0]["last_exit_code"] == 127


def test_summarize_audit_exposes_horizon0_deterioration() -> None:
    audit = {
        "audit_quality": {"is_actionable": True},
        "summary": {
            "model_log_loss": 2.0,
            "market_log_loss": 1.5,
            "by_horizon_days": {
                "0": {
                    "model_log_loss": 5.0,
                    "market_log_loss": 2.0,
                    "paper_roi_on_stake": -0.2,
                },
                "1": {
                    "model_log_loss": 1.4,
                    "market_log_loss": 1.2,
                    "paper_roi_on_stake": 0.4,
                },
            },
        },
    }

    summary = run_operator_console.summarize_audit(audit)

    assert summary["horizon0_model_market_log_loss_delta"] == 3.0
    assert summary["horizon0_paper_roi_on_stake"] == -0.2
    assert summary["horizon1_model_market_log_loss_delta"] == 0.19999999999999996
