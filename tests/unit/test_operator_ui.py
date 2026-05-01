from types import SimpleNamespace

from scripts import run_operator_ui


def test_build_operator_ui_html_contains_app_shell() -> None:
    html = run_operator_ui.build_operator_ui_html()

    assert "Operator Desk" in html
    assert "/api/dashboard" in html
    assert "/api/recover" in html
    assert "Recover" in html
    assert "Aprobar paper" in html
    assert "Copy & Edge" in html
    assert "Policy" in html
    assert "policyValue" in html


def test_parse_query_options_uses_defaults_for_invalid_values() -> None:
    defaults = SimpleNamespace(reference_date="latest", budget_usd=10.0, max_tickets=12)

    options = run_operator_ui.parse_query_options(
        "reference_date=2026-04-24&budget_usd=-5&max_tickets=nope",
        defaults,
    )

    assert options == {
        "reference_date": "2026-04-24",
        "budget_usd": 10.0,
        "max_tickets": 12,
    }


def test_find_reviewable_ticket_only_returns_approvable_review() -> None:
    payload = {
        "preflight": {"approval_allowed": True},
        "tickets": [
            {"ticket_id": "T01", "action": "NO_TRADE", "stake_suggestion_usd": 0.0},
            {"ticket_id": "T02", "action": "REVIEW", "stake_suggestion_usd": 5.0},
        ]
    }

    assert run_operator_ui.find_reviewable_ticket(payload, "T01") is None
    assert run_operator_ui.find_reviewable_ticket(payload, "T02") == payload["tickets"][1]


def test_find_reviewable_ticket_respects_preflight_lock() -> None:
    payload = {
        "preflight": {"approval_allowed": False},
        "tickets": [{"ticket_id": "T02", "action": "REVIEW", "stake_suggestion_usd": 5.0}],
    }

    assert run_operator_ui.find_reviewable_ticket(payload, "T02") is None


def test_normalize_recovery_reference_date_keeps_valid_date() -> None:
    assert run_operator_ui.normalize_recovery_reference_date("2026-04-26") == "2026-04-26"
