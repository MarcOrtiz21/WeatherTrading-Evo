from scripts.run_trader_behavior_profile import (
    compute_hours_before_event,
    summarize_trade_direction,
    summarize_trade_timing,
)


def test_compute_hours_before_event_uses_event_date_end_of_day():
    hours = compute_hours_before_event(1776038400, "2026-04-14")

    assert round(hours, 2) == 48.0


def test_summarize_trade_direction_breaks_down_yes_no_and_sell():
    summary = summarize_trade_direction(
        [
            {"outcome": "Yes", "side": "BUY"},
            {"outcome": "No", "side": "BUY"},
            {"outcome": "Yes", "side": "SELL"},
        ]
    )

    assert summary["yes_buy_share"] == 1 / 3
    assert summary["no_buy_share"] == 1 / 3
    assert summary["sell_share"] == 1 / 3


def test_summarize_trade_timing_builds_window_shares():
    summary = summarize_trade_timing(
        [
            {"hours_before_event": 10, "trade_datetime_utc": "2026-04-13T10:00:00+00:00"},
            {"hours_before_event": 30, "trade_datetime_utc": "2026-04-12T14:00:00+00:00"},
            {"hours_before_event": 100, "trade_datetime_utc": "2026-04-10T16:00:00+00:00"},
        ]
    )

    assert summary["same_day_share"] == 1 / 3
    assert summary["one_to_three_day_share"] == 1 / 3
    assert summary["more_than_three_day_share"] == 1 / 3
