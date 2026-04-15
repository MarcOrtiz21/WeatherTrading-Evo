from __future__ import annotations

from datetime import date


def compute_previous_runs_past_days(
    as_of_date: date,
    lookback_days: int,
    max_horizon_days: int,
    *,
    reference_today: date | None = None,
) -> int:
    today = date.today() if reference_today is None else reference_today
    if as_of_date > today:
        raise ValueError("La fecha de referencia no puede estar en el futuro.")

    days_before_today = (today - as_of_date).days
    return max(1, int(lookback_days) + int(max_horizon_days) + days_before_today)
