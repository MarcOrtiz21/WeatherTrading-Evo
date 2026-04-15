from datetime import datetime

import pytest

from weather_trading.domain.models import ForecastDistribution


def test_forecast_distribution_probability_at_or_above_uses_ceiling_bucket():
    distribution = ForecastDistribution(
        market_id="m1",
        generated_at_utc=datetime(2026, 4, 5, 12, 0, 0),
        model_name="test",
        calibration_version="v1",
        probabilities_by_temp_c={20: 0.2, 21: 0.3, 22: 0.5},
    )

    assert distribution.probability_at_or_above(21.1) == 0.5


def test_forecast_distribution_probability_between_respects_open_bounds():
    distribution = ForecastDistribution(
        market_id="m1",
        generated_at_utc=datetime(2026, 4, 5, 12, 0, 0),
        model_name="test",
        calibration_version="v1",
        probabilities_by_temp_c={18: 0.1, 19: 0.2, 20: 0.3, 21: 0.4},
    )

    assert distribution.probability_between(low_c=18.2, high_c=20.8) == pytest.approx(0.5)
    assert distribution.probability_between(low_c=None, high_c=19.4) == pytest.approx(0.3)
    assert distribution.probability_between(low_c=20.2, high_c=None) == pytest.approx(0.4)


def test_forecast_distribution_most_likely_temperature_handles_empty_distribution():
    distribution = ForecastDistribution(
        market_id="m1",
        generated_at_utc=datetime(2026, 4, 5, 12, 0, 0),
        model_name="test",
        calibration_version="v1",
        probabilities_by_temp_c={},
    )

    assert distribution.most_likely_temperature() is None
