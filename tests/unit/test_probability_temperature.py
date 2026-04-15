from datetime import datetime

from weather_trading.domain.models import ForecastDistribution
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.forecast_engine.probability_temperature import (
    apply_probability_temperature,
    get_probability_temperature_alpha,
    get_probability_temperature_alpha_for_unit,
)


def test_probability_temperature_reads_alpha_from_config(monkeypatch):
    monkeypatch.setattr(
        ConfigLoader,
        "get",
        lambda key, default=None: 0.85 if key == "forecast_policy.probability_temperature_alpha" else default,
    )

    assert get_probability_temperature_alpha() == 0.85


def test_apply_probability_temperature_flattens_distribution_for_alpha_below_one():
    distribution = ForecastDistribution(
        market_id="m1",
        generated_at_utc=datetime(2026, 4, 8, 10, 0, 0),
        model_name="test",
        calibration_version="1.0",
        probabilities_by_temp_c={20: 0.8, 21: 0.2},
        notes=(),
    )

    calibrated = apply_probability_temperature(distribution, alpha=0.5)

    assert calibrated.probabilities_by_temp_c[20] < 0.8
    assert calibrated.probabilities_by_temp_c[21] > 0.2
    assert abs(sum(calibrated.probabilities_by_temp_c.values()) - 1.0) < 1e-9
    assert "probability_temperature_alpha=0.500" in calibrated.notes


def test_probability_temperature_reads_unit_override_from_config(monkeypatch):
    def fake_get(key, default=None):
        if key == "forecast_policy.probability_temperature_alpha":
            return 0.8
        if key == "forecast_policy.probability_temperature_alpha_by_unit":
            return {"fahrenheit": 0.55}
        return default

    monkeypatch.setattr(ConfigLoader, "get", fake_get)

    assert get_probability_temperature_alpha_for_unit("fahrenheit") == 0.55
    assert get_probability_temperature_alpha_for_unit("celsius") == 0.8
