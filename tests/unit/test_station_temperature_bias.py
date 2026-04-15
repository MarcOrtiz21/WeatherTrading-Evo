from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.forecast_engine.station_temperature_bias import (
    apply_station_temperature_bias,
    apply_station_temperature_bias_to_models,
    get_station_temperature_bias_c,
)


def test_station_temperature_bias_reads_mapping(monkeypatch):
    monkeypatch.setattr(
        ConfigLoader,
        "get",
        lambda key, default=None: {"KLGA": 1.2} if key == "forecast_policy.station_temperature_bias_c" else default,
    )

    assert get_station_temperature_bias_c("KLGA") == 1.2
    assert get_station_temperature_bias_c("EGLL") == 0.0


def test_apply_station_temperature_bias_adjusts_scalar(monkeypatch):
    monkeypatch.setattr(
        ConfigLoader,
        "get",
        lambda key, default=None: {"EGLL": 0.8} if key == "forecast_policy.station_temperature_bias_c" else default,
    )

    assert apply_station_temperature_bias(11.0, "EGLL") == 11.8
    assert apply_station_temperature_bias(None, "EGLL") is None


def test_apply_station_temperature_bias_adjusts_model_mapping(monkeypatch):
    monkeypatch.setattr(
        ConfigLoader,
        "get",
        lambda key, default=None: {"LTAC": -0.7} if key == "forecast_policy.station_temperature_bias_c" else default,
    )

    adjusted = apply_station_temperature_bias_to_models({"best_match": 10.0, "gfs": 11.0}, "LTAC")

    assert adjusted == {"best_match": 9.3, "gfs": 10.3}
