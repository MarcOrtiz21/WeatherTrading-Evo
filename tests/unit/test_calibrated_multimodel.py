from weather_trading.services.forecast_engine.calibrated_multimodel import (
    CalibratedMultiModelDistributionBuilder,
)


def test_extract_calibration_errors_groups_by_model():
    builder = CalibratedMultiModelDistributionBuilder()

    errors = builder.extract_calibration_errors(
        forecast_history={
            "2026-04-01": {"best_match": 20.0, "gfs_seamless": 21.0},
            "2026-04-02": {"best_match": 22.0},
        },
        actual_history={
            "2026-04-01": 19.0,
            "2026-04-02": 23.0,
        },
    )

    assert errors["best_match"] == [-1.0, 1.0]
    assert errors["gfs_seamless"] == [-2.0]


def test_calibrated_multimodel_builder_normalizes_distribution():
    builder = CalibratedMultiModelDistributionBuilder()

    forecast = builder.build(
        market_id="test-market",
        model_values_by_name={"best_match": 20.0, "ecmwf_ifs025": 21.0, "gfs_seamless": 19.5},
        calibration_errors_by_model={
            "best_match": [0.5, -0.2],
            "ecmwf_ifs025": [0.2, 0.1],
            "gfs_seamless": [-0.6, -0.4],
        },
        horizon_days=2,
        ensemble_members_c=[19.8, 20.4, 21.2],
    )

    assert abs(forecast.total_probability() - 1.0) < 1e-9
    assert forecast.most_likely_temperature() is not None
    assert any(note.startswith("weighted_mae=") for note in forecast.notes)


def test_calibrated_multimodel_builder_weights_low_error_models_more():
    builder = CalibratedMultiModelDistributionBuilder()

    forecast = builder.build(
        market_id="weighted-test",
        model_values_by_name={
            "best_match": 20.0,
            "ecmwf_ifs025": 24.0,
        },
        calibration_errors_by_model={
            "best_match": [4.0, 5.0, 4.5],
            "ecmwf_ifs025": [0.1, -0.1, 0.2],
        },
        horizon_days=1,
        ensemble_members_c=None,
    )

    center_note = next(note for note in forecast.notes if note.startswith("center="))
    center_value = float(center_note.split("=", 1)[1])

    assert center_value > 23.0
