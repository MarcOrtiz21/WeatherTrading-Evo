from weather_trading.services.forecast_engine.openmeteo_distribution import OpenMeteoDistributionBuilder


def test_openmeteo_distribution_builder_normalizes_probabilities():
    builder = OpenMeteoDistributionBuilder()

    forecast = builder.build(
        market_id="test-market",
        model_max_temp_c=26.4,
        horizon_days=2,
    )

    assert abs(forecast.total_probability() - 1.0) < 1e-9
    assert forecast.most_likely_temperature() in {26, 27}
    assert forecast.probability_between(26, 27) > 0


def test_openmeteo_distribution_builder_uses_hourly_path_when_available():
    builder = OpenMeteoDistributionBuilder(sample_count=2048)

    hourly_temperatures = [
        11.0, 10.8, 10.4, 10.1, 9.9, 10.3, 11.2, 12.8,
        14.1, 15.2, 16.1, 16.8, 17.3, 17.6, 17.8, 17.7,
        17.2, 16.4, 15.5, 14.7, 13.6, 12.8, 12.0, 11.5,
    ]

    forecast = builder.build(
        market_id="test-hourly-market",
        model_max_temp_c=17.8,
        horizon_days=1,
        hourly_temperatures_c=hourly_temperatures,
        cloud_cover_avg=42.0,
    )

    assert abs(forecast.total_probability() - 1.0) < 1e-9
    assert forecast.model_name == "openmeteo_hourly_path_v1"
    assert any(note.startswith("hourly_points=24") for note in forecast.notes)
    assert any(note.startswith("peak_hour=") for note in forecast.notes)
    assert forecast.most_likely_temperature() in {17, 18}
    assert forecast.probability_between(17, 17.9) > 0.10


def test_openmeteo_distribution_builder_hourly_path_is_deterministic():
    builder = OpenMeteoDistributionBuilder(sample_count=1024)
    hourly_temperatures = [12.0 + (hour / 10) for hour in range(24)]

    first = builder.build(
        market_id="deterministic-market",
        model_max_temp_c=14.3,
        horizon_days=2,
        hourly_temperatures_c=hourly_temperatures,
        cloud_cover_avg=20.0,
    )
    second = builder.build(
        market_id="deterministic-market",
        model_max_temp_c=14.3,
        horizon_days=2,
        hourly_temperatures_c=hourly_temperatures,
        cloud_cover_avg=20.0,
    )

    assert first.probabilities_by_temp_c == second.probabilities_by_temp_c
    assert first.notes == second.notes


def test_openmeteo_distribution_builder_intraday_max_so_far_blocks_lower_bins():
    builder = OpenMeteoDistributionBuilder(sample_count=2048)
    hourly_temperatures = [
        12.0, 12.3, 12.5, 12.8, 13.0, 13.2, 13.4, 13.9,
        14.4, 14.8, 14.6, 14.4, 14.2, 14.0, 13.7, 13.5,
        13.3, 13.1, 12.9, 12.8, 12.7, 12.6, 12.5, 12.4,
    ]

    forecast = builder.build(
        market_id="intraday-market",
        model_max_temp_c=14.8,
        horizon_days=0,
        hourly_temperatures_c=hourly_temperatures,
        cloud_cover_avg=60.0,
        intraday_max_so_far_c=14.8,
        intraday_hours_elapsed=19,
        intraday_last_local_hour=18,
    )

    assert forecast.model_name == "openmeteo_intraday_max_so_far_v1"
    assert all(bucket >= 14 for bucket in forecast.probabilities_by_temp_c)
    assert forecast.probability_between(None, 13.9) == 0
    assert any(note.startswith("intraday_max_so_far=14.8") for note in forecast.notes)
