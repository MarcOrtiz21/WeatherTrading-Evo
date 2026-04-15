from weather_trading.infrastructure.config import ConfigLoader


def get_station_temperature_bias_c(station_code: str, default: float = 0.0) -> float:
    raw_mapping = ConfigLoader.get("forecast_policy.station_temperature_bias_c", {})
    if not isinstance(raw_mapping, dict):
        return default
    try:
        value = raw_mapping.get(str(station_code))
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def apply_station_temperature_bias(value_c: float | None, station_code: str) -> float | None:
    if value_c is None:
        return None
    return float(value_c) + get_station_temperature_bias_c(station_code)


def apply_station_temperature_bias_to_models(
    model_values_by_name: dict[str, float],
    station_code: str,
) -> dict[str, float]:
    bias_c = get_station_temperature_bias_c(station_code)
    if bias_c == 0.0:
        return dict(model_values_by_name)
    return {
        model_name: float(value) + bias_c
        for model_name, value in model_values_by_name.items()
    }
