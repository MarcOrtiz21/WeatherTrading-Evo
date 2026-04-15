import math
from statistics import fmean

from weather_trading.domain.models import ForecastDistribution
from weather_trading.infrastructure.utils import utc_now


class CalibratedMultiModelDistributionBuilder:
    """Construye una distribución usando varios modelos y sesgo reciente local."""

    def __init__(self, model_name: str = "openmeteo_calibrated_multimodel_v1"):
        self.model_name = model_name

    def build(
        self,
        market_id: str,
        model_values_by_name: dict[str, float],
        calibration_errors_by_model: dict[str, list[float]],
        horizon_days: int,
        ensemble_members_c: list[float] | None = None,
    ) -> ForecastDistribution:
        if not model_values_by_name:
            raise ValueError("Se necesita al menos un forecast determinista para construir la distribución.")

        raw_deterministic_points: list[tuple[float, float]] = []
        adjusted_points: list[tuple[float, float]] = []
        model_maes: list[float] = []
        for model_name, current_value in model_values_by_name.items():
            errors = calibration_errors_by_model.get(model_name, [])
            bias = fmean(errors) if errors else 0.0
            mae = fmean(abs(error) for error in errors) if errors else 1.5
            weight = 1.0 / max(mae, 0.6) ** 2
            raw_deterministic_points.append((current_value, weight))
            adjusted_points.append((current_value + bias, weight))
            model_maes.append(mae)

        if ensemble_members_c:
            deterministic_bias = self._weighted_center(adjusted_points) - self._weighted_center(raw_deterministic_points)
            ensemble_weight = sum(weight for _, weight in adjusted_points) * 0.35 / max(len(ensemble_members_c), 1)
            for member in ensemble_members_c:
                adjusted_points.append((member + deterministic_bias, ensemble_weight))

        weighted_center = self._weighted_center(adjusted_points)
        weighted_spread = self._weighted_spread(adjusted_points, weighted_center)
        weighted_mae = fmean(model_maes) if model_maes else 1.5
        sigma = max(0.9, weighted_spread, weighted_mae)
        sigma += max(horizon_days - 1, 0) * 0.12

        lower_bound = math.floor(min(point for point, _ in adjusted_points) - 4 * sigma)
        upper_bound = math.ceil(max(point for point, _ in adjusted_points) + 4 * sigma)

        probabilities: dict[int, float] = {}
        for temperature_c in range(lower_bound, upper_bound + 1):
            probability = 0.0
            for center, weight in adjusted_points:
                exponent = -0.5 * ((temperature_c - center) / sigma) ** 2
                probability += weight * math.exp(exponent)
            probabilities[temperature_c] = probability

        total = sum(probabilities.values()) or 1.0
        normalized = {
            temperature_c: probability / total
            for temperature_c, probability in probabilities.items()
        }

        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="1.0",
            probabilities_by_temp_c=normalized,
            notes=(
                f"center={weighted_center:.2f}",
                f"std_dev={sigma:.2f}",
                f"model_count={len(model_values_by_name)}",
                f"ensemble_members={len(ensemble_members_c or [])}",
                f"weighted_mae={weighted_mae:.2f}",
            ),
        )

    def extract_calibration_errors(
        self,
        forecast_history: dict[str, dict[str, float]],
        actual_history: dict[str, float],
    ) -> dict[str, list[float]]:
        errors_by_model: dict[str, list[float]] = {}
        for day, forecasts_by_model in forecast_history.items():
            actual_value = actual_history.get(day)
            if actual_value is None:
                continue
            for model_name, forecast_value in forecasts_by_model.items():
                errors_by_model.setdefault(model_name, []).append(actual_value - forecast_value)
        return errors_by_model

    def _weighted_center(self, points: list[tuple[float, float]]) -> float:
        total_weight = sum(weight for _, weight in points) or 1.0
        return sum(value * weight for value, weight in points) / total_weight

    def _weighted_spread(self, points: list[tuple[float, float]], center: float) -> float:
        total_weight = sum(weight for _, weight in points) or 1.0
        variance = sum(weight * (value - center) ** 2 for value, weight in points) / total_weight
        return math.sqrt(max(variance, 0.0))
