import hashlib
import math

import numpy as np

from weather_trading.domain.models import ForecastDistribution
from weather_trading.infrastructure.utils import utc_now


class OpenMeteoDistributionBuilder:
    """Convierte un forecast puntual de Open-Meteo en una distribución discreta usable."""

    def __init__(
        self,
        model_name: str = "openmeteo_hourly_path_v1",
        fallback_model_name: str = "openmeteo_gaussian_v1",
        sample_count: int = 4096,
    ):
        self.model_name = model_name
        self.fallback_model_name = fallback_model_name
        self.sample_count = sample_count

    def build(
        self,
        market_id: str,
        model_max_temp_c: float,
        horizon_days: int,
        hourly_temperatures_c: list[float] | None = None,
        cloud_cover_avg: float | None = None,
        intraday_max_so_far_c: float | None = None,
        intraday_hours_elapsed: int | None = None,
        intraday_last_local_hour: int | None = None,
    ) -> ForecastDistribution:
        cleaned_hourly_temperatures = [
            float(value)
            for value in (hourly_temperatures_c or [])
            if value is not None
        ]
        if (
            cleaned_hourly_temperatures
            and intraday_max_so_far_c is not None
            and intraday_hours_elapsed is not None
            and max(0, horizon_days) == 0
        ):
            return self._build_from_intraday_path(
                market_id=market_id,
                model_max_temp_c=model_max_temp_c,
                hourly_temperatures_c=cleaned_hourly_temperatures,
                intraday_max_so_far_c=float(intraday_max_so_far_c),
                intraday_hours_elapsed=max(1, int(intraday_hours_elapsed)),
                intraday_last_local_hour=0 if intraday_last_local_hour is None else int(intraday_last_local_hour),
                cloud_cover_avg=cloud_cover_avg,
            )
        if cleaned_hourly_temperatures:
            return self._build_from_hourly_path(
                market_id=market_id,
                model_max_temp_c=model_max_temp_c,
                horizon_days=horizon_days,
                hourly_temperatures_c=cleaned_hourly_temperatures,
                cloud_cover_avg=cloud_cover_avg,
            )

        return self._build_gaussian_fallback(
            market_id=market_id,
            model_max_temp_c=model_max_temp_c,
            horizon_days=horizon_days,
        )

    def _build_gaussian_fallback(
        self,
        market_id: str,
        model_max_temp_c: float,
        horizon_days: int,
    ) -> ForecastDistribution:
        std_dev = min(4.0, max(1.5, 1.5 + max(horizon_days, 0) * 0.35))
        lower_bound = math.floor(model_max_temp_c - 4 * std_dev)
        upper_bound = math.ceil(model_max_temp_c + 4 * std_dev)

        probabilities: dict[int, float] = {}
        for temperature_c in range(lower_bound, upper_bound + 1):
            exponent = -0.5 * ((temperature_c - model_max_temp_c) / std_dev) ** 2
            probabilities[temperature_c] = math.exp(exponent)

        total = sum(probabilities.values()) or 1.0
        normalized = {
            temperature_c: probability / total
            for temperature_c, probability in probabilities.items()
        }

        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.fallback_model_name,
            calibration_version="1.0",
            probabilities_by_temp_c=normalized,
            notes=(f"center={model_max_temp_c:.1f}", f"std_dev={std_dev:.2f}", f"horizon_days={horizon_days}"),
        )

    def _build_from_hourly_path(
        self,
        market_id: str,
        model_max_temp_c: float,
        horizon_days: int,
        hourly_temperatures_c: list[float],
        cloud_cover_avg: float | None,
    ) -> ForecastDistribution:
        hourly_profile = np.array(hourly_temperatures_c, dtype=float)
        peak_hour_index = int(np.argmax(hourly_profile))
        peak_temperature = float(np.max(hourly_profile))
        near_peak_mask = hourly_profile >= peak_temperature - 0.75
        plateau_hours = max(1, int(np.count_nonzero(near_peak_mask)))

        horizon_penalty = max(horizon_days - 1, 0)
        global_bias_sigma = 0.85 + 0.16 * horizon_penalty
        local_noise_sigma = 0.32 + 0.04 * horizon_penalty
        if plateau_hours <= 2:
            local_noise_sigma += 0.08

        if cloud_cover_avg is not None:
            cloud_cover_ratio = max(0.0, min(float(cloud_cover_avg), 100.0)) / 100.0
            global_bias_sigma += 0.10 * cloud_cover_ratio

        rho = 0.84
        rng = np.random.default_rng(
            self._stable_seed(
                market_id,
                model_max_temp_c,
                horizon_days,
                peak_hour_index,
                plateau_hours,
            )
        )

        global_bias = rng.normal(loc=0.0, scale=global_bias_sigma, size=self.sample_count)
        local_noise = np.zeros((self.sample_count, hourly_profile.size), dtype=float)
        local_noise[:, 0] = rng.normal(loc=0.0, scale=local_noise_sigma, size=self.sample_count)
        innovation_sigma = math.sqrt(max(1e-9, 1.0 - rho**2)) * local_noise_sigma
        for hour_index in range(1, hourly_profile.size):
            local_noise[:, hour_index] = (
                rho * local_noise[:, hour_index - 1]
                + rng.normal(loc=0.0, scale=innovation_sigma, size=self.sample_count)
            )

        simulated_maxima = np.max(hourly_profile + global_bias[:, None] + local_noise, axis=1)
        bucketed_maxima = np.floor(simulated_maxima).astype(int)

        unique_buckets, counts = np.unique(bucketed_maxima, return_counts=True)
        total = int(np.sum(counts)) or 1
        probabilities = {
            int(bucket): float(count) / total
            for bucket, count in zip(unique_buckets, counts, strict=False)
        }

        center = float(np.mean(simulated_maxima))
        std_dev = float(np.std(simulated_maxima))
        blended_center = (center * 0.8) + (float(model_max_temp_c) * 0.2)

        adjusted_probabilities = {}
        for bucket, probability in probabilities.items():
            adjustment = math.exp(-0.5 * ((bucket - blended_center) / max(std_dev, 0.9)) ** 2)
            adjusted_probabilities[bucket] = probability * adjustment

        adjusted_total = sum(adjusted_probabilities.values()) or 1.0
        normalized = {
            bucket: probability / adjusted_total
            for bucket, probability in adjusted_probabilities.items()
        }

        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="1.1",
            probabilities_by_temp_c=normalized,
            notes=(
                f"center={center:.2f}",
                f"std_dev={max(std_dev, 0.1):.2f}",
                f"horizon_days={horizon_days}",
                f"hourly_points={hourly_profile.size}",
                f"peak_hour={peak_hour_index}",
                f"plateau_hours={plateau_hours}",
                f"cloud_cover_avg={0.0 if cloud_cover_avg is None else float(cloud_cover_avg):.1f}",
            ),
        )

    def _build_from_intraday_path(
        self,
        market_id: str,
        model_max_temp_c: float,
        hourly_temperatures_c: list[float],
        intraday_max_so_far_c: float,
        intraday_hours_elapsed: int,
        intraday_last_local_hour: int,
        cloud_cover_avg: float | None,
    ) -> ForecastDistribution:
        hourly_profile = np.array(hourly_temperatures_c, dtype=float)
        elapsed_hours = min(max(intraday_hours_elapsed, 1), hourly_profile.size)
        observed_max = float(intraday_max_so_far_c)
        future_profile = hourly_profile[elapsed_hours:]
        remaining_hours = future_profile.size

        if remaining_hours == 0:
            bucket = math.floor(observed_max)
            return ForecastDistribution(
                market_id=market_id,
                generated_at_utc=utc_now(),
                model_name="openmeteo_intraday_max_so_far_v1",
                calibration_version="1.0",
                probabilities_by_temp_c={bucket: 1.0},
                notes=(
                    f"center={observed_max:.2f}",
                    "std_dev=0.05",
                    "horizon_days=0",
                    f"intraday_max_so_far={observed_max:.1f}",
                    f"intraday_hours_elapsed={elapsed_hours}",
                    f"intraday_remaining_hours={remaining_hours}",
                    f"intraday_last_local_hour={intraday_last_local_hour}",
                    "intraday_source=complete_day_proxy",
                ),
            )

        cloud_cover_ratio = 0.0
        if cloud_cover_avg is not None:
            cloud_cover_ratio = max(0.0, min(float(cloud_cover_avg), 100.0)) / 100.0

        global_bias_sigma = 0.22 + 0.05 * (remaining_hours / 24)
        local_noise_sigma = 0.12 + 0.18 * (remaining_hours / 24)
        if future_profile.size <= 3:
            local_noise_sigma *= 0.75
        global_bias_sigma += 0.04 * cloud_cover_ratio

        rng = np.random.default_rng(
            self._stable_seed(
                market_id,
                model_max_temp_c,
                0,
                intraday_last_local_hour,
                max(1, remaining_hours),
            )
        )
        global_bias = rng.normal(loc=0.0, scale=global_bias_sigma, size=self.sample_count)
        local_noise = rng.normal(loc=0.0, scale=local_noise_sigma, size=(self.sample_count, future_profile.size))

        future_maxima = np.max(future_profile + global_bias[:, None] + local_noise, axis=1)
        simulated_maxima = np.maximum(observed_max, future_maxima)
        bucketed_maxima = np.floor(simulated_maxima).astype(int)

        unique_buckets, counts = np.unique(bucketed_maxima, return_counts=True)
        total = int(np.sum(counts)) or 1
        probabilities = {
            int(bucket): float(count) / total
            for bucket, count in zip(unique_buckets, counts, strict=False)
            if bucket >= math.floor(observed_max)
        }
        normalized_total = sum(probabilities.values()) or 1.0
        normalized = {
            bucket: probability / normalized_total
            for bucket, probability in probabilities.items()
        }

        center = float(np.mean(simulated_maxima))
        std_dev = float(np.std(simulated_maxima))
        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name="openmeteo_intraday_max_so_far_v1",
            calibration_version="1.0",
            probabilities_by_temp_c=normalized,
            notes=(
                f"center={center:.2f}",
                f"std_dev={max(std_dev, 0.05):.2f}",
                "horizon_days=0",
                f"intraday_max_so_far={observed_max:.1f}",
                f"intraday_hours_elapsed={elapsed_hours}",
                f"intraday_remaining_hours={remaining_hours}",
                f"intraday_last_local_hour={intraday_last_local_hour}",
                f"cloud_cover_avg={0.0 if cloud_cover_avg is None else float(cloud_cover_avg):.1f}",
                "intraday_source=max_so_far_plus_remaining_path",
            ),
        )

    def _stable_seed(
        self,
        market_id: str,
        model_max_temp_c: float,
        horizon_days: int,
        peak_hour_index: int,
        plateau_hours: int,
    ) -> int:
        seed_material = (
            f"{market_id}|{model_max_temp_c:.3f}|{horizon_days}|{peak_hour_index}|{plateau_hours}|{self.sample_count}"
        )
        digest = hashlib.sha256(seed_material.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big", signed=False)
