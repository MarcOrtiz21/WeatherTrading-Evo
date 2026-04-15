import math
from datetime import datetime
from typing import List, Dict
from weather_trading.domain.models import WeatherObservation, ForecastDistribution
from weather_trading.infrastructure.utils import utc_now

class BaselineForecastModel:
    """Modelo de baseline para predecir la temperatura máxima diaria."""

    def __init__(self, model_name: str = "baseline_v1"):
        self.model_name = model_name

    def estimate_max_distribution(
        self, 
        market_id: str, 
        observations: List[WeatherObservation],
        current_time_local: datetime
    ) -> ForecastDistribution:
        """
        Genera una distribución de probabilidad para T_max basada en observaciones intradía.
        Lógica simple: T_max_est = max_obs + delta_esperado(hora)
        """
        if not observations:
            # Si no hay datos, devolver una distribución plana (muy incierta)
            return self._flat_distribution(market_id, 20, 10)

        max_observed = max(o.temp_c for o in observations)
        
        # Estimar cuánto más puede subir según la hora (0-24h)
        # Típicamente el pico es a las 15:00-16:00 local.
        hour = current_time_local.hour + current_time_local.minute / 60.0
        
        # Heurística: delta disminuye conforme se acerca a las 16:00
        if hour < 16:
            remaining_rise = max(0, (16 - hour) * 0.8) # Sube ~0.8°C por hora hasta las 16h
        else:
            remaining_rise = 0 # Pasada la hora pico, asumimos que ya vimos la máxima

        expected_max = max_observed + remaining_rise
        
        # Generar distribución normal alrededor de expected_max
        # La incertidumbre (std) disminuye conforme pasa el día
        std_dev = max(1.0, (24 - hour) / 4.0) 
        
        probs = {}
        # Cubrir rango de -10 a +10 grados alrededor de la media
        for t in range(int(expected_max - 10), int(expected_max + 11)):
            # PDF de la normal
            p = (1 / (std_dev * math.sqrt(2 * math.pi))) * math.exp(-0.5 * ((t - expected_max) / std_dev) ** 2)
            probs[t] = p

        # Normalizar para que sumen 1
        total = sum(probs.values())
        normalized_probs = {t: p / total for t, p in probs.items()}

        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="1.0",
            probabilities_by_temp_c=normalized_probs
        )

    def _flat_distribution(self, market_id: str, center: int, width: int) -> ForecastDistribution:
        p = 1.0 / (2 * width + 1)
        probs = {t: p for t in range(center - width, center + width + 1)}
        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="0.0",
            probabilities_by_temp_c=probs
        )
