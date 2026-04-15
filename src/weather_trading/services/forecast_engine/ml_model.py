import math
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Any
from weather_trading.domain.models import WeatherObservation, ForecastDistribution
from weather_trading.infrastructure.utils import utc_now

class MLForecastEngine:
    """Motor de forecast refinado que combina observaciones y modelos numéricos."""

    def __init__(self, model_name: str = "ensemble_v1"):
        self.model_name = model_name

    def estimate_max_distribution(
        self, 
        market_id: str, 
        observations: List[WeatherObservation],
        model_forecast: Optional[Dict[str, Any]],
        current_time_local: datetime
    ) -> ForecastDistribution:
        """
        Genera una distribución probabilística refinada (Ensamble).
        Mezcla el baseline de observación con el pronóstico numérico externo.
        """
        if not observations:
            return self._fallback_distribution(market_id, model_forecast)

        max_obs = max(o.temp_c for o in observations)
        hour = current_time_local.hour + current_time_local.minute / 60.0
        
        # 1. Baseline de observación (Heurística intradía)
        if hour < 16:
            obs_expected_max = max_obs + max(0, (16 - hour) * 0.7)
            obs_uncertainty = (24 - hour) / 3.0
        else:
            obs_expected_max = max_obs
            obs_uncertainty = 1.0 # Muy baja incertidumbre, ya pasó el pico

        # 2. Señal del modelo numérico (Open-Meteo)
        if model_forecast and model_forecast.get("model_max_temp") is not None:
            model_max = model_forecast["model_max_temp"]
            # El modelo numérico tiene su propia incertidumbre (bias típico de 1.5°C)
            model_uncertainty = 2.5 
            
            # 3. Ensamble (Promedio ponderado por varianza inversa)
            # w = 1/var
            w_obs = 1.0 / (obs_uncertainty ** 2)
            w_model = 1.0 / (model_uncertainty ** 2)
            
            # Ponderación dinámica: a primera hora manda el modelo, a mediodía manda la observación
            # Multiplicamos peso de observación por factor de "realidad"
            w_obs *= (hour / 12.0) if hour < 12 else 1.0

            combined_mean = (obs_expected_max * w_obs + model_max * w_model) / (w_obs + w_model)
            combined_std = math.sqrt(1.0 / (w_obs + w_model))
        else:
            combined_mean = obs_expected_max
            combined_std = obs_uncertainty

        # 4. Generar distribución discreta (Normal discretizada)
        probs = {}
        for t in range(int(combined_mean - 10), int(combined_mean + 11)):
            # PDF Normal
            p = (1 / (combined_std * math.sqrt(2 * math.pi))) * math.exp(-0.5 * ((t - combined_mean) / combined_std) ** 2)
            probs[t] = p

        # Normalizar
        total = sum(probs.values())
        normalized_probs = {t: p / total for t, p in probs.items()}

        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="2.0-ensemble",
            probabilities_by_temp_c=normalized_probs,
            notes=(f"Mean: {combined_mean:.2f}", f"Std: {combined_std:.2f}", f"MaxObs: {max_obs:.1f}")
        )

    def _fallback_distribution(self, market_id: str, model_forecast: Optional[Dict[str, Any]]) -> ForecastDistribution:
        # Si no hay observaciones pero sí modelo, usamos el modelo con incertidumbre alta
        if model_forecast and model_forecast.get("model_max_temp") is not None:
            mean = model_forecast["model_max_temp"]
            std = 4.0 # Mucha incertidumbre sin METARs
        else:
            mean, std = 20.0, 10.0 # Blind guess
            
        probs = {t: (1 / (std * math.sqrt(2 * math.pi))) * math.exp(-0.5 * ((t - mean) / std) ** 2) 
                 for t in range(int(mean - 15), int(mean + 16))}
        total = sum(probs.values())
        return ForecastDistribution(
            market_id=market_id,
            generated_at_utc=utc_now(),
            model_name=self.model_name,
            calibration_version="0.5-fallback",
            probabilities_by_temp_c={t: p / total for t, p in probs.items()}
        )
