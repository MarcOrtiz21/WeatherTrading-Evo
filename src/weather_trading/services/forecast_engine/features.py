import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict
from weather_trading.domain.models import WeatherObservation, MarketSpec

class FeatureBuilder:
    """Transforma observaciones y especificaciones de mercado en features para ML."""

    def build_feature_row(
        self, 
        spec: MarketSpec, 
        observations: List[WeatherObservation], 
        current_time_local: datetime
    ) -> pd.DataFrame:
        """Crea una única fila de features para inferencia."""
        if not observations:
            return pd.DataFrame()

        # Ordenar por tiempo
        obs_sorted = sorted(observations, key=lambda o: o.observed_at_utc)
        latest = obs_sorted[-1]
        
        # 1. Features básicas
        features = {
            "hour_local": current_time_local.hour + current_time_local.minute / 60.0,
            "day_of_year": current_time_local.timetuple().tm_yday,
            "current_temp": latest.temp_c,
            "max_so_far": max(o.temp_c for o in obs_sorted),
            "min_so_far": min(o.temp_c for o in obs_sorted),
        }

        # 2. Tendencias (si hay suficientes datos)
        if len(obs_sorted) > 1:
            features["temp_trend_1h"] = latest.temp_c - obs_sorted[-2].temp_c
        else:
            features["temp_trend_1h"] = 0.0

        # 3. Features meteorológicas avanzadas
        if latest.dewpoint_c is not None:
            features["dewpoint"] = latest.dewpoint_c
            # Humedad relativa aproximada
            features["rel_humidity_approx"] = 100 - 5 * (latest.temp_c - latest.dewpoint_c)
        else:
            features["dewpoint"] = latest.temp_c # Fallback
            features["rel_humidity_approx"] = 50.0

        if latest.pressure_hpa is not None:
            features["pressure"] = latest.pressure_hpa

        # 4. Target context (opcional para el modelo)
        features["target_threshold"] = spec.threshold_c if spec.threshold_c else 0.0
        
        return pd.DataFrame([features])
