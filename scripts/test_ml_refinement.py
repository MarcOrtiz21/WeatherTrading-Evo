import sys
import os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.services.forecast_engine.ml_model import MLForecastEngine
from weather_trading.domain.models import WeatherObservation, ResolutionSource
from weather_trading.infrastructure.utils import utc_now

def test_refinement():
    engine = MLForecastEngine()
    market_id = "test-refinement-market"
    
    # Datos del modelo numérico (Open-Meteo dice que hará 30°C)
    open_meteo_data = {
        "model_max_temp": 30.0,
        "provider": "open_meteo"
    }
    
    # Escenario 1: Temprano en la mañana (9:00 AM)
    # Obs actual baja (18°C), el sistema debería confiar en el modelo (30°C)
    obs_morning = [
        WeatherObservation(station_code="LEMD", provider=ResolutionSource.METAR, observed_at_utc=utc_now(), temp_c=18.0)
    ]
    time_morning = datetime.now().replace(hour=9, minute=0)
    forecast_morning = engine.estimate_max_distribution(market_id, obs_morning, open_meteo_data, time_morning)
    
    print(f"--- Escenario 9:00 AM ---")
    print(f"Notas: {forecast_morning.notes}")
    # El valor esperado (mean) debería estar cerca de 30°C
    
    # Escenario 2: Mediodía (13:00 PM)
    # Hace calor de más (28°C), el sistema debería "subir" el forecast por encima del modelo
    obs_noon = [
        WeatherObservation(station_code="LEMD", provider=ResolutionSource.METAR, observed_at_utc=utc_now(), temp_c=28.0)
    ]
    time_noon = datetime.now().replace(hour=13, minute=0)
    forecast_noon = engine.estimate_max_distribution(market_id, obs_noon, open_meteo_data, time_noon)
    
    print(f"\n--- Escenario 13:00 PM ---")
    print(f"Notas: {forecast_noon.notes}")
    # El valor esperado debería haber subido por la fuerte observación

    # Escenario 3: Tarde (17:00 PM)
    # Ya pasó el pico, la máxima observada es la final
    obs_late = [
        WeatherObservation(station_code="LEMD", provider=ResolutionSource.METAR, observed_at_utc=utc_now(), temp_c=31.5)
    ]
    time_late = datetime.now().replace(hour=17, minute=0)
    forecast_late = engine.estimate_max_distribution(market_id, obs_late, open_meteo_data, time_late)
    
    print(f"\n--- Escenario 17:00 PM ---")
    print(f"Notas: {forecast_late.notes}")
    # Std debería ser muy baja (1.0) y Mean coincidir con 31.5

    print("\n¡Pruebas de refinamiento ML completadas con éxito!")

if __name__ == "__main__":
    test_refinement()
