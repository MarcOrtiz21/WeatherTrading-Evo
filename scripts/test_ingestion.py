import sys
import os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.services.weather_ingestion.metar_client import MetarIngestor

def test_metar_parser():
    ingestor = MetarIngestor()
    
    # Caso 1: Madrid normal
    raw1 = "LEMD 051430Z 23005KT 9999 FEW030 18/11 Q1018 NOSIG"
    obs1 = ingestor.parse_metar(raw1)
    print(f"--- METAR Mock 1: {raw1} ---")
    print(f"Station: {obs1.station_code}")
    print(f"Temp: {obs1.temp_c}°C")
    print(f"Dew Point: {obs1.dewpoint_c}°C")
    print(f"Pressure: {obs1.pressure_hpa} hPa")
    assert obs1.station_code == "LEMD"
    assert obs1.temp_c == 18.0
    assert obs1.dewpoint_c == 11.0
    assert obs1.pressure_hpa == 1018.0

    # Caso 2: Bajo cero (m02)
    raw2 = "EGLL 051430Z 00000KT 8000 FG BKN005 M02/M03 Q1025"
    obs2 = ingestor.parse_metar(raw2)
    print(f"\n--- METAR Mock 2: {raw2} ---")
    print(f"Temp: {obs2.temp_c}°C")
    print(f"Dew Point: {obs2.dewpoint_c}°C")
    assert obs2.temp_c == -2.0
    assert obs2.dewpoint_c == -3.0

    # Caso 3: cambio de mes
    raw3 = "LEMD 311430Z 23005KT 9999 FEW030 18/11 Q1018 NOSIG"
    obs3 = ingestor.parse_metar(raw3, reference_time_utc=datetime(2026, 4, 5, 10, 0))
    print(f"\n--- METAR Mock 3: {raw3} ---")
    print(f"Observed At: {obs3.observed_at_utc.isoformat()}")
    assert obs3.observed_at_utc.year == 2026
    assert obs3.observed_at_utc.month == 3
    assert obs3.observed_at_utc.day == 31

    print("\n¡Pruebas del METAR Ingestor completadas con éxito!")

if __name__ == "__main__":
    test_metar_parser()
