import sys
import os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.domain.models import ResolutionSource, RoundingMethod

def test_parser():
    parser = DeterministicParser()
    mapper = StationMapperService()

    # Mock 1: Madrid Threshold
    mock1 = {
        "id": "mad-33-apr-15",
        "question": "¿Hará 33°C o más en Madrid el 15 de abril?",
        "description": "Este mercado se resolverá a 'Sí' si la temperatura máxima diaria registrada en el Aeropuerto de Madrid (LEMD) alcanza o supera los 33.0 grados Celsius el 15 de abril de 2026. En caso contrario, se resolverá a 'No'.",
        "rules": "Fuente: Wunderground (History Daily para LEMD). Variable: 'Max Temperature'. Redondeo: Se usará el primer decimal reportado.",
        "outcomes": ["Yes", "No"]
    }

    spec1 = parser.parse(mock1)
    spec1 = mapper.enrich(spec1)
    print(f"--- Mock 1: {spec1.question} ---")
    print(f"Station: {spec1.station_code}")
    print(f"City: {spec1.city}")
    print(f"Country: {spec1.country}")
    print(f"Timezone: {spec1.timezone}")
    assert spec1.city == "Madrid"
    assert spec1.country == "Spain"
    assert spec1.local_date == date(2026, 4, 15)

    # Mock 2: London Bins (Fahrenheit conversion example)
    mock2 = {
        "id": "lon-bins-may-20",
        "question": "¿Cuál será la temperatura máxima en Londres el 20 de mayo?",
        "description": "Este mercado se resolverá según la temperatura máxima registrada en el Aeropuerto de Heathrow (EGLL) el 20 de mayo de 2026. Si el reporte dice 68 degrees Fahrenheit, se aplica la regla.",
        "rules": "Fuente: NOAA. Redondeo al entero más cercano (nearest integer).",
        "outcomes": ["15 or less", "16", "17", "18", "19 or more"]
    }

    spec2 = parser.parse(mock2)
    print(f"\n--- Mock 2: {spec2.question} ---")
    print(f"Station: {spec2.station_code}")
    print(f"Source: {spec2.resolution_source}")
    print(f"Threshold (from F): {spec2.threshold_c}°C")
    print(f"Rounding: {spec2.rounding_method}")
    print(f"Confidence: {spec2.confidence_score}")
    assert spec2.station_code == "EGLL"
    assert spec2.resolution_source == ResolutionSource.NOAA
    # 68F = 20C
    assert spec2.threshold_c == 20.0
    assert spec2.rounding_method == RoundingMethod.NEAREST_HALF_UP

    print("\n¡Pruebas del parser completadas con éxito!")

if __name__ == "__main__":
    test_parser()
