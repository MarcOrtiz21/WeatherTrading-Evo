from datetime import date
import math

from weather_trading.domain.models import MetricKind, ResolutionSource, RoundingMethod
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


def test_parser_extracts_spanish_date_and_threshold():
    parser = DeterministicParser()
    mock = {
        "id": "mad-33-apr-15",
        "question": "¿Hará 33°C o más en Madrid el 15 de abril de 2026?",
        "description": (
            "Este mercado se resolverá a 'Sí' si la temperatura máxima diaria registrada en "
            "el Aeropuerto de Madrid (LEMD) alcanza o supera los 33.0 grados Celsius."
        ),
        "rules": (
            "Fuente: Wunderground (History Daily para LEMD). "
            "Redondeo: Se usará el primer decimal reportado."
        ),
        "outcomes": ["Yes", "No"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.station_code == "LEMD"
    assert spec.local_date == date(2026, 4, 15)
    assert spec.threshold_c == 33.0
    assert spec.resolution_source == ResolutionSource.WUNDERGROUND


def test_parser_marks_missing_date_for_manual_review():
    parser = DeterministicParser()
    mock = {
        "id": "bar-heat",
        "question": "¿Día caluroso en Barcelona?",
        "description": "Si hace más de 30 grados en Barcelona el próximo lunes, el mercado es Sí.",
        "rules": "Fuente: AEMET.",
        "outcomes": ["Yes", "No"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.local_date is None
    assert spec.requires_manual_review
    assert "missing_or_unparsed_local_date" in spec.notes
    assert spec.resolution_source == ResolutionSource.AEMET


def test_parser_recognizes_rounding_rule_and_fahrenheit_threshold():
    parser = DeterministicParser()
    mock = {
        "id": "lon-bins-may-20",
        "question": "¿Cuál será la temperatura máxima en Londres el 20 de mayo de 2026?",
        "description": (
            "Este mercado se resolverá según la temperatura máxima registrada en Heathrow (EGLL). "
            "Si el reporte dice 68 degrees Fahrenheit, se aplica la regla."
        ),
        "rules": "Fuente: NOAA. Redondeo al entero más cercano (nearest integer).",
        "outcomes": ["15 or less", "16", "17", "18", "19 or more"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.threshold_c == 20.0
    assert spec.rounding_method == RoundingMethod.NEAREST_HALF_UP


def test_parser_extracts_exact_temperature_bin_from_polymarket_daily_market():
    parser = DeterministicParser()
    mock = {
        "id": "hk-25-apr-05",
        "question": "¿Será la temperatura más alta en Hong Kong de 25°C el 5 de abril?",
        "description": (
            "Este mercado se resolverá según la temperatura máxima observada el 5 de abril de 2026 "
            "según Wunderground para la estación de referencia."
        ),
        "rules": "Resolution source: Wunderground.",
        "event_date": "2026-04-05",
        "outcomes": ["Yes", "No"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.metric == MetricKind.TEMPERATURE_BIN
    assert spec.local_date == date(2026, 4, 5)
    assert spec.bin_low_c == 25.0
    assert spec.bin_high_c == 25.9
    assert spec.threshold_c is None
    assert spec.resolution_source == ResolutionSource.WUNDERGROUND


def test_parser_extracts_fahrenheit_range_bin_from_polymarket_daily_market():
    parser = DeterministicParser()
    mock = {
        "id": "atl-range-apr-05",
        "question": "Will the highest temperature in Atlanta be between 70-71°F on April 5?",
        "description": "Resolution source: Wunderground for Atlanta on April 5, 2026.",
        "rules": "Resolution source: Wunderground.",
        "event_date": "2026-04-05",
        "outcomes": ["Yes", "No"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.metric == MetricKind.TEMPERATURE_BIN
    assert spec.local_date == date(2026, 4, 5)
    assert math.isclose(spec.bin_low_c, (69.5 - 32) * 5 / 9, rel_tol=0, abs_tol=1e-9)
    assert math.isclose(spec.bin_high_c, (71.5 - 32) * 5 / 9, rel_tol=0, abs_tol=1e-9)
    assert spec.threshold_c is None


def test_parser_detects_hong_kong_observatory_before_nws():
    parser = DeterministicParser()
    mock = {
        "id": "hk-apr-04",
        "question": "Highest temperature in Hong Kong on April 4?",
        "description": (
            "This market resolves using the Hong Kong Observatory Daily Extract available at "
            "https://www.weather.gov.hk/en/cis/climat.htm."
        ),
        "rules": "Resolution source: Hong Kong Observatory.",
        "event_date": "2026-04-04",
        "outcomes": ["Yes", "No"],
    }

    spec = parser.parse(mock)

    assert spec is not None
    assert spec.resolution_source == ResolutionSource.HONG_KONG_OBSERVATORY
