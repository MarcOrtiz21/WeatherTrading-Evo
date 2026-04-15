from datetime import datetime

from weather_trading.services.weather_ingestion.metar_client import MetarIngestor


def test_parse_metar_handles_negative_temperatures():
    ingestor = MetarIngestor()
    raw = "EGLL 051430Z 00000KT 8000 FG BKN005 M02/M03 Q1025"

    obs = ingestor.parse_metar(raw)

    assert obs is not None
    assert obs.temp_c == -2.0
    assert obs.dewpoint_c == -3.0
    assert obs.pressure_hpa == 1025.0


def test_parse_metar_handles_month_rollover():
    ingestor = MetarIngestor()
    raw = "LEMD 311430Z 23005KT 9999 FEW030 18/11 Q1018 NOSIG"

    obs = ingestor.parse_metar(raw, reference_time_utc=datetime(2026, 4, 5, 10, 0))

    assert obs is not None
    assert obs.observed_at_utc == datetime(2026, 3, 31, 14, 30)
