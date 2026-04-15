import math
import re
from datetime import datetime, date
from typing import Any, Dict, Optional
import logging
from weather_trading.domain.models import (
    MarketSpec, ResolutionSource, MetricKind, TimeAggregation, RoundingMethod
)

logger = logging.getLogger(__name__)

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

class DeterministicParser:
    """Parser basado en reglas y regex para extraer MarketSpec de texto libre."""

    def parse(self, market_data: Dict[str, Any]) -> Optional[MarketSpec]:
        """Extrae un MarketSpec de los datos de un mercado de Polymarket."""
        title = market_data.get("question", "")
        description = market_data.get("description", "")
        rules = market_data.get("rules", "")
        resolution_source_url = market_data.get("resolution_source_url", "")
        
        # Guardar original para buscar códigos ICAO (suelen ir en mayúsculas o paréntesis)
        full_text_raw = f"{title}\n{description}\n{rules}\n{resolution_source_url}"
        full_text_lower = full_text_raw.lower()

        # 1. Identificar Ciudad / Estación
        # Prioridad 1: Código en paréntesis (LEMD) o (EGLL)
        station_match = re.search(r"\(([A-Z]{4})\)", full_text_raw)
        if not station_match:
            # Prioridad 2: Código de 4 letras que no sea una palabra común
            # Intentamos buscar palabras de 4 letras en mayúsculas que no sean "WILL", "WHAT", etc.
            matches = re.findall(r"\b([A-Z]{4})\b", full_text_raw)
            common_words = {"WILL", "WHAT", "THIS", "THAT", "EACH", "HIGH", "DATE", "NOAA", "NWS", "AEMET"}
            for m in matches:
                if m not in common_words:
                    station_match = re.match(r".*", m) # Dummy match object
                    station_code = m
                    break
            else:
                station_code = "UNKNOWN"
        else:
            station_code = station_match.group(1)

        # 2. Identificar Fecha
        local_date = self._extract_local_date(
            full_text_raw,
            full_text_lower,
            market_data.get("event_date"),
            market_data.get("endDate"),
        )

        # 3. Identificar Fuente de Resolución
        source = ResolutionSource.UNKNOWN
        if "wunderground" in full_text_lower:
            source = ResolutionSource.WUNDERGROUND
        elif "noaa" in full_text_lower:
            source = ResolutionSource.NOAA
        elif "weather.gov.hk" in full_text_lower or "hong kong observatory" in full_text_lower:
            source = ResolutionSource.HONG_KONG_OBSERVATORY
        elif "aviationweather" in full_text_lower or "aviation weather" in full_text_lower:
            source = ResolutionSource.AVIATION_WEATHER
        elif "metar" in full_text_lower:
            source = ResolutionSource.METAR
        elif "open-meteo" in full_text_lower or "open meteo" in full_text_lower:
            source = ResolutionSource.OPEN_METEO
        elif "aemet" in full_text_lower:
            source = ResolutionSource.AEMET
        elif "meteostat" in full_text_lower:
            source = ResolutionSource.METEOSTAT
        elif "weather.gov" in full_text_lower or "national weather service" in full_text_lower:
            source = ResolutionSource.NWS
        elif "ecmwf" in full_text_lower:
            source = ResolutionSource.ECMWF

        # 4. Identificar Métrica y Agregación
        metric = MetricKind.MAX_TEMP_C
        aggregation = TimeAggregation.DAILY_MAX
        if "min" in full_text_lower or "lowest" in full_text_lower:
            metric = MetricKind.MIN_TEMP_C
            aggregation = TimeAggregation.DAILY_MIN

        # 5. Identificar Umbral o Bin Exacto
        threshold_c = None
        bin_low_c, bin_high_c = self._extract_temperature_bin(full_text_lower)
        if bin_low_c is not None or bin_high_c is not None:
            metric = MetricKind.TEMPERATURE_BIN
        else:
            threshold_match = re.search(
                r"(-?\d{1,3}(\.\d)?)\s*(°?c|degrees celsius|grados celsius)",
                full_text_lower,
            )
            if threshold_match:
                threshold_c = float(threshold_match.group(1))
            else:
                # Buscar Fahrenheit y convertir
                f_match = re.search(r"(-?\d{1,3}(\.\d)?)\s*(°?f|degrees fahrenheit|grados fahrenheit)", full_text_lower)
                if f_match:
                    threshold_f = float(f_match.group(1))
                    threshold_c = round((threshold_f - 32) * 5 / 9, 1)

        # 6. Identificar Regla de Redondeo
        rounding = RoundingMethod.NONE
        if "nearest" in full_text_lower or "redondeo al entero" in full_text_lower:
            rounding = RoundingMethod.NEAREST_HALF_UP
        elif "decimal" in full_text_lower:
            rounding = RoundingMethod.STRICT_DECIMAL

        # 7. Confianza
        notes: list[str] = []
        confidence = 0.0
        if station_code != "UNKNOWN":
            confidence += 0.3
        else:
            notes.append("missing_station_code")

        if source != ResolutionSource.UNKNOWN:
            confidence += 0.3
        else:
            notes.append("missing_resolution_source")

        if threshold_c is not None or bin_low_c is not None or bin_high_c is not None:
            confidence += 0.3
        else:
            notes.append("missing_pricing_target")

        if local_date is not None:
            confidence += 0.1
        else:
            notes.append("missing_or_unparsed_local_date")

        return MarketSpec(
            market_id=market_data.get("id", "UNKNOWN"),
            question=title,
            rules_text=rules,
            city="UNKNOWN", 
            country="UNKNOWN",
            station_code=station_code,
            timezone="UTC", 
            local_date=local_date,
            resolution_source=source,
            metric=metric,
            aggregation=aggregation,
            rounding_method=rounding,
            threshold_c=threshold_c,
            bin_low_c=bin_low_c,
            bin_high_c=bin_high_c,
            outcomes=tuple(market_data.get("outcomes", [])),
            confidence_score=min(confidence, 1.0),
            notes=tuple(notes)
        )

    def _extract_local_date(
        self,
        full_text_raw: str,
        full_text_lower: str,
        event_date: str | None = None,
        end_date: str | None = None,
    ) -> date | None:
        iso_match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", full_text_raw)
        if iso_match:
            year, month, day = map(int, iso_match.groups())
            return date(year, month, day)

        english_match = re.search(r"\b([A-Za-z]+ \d{1,2}, \d{4})\b", full_text_raw)
        if english_match:
            try:
                return datetime.strptime(english_match.group(1), "%B %d, %Y").date()
            except ValueError:
                logger.debug("No se pudo parsear fecha inglesa: %s", english_match.group(1))

        english_alt_match = re.search(r"\b(\d{1,2} [A-Za-z]+ \d{4})\b", full_text_raw)
        if english_alt_match:
            try:
                return datetime.strptime(english_alt_match.group(1), "%d %B %Y").date()
            except ValueError:
                logger.debug("No se pudo parsear fecha inglesa alternativa: %s", english_alt_match.group(1))

        spanish_match = re.search(r"\b(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})\b", full_text_lower)
        if spanish_match:
            day_s, month_name, year_s = spanish_match.groups()
            month = SPANISH_MONTHS.get(month_name)
            if month is None:
                return None
            return date(int(year_s), month, int(day_s))

        for fallback in (event_date, end_date):
            parsed = self._parse_iso_date_prefix(fallback)
            if parsed is not None:
                return parsed

        return None

    def _extract_temperature_bin(self, full_text_lower: str) -> tuple[float | None, float | None]:
        bin_context = "temperatura más alta" in full_text_lower or "highest temperature" in full_text_lower
        if not bin_context:
            return None, None

        unit_kind = self._infer_temperature_unit_kind(full_text_lower)

        between_match = re.search(
            r"(?:between|entre)\s+(-?\d{1,3}(?:\.\d)?)\s*-\s*(-?\d{1,3}(?:\.\d)?)\s*(°?c|°?f|degrees celsius|degrees fahrenheit|grados celsius|grados fahrenheit)",
            full_text_lower,
        )
        if between_match:
            return self._build_temperature_range(
                low_raw=float(between_match.group(1)),
                high_raw=float(between_match.group(2)),
                raw_unit=between_match.group(3),
                unit_kind=unit_kind,
                lower_tail=False,
                upper_tail=False,
            )

        lower_match = re.search(
            r"(-?\d{1,3}(?:\.\d)?)\s*(°?c|°?f|degrees celsius|degrees fahrenheit|grados celsius|grados fahrenheit)\s*(?:o menos|or less|or below|or lower)",
            full_text_lower,
        )
        if lower_match:
            _, high_value = self._build_temperature_range(
                low_raw=float(lower_match.group(1)),
                high_raw=float(lower_match.group(1)),
                raw_unit=lower_match.group(2),
                unit_kind=unit_kind,
                lower_tail=True,
                upper_tail=False,
            )
            return None, high_value

        upper_match = re.search(
            r"(-?\d{1,3}(?:\.\d)?)\s*(°?c|°?f|degrees celsius|degrees fahrenheit|grados celsius|grados fahrenheit)\s*(?:o m[aá]s|or more|or above|or higher)",
            full_text_lower,
        )
        if upper_match:
            low_value, _ = self._build_temperature_range(
                low_raw=float(upper_match.group(1)),
                high_raw=float(upper_match.group(1)),
                raw_unit=upper_match.group(2),
                unit_kind=unit_kind,
                lower_tail=False,
                upper_tail=True,
            )
            return low_value, None

        exact_match = re.search(
            r"(?:ser[aá]\s+la\s+temperatura\s+m[aá]s\s+alta.*?\s+de|ser[aá]\s+de|will\s+the\s+highest\s+temperature.*?\s+be|highest\s+temperature.*?\s+be)\s+(-?\d{1,3}(?:\.\d)?)\s*(°?c|°?f|degrees celsius|degrees fahrenheit|grados celsius|grados fahrenheit)",
            full_text_lower,
        )
        if exact_match:
            return self._build_temperature_range(
                low_raw=float(exact_match.group(1)),
                high_raw=float(exact_match.group(1)),
                raw_unit=exact_match.group(2),
                unit_kind=unit_kind,
                lower_tail=False,
                upper_tail=False,
            )

        return None, None

    def _normalize_temp_to_c(self, raw_value: str, raw_unit: str) -> float:
        value = float(raw_value)
        unit = raw_unit.lower()
        if "f" in unit or "fahrenheit" in unit:
            return round((value - 32) * 5 / 9, 1)
        return value

    def _infer_temperature_unit_kind(self, full_text_lower: str) -> str:
        if "whole degrees fahrenheit" in full_text_lower or "degrees fahrenheit" in full_text_lower:
            return "fahrenheit_whole"
        if "one decimal place" in full_text_lower or "to one decimal place" in full_text_lower:
            return "celsius_tenth"
        if "degrees celsius" in full_text_lower or "grados celsius" in full_text_lower or "°c" in full_text_lower:
            return "celsius_tenth"
        return "generic"

    def _build_temperature_range(
        self,
        low_raw: float,
        high_raw: float,
        raw_unit: str,
        unit_kind: str,
        lower_tail: bool,
        upper_tail: bool,
    ) -> tuple[float | None, float | None]:
        unit = raw_unit.lower()
        is_fahrenheit = "f" in unit or "fahrenheit" in unit

        if is_fahrenheit or unit_kind == "fahrenheit_whole":
            if lower_tail:
                return None, self._fahrenheit_to_c(high_raw + 0.5)
            if upper_tail:
                return self._fahrenheit_to_c(low_raw - 0.5), None
            return (
                self._fahrenheit_to_c(low_raw - 0.5),
                self._fahrenheit_to_c(high_raw + 0.5),
            )

        if lower_tail:
            return None, round(high_raw + 0.9, 1)
        if upper_tail:
            return low_raw, None
        return low_raw, round(high_raw + 0.9, 1)

    def _fahrenheit_to_c(self, value_f: float) -> float:
        return (value_f - 32) * 5 / 9

    def _parse_iso_date_prefix(self, value: str | None) -> date | None:
        if not value:
            return None

        match = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
        if not match:
            return None

        year, month, day = map(int, match.groups())
        return date(year, month, day)
