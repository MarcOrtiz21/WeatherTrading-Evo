import re
import logging
from weather_trading.domain.models import MarketSpec
from weather_trading.infrastructure.config import ConfigLoader

logger = logging.getLogger(__name__)

class StationMapperService:
    """Enriquece un MarketSpec con datos de la estación desde la configuración."""

    def enrich(self, spec: MarketSpec) -> MarketSpec:
        """Busca el station_code en la configuración y rellena los campos faltantes."""
        if spec.station_code == "UNKNOWN":
            inferred_station = self.infer_station_code_from_text(f"{spec.question}\n{spec.rules_text}")
            if inferred_station:
                spec.station_code = inferred_station
                spec.confidence_score = min(1.0, spec.confidence_score + 0.15)
                spec.notes = tuple(dict.fromkeys(spec.notes + (f"inferred_station_code:{inferred_station}",)))
            else:
                return spec

        info = self.get_station(spec.station_code)
        
        if not info:
            logger.warning(f"Estación {spec.station_code} no encontrada en la configuración externa.")
            spec.notes = tuple(dict.fromkeys(spec.notes + (f"missing_station_catalog:{spec.station_code}",)))
            return spec

        spec.city = info.get("city", spec.city)
        spec.country = info.get("country", spec.country)
        spec.timezone = info.get("timezone", spec.timezone)

        provider_key = self.get_provider_key_for_source(spec.resolution_source)
        if provider_key and not self.get_provider_mapping(spec.station_code, provider_key):
            spec.notes = tuple(dict.fromkeys(spec.notes + (f"missing_provider_mapping:{provider_key}",)))

        return spec

    def get_station(self, station_code: str) -> dict | None:
        return ConfigLoader.get(f"stations.{station_code}")

    def get_provider_mapping(self, station_code: str, provider_key: str) -> dict | None:
        return ConfigLoader.get(f"stations.{station_code}.provider_mappings.{provider_key}")

    def get_source_definition(self, provider_key: str) -> dict | None:
        return ConfigLoader.get(f"sources.{provider_key}")

    def get_station_region(self, station_code: str) -> str | None:
        station = self.get_station(station_code)
        if not station:
            return None
        region = station.get("region")
        return None if region is None else str(region)

    def get_allowed_regions(self) -> tuple[str, ...]:
        raw_regions = ConfigLoader.get(
            "operating_universe.allowed_regions",
            ("Europe", "North America", "South America"),
        )
        if not isinstance(raw_regions, (list, tuple)):
            return ("Europe", "North America", "South America")
        normalized = tuple(
            str(region).strip()
            for region in raw_regions
            if str(region).strip()
        )
        return normalized or ("Europe", "North America", "South America")

    def is_region_allowed(self, region: str | None) -> bool:
        if not region:
            return False
        return str(region) in set(self.get_allowed_regions())

    def is_station_allowed(self, station_code: str) -> bool:
        return self.is_region_allowed(self.get_station_region(station_code))

    def list_station_codes(self) -> list[str]:
        stations = ConfigLoader.get("stations", {})
        return sorted(stations.keys())

    def infer_station_code_from_text(self, text: str) -> str | None:
        normalized_text = self._normalize_text(text)
        stations = ConfigLoader.get("stations", {})

        alias_candidates: list[tuple[str, str]] = []
        for station_code, info in stations.items():
            alias_candidates.append((station_code, station_code))
            alias_candidates.append((station_code, info.get("city", "")))
            for alias in info.get("aliases", []):
                alias_candidates.append((station_code, alias))

        alias_candidates.sort(key=lambda item: len(item[1]), reverse=True)
        for station_code, alias in alias_candidates:
            if not alias:
                continue
            if self._normalize_text(alias) in normalized_text:
                return station_code

        return None

    def get_provider_key_for_source(self, source) -> str | None:
        source_value = getattr(source, "value", source)
        if source_value in {None, "unknown", "manual_review", "polymarket"}:
            return None
        return str(source_value)

    def _normalize_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value.strip().lower())
