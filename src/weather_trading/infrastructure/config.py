import yaml
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATHS = (
    PROJECT_ROOT / "config" / "settings.yaml",
    PROJECT_ROOT / "config" / "forecast_policy.yaml",
    PROJECT_ROOT / "config" / "station_catalog.yaml",
    PROJECT_ROOT / "config" / "source_registry.yaml",
    PROJECT_ROOT / "config" / "watchlist.yaml",
)


class ConfigLoader:
    """Cargador de configuración YAML."""
    
    _config: Dict[str, Any] = {}

    @classmethod
    def load(cls, config_paths: tuple[Path, ...] | None = None) -> Dict[str, Any]:
        """Carga y cachea la configuración combinando varios YAML."""
        if not cls._config:
            merged: Dict[str, Any] = {}
            for config_path in config_paths or DEFAULT_CONFIG_PATHS:
                if not config_path.exists():
                    continue
                with config_path.open("r", encoding="utf-8") as handle:
                    payload = yaml.safe_load(handle) or {}
                merged = cls._deep_merge(merged, payload)
            cls._config = merged
        return cls._config

    @classmethod
    def get(cls, key_path: str, default: Any = None) -> Any:
        """Obtiene un valor anidado usando puntos (ej: 'polymarket.chain_id')."""
        config = cls.load()
        keys = key_path.split(".")
        value = config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    @classmethod
    def clear_cache(cls) -> None:
        cls._config = {}

    @staticmethod
    def _deep_merge(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(base)
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = ConfigLoader._deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
