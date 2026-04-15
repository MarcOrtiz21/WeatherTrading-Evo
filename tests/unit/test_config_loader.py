from pathlib import Path

from weather_trading.infrastructure.config import ConfigLoader


def test_config_loader_merges_multiple_yaml_files(tmp_path: Path):
    base_config = tmp_path / "base.yaml"
    extra_config = tmp_path / "extra.yaml"

    base_config.write_text(
        """
app:
  env: test
  features:
    parser: true
stations:
  LEMD:
    city: Madrid
    provider_mappings:
      wunderground:
        airport_code: LEMD
""".strip(),
        encoding="utf-8",
    )
    extra_config.write_text(
        """
app:
  log_level: DEBUG
  features:
    trading: true
stations:
  LEMD:
    country: Spain
    provider_mappings:
      open_meteo:
        icao: LEMD
""".strip(),
        encoding="utf-8",
    )

    merged = ConfigLoader.load((base_config, extra_config))

    assert merged["app"] == {
        "env": "test",
        "log_level": "DEBUG",
        "features": {
            "parser": True,
            "trading": True,
        },
    }
    assert merged["stations"]["LEMD"]["city"] == "Madrid"
    assert merged["stations"]["LEMD"]["country"] == "Spain"
    assert merged["stations"]["LEMD"]["provider_mappings"] == {
        "wunderground": {"airport_code": "LEMD"},
        "open_meteo": {"icao": "LEMD"},
    }


def test_config_loader_get_returns_default_for_missing_key():
    ConfigLoader._config = {"app": {"env": "test"}}

    assert ConfigLoader.get("app.env") == "test"
    assert ConfigLoader.get("app.missing", "fallback") == "fallback"
    assert ConfigLoader.get("missing.path", 42) == 42
