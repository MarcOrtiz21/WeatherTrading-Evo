from weather_trading.infrastructure import database


def test_default_database_url_uses_project_root(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    url = database.build_database_url()

    assert url == f"sqlite+aiosqlite:///{database.DEFAULT_DB_PATH.as_posix()}"
    assert database.DEFAULT_DB_PATH.is_absolute()
    assert url.endswith("/weather_trading.db")


def test_database_url_respects_environment_override(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///custom.db")

    assert database.build_database_url() == "sqlite+aiosqlite:///custom.db"
