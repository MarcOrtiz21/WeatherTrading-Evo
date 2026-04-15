import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from weather_trading.infrastructure.models_orm import Base

import scripts.run_full_cycle as run_full_cycle_module


@pytest.mark.asyncio
async def test_run_full_cycle_completes_with_mocked_network(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "run_full_cycle.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def fake_init_db():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def fake_fetch_metar(self, station_code: str):
        raise RuntimeError("network down")

    monkeypatch.setattr(run_full_cycle_module, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(run_full_cycle_module, "init_db", fake_init_db)
    monkeypatch.setattr(run_full_cycle_module.MetarIngestor, "fetch_metar", fake_fetch_metar)

    try:
        await run_full_cycle_module.run_full_cycle()
        output = capsys.readouterr().out
        assert "usando mock" in output
        assert "CICLO COMPLETADO CON ÉXITO" in output
    finally:
        await engine.dispose()
