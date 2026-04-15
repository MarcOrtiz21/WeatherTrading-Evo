import sys
from pathlib import Path

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.infrastructure.models_orm import Base


@pytest_asyncio.fixture(autouse=True)
async def clear_config_cache():
    ConfigLoader.clear_cache()
    yield
    ConfigLoader.clear_cache()


@pytest_asyncio.fixture
async def session_factory(tmp_path):
    db_path = tmp_path / "weather_trading_test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        yield factory
    finally:
        await engine.dispose()
