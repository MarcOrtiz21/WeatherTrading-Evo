from __future__ import annotations
from datetime import datetime, date
from sqlalchemy import Column, String, Float, DateTime, Date, JSON, ForeignKey, Boolean, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from weather_trading.infrastructure.utils import utc_now

class Base(DeclarativeBase):
    pass

class MarketSpecORM(Base):
    __tablename__ = "market_specs"
    
    market_id: Mapped[str] = mapped_column(String, primary_key=True)
    question: Mapped[str] = mapped_column(String)
    rules_text: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String)
    station_code: Mapped[str] = mapped_column(String)
    timezone: Mapped[str] = mapped_column(String)
    local_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    resolution_source: Mapped[str] = mapped_column(String)
    metric: Mapped[str] = mapped_column(String)
    aggregation: Mapped[str] = mapped_column(String)
    rounding_method: Mapped[str] = mapped_column(String)
    threshold_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    outcomes: Mapped[list[str]] = mapped_column(JSON)
    confidence_score: Mapped[float] = mapped_column(Float)
    notes: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)

class WeatherObservationORM(Base):
    __tablename__ = "weather_observations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    station_code: Mapped[str] = mapped_column(String, index=True)
    provider: Mapped[str] = mapped_column(String)
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime, index=True)
    temp_c: Mapped[float] = mapped_column(Float)
    dewpoint_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    pressure_hpa: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_reference: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
