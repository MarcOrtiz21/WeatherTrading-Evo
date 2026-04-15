from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from weather_trading.domain.models import MarketSpec, WeatherObservation
from weather_trading.infrastructure.models_orm import MarketSpecORM, WeatherObservationORM

class WeatherRepository:
    """Repositorio para persistencia de datos de mercado y meteorológicos."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_market_spec(self, spec: MarketSpec):
        """Guarda o actualiza una especificación de mercado."""
        if spec.local_date is None:
            raise ValueError("No se puede persistir un MarketSpec sin local_date resuelta.")

        orm_spec = MarketSpecORM(
            market_id=spec.market_id,
            question=spec.question,
            rules_text=spec.rules_text,
            city=spec.city,
            country=spec.country,
            station_code=spec.station_code,
            timezone=spec.timezone,
            local_date=spec.local_date,
            resolution_source=spec.resolution_source,
            metric=spec.metric,
            aggregation=spec.aggregation,
            rounding_method=spec.rounding_method,
            threshold_c=spec.threshold_c,
            outcomes=list(spec.outcomes),
            confidence_score=spec.confidence_score,
            notes=list(spec.notes)
        )
        await self.session.merge(orm_spec)
        await self.session.commit()

    async def get_market_spec(self, market_id: str) -> Optional[MarketSpec]:
        """Recupera una especificación de mercado."""
        result = await self.session.execute(select(MarketSpecORM).where(MarketSpecORM.market_id == market_id))
        orm_spec = result.scalar_one_or_none()
        if not orm_spec:
            return None
        return MarketSpec(
            market_id=orm_spec.market_id,
            question=orm_spec.question,
            rules_text=orm_spec.rules_text,
            city=orm_spec.city,
            country=orm_spec.country,
            station_code=orm_spec.station_code,
            timezone=orm_spec.timezone,
            local_date=orm_spec.local_date,
            resolution_source=orm_spec.resolution_source,
            metric=orm_spec.metric,
            aggregation=orm_spec.aggregation,
            rounding_method=orm_spec.rounding_method,
            threshold_c=orm_spec.threshold_c,
            outcomes=tuple(orm_spec.outcomes),
            confidence_score=orm_spec.confidence_score,
            notes=tuple(orm_spec.notes)
        )

    async def save_observation(self, obs: WeatherObservation):
        """Guarda una observación meteorológica."""
        orm_obs = WeatherObservationORM(
            station_code=obs.station_code,
            provider=self._provider_value(obs),
            observed_at_utc=obs.observed_at_utc,
            temp_c=obs.temp_c,
            dewpoint_c=obs.dewpoint_c,
            pressure_hpa=obs.pressure_hpa,
            raw_reference=obs.raw_reference
        )
        self.session.add(orm_obs)
        await self.session.commit()

    async def upsert_observation(self, obs: WeatherObservation) -> bool:
        """Guarda o actualiza una observación usando station/provider/timestamp como clave natural."""
        provider_value = self._provider_value(obs)
        result = await self.session.execute(
            select(WeatherObservationORM).where(
                WeatherObservationORM.station_code == obs.station_code,
                WeatherObservationORM.provider == provider_value,
                WeatherObservationORM.observed_at_utc == obs.observed_at_utc,
            )
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            orm_obs = WeatherObservationORM(
                station_code=obs.station_code,
                provider=provider_value,
                observed_at_utc=obs.observed_at_utc,
                temp_c=obs.temp_c,
                dewpoint_c=obs.dewpoint_c,
                pressure_hpa=obs.pressure_hpa,
                raw_reference=obs.raw_reference,
            )
            self.session.add(orm_obs)
            await self.session.commit()
            return True

        existing.temp_c = obs.temp_c
        existing.dewpoint_c = obs.dewpoint_c
        existing.pressure_hpa = obs.pressure_hpa
        existing.raw_reference = obs.raw_reference
        await self.session.commit()
        return False

    async def get_latest_observations(self, station_code: str, limit: int = 10) -> List[WeatherObservation]:
        """Recupera las últimas observaciones de una estación."""
        result = await self.session.execute(
            select(WeatherObservationORM)
            .where(WeatherObservationORM.station_code == station_code)
            .order_by(WeatherObservationORM.observed_at_utc.desc())
            .limit(limit)
        )
        orm_list = result.scalars().all()
        return [
            WeatherObservation(
                station_code=o.station_code,
                provider=o.provider,
                observed_at_utc=o.observed_at_utc,
                temp_c=o.temp_c,
                dewpoint_c=o.dewpoint_c,
                pressure_hpa=o.pressure_hpa,
                raw_reference=o.raw_reference
            ) for o in orm_list
        ]

    @staticmethod
    def _provider_value(obs: WeatherObservation) -> str:
        return str(getattr(obs.provider, "value", obs.provider))
