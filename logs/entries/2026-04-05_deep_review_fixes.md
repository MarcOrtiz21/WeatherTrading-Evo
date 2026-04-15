# 2026-04-05 - correcciones tras revision profunda

## Contexto

Se ha realizado una revision tecnica profunda del MVP para corregir problemas estructurales detectados en empaquetado, parseo semantico, pricing contractual, parseo temporal de METAR y robustez de los scripts de validacion.

## Hallazgos corregidos

- declaradas las dependencias runtime reales en `pyproject.toml`
- normalizados los imports del proyecto para usar `weather_trading` como paquete real
- corregido el parser para soportar fechas en ingles, espanol e ISO
- eliminado el fallback silencioso a la fecha actual cuando no se puede parsear la fecha
- corregido el pricing de thresholds decimales para no truncar con `int(...)`
- anadido bloqueo explicito cuando una distribucion entera se usa sobre contratos con threshold decimal estricto
- corregido el parseo temporal de METAR para cambios de mes y fin de mes
- corregido el fallback de `run_full_cycle.py` para que use mock si falla la red
- endurecida la demo para abortar si el `MarketSpec` sigue siendo ambiguo
- protegido el repositorio para no persistir `MarketSpec` sin fecha contractual resuelta

## Archivos modificados

- `pyproject.toml`
- `src/weather_trading/domain/models.py`
- `src/weather_trading/domain/__init__.py`
- `src/weather_trading/infrastructure/database.py`
- `src/weather_trading/infrastructure/models_orm.py`
- `src/weather_trading/infrastructure/utils.py`
- `src/weather_trading/services/market_discovery/gamma_client.py`
- `src/weather_trading/services/rule_parser/deterministic_parser.py`
- `src/weather_trading/services/station_mapper/service.py`
- `src/weather_trading/services/weather_ingestion/metar_client.py`
- `src/weather_trading/services/weather_ingestion/openmeteo_client.py`
- `src/weather_trading/services/forecast_engine/features.py`
- `src/weather_trading/services/forecast_engine/baseline.py`
- `src/weather_trading/services/forecast_engine/ml_model.py`
- `src/weather_trading/services/pricing_engine/service.py`
- `src/weather_trading/services/persistence/repository.py`
- `src/weather_trading/services/execution_engine/order_router.py`
- `scripts/test_parser.py`
- `scripts/test_ingestion.py`
- `scripts/test_persistence.py`
- `scripts/test_ml_refinement.py`
- `scripts/test_execution_engine.py`
- `scripts/run_full_cycle.py`
- `scripts/test_pricing.py`

## Decisiones tecnicas

- `MarketSpec.local_date` pasa a ser opcional para evitar semantica falsa cuando la fecha no se puede resolver
- el sistema debe marcar para revision manual cualquier `MarketSpec` sin fecha local parseada
- los thresholds decimales ya no se truncaran nunca al entero inferior
- mientras el forecast siga siendo entero, los contratos con precision decimal estricta deben tratarse con cautela y bloqueo explicito
- los scripts deben funcionar desde el repo sin depender de `PYTHONPATH=.` manual

## Verificacion local prevista

- `scripts/test_parser.py`
- `scripts/test_ingestion.py`
- `scripts/test_persistence.py`
- `scripts/test_ml_refinement.py`
- `scripts/test_execution_engine.py`
- `scripts/test_pricing.py`
- `scripts/run_full_cycle.py`

## Siguiente paso recomendado

Construir una capa formal de validacion de `MarketSpec` y un registro de bloqueos de riesgo para diferenciar:

- parser ambiguo
- fuente contractual incierta
- precision contractual no soportada por el forecast
- observaciones stale o inconsistentes
