# 2026-04-05 - MVP Base Completa: Discovery, Parsing, Mapping e Ingestión METAR

## Contexto
Se ha completado la primera fase funcional del sistema. El bot ya es capaz de pasar de un mercado de Polymarket a una observación meteorológica real de forma estructurada.

## Tareas completadas
- **Domain Models:** Estabilizados.
- **GammaClient:** Funcional para discovery.
- **Rule Parser:** `DeterministicParser` validado con mocks realistas.
- **Station Mapper:** `StationMapperService` enriqueciendo con `AIRPORTS` DB.
- **Weather Ingestion:** Implementado `MetarIngestor` con soporte para parseo de temperaturas negativas y presión.
- **Tests:** `scripts/test_parser.py` y `scripts/test_ingestion.py` pasan correctamente.

## Resumen técnico
- El sistema puede identificar que un mercado de Madrid requiere la fuente Wunderground para la estación LEMD y que debe comparar contra un umbral de temperatura.
- El sistema puede descargar y parsear el METAR actual de LEMD para obtener la temperatura observada.

## Siguientes pasos recomendados
1. **Persistencia:** Implementar SQLAlchemy/Postgres para guardar los mercados y observaciones.
2. **Forecast Engine:** Crear el primer `baseline_model.py` que use la temperatura actual + climatología.
3. **Pricing Engine:** Comparar la probabilidad del modelo con el precio del market.
