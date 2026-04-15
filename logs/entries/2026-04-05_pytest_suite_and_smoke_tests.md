# 2026-04-05 - suite pytest y smoke tests del pipeline

## Contexto

Se ha convertido la validacion actual basada en scripts sueltos en una suite reproducible con `pytest`, con foco en comprobar si el MVP realmente aguanta bien los casos base antes de optimizar.

## Cambios realizados

- creada la carpeta `tests/`
- anadido `tests/conftest.py` con fixtures comunes y base temporal para SQLite
- anadidos tests unitarios para:
  - parser
  - metar ingestion
  - pricing
  - station catalog
- anadidos tests de integracion para:
  - repository
  - pipeline smoke
  - `run_full_cycle` con red simulada en fallo
- configurado `pytest` en `pyproject.toml`

## Objetivo operativo

Esta suite permite validar rapidamente:

- semantica contractual basica
- robustez del parseo temporal
- comportamiento del pricing
- persistencia
- smoke flow del pipeline

## Resultado de validacion

Ejecucion local de referencia:

- `venv/bin/python -m pytest -q`
- resultado: `11 passed`

Interpretacion:

- la base actual del MVP es coherente en sus happy paths y smoke paths controlados
- todavia no sustituye validacion con datos live ni pruebas contra mercados reales

## Siguiente paso recomendado

Llevar parte de la heuristica actual a metricas comparables:

- tasa de specs con `requires_manual_review`
- tasa de mercados con `missing_provider_mapping:*`
- porcentaje de senales bloqueadas por precision contractual no soportada
