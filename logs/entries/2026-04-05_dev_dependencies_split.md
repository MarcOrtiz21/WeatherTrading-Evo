# 2026-04-05 - separacion de dependencias runtime y desarrollo

## Contexto

Tras revisar el empaquetado del proyecto, se ha dejado separada la capa de dependencias runtime de las herramientas de desarrollo para facilitar instalaciones limpias y preparar mejor CI.

## Cambios realizados

- mantenidas las dependencias de ejecucion en `project.dependencies`
- anadido `project.optional-dependencies.dev` en `pyproject.toml`
- incluidas herramientas base de desarrollo:
  - `pytest`
  - `pytest-asyncio`
  - `ruff`

## Decision tecnica

El proyecto queda preparado para:

- instalar solo runtime en entornos de ejecucion
- instalar `.[dev]` en entornos de desarrollo o CI

## Siguiente paso recomendado

Si quieres, el siguiente ajuste natural es convertir los scripts de prueba en una suite `pytest` real para que CI pueda ejecutarlos de forma estandar.
