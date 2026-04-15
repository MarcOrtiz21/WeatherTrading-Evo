# WeatherTrading Evo

Sistema de investigacion y validacion para operar mercados meteorologicos de Polymarket con foco en temperatura diaria.

El objetivo no es "predecir el tiempo" en abstracto, sino estimar con la mayor precision posible la variable contractual que resuelve cada mercado: estacion, proveedor, metrica, agregacion, redondeo y condiciones de settlement.

## Estado del proyecto

El proyecto se encuentra en fase de `late R&D / disciplined paper-trading`.

La base actual ya cubre de extremo a extremo:

- discovery de mercados
- parseo de reglas
- mapeo de estaciones y fuentes
- ingesta de observaciones y forecasts
- construccion de distribuciones probabilisticas
- pricing y filtros de calidad
- gates de evidencia operativa
- paper audit ex post
- overlay de watchlist y veto de riesgo

La operativa real sigue desactivada. El sistema esta orientado a:

1. validar edge en paper
2. entender fallos de calibracion
3. reforzar filtros antes de cualquier paso a `dry_run` o `live`

## Principios de trabajo

- La verdad relevante es contractual antes que meteorologica.
- El parser, la estacion y la fuente de settlement importan tanto como el modelo.
- Las decisiones se toman sobre distribuciones y probabilidades, no sobre un unico punto.
- Riesgo y calidad de mercado pueden vetar cualquier operacion.
- Todo cambio relevante queda registrado en `logs/`.

## Estructura

- `config/`: politica de forecast, settings, catalogo de estaciones, fuentes y watchlist.
- `docs/`: notas tecnicas y documentos de referencia del proyecto.
- `logs/`: snapshots, auditorias y registro versionado de cambios.
- `scripts/`: runners operativos, backtests, auditorias y utilidades de validacion.
- `src/weather_trading/`: codigo fuente del sistema.
- `tests/`: suite automatizada de pruebas.

## Instalacion

```bash
python -m venv venv
python -m pip install --upgrade pip
pip install -e .[dev]
python -m pytest -q
```

## Notas de portabilidad

- No copies `venv/` entre maquinas; recrealo.
- La base local `weather_trading.db` no forma parte del repositorio.
- Las rutas y automatizaciones deben revisarse si cambias de sistema o workspace.
- La guia de Windows esta en `docs/windows_setup.md`.

## Ejecucion habitual

Ejemplos de comandos utiles:

```bash
venv/bin/python scripts/run_blind_live_validation.py --as-of-date 2026-04-15 --max-events 20 --min-horizon-days 1 --max-horizon-days 4
venv/bin/python scripts/run_observation_backfill.py --reference-date 2026-04-15
venv/bin/python scripts/run_blind_snapshot_resolution_audit.py --reference-date 2026-04-15
venv/bin/python scripts/run_watchlist_strategy_simulation.py --reference-date 2026-04-15
```

## Logs y trazabilidad

Cada iteracion relevante se documenta con:

- entrada versionada en `logs/entries/`
- snapshot en `logs/snapshots/` cuando aplica

Esto permite reconstruir:

- que cambio
- por que cambio
- como se valido
- que resultado dio

## Seguridad del repositorio

El repositorio ignora por defecto:

- bases locales y caches
- entornos virtuales
- ficheros `.env`
- certificados y claves
- documentos privados de contexto no pensados para publicarse

## Siguiente criterio de avance

Los siguientes pasos del proyecto se deciden en funcion de evidencia, no de intuicion:

- mejora en `paper PnL`
- estabilidad de `ROI`
- evolucion de `log loss` y `Brier`
- comportamiento por cohorte, familia de bin y calidad de mercado
- concordancia con settlement contractual real
