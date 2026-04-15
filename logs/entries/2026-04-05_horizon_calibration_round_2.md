# 2026-04-05 - horizon calibration round 2

## Objetivo

Mejorar la calibracion del sistema sin anadir nuevas fuentes externas, diferenciando el comportamiento del forecast a 1, 2, 3 y 4 dias vista.

## Cambios aplicados

- se anadio `open_meteo_previous_runs_url` en `config/settings.yaml`
- `OpenMeteoClient` ahora soporta:
  - `fetch_previous_runs_history`
  - `fetch_horizon_calibration_window`
- el pipeline live en `scripts/run_blind_live_validation.py` ya usa calibracion por horizonte cuando la fecha de referencia coincide con hoy
- si `Previous Runs` no esta disponible, el sistema sigue degradando al esquema anterior
- se creo `scripts/run_recent_horizon_temperature_backtest.py` para medir precision reciente por horizonte real
- se anadieron tests para la logica de filtrado y fallback de calibracion por horizonte

## Validacion ejecutada

Comandos usados:

```bash
venv/bin/python -m pytest -q
venv/bin/python scripts/run_recent_horizon_temperature_backtest.py --as-of-date 2026-04-05 --lookback-days 10 --max-events 20 --max-horizon-days 4
venv/bin/python scripts/run_blind_live_validation.py --as-of-date 2026-04-05 --min-horizon-days 1 --max-events 10 --max-horizon-days 4
```

Resultado tests:

- `25 passed`

Artefactos generados o actualizados:

- `logs/snapshots/2026-04-05_recent_horizon_temperature_backtest.json`
- `logs/snapshots/2026-04-05_polymarket_blind_live_validation.json`

## Resultado del backtest reciente por horizonte

Muestra evaluada:

- `12` filas
- `3` eventos resueltos
- `4` horizontes por evento
- `0` descartados

Resultado global baseline vs optimized:

- `hit_rate`: `0.0%` -> `66.7%`
- `avg_winner_prob`: `0.118` -> `0.269`
- `log_loss`: `2.150` -> `1.318`
- `brier`: `0.895` -> `0.637`
- `winner_prob_improvement_rate`: `100%`

Resultado por horizonte:

- `H1`: `log_loss=1.187`, `brier=0.589`, `hit=66.7%`
- `H2`: `log_loss=1.279`, `brier=0.625`, `hit=66.7%`
- `H3`: `log_loss=1.364`, `brier=0.655`, `hit=66.7%`
- `H4`: `log_loss=1.443`, `brier=0.681`, `hit=66.7%`

Lectura:

- la calidad cae gradualmente con el horizonte, como era esperable
- aun asi, la version optimizada sigue mejorando de forma consistente frente al baseline en 1, 2, 3 y 4 dias
- esta es una validacion mas util que mirar solo el modo live contra el mercado, porque mide contra temperatura real resuelta

## Resultado live ciego actualizado

- eventos futuros evaluados: `10`
- `mode_matches`: `3 / 10`
- `positive_top_edges`: `2 / 10`
- `avg_top_edge`: `0.0077`
- media de `calibration_days`: `6.4`

Lectura:

- el snapshot live sigue siendo conservador en edges
- despues de introducir calibracion por horizonte, el sistema mantiene pocas oportunidades netas positivas
- esto refuerza la idea de que la mejora actual esta ayudando mas a calibrar que a forzar trades

## Cautela operativa

- el backtest por horizonte depende de `Previous Runs` y por eso esta implementado como evaluacion de fecha actual, no como replay arbitrario para cualquier fecha pasada
- la muestra sigue siendo pequena; la siguiente palanca importante es ampliar discovery de eventos resueltos para medir con mas casos
