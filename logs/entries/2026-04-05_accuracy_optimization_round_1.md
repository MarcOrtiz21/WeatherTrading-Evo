# 2026-04-05 - accuracy optimization round 1

## Objetivo

Afinar el sistema para que la distribucion forecast refleje mejor la semantica real de los mercados de temperatura y medir si la optimizacion mejora frente al baseline anterior.

## Cambios aplicados

- el runner live `scripts/run_blind_live_validation.py` ya no usa solo `OpenMeteoDistributionBuilder`
- el flujo live pasa a construir la distribucion principal con:
  - forecast multimodelo de Open-Meteo
  - calibracion local reciente de 7 dias
  - ensemble actual de 30 miembros
- si falla multimodelo, ensemble o calibracion, el pipeline degrada al baseline sin abortar la validacion
- el snapshot live ahora registra:
  - `forecast_model_name`
  - `forecast_std_dev_c`
  - `ensemble_members`
  - `calibration_days`
- el backtest historico `scripts/run_historical_temperature_backtest.py` ahora calcula tambien:
  - `baseline_brier`
  - `optimized_brier`
  - `winner_prob_improvement_rate`
- se reforzo la suite con un test que comprueba que el builder multimodelo da mas peso a los modelos con menor error reciente

## Validacion ejecutada

Comandos usados:

```bash
venv/bin/python -m pytest -q
venv/bin/python scripts/run_historical_temperature_backtest.py --end-date 2026-04-05 --lookback-days 10 --max-events 50
venv/bin/python scripts/run_blind_live_validation.py --as-of-date 2026-04-05 --min-horizon-days 1 --max-events 10 --max-horizon-days 4
```

Resultado tests:

- `23 passed`

Artefactos actualizados:

- `logs/snapshots/2026-04-05_historical_temperature_backtest.json`
- `logs/snapshots/2026-04-05_polymarket_blind_live_validation.json`

## Resultado historico

Muestra historica evaluada:

- `3` eventos
- `0` descartados

Comparativa baseline vs optimized:

- `hit_rate`: `0.0%` -> `66.7%`
- `avg_winner_prob`: `0.132` -> `0.306`
- `log_loss`: `2.053` -> `1.187`
- `brier`: `0.876` -> `0.589`
- `winner_prob_improvement_rate`: `100%`

Lectura:

- la optimizacion sube la probabilidad asignada al ganador real en todos los eventos medidos
- el beneficio no es solo de top-1 hit, tambien mejora la calibracion probabilistica
- la muestra sigue siendo pequena y muy concentrada, asi que todavia no es una validacion global del sistema

## Resultado live ciego

Snapshot live actualizado:

- eventos futuros evaluados: `10`
- eventos descartados: `0`
- `mode_matches`: `3 / 10`
- `positive_top_edges`: `2 / 10`
- `avg_top_edge`: `0.0085`
- `forecast_model_name`: `openmeteo_calibrated_multimodel_v1`
- media de `ensemble_members`: `30`
- media de `calibration_days`: `7`

Lectura:

- frente al snapshot anterior, el sistema ahora es mas conservador con los edges
- esto encaja con una distribucion mejor calibrada y menos agresiva, no necesariamente con peor precision final
- para valorar si este cambio mejora de verdad en live, hara falta esperar a resolucion o ampliar el backtest historico con mas eventos

## Siguiente paso recomendado

- ampliar la muestra historica con mas eventos de temperatura resueltos
- estudiar calibracion por horizonte real de 1, 2, 3 y 4 dias
- incorporar scoring diario automatizado sobre snapshots ciegos cuando los mercados queden resueltos
