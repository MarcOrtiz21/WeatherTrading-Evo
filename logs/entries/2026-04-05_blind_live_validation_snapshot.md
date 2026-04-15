# 2026-04-05 - Blind Live Validation Snapshot

## Objetivo

Validar el proyecto con mercados reales de Polymarket sin usar resoluciones ya conocidas y sin contaminar la comparación con mercados del mismo día.

## Cambios aplicados

- Se añadió soporte de dominio para `TEMPERATURE_BIN` con `bin_low_c` y `bin_high_c`.
- El parser ahora entiende:
  - bins exactos en Celsius;
  - bins exactos y rangos en Fahrenheit;
  - fechas de evento derivadas desde el slug si el payload no trae `event_date`.
- El pricing ahora valora bins exactos y, si el forecast tiene metadata gaussiana, usa aproximación continua para bins decimales derivados de Fahrenheit.
- Se añadió `OpenMeteoDistributionBuilder` para generar una distribución discreta desde un forecast puntual.
- Se añadió `PolymarketPublicPageClient` para extraer eventos públicos desde las páginas de Polymarket.
- Se creó `scripts/run_blind_live_validation.py` para congelar snapshots ciegos sobre mercados futuros.
- Se amplió el catálogo con estaciones observadas en la muestra live:
  - `EGLC`
  - `VHHH`
  - `LTAC`
  - `LTFM`
  - `ZBAA`
  - `RKPK`
  - `ZUUU`
  - `ZUCK`
  - `KDAL`
  - `KHOU`

## Ejecución

Comando usado:

```bash
venv/bin/python scripts/run_blind_live_validation.py --as-of-date 2026-04-05 --min-horizon-days 1 --max-events 10 --max-horizon-days 4
```

Artefacto generado:

- `logs/snapshots/2026-04-05_polymarket_blind_live_validation.json`

## Resultado del snapshot

- Eventos futuros evaluados: `10`
- Eventos descartados: `0`
- Coincidencia entre modo del modelo y modo del mercado: `5 / 10`
- Mercados con `top_edge_net > 0`: `5 / 10`
- `avg_top_edge`: `0.0243`

## Lectura operativa

- El sistema ya puede procesar una muestra real y heterogénea de mercados futuros de temperatura diaria.
- La cobertura ha dejado de ser el cuello de botella principal: ahora el límite es la calidad/calibración del forecast.
- Los desacuerdos más fuertes siguen apareciendo en bins desplazados respecto al centro del forecast y en eventos con fuerte consenso de mercado.
- La validación todavía es una foto ciega de divergencia frente al mercado, no una medida de acierto final. Para medir precisión real hace falta esperar a la resolución de estos mismos eventos.

## Siguiente paso recomendado

- Guardar automáticamente estos snapshots por fecha y comparar después contra la resolución final para obtener:
  - hit rate del modo;
  - log loss;
  - Brier score;
  - PnL paper por señal.
