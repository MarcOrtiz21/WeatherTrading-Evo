# Weather Trading en Polymarket

Base inicial para construir un sistema que detecte mercados meteorologicos en Polymarket, modele la variable exacta de resolucion del contrato y tome decisiones de trading con trazabilidad completa.

## Idea central

El proyecto no busca "predecir el tiempo" en abstracto.
Busca estimar mejor que el mercado la variable contractual que resuelve cada mercado:

- que estacion cuenta
- que proveedor manda
- que metrica se usa
- que agregacion aplica
- que precision o redondeo decide el resultado

La cadena de valor del sistema es:

`discovery -> parseo de reglas -> mapeo estacion/fuente -> ingesta de observaciones -> forecast probabilistico -> pricing -> riesgo -> ejecucion -> auditoria`

## Principios de construccion

- La variable relevante es contractual, no meteorologica en abstracto.
- El parser y la fuente de resolucion importan tanto como el modelo.
- Se trabaja con probabilidades calibradas, no con una unica temperatura puntual.
- El `risk manager` tiene poder de veto sobre cualquier orden.
- El sistema debe poder explicar por que opero y por que decidio no operar.
- Primero `paper trading`, despues tamano pequeno, y solo al final live controlado.

## Alcance inicial recomendado

- Mercados de temperatura maxima diaria.
- Estaciones aeroportuarias o estaciones claramente identificables.
- Prioridad a mercados con reglas simples y alta confianza de parseo.
- Operativa inicial en modo paper.
- Uso de ordenes limit y filtros conservadores de edge neto.

## Estructura inicial del repo

- `docs/`: decisiones tecnicas y plan de MVP.
- `logs/`: registro operativo y de cambios del proyecto.
- `src/weather_trading/`: paquete Python con contratos base del dominio.
- `config/`: configuracion general, catalogo de estaciones y registro de fuentes.
- `polymarket_weather_*.txt`: documentos de contexto originales del proyecto.

## Documentos clave creados en esta base

- `docs/mvp_foundation.md`
- `logs/README.md`
- `logs/entries/2026-04-05_initial_foundation.md`
- `src/weather_trading/domain/models.py`
- `pyproject.toml`
- `config/station_catalog.yaml`
- `config/source_registry.yaml`
- `docs/station_catalog_and_sources.md`

## Instalacion y portabilidad

Las dependencias runtime y de desarrollo ya estan declaradas en `pyproject.toml`, asi que el proyecto puede reinstalarse en una maquina nueva sin depender del `venv` de este repo.

Pasos base recomendados:

1. Copia o clona el repo en la nueva maquina.
2. Crea un entorno virtual nuevo.
3. Instala el paquete en editable con extras de desarrollo.
4. Ejecuta la suite para validar el entorno.

Comandos genericos:

```bash
python -m venv venv
python -m pip install --upgrade pip
pip install -e .[dev]
python -m pytest -q
```

Notas:

- No copies `venv/` entre macOS y Windows; recrealo en la nueva maquina.
- Si quieres conservar el estado local, copia tambien `logs/`, `config/` y opcionalmente `weather_trading.db`.
- La guia especifica para Windows esta en `docs/windows_setup.md`.

## Siguientes pasos prioritarios

1. Definir un esquema robusto de `MarketSpec` a partir de ejemplos reales de mercados.
2. Crear un catalogo inicial de mercados, estaciones y fuentes de resolucion.
3. Implementar un parser determinista con fallback asistido.
4. Levantar el pipeline de observaciones con controles de calidad y freshness.
5. Construir el baseline probabilistico y su capa de pricing.
6. Conectar la capa de auditoria antes de cualquier decision automatica.
