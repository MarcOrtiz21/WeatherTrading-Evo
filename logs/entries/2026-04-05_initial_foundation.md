# 2026-04-05 - base inicial del proyecto

## Contexto

Revision de los documentos de arquitectura y contexto iniciales para convertir la idea del proyecto en una base operativa minima dentro del repositorio.

## Cambios realizados

- creado `README.md` con la vision, alcance inicial y prioridades
- creado `docs/mvp_foundation.md` con el orden de construccion del MVP
- creada la carpeta `logs/` con norma explicita de registro
- creada una base Python en `src/weather_trading/`
- definidos contratos de dominio iniciales en `src/weather_trading/domain/models.py`
- creado `pyproject.toml` para fijar el paquete y el runtime minimo
- creado `.gitignore` para preparar el entorno de trabajo

## Decisiones tomadas

- empezar por mercados de temperatura maxima diaria
- priorizar paper trading antes de cualquier ejecucion real
- usar contratos de dominio compartidos antes de construir servicios
- registrar desde el principio las decisiones del propio desarrollo
- dejar el parser y la auditoria como piezas de primera clase, no como anexos

## Supuestos

- Python sera el lenguaje principal del proyecto
- la primera version trabajara con probabilidades discretas por temperatura entera
- cualquier operativa live quedara bloqueada hasta validar acceso, riesgo y fuentes de resolucion

## Siguiente paso recomendado

Construir el esquema operativo de `MarketSpec` y preparar una primera muestra de mercados reales etiquetados para arrancar el parser determinista.
