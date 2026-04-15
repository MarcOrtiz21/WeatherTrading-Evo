# MVP tecnico y orden de construccion

## Objetivo del MVP

Llegar a un primer sistema capaz de:

1. detectar mercados meteorologicos aptos
2. convertir sus reglas a una especificacion estructurada
3. mapear cada mercado a una fuente y estacion de resolucion
4. mantener observaciones utiles y frescas
5. estimar una distribucion discreta de temperatura relevante
6. comparar contra el precio de mercado
7. decidir si hay edge neto suficiente
8. simular la operativa en `paper trading`

## Scope v0

Para reducir complejidad, la primera version deberia asumir:

- solo mercados de temperatura maxima diaria
- solo reglas con fecha local y estacion identificable
- solo contratos con fuente de resolucion trazable
- solo paper trading
- solo ordenes limit
- veto automatico ante ambiguedad, stale data o edge insuficiente

## Modulos minimos

### 1. Market discovery

Responsabilidad:
- listar mercados candidatos
- filtrar weather/temperature
- guardar texto bruto de reglas y metadatos

Salida minima:
- `market_id`
- `title`
- `description`
- `rules_text`
- `outcomes`
- `close_time`

### 2. Rule parser

Responsabilidad:
- extraer un `MarketSpec` normalizado
- puntuar confianza
- marcar ambiguedades

Orden recomendado:
1. parser determinista
2. validadores
3. fallback asistido para casos dudosos

### 3. Station and source mapper

Responsabilidad:
- resolver ciudad -> estacion
- resolver proveedor contractual
- fijar timezone local

### 4. Weather ingestion

Responsabilidad:
- descargar observaciones y fuentes auxiliares
- guardar payload bruto y dato normalizado
- aplicar quality checks

Checks minimos:
- monotonicidad temporal
- rangos plausibles
- stale source detection
- huecos de observacion

### 5. Forecast baseline

Responsabilidad:
- estimar `P(T_resol = k)` para temperatura entera

Baseline recomendado:
- maxima observada del dia
- hora local
- tendencia de temperatura
- señal auxiliar de forecast

### 6. Pricing engine

Responsabilidad:
- convertir distribucion en fair probability
- comparar con precio implicito de mercado
- descontar costes y margen de seguridad

Regla inicial:

`edge_neto = p_modelo - p_mercado - costes - safety_margin`

### 7. Decision and risk gate

Responsabilidad:
- bloquear decisiones peligrosas
- permitir solo senales auditables

Condiciones minimas para operar:
- parser con confianza alta
- fuente de resolucion fresca
- order book disponible
- edge neto positivo
- exposicion dentro de limites

### 8. Audit trail

Responsabilidad:
- guardar la justificacion completa de cada decision

Registro minimo por decision:
- `MarketSpec` usado
- observaciones consumidas
- salida del modelo
- precio de mercado observado
- edge calculado
- motivo de entrada o bloqueo

## Modelo operativo recomendado

La primera cadena funcional deberia ser:

`discover -> parse -> map -> ingest -> baseline -> price -> gate -> audit`

No hace falta conectar ejecucion real al principio.
Hace mas falta demostrar que el sistema:

- entiende bien el contrato
- modela la variable correcta
- detecta cuando no debe operar

## Riesgos que el MVP debe cubrir desde el dia 1

- confundir fuente util con fuente contractual
- usar informacion futura en backtests
- asumir liquidez que no existe
- operar con parser ambiguo
- ignorar freshness de datos
- sobreestimar edge por falta de costes reales

## Backlog inmediato

### Iteracion 1

- formalizar `MarketSpec`
- formalizar `WeatherObservation`
- formalizar `ForecastDistribution`
- preparar un registro estable de cambios

### Iteracion 2

- recolectar ejemplos reales de mercados
- etiquetar manualmente una muestra inicial
- implementar parser determinista para los casos frecuentes

### Iteracion 3

- construir clientes de ingesta meteorologica
- guardar raw payloads y normalizados
- calcular freshness y checks minimos

### Iteracion 4

- crear baseline probabilistico
- convertirlo en fair values
- registrar decisiones de paper trading

## Definition of done para esta fase base

La fase base se puede considerar bien asentada cuando tengamos:

- estructura del repo clara
- contratos del dominio compartidos
- estrategia de MVP escrita
- registro de cambios activo
- backlog tecnico priorizado
- criterio explicito de "no operar"
