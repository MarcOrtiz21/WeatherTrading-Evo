# Catalogo de estaciones y registro de fuentes

## Objetivo

Separar tres capas que antes estaban mezcladas:

1. catalogo de estaciones
2. mapeos por proveedor
3. registro semantico de fuentes

## Archivos

- `config/station_catalog.yaml`
- `config/source_registry.yaml`

## Estructura del catalogo de estaciones

Cada estacion define:

- ciudad
- pais
- region
- timezone
- latitud / longitud
- aliases de texto libre
- `provider_mappings`

Los `provider_mappings` permiten resolver rapidamente que identificador usar segun la fuente:

- `icao`
- `airport_code`
- `station_id`
- `latitude/longitude`

## Estructura del registro de fuentes

Cada fuente define:

- etiqueta
- rol en el sistema
- si es oficial o auxiliar
- estrategia de mapeo
- capacidades
- notas operativas

Ademas se mantiene `source_priority` para:

- resolucion contractual
- observacion en tiempo real
- historico
- forecast auxiliar

## Uso actual en codigo

`StationMapperService` ya utiliza:

- enriquecimiento por `station_code`
- inferencia por alias si el parser no encontro ICAO
- consulta de mappings por proveedor
- consulta de definiciones de fuente

## Regla de mantenimiento

Antes de anadir una fuente nueva o una estacion nueva:

1. decidir si la fuente es contractual, observacional o auxiliar
2. definir la estrategia de mapeo
3. anadir aliases solo si reducen ambiguedad y no la introducen
4. no inventar ids de proveedor que no esten verificados
