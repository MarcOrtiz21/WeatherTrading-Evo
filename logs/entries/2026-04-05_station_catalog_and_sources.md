# 2026-04-05 - catalogo de estaciones y registro de fuentes

## Contexto

Se ha aplicado la recomendacion de separar el conocimiento operativo de estaciones y fuentes en configuracion estructurada, para dejar de depender de listas pequenas embebidas o mappings duplicados.

## Cambios realizados

- creado `config/station_catalog.yaml` como catalogo unico de estaciones
- ampliado el universo inicial con hubs de Espana, Portugal, Europa y EE. UU.
- anadidos aliases por ciudad y aeropuerto para inferencia textual
- creado `config/source_registry.yaml` con fuentes contractuales, observacionales y auxiliares
- anadidos `provider_mappings` por estacion para `aviation_weather`, `metar`, `wunderground`, `noaa`, `open_meteo`, `meteostat`, `nws` y `ecmwf` cuando aplica
- actualizado `ConfigLoader` para combinar multiples archivos YAML
- ampliado `ResolutionSource` para soportar proveedores nuevos
- actualizado `StationMapperService` para inferir `station_code` por alias y consultar mappings por proveedor
- documentada la estructura en `docs/station_catalog_and_sources.md`
- creada prueba dedicada `scripts/test_station_catalog.py`

## Decisiones tecnicas

- el catalogo de estaciones pasa a vivir fuera del codigo para escalar sin tocar servicios
- el registro de fuentes se mantiene separado del catalogo para no mezclar geografia con semantica de proveedor
- la inferencia por alias suma utilidad, pero no debe sustituir una validacion semantica fuerte
- no se han inventado ids propietarios dudosos; cuando no habia seguridad se ha dejado la fuente registrada pero sin mapping especifico

## Siguiente paso recomendado

Hacer que el parser y la capa de validacion consuman explicitamente:

- `source_priority`
- `provider_mappings`
- `missing_provider_mapping:*`

para bloquear automaticamente mercados cuyo proveedor contractual este citado pero no tenga mapping resoluble.
