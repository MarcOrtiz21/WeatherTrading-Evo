# Registro de cambios

Esta carpeta guarda el historial operativo y tecnico del proyecto.

## Regla de trabajo

Cada cambio relevante debe dejar una entrada nueva en `logs/entries/` con el formato:

`YYYY-MM-DD_vX.Y.Z_descripcion_corta.md`

La version debe avanzar con criterio semantico:

- `major`: cambio amplio o redireccion importante del sistema
- `minor`: nueva capacidad relevante
- `patch`: ajuste incremental, fix o mejora acotada

## Contenido minimo de cada entrada

- version del cambio
- contexto del cambio
- archivos creados o modificados
- decisiones tecnicas tomadas
- riesgos o supuestos
- siguiente paso recomendado

## Nota

Este registro complementa los documentos de arquitectura y nos sirve como pista de auditoria del propio desarrollo del sistema.
