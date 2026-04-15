# Windows Setup

Guia corta para mover este proyecto desde macOS a Windows sin arrastrar dependencias locales del entorno anterior.

## Requisitos

- Windows 10 o 11
- Python `3.11` o superior
- Git opcional si vas a clonar el repo en vez de copiarlo
- PowerShell

## Que copiar a Windows

- Todo el repo
- `config/`
- `logs/`
- `tests/`
- `docs/`
- Opcionalmente `weather_trading.db` si quieres mantener la base local

## Que no copiar

- `venv/`

Ese entorno virtual es especifico de la maquina y del sistema operativo de origen.

## Instalacion manual

Desde la raiz del repo:

```powershell
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .[dev]
python -m pytest -q
```

Si PowerShell bloquea la activacion del entorno virtual:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\venv\Scripts\Activate.ps1
```

## Bootstrap automatico

Tambien puedes usar el script:

```powershell
.\scripts\bootstrap_windows.ps1
```

Opciones utiles:

```powershell
.\scripts\bootstrap_windows.ps1 -SkipTests
.\scripts\bootstrap_windows.ps1 -PythonVersion 3.12
```

## Comandos recomendados en Windows

En vez de usar rutas tipo `venv/bin/python`, usa:

```powershell
.\venv\Scripts\python.exe -m pytest -q
.\venv\Scripts\python.exe scripts\run_blind_live_validation.py --as-of-date 2026-04-06 --min-horizon-days 1 --max-events 20 --max-horizon-days 4
.\venv\Scripts\python.exe scripts\run_blind_snapshot_resolution_audit.py --reference-date 2026-04-07
```

Tambien puedes activar el entorno y usar `python` directamente.

## Portabilidad actual del proyecto

- El paquete y sus dependencias ya se instalan desde `pyproject.toml`.
- La base de datos por defecto usa una ruta relativa: `sqlite+aiosqlite:///./weather_trading.db`.
- Las corridas nuevas de estabilidad y auditoria guardan rutas relativas dentro de los snapshots para no arrastrar paths absolutos del sistema anterior.

## Limitaciones conocidas

- Algunos snapshots historicos ya generados en macOS siguen incluyendo rutas absolutas antiguas dentro del JSON.
- Eso no impide ejecutar el sistema en Windows; solo afecta a metadatos de auditoria ya guardados.

## Recomendacion practica

Cuando abras el proyecto en Windows por primera vez, haz esto:

1. Instala dependencias.
2. Ejecuta `python -m pytest -q`.
3. Lanza una validacion corta live.
4. Comprueba que los nuevos snapshots se escriben con rutas relativas.
