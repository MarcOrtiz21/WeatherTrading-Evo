param(
    [string]$PythonVersion = "3.11",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $RepoRoot "venv\Scripts\python.exe"

Push-Location $RepoRoot
try {
    if (-not (Test-Path $VenvPython)) {
        Write-Host "Creando entorno virtual con Python $PythonVersion..."
        & py "-$PythonVersion" -m venv venv
    }

    Write-Host "Actualizando pip..."
    & $VenvPython -m pip install --upgrade pip

    Write-Host "Instalando dependencias del proyecto..."
    & $VenvPython -m pip install -e ".[dev]"

    if (-not $SkipTests) {
        Write-Host "Ejecutando tests..."
        & $VenvPython -m pytest -q
    }

    Write-Host ""
    Write-Host "Entorno listo."
    Write-Host "Python del entorno: $VenvPython"
}
finally {
    Pop-Location
}
