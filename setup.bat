@echo off
chcp 65001 > nul
title Fund Analyzer - Setup

echo.
echo ╔════════════════════════════════════════╗
echo ║     FUND ANALYZER - SETUP INICIAL      ║
echo ╚════════════════════════════════════════╝
echo.

REM ── Verificar Python ─────────────────────────────────────────
python --version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no encontrado.
    echo.
    echo Instala Python desde: https://www.python.org/downloads/
    echo Asegurate de marcar "Add Python to PATH" durante la instalacion.
    echo.
    pause
    exit /b 1
)
echo [OK] Python detectado.

REM ── Descargar repo desde GitHub ──────────────────────────────
echo.
echo [1/4] Descargando Fund Analyzer desde GitHub...
powershell -Command "Invoke-WebRequest -Uri 'https://github.com/rgarciadominguez/fund-analyzer/archive/refs/heads/main.zip' -OutFile 'fund-analyzer.zip'" 2>nul
if errorlevel 1 (
    echo [ERROR] No se pudo descargar. Verifica tu conexion a internet.
    pause
    exit /b 1
)
echo [OK] Descargado.

REM ── Extraer ZIP ──────────────────────────────────────────────
echo.
echo [2/4] Extrayendo archivos...
powershell -Command "Expand-Archive -Path 'fund-analyzer.zip' -DestinationPath '.' -Force" 2>nul
if exist "fund-analyzer-main" (
    if exist "fund-analyzer" rmdir /s /q "fund-analyzer"
    rename "fund-analyzer-main" "fund-analyzer"
)
del "fund-analyzer.zip" > nul 2>&1
echo [OK] Extraido en carpeta fund-analyzer\

REM ── Instalar dependencias Python ─────────────────────────────
echo.
echo [3/4] Instalando dependencias Python...
cd fund-analyzer
pip install -r requirements.txt -q
if errorlevel 1 (
    echo [ERROR] Fallo al instalar dependencias.
    pause
    exit /b 1
)
echo [OK] Dependencias instaladas.

REM ── Configurar API Key ───────────────────────────────────────
echo.
echo [4/4] Configuracion API Key de Anthropic
echo.

if exist ".env" (
    echo [OK] Archivo .env ya existe. Saltando.
    goto :done
)

echo Necesitas tu API Key de Anthropic para extraer datos cualitativos.
echo Consiguelo en: https://console.anthropic.com/
echo.
set /p APIKEY="Pega tu Anthropic API Key (sk-ant-...): "

if "%APIKEY%"=="" (
    echo [AVISO] No se configuro API Key. La extraccion cualitativa no funcionara.
    echo          Puedes configurarla despues editando el archivo .env
    echo ANTHROPIC_API_KEY=sk-ant-TU_KEY_AQUI > .env
) else (
    echo ANTHROPIC_API_KEY=%APIKEY%> .env
    echo [OK] API Key guardada en .env
)

:done
echo.
echo ╔════════════════════════════════════════╗
echo ║            SETUP COMPLETADO            ║
echo ║                                        ║
echo ║  Ahora ejecuta: analizar_fondo.bat     ║
echo ╚════════════════════════════════════════╝
echo.

REM Copiar el script de analisis a la carpeta raiz
copy "..\analizar_fondo.bat" "analizar_fondo.bat" > nul 2>&1

pause
