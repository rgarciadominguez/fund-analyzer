@echo off
chcp 65001 > nul
title Fund Analyzer

:menu
cls
echo.
echo ╔════════════════════════════════════════════════╗
echo ║           FUND ANALYZER - FONDOS ES           ║
echo ╚════════════════════════════════════════════════╝
echo.

REM ── Verificar que estamos en la carpeta correcta ─────────────
if not exist "extractor.py" (
    echo [ERROR] No se encuentra extractor.py
    echo Asegurate de ejecutar este script desde la carpeta fund-analyzer\
    pause
    exit /b 1
)

REM ── Mostrar fondos ya analizados ─────────────────────────────
echo Fondos ya analizados:
echo.
set COUNT=0
for %%f in (data\ES*.json) do (
    set /a COUNT+=1
    for /f "tokens=*" %%a in ('python -c "import json; d=json.load(open('%%f')); print(f'  %%f  ^|  {d[\"meta\"].get(\"nombre\",\"\")}  ^|  {d[\"meta\"][\"extraccion_estado\"][\"cualitativo\"]}')" 2^>nul') do echo %%a
)
if %COUNT%==0 echo   (ninguno todavia)
echo.
echo ─────────────────────────────────────────────────────────────
echo.

REM ── Pedir ISIN ───────────────────────────────────────────────
set /p ISIN="Introduce el ISIN del fondo (ESxxxxxxxxxx) o ENTER para salir: "

if "%ISIN%"=="" (
    echo Saliendo...
    exit /b 0
)

REM Convertir a mayusculas y limpiar espacios
set ISIN=%ISIN: =%

REM Validar formato basico ES + 10 caracteres
echo %ISIN% | findstr /r "^ES[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$" > nul
if errorlevel 1 (
    echo.
    echo [ERROR] ISIN invalido: %ISIN%
    echo El ISIN debe empezar por ES y tener 12 caracteres en total.
    echo Ejemplo: ES0112231008
    echo.
    pause
    goto :menu
)

REM ── Preguntar URL gestora (opcional) ─────────────────────────
echo.
echo URL de la web de la gestora (opcional, mejora la extraccion):
echo Ejemplo: https://www.avantagecapital.com
echo Pulsa ENTER para detectar automaticamente.
echo.
set /p GESTORA_URL="URL gestora: "

REM ── Preguntar si forzar re-extraccion ────────────────────────
set FORCE_FLAG=
if exist "data\%ISIN%.json" (
    echo.
    echo [AVISO] Ya existe un analisis para %ISIN%.
    set /p FORCE="Quieres actualizar los datos existentes? (s/n): "
    if /i "%FORCE%"=="s" set FORCE_FLAG=--force
)

REM ── Ejecutar extractor ───────────────────────────────────────
echo.
echo ════════════════════════════════════════════════
echo  Analizando fondo: %ISIN%
echo ════════════════════════════════════════════════
echo.

if "%GESTORA_URL%"=="" (
    python extractor.py %ISIN% %FORCE_FLAG%
) else (
    python extractor.py %ISIN% --gestora-web %GESTORA_URL% %FORCE_FLAG%
)

echo.
if errorlevel 1 (
    echo [ERROR] La extraccion fallo. Revisa los mensajes de error arriba.
) else (
    echo ════════════════════════════════════════════════
    echo  JSON guardado en: data\%ISIN%.json
    echo ════════════════════════════════════════════════
    echo.

    REM Ofrecer abrir el JSON
    set /p OPEN="Quieres abrir el JSON resultante? (s/n): "
    if /i "%OPEN%"=="s" start notepad "data\%ISIN%.json"

    REM Ofrecer subir a GitHub
    echo.
    set /p PUSH="Quieres subir el resultado a GitHub? (s/n): "
    if /i "%PUSH%"=="s" (
        call push_github.bat %ISIN%
    )
)

echo.
set /p OTRO="Analizar otro fondo? (s/n): "
if /i "%OTRO%"=="s" goto :menu

echo.
echo Hasta luego.
timeout /t 2 > nul
