@echo off
chcp 65001 > nul 2>&1
title Fund Analyzer
color 0A

:inicio
cls
echo.
echo  ================================================
echo   FUND ANALYZER
echo  ================================================
echo.

REM Verificar carpeta correcta
if not exist "extractor.py" (
    echo  [ERROR] Ejecuta desde la carpeta fund-analyzer\
    pause
    exit /b 1
)

REM Verificar .env
if not exist ".env" (
    echo  [AVISO] No hay .env con API Key.
    echo  Ejecuta primero setup.bat
    echo.
    pause
    exit /b 1
)

REM Mostrar fondos ya analizados
echo  Fondos ya analizados:
echo.
dir /b data\ES*.json 2>nul | findstr /r "." > nul
if errorlevel 1 (
    echo    (ninguno todavia)
) else (
    for %%f in (data\ES*.json) do (
        python -c "import json,sys; d=json.load(open('%%f')); m=d['meta']; print(f'   {m[\"isin\"]}  {m.get(\"nombre\",\"\")[:40]:<40}  [{m[\"extraccion_estado\"][\"cualitativo\"]}]')" 2>nul
    )
)

echo.
echo  ------------------------------------------------
echo.
set /p ISIN="  ISIN del fondo a analizar (Enter para salir): "

if "%ISIN%"=="" (
    echo.
    echo  Saliendo...
    timeout /t 2 > nul
    exit /b 0
)

REM Limpiar espacios
set ISIN=%ISIN: =%

echo.
echo  URL de la gestora (opcional, Enter para saltar):
set /p GESTORA="  URL: "

echo.
echo  ================================================
echo   Analizando: %ISIN%
echo  ================================================
echo.

if "%GESTORA%"=="" (
    python extractor.py %ISIN%
) else (
    python extractor.py %ISIN% --gestora-web %GESTORA%
)

echo.
if errorlevel 1 (
    echo  [ERROR] Revisa los mensajes de error arriba.
) else (
    echo  ================================================
    echo   Completado. JSON en: data\%ISIN%.json
    echo  ================================================
    echo.
    set /p ABRIR="  Abrir el JSON? (s/n): "
    if /i "%ABRIR%"=="s" start notepad "data\%ISIN%.json"
)

echo.
set /p OTRO="  Analizar otro fondo? (s/n): "
if /i "%OTRO%"=="s" goto :inicio

echo.
pause
