@echo off
chcp 65001 > nul 2>&1
title Fund Analyzer - Setup
color 0A

echo.
echo  ================================================
echo   FUND ANALYZER - SETUP INICIAL
echo  ================================================
echo.

REM Verificar carpeta correcta
if not exist "requirements.txt" (
    echo  [ERROR] Ejecuta este script desde dentro de la carpeta fund-analyzer\
    echo.
    pause
    exit /b 1
)

REM Verificar Python
echo  Verificando Python...
python --version
if errorlevel 1 (
    echo.
    echo  [ERROR] Python no encontrado.
    echo  Instala desde: https://www.python.org/downloads/
    echo  Marca "Add Python to PATH" durante la instalacion.
    echo.
    pause
    exit /b 1
)
echo  [OK] Python encontrado.
echo.

REM Instalar dependencias
echo  Instalando dependencias (1-2 min la primera vez)...
echo.
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo.
    echo  [ERROR] Fallo instalacion. Ejecuta manualmente:
    echo    python -m pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)
echo.
echo  [OK] Dependencias instaladas.
echo.

REM Configurar API Key
if exist ".env" (
    echo  [OK] .env ya existe. Saltando configuracion.
    goto :fin
)

echo  ------------------------------------------------
echo   ANTHROPIC API KEY
echo  ------------------------------------------------
echo.
echo  Obtenla en: https://console.anthropic.com/
echo.
set /p APIKEY="Pega tu API Key (sk-ant-...): "
echo ANTHROPIC_API_KEY=%APIKEY%> .env
echo  [OK] Guardada en .env
echo.

:fin
echo  ================================================
echo   SETUP COMPLETADO
echo  ================================================
echo.
echo  Siguiente paso: doble clic en analizar_fondo.bat
echo.
pause
