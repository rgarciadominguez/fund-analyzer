@echo off
REM iniciar.bat - Doble-click para arrancar todo el fund-analyzer.
REM Llama a iniciar.ps1 que monta server + tunel + auto-conexion.
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0iniciar.ps1"
