@echo off
REM healthcheck.bat - canarios diarios de dependencias externas + auto-cura + alerta.
REM Programar como tarea de Windows (ver register_healthcheck_task.ps1).
cd /d "%~dp0"
set LOG=data\healthcheck_alerts.log
for /f "tokens=*" %%i in ('powershell -NoProfile -Command "Get-Date -Format o"') do set NOW=%%i

python -m tools.healthcheck
set RC=%ERRORLEVEL%

if %RC% NEQ 0 (
  echo [%NOW%] HEALTHCHECK ROTO ^(rc=%RC%^) - lanzando auto-cura>> "%LOG%"
  python -m tools.dep_autocure >> "%LOG%" 2>&1
  echo [%NOW%] Ver data\healthcheck_status.json y data\dep_autocure_proposal.json>> "%LOG%"
) else (
  echo [%NOW%] healthcheck SANO>> "%LOG%"
)
exit /b %RC%
