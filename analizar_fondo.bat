@echo off
setlocal
chcp 65001 >nul
REM ====================================================================
REM analizar_fondo.bat -- Pipeline cowork-skill (v2-cowork branch)
REM Orquesta: Python prep -> Skill analyst-cowork (Claude Max) -> Python consume
REM Fallback: si la skill falla, ejecuta analyst legacy con API Anthropic.
REM Legacy interactivo movido a analizar_fondo.legacy.bat
REM ====================================================================
set ISIN=%1
if "%ISIN%"=="" (
    echo Uso: analizar_fondo.bat ^<ISIN^>
    echo Ejemplo: analizar_fondo.bat ES0112231008
    exit /b 1
)

echo.
echo === Paso 1/3: Prep determinista (Python) ===
python -m agents.orchestrator --isin %ISIN% --prep-only
if errorlevel 1 (
    echo Error en prep
    exit /b 1
)

echo.
echo === Paso 2/3: Skill analyst en Claude Max (headless) ===
REM Verifica que claude esta en PATH; si no, abrir manualmente Claude Code/Cowork.
where claude >nul 2>&1
if errorlevel 1 (
    echo [WARN] 'claude' no esta en PATH.
    echo Abre Claude Code o Cowork manualmente en esta carpeta y di:
    echo   "analyst cowork %ISIN%"
    echo Despues vuelve a esta consola y pulsa Enter para continuar al Paso 3.
    pause
) else (
    claude -p "analyst cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" --cwd "%CD%"
    if errorlevel 1 (
        echo [WARN] Skill fallo. Fallback a analyst legacy con API...
        python -m agents.orchestrator --isin %ISIN% --auto
        exit /b %errorlevel%
    )
)

echo.
echo === Paso 3/3: Consume + dashboard ===
python -m agents.orchestrator --isin %ISIN% --consume-cowork
if errorlevel 1 (
    echo Error en consume
    exit /b 1
)

echo.
echo === Listo ===
echo Dashboard: dashboard\fund-%ISIN%.html
endlocal
