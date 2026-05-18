@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
REM ====================================================================
REM analizar_fondo.bat -- Pipeline cowork-skill (v2-cowork branch, Refactor L2)
REM Orquesta 6 pasos: prep determinista + 4 skills cowork + consume final.
REM
REM Uso normal (modo cowork, ~$0.05 API cost: solo Serper + hints marginales):
REM   analizar_fondo.bat ES0112231008
REM
REM Modo legacy API (cuando Claude Max no esta disponible o se necesita testing):
REM   analizar_fondo.bat ES0112231008 --allow-api-fallback
REM
REM Behavior:
REM - Skill failures DO NOT abort the pipeline (continue-on-error).
REM   Each failure is logged and the bat continues with the next step.
REM   Final exit code = 0 if all OK, != 0 if any step failed (caller can check).
REM - If pending_manager_deep.json is missing after prep, auto-generate a
REM   minimal one from fund_name + gestora so manager-deep-cowork has something
REM   to work with even if manager_profiler legacy failed (e.g. Gemini denied).
REM
REM Legacy interactivo (menu color del bat antiguo) en analizar_fondo.legacy.bat
REM ====================================================================
set ISIN=%1
set ALLOW_FALLBACK=
set FAILED_STEPS=

if "%ISIN%"=="" (
    echo Uso: analizar_fondo.bat ^<ISIN^> [--allow-api-fallback]
    echo Ejemplo: analizar_fondo.bat ES0112231008
    exit /b 1
)

REM ====================================================================
REM Fix coste: vaciar ANTHROPIC_API_KEY del env del bat para que las 4
REM invocaciones de `claude -p` (skills cowork) usen el login Claude Max
REM en lugar de consumir balance prepago de la API console.
REM
REM - Sin esto: claude -p hereda ANTHROPIC_API_KEY del env (cargada por
REM   dotenv en algun proceso padre) -> modo API -> ~$3-13 por fondo
REM   (Opus 4 con 1M context es caro en API).
REM - Con esto: claude -p NO ve la key -> usa subscripcion Claude Max
REM   (sin coste por uso, cubierto por mensualidad).
REM
REM Python (paso 1, 6) NO se ve afectado porque load_dotenv() lee .env
REM directamente y popula os.environ del proceso Python. La key sigue
REM disponible para readings_collector_haiku_extract y demas usos
REM "extremadamente necesarios" via API directa.
REM
REM Scope: solo afecta cmd.exe del bat (no propaga al PowerShell padre).
REM ====================================================================
set "ANTHROPIC_API_KEY="

REM Caveat 1 (Refactor L2): set FUND_ANALYZER_MODE BEFORE the first python
REM invocation. argparse in orchestrator also flips it, but defense-in-depth.
if "%2"=="--allow-api-fallback" (
    set ALLOW_FALLBACK=--api-fallback
    set FUND_ANALYZER_MODE=api
    echo [WARN] --allow-api-fallback activo: agentes refactorizados volveran a la rama API legacy
    echo [WARN] Coste real esperado: 1-3 USD por fondo segun cobertura cnmv/intl
    echo.
)

REM ----------------------------------------------------------------------
echo === Paso 1/6: Prep determinista (Python, sin LLM core) ===
echo CNMV bulkdata + descarga PDFs + scraping Serper + identidad regulator
echo.
python -m agents.orchestrator --isin %ISIN% --prep-only --auto %ALLOW_FALLBACK%
if errorlevel 1 (
    echo [ERROR] Prep fallo. Es bloqueante: sin prep no hay manifests para las skills.
    echo Revisa data\funds\%ISIN%\ y progress.log
    exit /b 1
)
echo.

REM Pre-flight: verificar que claude CLI existe antes de lanzar las 4 skills.
where claude >nul 2>&1
if errorlevel 1 (
    echo [ERROR] 'claude' no esta en PATH. Abre Claude Code manualmente:
    echo   1. cd "%CD%"
    echo   2. Di sucesivamente: "extract pdfs cowork %ISIN%", "manager deep cowork %ISIN%",
    echo      "letters extract cowork %ISIN%", "analyst cowork %ISIN%"
    echo   3. Cuando termines, ejecuta: python -m agents.orchestrator --isin %ISIN% --consume-all-cowork
    exit /b 2
)

REM Auto-generar pending_manager_deep.json si no existe (manager_profiler
REM legacy puede haber fallado sin Gemini). Sin manifest la skill manager-deep
REM aborta. Generamos uno minimo con fund_name + gestora para que la skill
REM al menos pueda buscar online.
if not exist "data\funds\%ISIN%\pending_manager_deep.json" (
    echo [INFO] pending_manager_deep.json no existe -- auto-generando manifest minimo
    python -m tools.auto_gen_manager_manifest %ISIN%
    if errorlevel 1 (
        echo [WARN] Auto-genera manifest fallo -- manager-deep-cowork puede abortar
    )
)
echo.

REM ----------------------------------------------------------------------
echo === Paso 2/6: Skill extract-pdfs-cowork (Claude Max) ===
echo Procesa pending_extraction.json -^> data\funds\%ISIN%\extracted\
echo (output redirigido a logs\skill_extract_pdfs_%ISIN%.log para evitar contaminacion cmd.exe)
echo.
if not exist "logs" mkdir "logs"
call claude -p "extract pdfs cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" > "logs\skill_extract_pdfs_%ISIN%.log" 2>&1
if errorlevel 1 (
    echo [WARN] Skill extract-pdfs-cowork fallo. Ver logs\skill_extract_pdfs_%ISIN%.log
    set FAILED_STEPS=!FAILED_STEPS! extract-pdfs
) else (
    echo [OK] Skill extract-pdfs-cowork OK. Output en logs\skill_extract_pdfs_%ISIN%.log
)
echo.

REM ----------------------------------------------------------------------
REM Paso 2.5 (Fix-CRIT-2): consume-extracted INMEDIATAMENTE tras la skill
REM extract-pdfs. Razon: extract-pdfs escribe a extracted/*.json (no toca
REM cnmv_data.json). El bundle re-export (paso 4.5) solo COPIA cnmv_data.json.
REM Sin este consume parcial, el bundle re-exportado NO lleva los campos
REM cualitativos (contexto_mercado, decisiones_tomadas, ...) y analyst-cowork
REM los lee vacios. Fix 3 solo entrega valor si este consume parcial corre AQUI.
echo === Paso 2.5/6: Consume-extracted (merge cualitativo a cnmv_data) ===
python -m agents.orchestrator --isin %ISIN% --consume-extracted %ALLOW_FALLBACK%
if errorlevel 1 (
    echo [WARN] consume-extracted fallo -- bundle no llevara cualitativo
    set FAILED_STEPS=!FAILED_STEPS! consume-extracted
)
echo.

REM ----------------------------------------------------------------------
echo === Paso 3/6: Skill manager-deep-cowork (Claude Max) ===
echo Identifica lead/co + enriquece manager_profile.json con articulos_completos
echo (output redirigido a logs\skill_manager_deep_%ISIN%.log)
echo.
call claude -p "manager deep cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep,WebFetch" > "logs\skill_manager_deep_%ISIN%.log" 2>&1
if errorlevel 1 (
    echo [WARN] Skill manager-deep-cowork fallo. Ver logs\skill_manager_deep_%ISIN%.log
    set FAILED_STEPS=!FAILED_STEPS! manager-deep
) else (
    echo [OK] Skill manager-deep-cowork OK. Output en logs\skill_manager_deep_%ISIN%.log
)
echo.

REM ----------------------------------------------------------------------
echo === Paso 4/6: Skill letters-extract-cowork (Claude Max) ===
echo Anade K15 (tesis, decisiones, contexto, citas, outlook) a cada carta
echo (output redirigido a logs\skill_letters_extract_%ISIN%.log)
echo.
call claude -p "letters extract cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" > "logs\skill_letters_extract_%ISIN%.log" 2>&1
if errorlevel 1 (
    echo [WARN] Skill letters-extract-cowork fallo. Ver logs\skill_letters_extract_%ISIN%.log
    set FAILED_STEPS=!FAILED_STEPS! letters-extract
) else (
    echo [OK] Skill letters-extract-cowork OK. Output en logs\skill_letters_extract_%ISIN%.log
)
echo.

REM ----------------------------------------------------------------------
REM Fix-2: re-exportar bundle con datos enriquecidos por skills 2-4 (extract,
REM manager-deep, letters-extract). Sin esto, analyst-cowork lee bundle/
REM obsoleto (sin perfiles ricos ni K15) y aborta o produce salida pobre.
echo === Paso 4.5/6: Re-exportando bundle (skills 2-4 enriquecieron datos) ===
python -m agents.bundle_exporter %ISIN%
if errorlevel 1 (
    echo [WARN] bundle re-export fallo -- analyst-cowork leera el bundle viejo
    set FAILED_STEPS=!FAILED_STEPS! bundle-reexport
)
echo.

REM ----------------------------------------------------------------------
echo === Paso 5/6: Skill analyst-cowork (Claude Max) ===
echo Genera analyst_synthesis_cowork.json con 8 secciones narrativas
echo (output redirigido a logs\skill_analyst_%ISIN%.log)
echo.
call claude -p "analyst cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" > "logs\skill_analyst_%ISIN%.log" 2>&1
if errorlevel 1 (
    echo [WARN] Skill analyst-cowork fallo. Ver logs\skill_analyst_%ISIN%.log
    set FAILED_STEPS=!FAILED_STEPS! analyst
) else (
    echo [OK] Skill analyst-cowork OK. Output en logs\skill_analyst_%ISIN%.log
)
echo.

REM ----------------------------------------------------------------------
REM Paso 6/6: backup defensivo + consume-all-cowork (integra analyst +
REM validation + meta + dashboard regen). NO tira quality_loop legacy.
echo === Paso 6/6: Backup defensivo + consume + dashboard (Python sin LLM) ===

REM Caveat: backup pre-consume para recuperacion manual si consume corrompe
if exist "data\funds\%ISIN%\output.json" (
    copy /Y "data\funds\%ISIN%\output.json" "data\funds\%ISIN%\output.json.pre_consume_bak" >nul 2>&1
    echo [BACKUP] data\funds\%ISIN%\output.json.pre_consume_bak creado
)

python -m agents.orchestrator --isin %ISIN% --consume-all-cowork %ALLOW_FALLBACK%
if errorlevel 1 (
    echo [WARN] Consume-all-cowork fallo. Si output.json esta corrupto, recupera con:
    echo   copy data\funds\%ISIN%\output.json.pre_consume_bak data\funds\%ISIN%\output.json
    set FAILED_STEPS=!FAILED_STEPS! consume-all-cowork
)
echo.

REM ----------------------------------------------------------------------
REM Paso 7: Sync a Supabase (Storage + tablas). NO bloquea el bat si falla.
REM Requiere .env con SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY y bucket 'funds-data'.
echo === Paso 7/7: Sync a Supabase (Storage + fund_groups + funds) ===
python -m tools.sync_to_supabase %ISIN%
if errorlevel 1 (
    echo [WARN] Sync to Supabase fallo. El analisis local sigue OK; reintenta luego con:
    echo   python -m tools.sync_to_supabase %ISIN%
    set FAILED_STEPS=!FAILED_STEPS! sync-supabase
)
echo.

REM ----------------------------------------------------------------------
if "!FAILED_STEPS!"=="" (
    echo === Listo -- todos los pasos OK ===
) else (
    echo === Listo CON AVISOS -- fallaron pasos:!FAILED_STEPS! ===
    echo El dashboard puede estar incompleto. Revisa los warnings arriba.
)
echo Dashboard: dashboard\fund-%ISIN%.html
echo Backup pre-consume: data\funds\%ISIN%\output.json.pre_consume_bak (puedes borrar si todo OK)
endlocal & if "%FAILED_STEPS%"=="" (exit /b 0) else (exit /b 10)