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
REM N5 (2026-05-20, branch v2-cowork) -- modo --resume:
REM   analizar_fondo.bat ES0112231008 --resume
REM
REM   En modo resume, cada paso comprueba si sus outputs ya estan en disco y,
REM   si si, lo skippea. consume-all-cowork y sync-supabase SIEMPRE corren
REM   (son idempotentes). Critical-files check al inicio aborta si el
REM   fund_dir no existe o esta vacio (no hay nada que resumir).
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
set RESUME_MODE=
set APPLY_FEEDBACK=

if "%ISIN%"=="" (
    echo Uso: analizar_fondo.bat ^<ISIN^> [--allow-api-fallback] [--resume] [--apply-feedback]
    echo Ejemplo: analizar_fondo.bat ES0112231008
    exit /b 1
)

REM Parse flags (position-independent across args 2-4)
if "%2"=="--resume" set RESUME_MODE=1
if "%3"=="--resume" set RESUME_MODE=1
if "%4"=="--resume" set RESUME_MODE=1
REM T3.5 (2026-05-28): --apply-feedback se pasa al consume-all-cowork del paso 6
if "%2"=="--apply-feedback" set APPLY_FEEDBACK=--apply-feedback
if "%3"=="--apply-feedback" set APPLY_FEEDBACK=--apply-feedback
if "%4"=="--apply-feedback" set APPLY_FEEDBACK=--apply-feedback

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
)
if "%3"=="--allow-api-fallback" (
    set ALLOW_FALLBACK=--api-fallback
    set FUND_ANALYZER_MODE=api
)
if defined ALLOW_FALLBACK (
    echo [WARN] --allow-api-fallback activo: agentes refactorizados volveran a la rama API legacy
    echo [WARN] Coste real esperado: 1-3 USD por fondo segun cobertura cnmv/intl
    echo.
)

REM N5 (2026-05-20): resume mode -- pre-flight check + announcement.
REM IMPORTANTE: el check se hace FUERA de un paren-block grande para que
REM `exit /b 3` propague correctamente (cmd.exe tiene quirks con exit /b
REM dentro de bloques anidados profundos).
if not defined RESUME_MODE goto :skip_resume_preflight
echo [RESUME] modo resume activo -- saltare pasos ya completados
if not exist "data\funds\%ISIN%" (
    echo [ERROR] --resume pedido pero data\funds\%ISIN% no existe.
    echo Lanza sin --resume para arranque en frio.
    exit /b 3
)
set HAS_ANY_PREP=
if exist "data\funds\%ISIN%\cnmv_data.json" set HAS_ANY_PREP=1
if exist "data\funds\%ISIN%\intl_data.json" set HAS_ANY_PREP=1
if not defined HAS_ANY_PREP (
    echo [ERROR] --resume pedido pero faltan archivos criticos
    echo         ^(ni cnmv_data.json ni intl_data.json existen en data\funds\%ISIN%\^)
    echo Lanza sin --resume para regenerar prep desde cero.
    exit /b 3
)
echo.
:skip_resume_preflight

REM ----------------------------------------------------------------------
REM N5 resume: skip prep si los 4 outputs principales ya estan
set SKIP_PREP=
if defined RESUME_MODE (
    if exist "data\funds\%ISIN%\cnmv_data.json" (
        if exist "data\funds\%ISIN%\intl_data.json" (
            if exist "data\funds\%ISIN%\manager_profile.json" (
                if exist "data\funds\%ISIN%\letters_data.json" set SKIP_PREP=1
            )
        )
    )
)
if defined SKIP_PREP (
    echo === Paso 1/6: [RESUME-SKIP] prep ya hecho ^(4 outputs presentes^) ===
    echo.
) else (
    echo === Paso 1/6: Prep determinista ^(Python, sin LLM core^) ===
    echo CNMV bulkdata + descarga PDFs + scraping Serper + identidad regulator
    echo.
    call python -m agents.orchestrator --isin %ISIN% --prep-only --auto %ALLOW_FALLBACK%
    if errorlevel 1 (
        echo [ERROR] Prep fallo. Es bloqueante: sin prep no hay manifests para las skills.
        echo Revisa data\funds\%ISIN%\ y progress.log
        exit /b 1
    )
    echo.
)

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
    call python -m tools.auto_gen_manager_manifest %ISIN%
    if errorlevel 1 (
        echo [WARN] Auto-genera manifest fallo -- manager-deep-cowork puede abortar
    )
)
echo.

REM ----------------------------------------------------------------------
REM N5 resume: skip extract-pdfs si log existe + extracted/ tiene contenido
set SKIP_EXTRACT=
if defined RESUME_MODE (
    if exist "logs\skill_extract_pdfs_%ISIN%.log" (
        if exist "data\funds\%ISIN%\extracted" set SKIP_EXTRACT=1
    )
)
if defined SKIP_EXTRACT (
    echo === Paso 2/6: [RESUME-SKIP] extract-pdfs-cowork ya hecho ===
    echo.
) else (
    echo === Paso 2/6: Skill extract-pdfs-cowork ^(Claude Max^) ===
    echo Procesa pending_extraction.json -^> data\funds\%ISIN%\extracted\
    echo ^(output redirigido a logs\skill_extract_pdfs_%ISIN%.log para evitar contaminacion cmd.exe^)
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
)

REM ----------------------------------------------------------------------
REM Paso 2.5 (Fix-CRIT-2): consume-extracted INMEDIATAMENTE tras la skill
REM extract-pdfs. Razon: extract-pdfs escribe a extracted/*.json (no toca
REM cnmv_data.json). El bundle re-export (paso 4.5) solo COPIA cnmv_data.json.
REM Sin este consume parcial, el bundle re-exportado NO lleva los campos
REM cualitativos (contexto_mercado, decisiones_tomadas, ...) y analyst-cowork
REM los lee vacios. Fix 3 solo entrega valor si este consume parcial corre AQUI.
echo === Paso 2.5/6: Consume-extracted (merge cualitativo a cnmv_data) ===
call python -m agents.orchestrator --isin %ISIN% --consume-extracted %ALLOW_FALLBACK%
if errorlevel 1 (
    echo [WARN] consume-extracted fallo -- bundle no llevara cualitativo
    set FAILED_STEPS=!FAILED_STEPS! consume-extracted
)
echo.

REM ----------------------------------------------------------------------
REM N5 resume: skip manager-deep si log existe + manager_profile.json existe
set SKIP_MGRDEEP=
if defined RESUME_MODE (
    if exist "logs\skill_manager_deep_%ISIN%.log" (
        if exist "data\funds\%ISIN%\manager_profile.json" set SKIP_MGRDEEP=1
    )
)
if defined SKIP_MGRDEEP (
    echo === Paso 3/6: [RESUME-SKIP] manager-deep-cowork ya hecho ===
    echo.
) else (
    echo === Paso 3/6: Skill manager-deep-cowork ^(Claude Max^) ===
    echo Identifica lead/co + enriquece manager_profile.json con articulos_completos
    echo ^(output redirigido a logs\skill_manager_deep_%ISIN%.log^)
    echo.
    call claude -p "manager deep cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep,WebFetch" > "logs\skill_manager_deep_%ISIN%.log" 2>&1
    if errorlevel 1 (
        echo [WARN] Skill manager-deep-cowork fallo. Ver logs\skill_manager_deep_%ISIN%.log
        set FAILED_STEPS=!FAILED_STEPS! manager-deep
    ) else (
        echo [OK] Skill manager-deep-cowork OK. Output en logs\skill_manager_deep_%ISIN%.log
    )
    echo.
)

REM ----------------------------------------------------------------------
REM N5 resume: skip letters-extract si log existe + letters_data.json existe
set SKIP_LETTERS=
if defined RESUME_MODE (
    if exist "logs\skill_letters_extract_%ISIN%.log" (
        if exist "data\funds\%ISIN%\letters_data.json" set SKIP_LETTERS=1
    )
)
if defined SKIP_LETTERS (
    echo === Paso 4/6: [RESUME-SKIP] letters-extract-cowork ya hecho ===
    echo.
) else (
    echo === Paso 4/6: Skill letters-extract-cowork ^(Claude Max^) ===
    echo Anade K15 ^(tesis, decisiones, contexto, citas, outlook^) a cada carta
    echo ^(output redirigido a logs\skill_letters_extract_%ISIN%.log^)
    echo.
    call claude -p "letters extract cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" > "logs\skill_letters_extract_%ISIN%.log" 2>&1
    if errorlevel 1 (
        echo [WARN] Skill letters-extract-cowork fallo. Ver logs\skill_letters_extract_%ISIN%.log
        set FAILED_STEPS=!FAILED_STEPS! letters-extract
    ) else (
        echo [OK] Skill letters-extract-cowork OK. Output en logs\skill_letters_extract_%ISIN%.log
    )
    echo.
)

REM ----------------------------------------------------------------------
REM Fix-2: re-exportar bundle con datos enriquecidos por skills 2-4 (extract,
REM manager-deep, letters-extract). Sin esto, analyst-cowork lee bundle/
REM obsoleto (sin perfiles ricos ni K15) y aborta o produce salida pobre.
echo === Paso 4.5/6: Re-exportando bundle (skills 2-4 enriquecieron datos) ===
call python -m agents.bundle_exporter %ISIN%
if errorlevel 1 (
    echo [WARN] bundle re-export fallo -- analyst-cowork leera el bundle viejo
    set FAILED_STEPS=!FAILED_STEPS! bundle-reexport
)
echo.

REM ----------------------------------------------------------------------
REM N5 resume: skip analyst si log existe + analyst_synthesis_cowork.json existe
set SKIP_ANALYST=
if defined RESUME_MODE (
    if exist "logs\skill_analyst_%ISIN%.log" (
        if exist "data\funds\%ISIN%\analyst_synthesis_cowork.json" set SKIP_ANALYST=1
    )
)
if defined SKIP_ANALYST (
    echo === Paso 5/6: [RESUME-SKIP] analyst-cowork ya hecho ===
    echo.
) else (
    echo === Paso 5/6: Skill analyst-cowork ^(Claude Max^) ===
    echo Genera analyst_synthesis_cowork.json con 8 secciones narrativas
    echo ^(output redirigido a logs\skill_analyst_%ISIN%.log^)
    echo.
    call claude -p "analyst cowork %ISIN%" --allowedTools "Read,Write,Bash,Edit,Agent,Glob,Grep" > "logs\skill_analyst_%ISIN%.log" 2>&1
    if errorlevel 1 (
        echo [WARN] Skill analyst-cowork fallo. Ver logs\skill_analyst_%ISIN%.log
        set FAILED_STEPS=!FAILED_STEPS! analyst
    ) else (
        echo [OK] Skill analyst-cowork OK. Output en logs\skill_analyst_%ISIN%.log
    )
    echo.
)

REM ----------------------------------------------------------------------
REM Paso 6/6: backup defensivo + consume-all-cowork (integra analyst +
REM validation + meta + dashboard regen). NO tira quality_loop legacy.
echo === Paso 6/6: Backup defensivo + consume + dashboard (Python sin LLM) ===

REM Caveat: backup pre-consume para recuperacion manual si consume corrompe
if exist "data\funds\%ISIN%\output.json" (
    copy /Y "data\funds\%ISIN%\output.json" "data\funds\%ISIN%\output.json.pre_consume_bak" >nul 2>&1
    echo [BACKUP] data\funds\%ISIN%\output.json.pre_consume_bak creado
)

call python -m agents.orchestrator --isin %ISIN% --consume-all-cowork %ALLOW_FALLBACK% %APPLY_FEEDBACK%
if errorlevel 1 (
    echo [WARN] Consume-all-cowork fallo. Si output.json esta corrupto, recupera con:
    echo   copy data\funds\%ISIN%\output.json.pre_consume_bak data\funds\%ISIN%\output.json
    set FAILED_STEPS=!FAILED_STEPS! consume-all-cowork
)
echo.

REM ----------------------------------------------------------------------
REM Paso 7: Sync a Supabase (Storage + tablas). NO bloquea el bat si falla.
REM Requiere .env con SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY y bucket 'funds-data'.
echo === Paso 7/7: Sync a Supabase ^(Storage + fund_groups + funds^) ===
call python -m tools.sync_to_supabase %ISIN%
if errorlevel 1 (
    echo [WARN] Sync to Supabase fallo. El analisis local sigue OK; reintenta luego con:
    echo   python -m tools.sync_to_supabase %ISIN%
    set FAILED_STEPS=!FAILED_STEPS! sync-supabase
)
echo.

REM ----------------------------------------------------------------------
REM Paso 7.5/7: Auto-commit + push del HTML regenerado para que Cloudflare
REM lo sirva al catalog publico. Sin esto el dashboard del catalog publico
REM queda desactualizado tras un run (Cloudflare sirve desde git, no de
REM Supabase Storage que sirve text/plain inalterable).
REM No bloqueante si falla — el analisis local sigue OK.
echo === Paso 7.5/7: Auto-commit + push del dashboard regenerado ===
if exist "dashboard\fund-%ISIN%.html" (
    git add "dashboard\fund-%ISIN%.html" >nul 2>&1
    git diff --cached --quiet "dashboard\fund-%ISIN%.html"
    if errorlevel 1 (
        git commit -m "auto: regen dashboard %ISIN%" >nul 2>&1
        if errorlevel 1 (
            echo [WARN] auto-commit fallo - revisa git status manualmente
            set FAILED_STEPS=!FAILED_STEPS! auto-git-commit
        ) else (
            git push origin v2-cowork >nul 2>&1
            if errorlevel 1 (
                echo [WARN] auto-push fallo - hazlo manual: git push origin v2-cowork
                set FAILED_STEPS=!FAILED_STEPS! auto-git-push
            ) else (
                echo [OK] dashboard/fund-%ISIN%.html commiteado y pusheado
            )
        )
    ) else (
        echo [SKIP] dashboard sin cambios desde ultimo commit
    )
) else (
    echo [SKIP] dashboard\fund-%ISIN%.html no existe
)
echo.

REM ----------------------------------------------------------------------
REM G12 (2026-05-19, branch v2-cowork): clasificar exit code en 3 niveles para
REM que web_server.py pueda distinguir done / completed_with_warnings / failed.
REM
REM   0  = todos los pasos OK
REM   5  = warnings non-critical (alguna skill fallo pero output.json existe
REM        y sync-supabase y consume-all-cowork pasaron)
REM   10 = critical fail (sin output.json, o fallo consume-all-cowork, o fallo sync)
REM
REM Los steps criticos son los que sin ellos la pipeline no entrega valor:
REM   - consume-all-cowork: sin esto output.json queda parcial/corrupto
REM   - sync-supabase: sin esto el front-end no ve el fondo
REM   - output.json ausente: no hay nada que servir
REM
REM Steps non-critical: extract-pdfs, manager-deep, letters-extract,
REM consume-extracted, bundle-reexport, analyst. Un warning en cualquiera
REM degrada la calidad pero el dashboard sigue funcionando.

set CRITICAL_FAILS=
if not exist "data\funds\%ISIN%\output.json" (
    set CRITICAL_FAILS=!CRITICAL_FAILS! output-missing
)
echo !FAILED_STEPS! | findstr /C:"consume-all-cowork" >nul && set CRITICAL_FAILS=!CRITICAL_FAILS! consume-all-cowork
echo !FAILED_STEPS! | findstr /C:"sync-supabase" >nul && set CRITICAL_FAILS=!CRITICAL_FAILS! sync-supabase

if defined CRITICAL_FAILS (
    echo === FAIL critico -- !CRITICAL_FAILS!  ^(otros fallos: !FAILED_STEPS!^) ===
    echo El analisis no es publicable. Revisa logs/ y data\funds\%ISIN%\
    set EXIT_CODE=10
) else if defined FAILED_STEPS (
    echo === Listo CON AVISOS -- fallaron pasos non-critical:!FAILED_STEPS! ===
    echo El dashboard puede estar incompleto pero output.json existe y sync paso.
    set EXIT_CODE=5
) else (
    echo === Listo -- todos los pasos OK ===
    set EXIT_CODE=0
)
echo Dashboard: dashboard\fund-%ISIN%.html
echo Backup pre-consume: data\funds\%ISIN%\output.json.pre_consume_bak (puedes borrar si todo OK)
echo Exit code: !EXIT_CODE!
endlocal & exit /b %EXIT_CODE%