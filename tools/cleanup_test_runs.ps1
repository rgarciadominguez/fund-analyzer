# cleanup_test_runs.ps1 -- Limpia completamente el estado de prueba del pipeline.
#
# Borra:
#   - data\funds\<ISIN> de la lista de fondos pasada como parametro
#   - data\.llm_cache\* (cache global LLM, TTL 24h pero contamina test fresh)
#   - data\fund_groups_cache\*.json (cache F6 fund_group)
#   - data\queue_state.json (estado in-memory de la cola del web_server, B3)
#
# NO toca: Supabase, gestoras_registry.json (a menos que pases -ResetRegistry),
#          trusted_sources.json, runs.jsonl historico, cost_log.jsonl
#
# Uso:
#   .\tools\cleanup_test_runs.ps1 -Isins FR001400CEK6,FR001400CEG4
#   .\tools\cleanup_test_runs.ps1 -Isins FR001400CEK6,FR001400CEG4,LU0168736675,IE00BDR0JY05,LU0289214628
#   .\tools\cleanup_test_runs.ps1 -Isins FR001400CEK6,FR001400CEG4 -ResetRegistry
#
param(
    [Parameter(Mandatory=$true)]
    [string[]]$Isins,

    [Parameter()]
    [switch]$ResetRegistry,

    [Parameter()]
    [switch]$KeepLlmCache,

    [Parameter()]
    [switch]$ResetSupabase,

    [Parameter()]
    [switch]$KeepDashboardsHtml
)

$ErrorActionPreference = "Continue"

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    Write-Host "=== cleanup_test_runs.ps1 ===" -ForegroundColor Cyan
    Write-Host "Repo: $repoRoot"
    Write-Host "ISINs: $($Isins -join ', ')"
    Write-Host ""

    # 1. Borrar carpetas de fondos
    foreach ($isin in $Isins) {
        $isinUp = $isin.ToUpper().Trim()
        $dir = "data\funds\$isinUp"
        if (Test-Path $dir) {
            Remove-Item -Recurse -Force $dir
            Write-Host "  [OK] borrado $dir" -ForegroundColor Green
        } else {
            Write-Host "  [SKIP] no existia $dir" -ForegroundColor DarkGray
        }
    }
    Write-Host ""

    # 1b. Borrar dashboards HTML (a menos que -KeepDashboardsHtml)
    if (-not $KeepDashboardsHtml) {
        foreach ($isin in $Isins) {
            $isinUp = $isin.ToUpper().Trim()
            $html = "dashboard\fund-$isinUp.html"
            if (Test-Path $html) {
                Remove-Item -Force $html
                Write-Host "  [OK] borrado $html" -ForegroundColor Green
            } else {
                Write-Host "  [SKIP] no existia $html" -ForegroundColor DarkGray
            }
        }
        Write-Host ""
    } else {
        Write-Host "  [KEEP] dashboards HTML conservados (-KeepDashboardsHtml)" -ForegroundColor Yellow
    }

    # 2. Cache F6 fund_groups
    if (Test-Path "data\fund_groups_cache") {
        $n = (Get-ChildItem "data\fund_groups_cache" -ErrorAction SilentlyContinue).Count
        Remove-Item -Recurse -Force "data\fund_groups_cache\*" -ErrorAction SilentlyContinue
        Write-Host "  [OK] borrados $n archivos en data\fund_groups_cache" -ForegroundColor Green
    } else {
        Write-Host "  [SKIP] data\fund_groups_cache no existia" -ForegroundColor DarkGray
    }

    # 3. Cache LLM global (a menos que el user lo pida conservar)
    if (-not $KeepLlmCache) {
        if (Test-Path "data\.llm_cache") {
            $n = (Get-ChildItem "data\.llm_cache" -ErrorAction SilentlyContinue).Count
            Remove-Item -Recurse -Force "data\.llm_cache\*" -ErrorAction SilentlyContinue
            Write-Host "  [OK] borrados $n entries en data\.llm_cache (LLM responses)" -ForegroundColor Green
        } else {
            Write-Host "  [SKIP] data\.llm_cache no existia" -ForegroundColor DarkGray
        }
    } else {
        Write-Host "  [KEEP] data\.llm_cache conservada (flag -KeepLlmCache)" -ForegroundColor Yellow
    }

    # 4. Estado de cola del web_server
    if (Test-Path "data\queue_state.json") {
        Remove-Item -Force "data\queue_state.json"
        Write-Host "  [OK] borrado data\queue_state.json" -ForegroundColor Green
    } else {
        Write-Host "  [SKIP] data\queue_state.json no existia" -ForegroundColor DarkGray
    }

    # 5. Opcional: reset entries auto_learned del registry (para validar G7 desde cero)
    if ($ResetRegistry) {
        Write-Host ""
        Write-Host "  -ResetRegistry: limpiando entries auto_learned..." -ForegroundColor Yellow
        try {
            $reg = Get-Content "data\gestoras_registry.json" -Raw | ConvertFrom-Json
            $removed = @()
            $names = $reg.gestoras.PSObject.Properties.Name
            foreach ($name in $names) {
                $entry = $reg.gestoras.$name
                if ($entry.auto_learned -eq $true) {
                    $reg.gestoras.PSObject.Properties.Remove($name)
                    $removed += $name
                }
            }
            $json = $reg | ConvertTo-Json -Depth 10
            [System.IO.File]::WriteAllText(
                (Join-Path (Get-Location) "data\gestoras_registry.json"),
                $json,
                (New-Object System.Text.UTF8Encoding($false))
            )
            if ($removed.Count -gt 0) {
                Write-Host "  [OK] borradas $($removed.Count) entries auto_learned: $($removed -join ', ')" -ForegroundColor Green
            } else {
                Write-Host "  [INFO] no habia entries auto_learned" -ForegroundColor DarkGray
            }
        } catch {
            Write-Host "  [ERROR] no se pudo resetear registry: $_" -ForegroundColor Red
        }
    }

    # 6. Cleanup de Supabase (DB + Storage) si se pidió
    if ($ResetSupabase) {
        Write-Host ""
        Write-Host "  -ResetSupabase: limpiando Supabase (DB + Storage)..." -ForegroundColor Yellow
        $isinsJoined = $Isins -join " "
        $cmd = "python -m tools.cleanup_supabase_isins $isinsJoined"
        Write-Host "  ejecutando: $cmd" -ForegroundColor DarkGray
        Invoke-Expression $cmd
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  [WARN] cleanup_supabase_isins terminó con código $LASTEXITCODE" -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "=== Verificacion ===" -ForegroundColor Cyan
    foreach ($isin in $Isins) {
        $isinUp = $isin.ToUpper().Trim()
        $dir = "data\funds\$isinUp"
        $exists = Test-Path $dir
        $tag = if ($exists) { "AUN EXISTE" } else { "OK borrado" }
        $color = if ($exists) { "Red" } else { "Green" }
        Write-Host "  $isinUp : $tag" -ForegroundColor $color
    }
    $llmCount = (Get-ChildItem "data\.llm_cache" -ErrorAction SilentlyContinue | Measure-Object).Count
    Write-Host "  data\.llm_cache : $llmCount entries"
    $queueExists = Test-Path "data\queue_state.json"
    Write-Host "  data\queue_state.json : $(if ($queueExists) { 'AUN existe' } else { 'OK borrado' })"

    Write-Host ""
    Write-Host "=== Siguiente paso ===" -ForegroundColor Cyan
    Write-Host "  Reiniciar web_server.py:"
    Write-Host "    Ctrl+C en la terminal del server, luego:"
    Write-Host "    python -m tools.web_server" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Despues, hard reload del catalog (Ctrl+Shift+R) y encolar:"
    Write-Host "    $($Isins -join ', ')" -ForegroundColor Yellow
}
finally {
    Pop-Location
}
