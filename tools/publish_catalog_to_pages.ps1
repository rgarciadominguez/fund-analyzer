# publish_catalog_to_pages.ps1 -- Publica el catalog + dashboards en GitHub Pages.
#
# Primera ejecucion (clona el repo desde cero):
#   .\tools\publish_catalog_to_pages.ps1 -Init
#
# Ejecuciones posteriores (solo copia + commit + push):
#   .\tools\publish_catalog_to_pages.ps1
#
# La URL publica resultante:
#   https://<TU_USER>.github.io/fund-analyzer-catalog/catalog.html
#
# Requisitos:
# - git instalado
# - repo `fund-analyzer-catalog` creado en github.com bajo el usuario configurado
# - autenticacion git ya configurada (token PAT o ssh)

param(
    [Parameter()]
    [switch]$Init,

    [Parameter()]
    [string]$GitHubUser = "rgarciadominguez",

    [Parameter()]
    [string]$RepoName = "fund-analyzer-catalog",

    [Parameter()]
    [switch]$IncludeAllDashboards,

    [Parameter()]
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$repoPath = Join-Path $repoRoot ".." "fund-analyzer-catalog-publish"
$dashboardSrc = Join-Path $repoRoot "dashboard"

Write-Host "=== publish_catalog_to_pages.ps1 ===" -ForegroundColor Cyan
Write-Host "Repo source:   $repoRoot"
Write-Host "Repo target:   $repoPath"
Write-Host "GitHub repo:   https://github.com/$GitHubUser/$RepoName"
Write-Host ""

# ── 1. Init: clonar el repo (solo primera vez) ─────────────────────────
if ($Init) {
    Write-Host "Modo INIT: clonando el repo..." -ForegroundColor Yellow
    if (Test-Path $repoPath) {
        Write-Host "  El directorio $repoPath ya existe. Borrando..."
        Remove-Item -Recurse -Force $repoPath
    }
    $cloneUrl = "https://github.com/$GitHubUser/$RepoName.git"
    Write-Host "  git clone $cloneUrl"
    if (-not $DryRun) {
        git clone $cloneUrl $repoPath
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] git clone fallo. Verifica que el repo existe y tienes acceso." -ForegroundColor Red
            exit 1
        }
    }
    # Anadir .nojekyll para que Pages no procese con Jekyll
    $nojekyll = Join-Path $repoPath ".nojekyll"
    if (-not $DryRun) { New-Item -ItemType File -Path $nojekyll -Force | Out-Null }
    Write-Host "  [OK] repo clonado" -ForegroundColor Green
}

if (-not (Test-Path $repoPath)) {
    Write-Host "[ERROR] No existe $repoPath. Lanza con -Init la primera vez." -ForegroundColor Red
    exit 1
}

# ── 2. Copiar catalog.html ─────────────────────────────────────────────
$catalogSrc = Join-Path $dashboardSrc "catalog.html"
$catalogDst = Join-Path $repoPath "catalog.html"
$indexDst   = Join-Path $repoPath "index.html"

if (-not (Test-Path $catalogSrc)) {
    Write-Host "[ERROR] No existe $catalogSrc" -ForegroundColor Red
    exit 1
}

Write-Host "Copiando catalog.html..."
if (-not $DryRun) {
    Copy-Item -Force $catalogSrc $catalogDst
    # Tambien como index.html para que la URL raiz funcione
    Copy-Item -Force $catalogSrc $indexDst
}
Write-Host "  [OK] catalog.html y index.html copiados" -ForegroundColor Green

# ── 3. Copiar dashboards individuales (opcional) ───────────────────────
if ($IncludeAllDashboards) {
    Write-Host "Copiando dashboards fund-*.html..."
    $dashFiles = Get-ChildItem -Path $dashboardSrc -Filter "fund-*.html"
    foreach ($f in $dashFiles) {
        if (-not $DryRun) { Copy-Item -Force $f.FullName (Join-Path $repoPath $f.Name) }
    }
    Write-Host "  [OK] $($dashFiles.Count) dashboards copiados" -ForegroundColor Green
} else {
    Write-Host "  [SKIP] dashboards individuales (usa -IncludeAllDashboards para incluirlos)" -ForegroundColor DarkGray
}

# ── 4. Git commit + push ───────────────────────────────────────────────
Push-Location $repoPath
try {
    Write-Host ""
    Write-Host "Git status..." -ForegroundColor Yellow
    git status --short

    Write-Host ""
    if ($DryRun) {
        Write-Host "[DRY RUN] No se hace commit ni push" -ForegroundColor Yellow
        return
    }

    git add -A
    $hasChanges = git diff --cached --quiet
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [SKIP] no hay cambios para commitear" -ForegroundColor DarkGray
    } else {
        $ts = Get-Date -Format "yyyy-MM-dd HH:mm"
        git commit -m "publish catalog $ts"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] git commit fallo" -ForegroundColor Red
            exit 1
        }
        Write-Host ""
        Write-Host "Pushing..." -ForegroundColor Yellow
        git push origin main
        if ($LASTEXITCODE -ne 0) {
            # Probar con master por si el repo usa otra rama default
            git push origin master 2>$null
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[ERROR] git push fallo" -ForegroundColor Red
                exit 1
            }
        }
        Write-Host "  [OK] push completo" -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "=== URL publica ===" -ForegroundColor Cyan
    Write-Host "  https://$GitHubUser.github.io/$RepoName/" -ForegroundColor Green
    Write-Host "  (espera 1-2 min tras el primer push para que Pages active)"
    Write-Host ""
    if ($Init) {
        Write-Host "=== IMPORTANTE — Habilita Pages en GitHub ===" -ForegroundColor Yellow
        Write-Host "  1. Ve a https://github.com/$GitHubUser/$RepoName/settings/pages"
        Write-Host "  2. Source: 'Deploy from a branch'"
        Write-Host "  3. Branch: 'main' (o 'master'), folder: '/ (root)'"
        Write-Host "  4. Save"
        Write-Host "  5. Espera 1-2 min y abre la URL de arriba"
    }
}
finally {
    Pop-Location
}
