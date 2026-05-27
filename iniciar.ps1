# iniciar.ps1 — Arranca TODO el fund-analyzer con un solo comando.
#
# Hace:
#   1. Lanza el web_server (Flask) en una ventana minimizada
#   2. Lanza cloudflared (túnel HTTPS) en otra ventana minimizada
#   3. Captura la URL del túnel automáticamente
#   4. La registra en Supabase (el catalog la lee y se auto-conecta)
#   5. Abre el catalog público en el navegador
#
# Tras esto, el usuario NO tiene que copiar/pegar nada. La web pública
# detecta el túnel sola y todas las acciones funcionan.
#
# Uso: doble-click en iniciar.bat (que llama a este script), o:
#   powershell -ExecutionPolicy Bypass -File iniciar.ps1

$ErrorActionPreference = "Continue"
$repo = "C:\Users\RafaelGarcía\OneDrive - Nazca\Escritorio\fund-analyzer"
$catalogUrl = "https://fund-analyzer.rafagdominguez96.workers.dev/catalog.html"
$tunnelLog = Join-Path $repo "tunnel_out.log"

Set-Location $repo

# Resolver `python` por si el PATH heredado del contexto del protocolo
# fundanalyzer:// no incluye Python (el shim de WindowsApps abre MS Store y
# rompe `-m tools.register_tunnel`). Cubre PATH normal + ubicaciones típicas.
function Resolve-PythonExe {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source -notlike '*WindowsApps*') { return $cmd.Source }
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python31?\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python3??\python.exe",
        "$env:ProgramFiles\Python31?\python.exe",
        "$env:ProgramFiles\Python3??\python.exe",
        "${env:ProgramFiles(x86)}\Python31?\python.exe"
    )
    foreach ($c in $candidates) {
        $resolved = Get-Item $c -ErrorAction SilentlyContinue |
                    Sort-Object FullName -Descending |
                    Select-Object -First 1
        if ($resolved) { return $resolved.FullName }
    }
    return $null
}
$python = Resolve-PythonExe
if (-not $python) {
    Write-Host "  [WARN] No se encuentra python.exe (se intentara con 'python' del PATH)." -ForegroundColor Yellow
    $python = "python"
} else {
    Write-Host "  python: $python"
}

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Iniciando Fund Analyzer (server + tunel + auto-conexion)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

# ── 1. Matar instancias previas del server/tunel (limpieza) ───────────
Write-Host "`n[1/5] Limpiando instancias previas..."
Get-Process | Where-Object {
    $_.ProcessName -eq 'cloudflared'
} | ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }

# ── 2. Arrancar web_server en ventana minimizada ──────────────────────
Write-Host "[2/5] Arrancando web_server (Flask)..."
Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$repo'; & '$python' -m tools.web_server"
) -WindowStyle Minimized

Start-Sleep -Seconds 4

# ── 3. Arrancar cloudflared, capturando su salida a un log ────────────
Write-Host "[3/5] Arrancando tunel Cloudflare..."
if (Test-Path $tunnelLog) { Remove-Item $tunnelLog -Force -ErrorAction SilentlyContinue }

# Resolver la ruta de cloudflared (el PATH no siempre está disponible cuando
# el script se lanza vía el protocolo fundanalyzer://). Busca en PATH + winget.
$cloudflared = $null
$cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($cmd) { $cloudflared = $cmd.Source }
if (-not $cloudflared) {
    $candidates = @(
        "$env:LOCALAPPDATA\Microsoft\WinGet\Links\cloudflared.exe",
        "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Cloudflare.cloudflared_*\cloudflared.exe",
        "$env:ProgramFiles\cloudflared\cloudflared.exe",
        "${env:ProgramFiles(x86)}\cloudflared\cloudflared.exe",
        "$env:USERPROFILE\.cloudflared\cloudflared.exe"
    )
    foreach ($c in $candidates) {
        $resolved = Get-Item $c -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($resolved) { $cloudflared = $resolved.FullName; break }
    }
}
if (-not $cloudflared) {
    Write-Host "  [ERROR] No se encuentra cloudflared.exe." -ForegroundColor Red
    Write-Host "  Instalalo con: winget install --id Cloudflare.cloudflared" -ForegroundColor Yellow
    Read-Host "Pulsa Enter para salir"
    exit 1
}
Write-Host "  cloudflared: $cloudflared"
Start-Process $cloudflared -ArgumentList @(
    "tunnel", "--url", "http://localhost:5000"
) -RedirectStandardError $tunnelLog -WindowStyle Minimized

# ── 4. Esperar a que aparezca la URL del tunel en el log ──────────────
Write-Host "[4/5] Esperando URL del tunel..."
$tunnelUrl = $null
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    if (Test-Path $tunnelLog) {
        $content = Get-Content $tunnelLog -Raw -ErrorAction SilentlyContinue
        if ($content -match "https://[a-z0-9-]+\.trycloudflare\.com") {
            $tunnelUrl = $matches[0]
            break
        }
    }
}

if (-not $tunnelUrl) {
    Write-Host "  [ERROR] No se pudo obtener la URL del tunel tras 60s." -ForegroundColor Red
    Write-Host "  Revisa $tunnelLog" -ForegroundColor Red
    Read-Host "Pulsa Enter para salir"
    exit 1
}
Write-Host "  [OK] Tunel: $tunnelUrl" -ForegroundColor Green

# ── 5. Registrar la URL en Supabase (el catalog la auto-detecta) ──────
Write-Host "[5/5] Registrando URL en Supabase..."
& $python -m tools.register_tunnel $tunnelUrl
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [WARN] No se pudo registrar en Supabase. La web no auto-conectara." -ForegroundColor Yellow
    Write-Host "  URL del tunel (pegala manualmente si hace falta): $tunnelUrl" -ForegroundColor Yellow
} else {
    Write-Host "  [OK] Registrado. El catalog se auto-conectara." -ForegroundColor Green
}

# ── Abrir el catalog en el navegador ──────────────────────────────────
Write-Host "`n================================================================" -ForegroundColor Green
Write-Host "  LISTO. Abriendo catalog en el navegador..." -ForegroundColor Green
Write-Host "  URL publica: $catalogUrl" -ForegroundColor Green
Write-Host "  Tunel:       $tunnelUrl" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host "`n  IMPORTANTE: deja ESTA ventana y las 2 minimizadas ABIERTAS." -ForegroundColor Yellow
Write-Host "  Para parar todo: cierra las 3 ventanas o ejecuta parar.ps1`n" -ForegroundColor Yellow

Start-Sleep -Seconds 3
Start-Process $catalogUrl

Write-Host "Sistema en marcha. Esta ventana puede minimizarse (no cerrar)."
Read-Host "Pulsa Enter para DETENER todo (server + tunel)"

# ── Al pulsar Enter: parar todo ───────────────────────────────────────
Write-Host "Deteniendo..."
Get-Process | Where-Object { $_.ProcessName -eq 'cloudflared' } | Stop-Process -Force -ErrorAction SilentlyContinue
Get-Process | Where-Object {
    $_.ProcessName -eq 'python' -and $_.CommandLine -like '*web_server*'
} | Stop-Process -Force -ErrorAction SilentlyContinue
# Limpiar URL en Supabase (modo offline)
& $python -m tools.register_tunnel --clear 2>$null
Write-Host "Detenido. Hasta la proxima."
