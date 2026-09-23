# Mantiene `claude remote-control` vivo en el servidor.
# Si el proceso termina (caida de red, error, cierre), espera y lo relanza.
# Log en %USERPROFILE%\claude-servidor\remote-control.log
#
# Uso manual:  powershell -ExecutionPolicy Bypass -File arrancar_remote_control.ps1 -Carpeta "C:\ruta\proyecto"

param(
    [string]$Carpeta = $env:USERPROFILE,
    [int]$EsperaSegundos = 15
)

$logDir = Join-Path $env:USERPROFILE "claude-servidor"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir "remote-control.log"

function Log($msg) {
    $linea = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $log -Value $linea
    Write-Host $linea
}

$claude = (Get-Command claude -ErrorAction SilentlyContinue).Source
if (-not $claude) {
    Log "ERROR: no encuentro 'claude' en el PATH. Instala Claude Code o revisa el PATH."
    exit 1
}

Set-Location $Carpeta
Log "Arranque supervisor. Carpeta: $Carpeta  claude: $claude"

while ($true) {
    # Esperar a tener internet antes de lanzar
    while (-not (Test-NetConnection -ComputerName "api.anthropic.com" -Port 443 -InformationLevel Quiet -WarningAction SilentlyContinue)) {
        Log "Sin conexion con api.anthropic.com, reintento en $EsperaSegundos s"
        Start-Sleep -Seconds $EsperaSegundos
    }

    Log "Lanzando: claude remote-control"
    & $claude remote-control
    Log "claude remote-control termino (codigo $LASTEXITCODE). Relanzo en $EsperaSegundos s"
    Start-Sleep -Seconds $EsperaSegundos
}
