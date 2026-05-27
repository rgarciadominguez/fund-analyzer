# registrar_protocolo.ps1 — Registra el protocolo "fundanalyzer://" en Windows.
#
# Tras ejecutarlo UNA VEZ, el botón "🚀 Arrancar server" del catalog web
# (que abre fundanalyzer://start) lanzará iniciar.bat automáticamente.
#
# NO requiere permisos de administrador (usa HKEY_CURRENT_USER).
#
# Uso: click derecho > "Ejecutar con PowerShell", o:
#   powershell -ExecutionPolicy Bypass -File registrar_protocolo.ps1

$ErrorActionPreference = "Stop"

$repo = $PSScriptRoot
$batPath = Join-Path $repo "iniciar.bat"

if (-not (Test-Path $batPath)) {
    Write-Host "[ERROR] No se encuentra iniciar.bat en $repo" -ForegroundColor Red
    Read-Host "Pulsa Enter para salir"
    exit 1
}

Write-Host "Registrando protocolo 'fundanalyzer://'..." -ForegroundColor Cyan
Write-Host "  Apuntando a: $batPath"

$base = "HKCU:\Software\Classes\fundanalyzer"

# Crear las claves del registro (HKCU, no requiere admin)
New-Item -Path $base -Force | Out-Null
Set-ItemProperty -Path $base -Name "(Default)" -Value "URL:Fund Analyzer Protocol"
Set-ItemProperty -Path $base -Name "URL Protocol" -Value ""

New-Item -Path "$base\shell\open\command" -Force | Out-Null
# El comando: cmd /c "ruta\iniciar.bat"  (ignora el argumento %1 del protocolo)
$cmd = "cmd /c `"$batPath`""
Set-ItemProperty -Path "$base\shell\open\command" -Name "(Default)" -Value $cmd

Write-Host ""
Write-Host "[OK] Protocolo registrado." -ForegroundColor Green
Write-Host ""
Write-Host "Ahora, en el catalog web, el boton '🚀 Arrancar server' lanzara" -ForegroundColor Green
Write-Host "iniciar.bat automaticamente (el navegador preguntara 1 vez si" -ForegroundColor Green
Write-Host "permites abrir 'Fund Analyzer' — marca 'recordar' para no repetir)." -ForegroundColor Green
Write-Host ""
Read-Host "Pulsa Enter para cerrar"
