# Configura el servidor Windows para que Claude Code (remote control) sobreviva
# a reinicios, suspensiones y cortes. Ejecutar UNA vez como Administrador:
#
#   powershell -ExecutionPolicy Bypass -File instalar_autoarranque.ps1 -Carpeta "C:\ruta\proyecto"
#
# Hace:
#   1. Desactiva suspension/hibernacion y la accion al cerrar la tapa (enchufado).
#   2. Registra la tarea programada "ClaudeRemoteControl" que arranca el
#      supervisor al iniciar sesion y lo relanza si muere.
#   3. Lo arranca ya.

param(
    [string]$Carpeta = $env:USERPROFILE
)

$ErrorActionPreference = "Stop"

$esAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $esAdmin) {
    Write-Host "Ejecuta este script como Administrador." -ForegroundColor Red
    exit 1
}

# 1. Energia: nunca suspender ni hibernar enchufado; tapa = no hacer nada
Write-Host "Configurando energia..."
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /hibernate off
powercfg /setacvalueindex SCHEME_CURRENT SUB_BUTTONS LIDACTION 0
powercfg /setactive SCHEME_CURRENT

# 2. Tarea programada al iniciar sesion, con reinicio si falla
$script = Join-Path $PSScriptRoot "arrancar_remote_control.ps1"
$accion = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Minimized -File `"$script`" -Carpeta `"$Carpeta`""
$disparador = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$ajustes = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName "ClaudeRemoteControl" -Action $accion -Trigger $disparador `
    -Settings $ajustes -Principal $principal -Force | Out-Null
Write-Host "Tarea 'ClaudeRemoteControl' registrada."

# 3. Arrancar ya
Start-ScheduledTask -TaskName "ClaudeRemoteControl"
Write-Host "Arrancado. Log: $env:USERPROFILE\claude-servidor\remote-control.log" -ForegroundColor Green
Write-Host ""
Write-Host "Pendiente manual (recomendado):"
Write-Host " - Inicio de sesion automatico: ejecutar 'netplwiz' y desmarcar 'Los usuarios deben escribir su nombre y contrasena'."
Write-Host " - BIOS/UEFI: 'Restore on AC power loss' = Power On, para arrancar solo tras un corte de luz."
Write-Host " - Windows Update: fijar 'horas activas' para evitar reinicios en mitad de tareas."
