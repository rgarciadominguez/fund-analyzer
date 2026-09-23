# Servidor Claude Code (remote control) — recuperación y autoarranque

Las conversaciones "Tarea ..." y las de VS Code se ejecutan **en el servidor Windows**.
Si el equipo se apaga, se suspende o pierde la red, todas aparecen como desconectadas
(`computer_unreachable`) desde otros ordenadores. Se recuperan en cuanto el servidor vuelve.

## Recuperar tras una caída (en el propio servidor)

1. Encender/despertar el equipo e iniciar sesión.
2. Si ya está instalado el autoarranque (abajo), no hay que hacer nada más: la tarea
   `ClaudeRemoteControl` lanza `claude remote-control` sola.
   Si no, abrir PowerShell en la carpeta de trabajo y ejecutar `claude remote-control`.
3. Para conversaciones de VS Code: abrir VS Code con la extensión de Claude.
4. Reabrir las conversaciones desde el otro ordenador.

## Instalar autoarranque (una sola vez, PowerShell como Administrador)

```powershell
cd <ruta-al-repo>\scripts\servidor
powershell -ExecutionPolicy Bypass -File instalar_autoarranque.ps1 -Carpeta "C:\ruta\carpeta\de\trabajo"
```

- Desactiva suspensión/hibernación y la acción de la tapa (enchufado).
- Registra la tarea programada `ClaudeRemoteControl` al iniciar sesión, con reinicio si falla.
- `arrancar_remote_control.ps1` espera a tener internet y relanza `claude remote-control` si termina.
- Log: `%USERPROFILE%\claude-servidor\remote-control.log`

Pasos manuales recomendados: inicio de sesión automático (`netplwiz`) y en la BIOS
"Restore on AC power loss = Power On".

## Comprobar estado

```powershell
Get-ScheduledTask ClaudeRemoteControl | Get-ScheduledTaskInfo
Get-Content $env:USERPROFILE\claude-servidor\remote-control.log -Tail 20
```
