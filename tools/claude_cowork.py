"""claude_cowork.py — Ejecuta una skill cowork (`claude -p`) con RESILIENCIA a cortes de red.

Motivo: el adaptador de red de la máquina se cae a ratos (`ENOTFOUND`/`getaddrinfo failed`) y
eso mataba el paso entero del pipeline (extract/manager/letters/analyst) → run degradado. Este
wrapper:
  1. Espera a que la red RESUELVA antes de lanzar `claude -p`.
  2. Si `claude -p` falla por RED (marcadores en el log), REINTENTA (hasta 4, backoff creciente).
  3. Si el fallo es de CUOTA (session limit), NO reintenta (no se recupera reintentando).
  4. Escribe la salida de claude en <logfile> (igual que antes) y devuelve el rc para el bat.

Uso (desde analizar_fondo.bat):
    call python -m tools.claude_cowork "<logfile>" "<prompt>" --model X --allowedTools "..."
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time

_NET_MARKERS = (
    "enotfound", "unable to connect", "getaddrinfo", "econnrefused", "etimedout",
    "network error", "fetch failed", "connection error", "socket hang up",
    "und_err_connect", "eai_again",
)
_QUOTA_MARKERS = ("session limit", "hit your", "usage limit")


def _net_up(host: str = "api.anthropic.com") -> bool:
    try:
        socket.gethostbyname(host)
        return True
    except Exception:
        return False


def _wait_net(max_wait: int = 180) -> bool:
    start = time.time()
    while time.time() - start < max_wait:
        if _net_up():
            return True
        time.sleep(5)
    return _net_up()


def _env_login() -> dict:
    """Entorno para `claude -p` SIN el token de `claude setup-token`.

    En el servidor hay una variable de máquina CLAUDE_CODE_OAUTH_TOKEN (para otros workers). Con ese
    token Claude Code arranca en modo "solo modelo" y NO carga los conectores de claude.ai (MyInvestor,
    Supabase...): la skill myinvestor-enrich dejó de ver `mcp__claude_ai_MyInvestor__*` desde el reinicio
    del 24-sep-2026, cuando los procesos del fund-analyzer empezaron a heredarla. Sin la variable, `claude`
    usa el login completo (~/.claude/.credentials.json), que es como funcionaba antes y como lanzan sus
    tareas los scripts del copiloto (hf-cowork-tarea.ps1). Mismo login Max, misma cuota."""
    import os as _os
    env = dict(_os.environ)
    env.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
    return env


def _heartbeat(logfile: str, proc: "subprocess.Popen") -> None:
    """Latido para el watchdog de la cola (web_server mira el mtime de logs/skill_*_{ISIN}*.log).
    En print-mode `claude -p` no vuelca nada hasta terminar, y con Fable 5.1 un paso de síntesis
    puede razonar más de los 40 min de umbral sin tocar ningún fichero → el run se marcaba muerto.
    Mientras el proceso vive, escribimos la hora en <log>_alive.log (mismo patrón de nombre que el
    skill log, así el watchdog lo ve). Al terminar se borra."""
    import os as _os
    import threading as _th
    alive = _os.path.splitext(logfile)[0] + "_alive.log"

    def _loop():
        while proc.poll() is None:
            try:
                with open(alive, "w", encoding="utf-8") as f:
                    f.write(time.strftime("%Y-%m-%d %H:%M:%S") + " claude -p vivo\n")
            except Exception:
                pass
            for _ in range(60):
                if proc.poll() is not None:
                    break
                time.sleep(1)
        try:
            _os.remove(alive)
        except Exception:
            pass

    _th.Thread(target=_loop, daemon=True).start()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 3:
        print("uso: python -m tools.claude_cowork <logfile> <prompt> [args...]")
        return 2
    logfile = sys.argv[1]
    prompt = sys.argv[2]
    passthrough = sys.argv[3:]
    max_tries = 4

    for attempt in range(1, max_tries + 1):
        if not _net_up():
            print(f"[claude_cowork] red caída → esperando a que vuelva (intento {attempt})",
                  flush=True)
            _wait_net(180)
        # cmd /c → resuelve claude.cmd/.exe en Windows; hereda el env del bat (API key vacía → Max)
        cmd = ["cmd", "/c", "claude", "-p", prompt] + passthrough
        with open(logfile, "w", encoding="utf-8", errors="replace") as fh:
            proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, env=_env_login())
            _heartbeat(logfile, proc)
            rc = proc.wait()
        # El hilo del latido es daemon y el proceso termina antes de que borre su fichero: se borra aqui.
        try:
            import os as _os
            _os.remove(_os.path.splitext(logfile)[0] + "_alive.log")
        except OSError:
            pass
        try:
            low = open(logfile, encoding="utf-8", errors="replace").read().lower()
        except Exception:
            low = ""

        if any(m in low for m in _QUOTA_MARKERS):
            # Propaga la línea 'session limit / resets ...' al STDOUT (→ run log) para que la
            # cola del web_server la detecte (lee el run log) y parsee la hora de reset (Madrid).
            import re as _re
            mm = _re.search(
                r"(you've hit your session limit[^\n]*|session limit[^\n]*|resets\s+\d[^\n]*)", low)
            info = mm.group(0).strip() if mm else "session limit"
            print(f"[claude_cowork] SESSION LIMIT — cuota agotada → {info} (sin reintentar)",
                  flush=True)
            return rc if rc else 1

        net_err = any(m in low for m in _NET_MARKERS)
        if rc == 0 and not net_err:
            return 0  # éxito limpio

        if net_err and attempt < max_tries:
            wait = 20 * attempt
            print(f"[claude_cowork] fallo de RED (intento {attempt}/{max_tries}, rc={rc}) → "
                  f"reintento en {wait}s", flush=True)
            time.sleep(wait)
            continue

        # otro fallo (no red / no cuota) o reintentos agotados
        return rc if rc else 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
