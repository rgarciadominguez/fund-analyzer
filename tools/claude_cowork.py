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
            rc = subprocess.call(cmd, stdout=fh, stderr=subprocess.STDOUT)
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
