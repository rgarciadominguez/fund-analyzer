"""
Process lock por fondo — evita ejecutar múltiples instancias del mismo agente
sobre el mismo ISIN al mismo tiempo.

Cada agente que escriba archivos del fondo (extractor, analyst, dashboard,
quality) DEBE adquirir el lock antes de empezar y liberarlo al salir. El lock
es un archivo `data/funds/{ISIN}/.{agent}.lock` que contiene el PID del proceso
actual.

Uso típico:

    from tools.process_lock import acquire_or_die, release

    lock = acquire_or_die("extractor", isin)  # aborta sys.exit(2) si hay otro
    try:
        # ... trabajo del agente ...
    finally:
        release(lock)  # NO necesario si usas atexit (registrado automáticamente)

El lock se libera automáticamente:
- En salida normal (atexit).
- Al recibir SIGINT/SIGTERM.
- Si el proceso muere abruptamente, el lock queda "stale": el siguiente
  acquire detecta que el PID del lock NO está vivo y lo limpia.

Reglas de detección:
- Si lock existe y PID vivo: bloquea (sys.exit 2).
- Si lock existe pero PID NO vive: lock stale → limpia y adquiere.
- Si lock no existe: adquiere normal.
"""
from __future__ import annotations

import atexit
import os
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent

# PIDs locked por este proceso (para cleanup atexit)
_HELD_LOCKS: set[Path] = set()


def _lock_path(agent: str, isin: str) -> Path:
    return ROOT / "data" / "funds" / isin.upper() / f".{agent}.lock"


def _pid_alive(pid: int) -> bool:
    """True si el proceso con `pid` está vivo. Multiplataforma básica."""
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            # Windows: usar tasklist con filtro PID
            import subprocess
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
                capture_output=True, text=True, timeout=10,
            )
            # Si el PID existe, tasklist devuelve una línea con el nombre del proceso.
            # Si NO existe, tasklist devuelve "INFO: No tasks are running ..."
            return f',"{pid}",' in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (OSError, ProcessLookupError, PermissionError, subprocess.TimeoutExpired):
        return False


def acquire_or_die(agent: str, isin: str, *, log_fn=None) -> Path:
    """Adquiere el lock para (agent, isin). Si ya hay otro proceso vivo con
    este lock, imprime mensaje y sys.exit(2). Si el lock está "stale" (PID
    muerto), lo limpia y adquiere.

    Args:
        agent: nombre del agente (ej. 'extractor', 'analyst', 'dashboard').
        isin: ISIN del fondo.
        log_fn: opcional, callable para loggear (recibe str). Si None, usa print.

    Returns:
        Path del lock adquirido.
    """
    log = log_fn or (lambda msg: print(msg, file=sys.stderr, flush=True))
    lock = _lock_path(agent, isin)
    lock.parent.mkdir(parents=True, exist_ok=True)

    if lock.exists():
        try:
            existing_pid = int(lock.read_text(encoding="utf-8").strip().split("\n")[0])
        except (ValueError, OSError):
            existing_pid = -1

        if existing_pid > 0 and _pid_alive(existing_pid):
            log(
                f"[LOCK] '{agent}' ya está corriendo para {isin} (PID {existing_pid}). "
                f"Aborto este proceso (PID {os.getpid()}) para no pisar archivos."
            )
            sys.exit(2)
        else:
            # Stale lock: limpiar
            log(f"[LOCK] Lock stale (PID {existing_pid} no vive) — limpiando")
            try:
                lock.unlink()
            except OSError:
                pass

    # Crear lock con nuestro PID + timestamp
    lock.write_text(f"{os.getpid()}\n{int(time.time())}\n", encoding="utf-8")
    _HELD_LOCKS.add(lock)
    log(f"[LOCK] Adquirido '{agent}' para {isin} (PID {os.getpid()})")
    return lock


def release(lock: Path) -> None:
    """Libera un lock. Idempotente — si ya no existe, ignora."""
    if lock in _HELD_LOCKS:
        _HELD_LOCKS.discard(lock)
    try:
        if lock.exists():
            lock.unlink()
    except OSError:
        pass


# Cleanup automático al salir
def _cleanup_all():
    for lock in list(_HELD_LOCKS):
        try:
            if lock.exists():
                lock.unlink()
        except OSError:
            pass
    _HELD_LOCKS.clear()


atexit.register(_cleanup_all)


def _signal_handler(signum, frame):
    _cleanup_all()
    sys.exit(128 + signum)


for _sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(_sig, _signal_handler)
    except (ValueError, OSError):
        pass


if __name__ == "__main__":
    # Diagnóstico: lista locks activos en el proyecto
    import json
    funds_dir = ROOT / "data" / "funds"
    if not funds_dir.exists():
        print("No hay data/funds/")
        sys.exit(0)
    found = []
    for fund in funds_dir.iterdir():
        if not fund.is_dir():
            continue
        for lock_file in fund.glob(".*.lock"):
            try:
                content = lock_file.read_text(encoding="utf-8").strip().split("\n")
                pid = int(content[0])
                ts = int(content[1]) if len(content) > 1 else 0
                age = int(time.time() - ts) if ts else 0
                alive = _pid_alive(pid)
                found.append({
                    "isin": fund.name,
                    "agent": lock_file.stem.lstrip("."),
                    "pid": pid,
                    "age_seconds": age,
                    "pid_alive": alive,
                    "stale": not alive,
                })
            except Exception as e:
                found.append({"isin": fund.name, "agent": lock_file.name, "error": str(e)})
    print(json.dumps(found, ensure_ascii=False, indent=2))
