"""services_guardian.py — vigila que los DOS servicios base del pipeline estén vivos y
los levanta si faltan, para que "todo desde el portal" nunca se quede a medias en silencio.

Servicios vigilados:
  1. web_server (localhost:5000)  → cola local + arranque de análisis + catálogo en vivo.
  2. poller del portal (portal_analyze_worker --loop) → puente portal→cola local (recoge
     los pendientes que Rafa manda desde la web y los encola/arranca).

Ambos ya auto-arrancan al iniciar sesión (accesos directos en Inicio) y el poller se
auto-relanza vía su .cmd. Este guardián es la RED DE SEGURIDAD para cuando uno se cae con
la sesión abierta (crash, kill manual, etc.): una tarea programada lo corre cada pocos
minutos y revive lo que falte. No requiere que Rafa vigile nada.

Detección (sin falsos positivos):
  - web: HTTP GET /api/queue (doble intento) — mide "está SIRVIENDO", no solo "hay proceso".
  - poller: PID vivo del lock `hf_portal_worker.lock` (el mismo que usa el poller para su
    exclusión mutua). Así jamás se lanza un segundo si ya hay uno vivo.

Uso:
  python -m tools.services_guardian           # una pasada: comprueba y levanta lo que falte
  python -m tools.services_guardian --check    # SOLO informa (no lanza nada)
  pythonw -m tools.services_guardian --daemon  # bucle infinito (cada 3 min) — el de Inicio
"""
from __future__ import annotations

import ctypes
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "logs" / "guardian.log"
WEB_URL = "http://127.0.0.1:5000/api/queue"
POLLER_LOCK = Path(tempfile.gettempdir()) / "hf_portal_worker.lock"

# creationflags Windows para lanzar procesos independientes que sobreviven al guardián
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_NO_WINDOW = 0x08000000


def log(msg: str) -> None:
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass


def web_alive(retries: int = 2) -> bool:
    """True si el web_server está SIRVIENDO. Doble intento con pausa para no confundir
    un arranque en curso (justo tras el logon) con una caída."""
    for i in range(retries):
        try:
            with urllib.request.urlopen(WEB_URL, timeout=6) as r:
                if getattr(r, "status", 200) == 200:
                    return True
        except Exception:
            pass
        if i < retries - 1:
            time.sleep(3)
    return False


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if h:
            ctypes.windll.kernel32.CloseHandle(h)
            return True
    except Exception:
        pass
    return False


def poller_alive() -> bool:
    """True si hay un poller vivo (PID del lock activo). Reusa el lock del propio poller,
    así el guardián nunca lanza un duplicado cuando ya hay uno corriendo."""
    if not POLLER_LOCK.exists():
        return False
    try:
        pid = int(POLLER_LOCK.read_text().strip())
    except Exception:
        return False
    return _pid_alive(pid)


def launch_web() -> None:
    """Lanza el web_server sin ventana (pythonw), independiente del guardián."""
    pyw = Path(sys.executable).with_name("pythonw.exe")
    exe = str(pyw) if pyw.exists() else sys.executable
    try:
        subprocess.Popen(
            [exe, "-m", "tools.web_server", "--no-cold-start", "--port", "5000"],
            cwd=str(ROOT),
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            close_fds=True,
        )
        log("web_server NO responde → lanzado (pythonw -m tools.web_server --no-cold-start)")
    except Exception as e:  # noqa: BLE001
        log(f"[ERROR] no pude lanzar web_server: {e}")


def launch_poller() -> None:
    """Relanza el poller como proceso python directo, oculto y detached (igual que el
    web_server). NO usamos el .cmd porque su 'timeout' necesita consola y lanzarlo oculto
    la rompe. El auto-relanzado lo da este propio guardián (si el poller cae, la siguiente
    pasada lo revive). El túnel público (no crítico) solo lo levanta el .cmd al iniciar
    sesión; en un rescate a media sesión se omite a propósito."""
    pyw = Path(sys.executable).with_name("pythonw.exe")
    exe = str(pyw) if pyw.exists() else sys.executable
    try:
        subprocess.Popen(
            [exe, "-m", "tools.portal_analyze_worker", "--loop", "30", "--limit", "1", "--wake"],
            cwd=str(ROOT),
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            close_fds=True,
        )
        log("poller CAÍDO → lanzado (pythonw -m tools.portal_analyze_worker --loop 30)")
    except Exception as e:  # noqa: BLE001
        log(f"[ERROR] no pude relanzar el poller: {e}")


_INPUTS_STAMP = ROOT / "data" / "_inputs_rafa_guardian.json"
INPUTS_INTERVAL = 3600  # 1 hora


def maybe_consume_inputs() -> None:
    """Una vez/hora lanza el consumer /inputs-rafa (portal → Supabase) como subproceso detached.
    El guardián ya corre siempre (cada 3 min), así que sirve de scheduler horario sin tarea admin."""
    try:
        try:
            last = float(json.loads(_INPUTS_STAMP.read_text(encoding="utf-8")).get("ts", 0))
        except Exception:
            last = 0.0
        if time.time() - last < INPUTS_INTERVAL:
            return
        pyw = Path(sys.executable).with_name("pythonw.exe")
        exe = str(pyw) if pyw.exists() else sys.executable
        subprocess.Popen(
            [exe, "-m", "tools.consume_inputs_rafa"],
            cwd=str(ROOT),
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            close_fds=True,
        )
        _INPUTS_STAMP.parent.mkdir(exist_ok=True)
        _INPUTS_STAMP.write_text(json.dumps({"ts": time.time()}), encoding="utf-8")
        log("consumer /inputs-rafa lanzado (sync portal→Supabase, horario)")
    except Exception as e:  # noqa: BLE001
        log(f"[inputs-rafa] no pude lanzar consumer: {e}")


_PUSH_INTERVAL = 1800  # 30 min
_last_push_check = 0.0


def maybe_push_pending() -> None:
    """Red de seguridad del deploy: si quedaron commits 'auto: regen dashboard' sin subir (push del
    run fallido por red/SSL), los sube. Sin esto el dashboard nuevo no llega al portal hasta que
    otro run haga push. Solo lanza el push si de verdad hay commits pendientes."""
    global _last_push_check
    try:
        if time.time() - _last_push_check < _PUSH_INTERVAL:
            return
        _last_push_check = time.time()
        from tools import git_autopush
        n = git_autopush.pending("v2-cowork")
        if n <= 0:
            return
        pyw = Path(sys.executable).with_name("pythonw.exe")
        exe = str(pyw) if pyw.exists() else sys.executable
        subprocess.Popen(
            [exe, "-m", "tools.git_autopush", "--branch", "v2-cowork"],
            cwd=str(ROOT),
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            close_fds=True,
        )
        log(f"{n} commit(s) del dashboard sin subir → push lanzado (git_autopush)")
    except Exception as e:  # noqa: BLE001
        log(f"[git_autopush] no pude comprobar/lanzar el push: {e}")


def main(check_only: bool = False) -> int:
    w = web_alive()
    p = poller_alive()
    if check_only:
        print(f"web_server: {'VIVO' if w else 'CAIDO'} | poller: {'VIVO' if p else 'CAIDO'}")
        return 0
    if not w:
        launch_web()
        time.sleep(4)  # dar margen a que ligue el puerto antes de que el poller le hable
    if not p:
        launch_poller()
    maybe_consume_inputs()   # scheduler horario del sync /inputs-rafa
    maybe_push_pending()     # red de seguridad del deploy (commits del dashboard sin subir)
    if w and p:
        # heartbeat silencioso 1/hora para no inflar el log (solo en minuto 00-02)
        if time.localtime().tm_min < 3:
            log("ok — web_server + poller vivos")
    return 0


_GUARD_LOCK = Path(tempfile.gettempdir()) / "hf_guardian.lock"


def _acquire_singleton() -> bool:
    """Lock de instancia única del guardián (evita que varios daemons se lancen pollers en paralelo,
    como pasó al reiniciar). PID-liveness + jitter + write-then-verify. True si soy el único."""
    def _pid() -> int:
        try:
            return int(_GUARD_LOCK.read_text().strip())
        except Exception:
            return 0
    if _pid_alive(_pid()) and _pid() != os.getpid():
        return False
    time.sleep(random.uniform(0.05, 0.5))
    if _pid_alive(_pid()) and _pid() != os.getpid():
        return False
    try:
        _GUARD_LOCK.write_text(str(os.getpid()))
    except Exception:
        pass
    time.sleep(0.15)
    return _pid() == os.getpid()


def daemon(interval: int = 180) -> None:
    """Bucle infinito: revisa cada `interval` segundos y revive lo que falte. Es el proceso
    que arranca al iniciar sesión (VBS oculto en Inicio). Nunca revienta: cada pasada va en
    try/except. Si el propio demonio muriera, el siguiente inicio de sesión lo relanza."""
    if not _acquire_singleton():
        log("otro guardián ya vivo (lock) — salgo para no duplicar")
        return
    log(f"daemon iniciado (cada {interval}s)")
    while True:
        try:
            main()
        except Exception as e:  # noqa: BLE001
            log(f"[daemon] pasada falló: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        daemon()
    else:
        sys.exit(main(check_only="--check" in sys.argv))
