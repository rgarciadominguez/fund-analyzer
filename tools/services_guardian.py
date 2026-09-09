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
    """Relanza el poller vía su .cmd (que también levanta el túnel y se auto-relanza).
    'start /min' le da su propia consola (necesaria para el 'timeout' del bucle)."""
    cmd = ROOT / "_portal-analyze-worker.cmd"
    if not cmd.exists():
        log(f"[ERROR] no existe {cmd.name} — no puedo relanzar el poller")
        return
    try:
        subprocess.Popen(
            f'start "" /min "{cmd}"',
            shell=True,
            cwd=str(ROOT),
            creationflags=CREATE_NO_WINDOW,  # oculta el cmd lanzador; el .cmd abre su propia consola min.
        )
        log("poller CAÍDO → _portal-analyze-worker.cmd relanzado")
    except Exception as e:  # noqa: BLE001
        log(f"[ERROR] no pude relanzar el poller: {e}")


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
    if w and p:
        # heartbeat silencioso 1/hora para no inflar el log (solo en minuto 00-02)
        if time.localtime().tm_min < 3:
            log("ok — web_server + poller vivos")
    return 0


def daemon(interval: int = 180) -> None:
    """Bucle infinito: revisa cada `interval` segundos y revive lo que falte. Es el proceso
    que arranca al iniciar sesión (VBS oculto en Inicio). Nunca revienta: cada pasada va en
    try/except. Si el propio demonio muriera, el siguiente inicio de sesión lo relanza."""
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
