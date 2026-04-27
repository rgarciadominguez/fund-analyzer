"""
Heartbeat + Watchdog para procesos largos.

Útil cuando un script puede colgarse silenciosamente (caso típico:
analyst_agent procesando una sección con LLM, pipeline INT con discovery,
etc.). Sin heartbeat no sabes si está vivo o muerto.

Uso simple — context manager que loggea "alive" cada N segundos:

    from tools.heartbeat import Heartbeat

    with Heartbeat("analyst-section-resumen", interval_s=30, isin="ES0...") as hb:
        # ... trabajo largo ...
        hb.tick("filtrando datos")    # Mensaje custom opcional
        # ... más trabajo ...

    # Al salir del context, logs total time + status

Modo watchdog — kill si no progresa:

    with Watchdog("download-pdfs", timeout_s=300) as wd:
        for url in urls:
            download(url)
            wd.heartbeat()  # resetea el timer
    # Si pasan 300s sin heartbeat() → kills the process

Ambos:
- No requieren cambiar lógica del agente, solo envolver
- Loggean a stdout y a logs/heartbeat.log
- Compatible con asyncio (usa thread)
"""
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
HB_LOG = ROOT / "logs" / "heartbeat.log"


def _log_line(line: str):
    HB_LOG.parent.mkdir(parents=True, exist_ok=True)
    print(line, flush=True)
    try:
        with open(HB_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


class Heartbeat:
    """Context manager que loggea 'alive' periódicamente desde un thread."""

    def __init__(self, name: str, interval_s: int = 30, isin: str = None):
        self.name = name
        self.interval_s = interval_s
        self.isin = (isin or "").strip().upper()
        self._thread = None
        self._stop = threading.Event()
        self._started = None
        self._last_msg = ""
        self._last_tick = None

    def __enter__(self):
        self._started = time.time()
        self._last_tick = time.time()
        self._stop.clear()
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()
        self._log("start")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        elapsed = time.time() - self._started
        status = "ERROR" if exc_type else "done"
        self._log(f"{status} (elapsed {elapsed:.1f}s)")
        return False

    def tick(self, msg: str = ""):
        """Reset el timer con mensaje opcional."""
        self._last_tick = time.time()
        self._last_msg = msg or ""

    def _beat(self):
        while not self._stop.wait(self.interval_s):
            elapsed = time.time() - self._started
            since_tick = time.time() - self._last_tick
            self._log(f"alive ({elapsed:.0f}s, last_tick {since_tick:.0f}s ago) {self._last_msg}")

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{ts}] [HB] [{self.name}]"
        if self.isin:
            prefix += f" [{self.isin}]"
        _log_line(f"{prefix} {msg}")


class Watchdog:
    """Heartbeat con kill: si pasan timeout_s sin heartbeat(), envía SIGTERM al proceso."""

    def __init__(self, name: str, timeout_s: int = 300, isin: str = None):
        self.name = name
        self.timeout_s = timeout_s
        self.isin = (isin or "").strip().upper()
        self._thread = None
        self._stop = threading.Event()
        self._last_hb = None
        self._started = None

    def __enter__(self):
        self._started = time.time()
        self._last_hb = time.time()
        self._stop.clear()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()
        self._log(f"start (timeout {self.timeout_s}s)")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        elapsed = time.time() - self._started
        self._log(f"done (elapsed {elapsed:.1f}s)")
        return False

    def heartbeat(self, msg: str = ""):
        self._last_hb = time.time()
        if msg:
            self._log(f"hb: {msg}")

    def _watch(self):
        check_every = max(5, self.timeout_s // 4)
        while not self._stop.wait(check_every):
            since = time.time() - self._last_hb
            if since > self.timeout_s:
                self._log(f"TIMEOUT — no heartbeat in {since:.0f}s, killing pid {os.getpid()}")
                # En Windows, usamos terminate; en POSIX, SIGTERM
                if sys.platform == "win32":
                    os._exit(2)
                else:
                    os.kill(os.getpid(), signal.SIGTERM)
                return

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{ts}] [WD] [{self.name}]"
        if self.isin:
            prefix += f" [{self.isin}]"
        _log_line(f"{prefix} {msg}")


if __name__ == "__main__":
    # Smoke test
    print("Test Heartbeat (10s, interval 3s):")
    with Heartbeat("smoke-test", interval_s=3, isin="TEST") as hb:
        for i in range(3):
            time.sleep(2.5)
            hb.tick(f"step {i}")
    print("\nTest Watchdog (would kill at 5s — but we heartbeat every 2s):")
    with Watchdog("smoke-wd", timeout_s=5) as wd:
        for i in range(3):
            time.sleep(2)
            wd.heartbeat(f"step {i}")
    print("OK")
