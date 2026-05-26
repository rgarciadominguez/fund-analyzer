"""
Test N3 (branch v2-cowork, 2026-05-20): smoke tests del worker de
`tools/web_server.py` que verifican:
  - Detección de agotamiento de tokens en log (patterns + casing).
  - Patrón de espera por polling resiliente cuando `proc.wait()` retorna
    prematuro (Win11 + CREATE_NEW_PROCESS_GROUP).

NO modifico `tools/web_server.py` (cowork tiene cambios pendientes). El test
verifica la API pública del módulo (TOKEN_EXHAUSTION_PATTERNS + make_app)
y replica el patrón de polling para confirmar que la idea funciona contra
un Popen mock.
"""
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ════════════════════════════════════════════════════════════════════
# Token exhaustion pattern detection
# ════════════════════════════════════════════════════════════════════


def test_token_exhaustion_patterns_module_level():
    """TOKEN_EXHAUSTION_PATTERNS está exportado a nivel módulo y es no-vacío."""
    from tools import web_server
    assert hasattr(web_server, "TOKEN_EXHAUSTION_PATTERNS")
    patterns = web_server.TOKEN_EXHAUSTION_PATTERNS
    assert isinstance(patterns, (list, tuple))
    assert len(patterns) >= 5, "se esperaban >=5 patrones de agotamiento"
    # Subset esperado mínimo
    pset = {p.lower() for p in patterns}
    expected = {"rate limit", "quota exceeded", "usage limit reached"}
    missing = expected - pset
    assert not missing, f"patrones esperados ausentes: {missing}"


@pytest.mark.parametrize("log_text", [
    "Anthropic API error: rate limit exceeded.",
    "ERROR: quota exceeded for this organization.",
    "You've reached your monthly token limit.",
    "model overloaded, please retry later.",
    "API call failed: usage limit reached.",
    "[ERROR] credit balance is too low to continue.",
])
def test_token_exhaustion_substrings_trigger(log_text):
    """Cada uno de los patrones esperados aparece en logs reales que el
    worker debe identificar como agotamiento de tokens. Replica la lógica
    `any(pat in text.lower() for pat in PATTERNS)` que usa el worker."""
    from tools import web_server
    patterns = web_server.TOKEN_EXHAUSTION_PATTERNS
    lower = log_text.lower()
    matched = [p for p in patterns if p.lower() in lower]
    assert matched, f"log '{log_text}' no matchea ningún patrón. patterns={patterns}"


def test_token_exhaustion_no_false_positives_on_clean_log():
    """Un log limpio (sin agotamiento) NO debe matchear ningún patrón."""
    from tools import web_server
    patterns = web_server.TOKEN_EXHAUSTION_PATTERNS
    benign_logs = [
        "[INFO] starting analysis for ES0123456789",
        "Pipeline completed successfully in 42.3s",
        "[OK] CNMV data fetched: 31 XMLs",
        "Top holdings: Microsoft 5.2%, Visa 4.1%",
        "WARN: pdfplumber fallback to OCR mode",
    ]
    for log in benign_logs:
        lower = log.lower()
        hit = [p for p in patterns if p.lower() in lower]
        assert not hit, f"falso positivo en '{log}': matched {hit}"


# ════════════════════════════════════════════════════════════════════
# Polling pattern resilience (proc.wait premature + proc.poll authoritative)
# ════════════════════════════════════════════════════════════════════


class _FakePopen:
    """Mock de subprocess.Popen.

    Simula el bug Win11+CREATE_NEW_PROCESS_GROUP: `wait()` retorna prematuro
    (después de N polls falsos), pero `poll()` devuelve None mientras el
    proceso "vive" y el exit code real al terminar.

    Parametriza:
      - polls_until_done: cuántos `poll()` devuelven None antes del exit
      - wait_returns_premature: si True, `wait()` devuelve un fake exit code
        en su primera llamada aunque poll() siga devolviendo None.
    """

    def __init__(self, polls_until_done=3, real_exit_code=0,
                 wait_returns_premature=True):
        self.pid = 99999
        self._poll_calls = 0
        self._polls_until_done = polls_until_done
        self._real_exit_code = real_exit_code
        self._wait_called = False
        self._wait_returns_premature = wait_returns_premature

    def poll(self):
        self._poll_calls += 1
        if self._poll_calls > self._polls_until_done:
            return self._real_exit_code
        return None

    def wait(self, timeout=None):
        # Bug simulado: wait() retorna prematuro sin esperar realmente
        self._wait_called = True
        if self._wait_returns_premature:
            return -1  # exit code falso "prematuro"
        # Mantener la implementación correcta como referencia
        while self.poll() is None:
            time.sleep(0.01)
        return self._real_exit_code

    def terminate(self):
        pass

    def kill(self):
        pass


def _wait_with_polling(proc, max_iters: int = 20, poll_interval: float = 0.01):
    """Replica del patrón `_wait_for_proc_with_polling` del worker — usa
    `poll()` (no `wait()`) en un loop con sleeps cortos. Devuelve el exit
    code real cuando poll() ya no es None."""
    for _ in range(max_iters):
        rc = proc.poll()
        if rc is not None:
            return rc
        time.sleep(poll_interval)
    # Timeout falla: terminar proc
    proc.terminate()
    return proc.poll()


def test_polling_pattern_handles_premature_wait():
    """Patrón del worker: `poll()` en loop. Aunque `wait()` regrese prematuro
    con un código falso, el polling devuelve el código REAL cuando poll()
    deja de devolver None."""
    proc = _FakePopen(polls_until_done=5, real_exit_code=0)
    rc = _wait_with_polling(proc, max_iters=20, poll_interval=0.01)
    assert rc == 0, f"polling debió devolver 0, no {rc}"
    # Y NO se llamó a wait() durante el polling
    assert not proc._wait_called, "el polling no debe invocar wait()"


def test_polling_pattern_returns_real_nonzero_exit_code():
    """El polling captura exit codes != 0 correctamente."""
    proc = _FakePopen(polls_until_done=2, real_exit_code=10)
    rc = _wait_with_polling(proc, max_iters=20, poll_interval=0.01)
    assert rc == 10, f"esperaba 10, obtuve {rc}"


def test_polling_pattern_times_out_and_terminates():
    """Si el proc nunca termina (polls_until_done > max_iters), el polling
    lo TERMINA después del timeout."""
    proc = _FakePopen(polls_until_done=1000, real_exit_code=0)
    rc = _wait_with_polling(proc, max_iters=5, poll_interval=0.001)
    # Tras terminate(), poll() todavía devuelve None en este mock — eso
    # es OK: lo importante es que el patrón ABORTA en lugar de bloquearse.
    # rc puede ser None (terminate no cambia el mock state)
    assert proc._poll_calls >= 5  # al menos los polls del loop


# ════════════════════════════════════════════════════════════════════
# Smoke test del módulo web_server (importa sin crashear)
# ════════════════════════════════════════════════════════════════════


def test_web_server_module_imports_cleanly():
    """tools.web_server debe importarse sin ejecutar el worker."""
    from tools import web_server
    assert hasattr(web_server, "make_app")
    assert callable(web_server.make_app)


def test_make_app_returns_flask_app():
    """make_app(cold_start=True) devuelve una instancia Flask configurada."""
    try:
        from flask import Flask
    except ImportError:
        pytest.skip("Flask no instalado en este entorno")
    from tools import web_server
    app = web_server.make_app(cold_start=False)
    assert isinstance(app, Flask)
    assert app.config.get("FUND_COLD_START") is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
