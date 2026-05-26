"""Tests Cost-Opt Fase 1 (2026-05-02): valida cache + cost monitor + skip + fallback.

Cubre:
- O1: cache local llm_cache (set/get/expiry/disabled)
- O11: cost_monitor (log_call/summary_today/summary_month)
- O2: SKIP_EXISTING_SECTIONS env flag
- O9: QUALITY_LOOP_MAX_ITER env override
- Op A: HAIKU_MODEL constante + GEMINI_FALLBACK_ANTHROPIC default

Run: python tests/test_cost_opt.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

failures = []


def assert_equal(actual, expected, msg):
    if actual != expected:
        failures.append(f"FAIL {msg}: expected={expected!r}, actual={actual!r}")
        return False
    print(f"  PASS {msg}")
    return True


def assert_true(cond, msg):
    if not cond:
        failures.append(f"FAIL {msg}")
        return False
    print(f"  PASS {msg}")
    return True


# ── O1: llm_cache ──────────────────────────────────────────────────────────

def test_o1_cache_set_get():
    print("\n[O1] llm_cache set/get:")
    from tools.llm_cache import set_cached, get_cached, _make_key
    # Verifica determinismo del key
    k1 = _make_key("test-model", "prompt-x", "ctx-y")
    k2 = _make_key("test-model", "prompt-x", "ctx-y")
    assert_equal(k1, k2, "key determinista")
    # Set + get
    set_cached("test-model", "test prompt", "test ctx", "test result")
    got = get_cached("test-model", "test prompt", "test ctx")
    assert_equal(got, "test result", "set+get round trip")
    # Diferente key → no hit
    other = get_cached("test-model", "OTHER prompt", "test ctx")
    assert_equal(other, None, "key distinta no devuelve")


def test_o1_cache_expiry():
    print("\n[O1] llm_cache expiry:")
    from tools.llm_cache import set_cached, get_cached
    set_cached("test-exp", "p", "c", "value")
    # ttl=0.0001 horas → ya debería haber expirado
    time.sleep(0.5)
    expired = get_cached("test-exp", "p", "c", ttl_hours=0.0001)
    assert_equal(expired, None, "TTL expirada no devuelve cached")
    fresh = get_cached("test-exp", "p", "c", ttl_hours=24)
    assert_equal(fresh, "value", "TTL larga sí devuelve")


def test_o1_cache_disabled_env():
    print("\n[O1] llm_cache disabled via env:")
    os.environ["LLM_CACHE_DISABLED"] = "1"
    # Reload module para que pille el env
    import importlib
    import tools.llm_cache
    importlib.reload(tools.llm_cache)
    from tools.llm_cache import set_cached, get_cached
    set_cached("test-d", "p", "c", "v")
    got = get_cached("test-d", "p", "c")
    assert_equal(got, None, "DISABLED=1 no devuelve nada")
    # Restaurar
    del os.environ["LLM_CACHE_DISABLED"]
    importlib.reload(tools.llm_cache)


# ── O11: cost_monitor ──────────────────────────────────────────────────────

def test_o11_cost_log():
    print("\n[O11] cost_monitor log_call + summary:")
    from tools.cost_monitor import log_call, summary_today
    log_call("test_agent", "test_model", "TEST_ISIN", 1000, 500, 0.005)
    s = summary_today()
    assert_true("test_agent" in s.get("by_agent", {}),
                "log_call agregado en summary_today")


# ── O2/O9: env vars ────────────────────────────────────────────────────────

def test_o2_skip_env():
    print("\n[O2] SKIP_EXISTING_SECTIONS env vivo:")
    # Verifica que el código del analyst LEE la env (sin lanzar Gemini)
    import inspect
    from agents.analyst_agent import AnalystAgent
    src = inspect.getsource(AnalystAgent._run_capa3)
    assert_true("SKIP_EXISTING_SECTIONS" in src, "_run_capa3 lee env")


def test_o9_quality_loop_env():
    print("\n[O9] QUALITY_LOOP_MAX_ITER env vivo:")
    import inspect
    import agents.orchestrator as orch
    src = inspect.getsource(orch)
    assert_true("QUALITY_LOOP_MAX_ITER" in src, "orchestrator lee env max_iter")


# ── Op A: Fallback Anthropic — DEPRECATED en Refactor L2 (2026-05-05) ──────
# El fallback Anthropic se eliminó. El sistema solo corre bajo Claude Max
# (modo cowork) o Gemini puro (modo --api-fallback). Ya no hay safety net
# pagada en Anthropic API.

def test_op_a_no_anthropic_fallback():
    print("\n[Op A — DEPRECATED] verificar que GEMINI_FALLBACK_ANTHROPIC ya NO existe:")
    from agents.analyst_agent import AnalystAgent
    a = AnalystAgent.__new__(AnalystAgent)
    # Refactor L2: el flag fue eliminado
    assert_true(not hasattr(a, "GEMINI_FALLBACK_ANTHROPIC"),
                "GEMINI_FALLBACK_ANTHROPIC eliminado (Refactor L2)")


def main():
    print("=" * 60)
    print("Tests Cost-Opt Fase 1 (cache + monitor + skip + fallback)")
    print("=" * 60)
    test_o1_cache_set_get()
    test_o1_cache_expiry()
    test_o1_cache_disabled_env()
    test_o11_cost_log()
    test_o2_skip_env()
    test_o9_quality_loop_env()
    test_op_a_no_anthropic_fallback()
    print("\n" + "=" * 60)
    if failures:
        print(f"FAIL: {len(failures)} fallos")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    print("PASS: todos los tests Cost-Opt OK")


if __name__ == "__main__":
    main()
