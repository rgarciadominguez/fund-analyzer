"""Tests de integración para herramientas Fase A/B/C: sibling_finder,
output_merger, output_accessor, regression_guard, cnmv_enrichment.

Smoke tests rápidos (sin LLM) para validar que las integraciones no rompen.
Ejecutar: python tests/test_integration_tools.py"""
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def test_output_accessor():
    """Accessor lee paths canónicos correctamente."""
    from tools.output_accessor import (
        get_perfiles, get_kpis, get_posiciones_actuales,
        get_nombre, get_gestora, get_serie_aum,
    )
    isin = "ES0112231008"  # Avantage
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        print("  SKIP: Avantage output.json missing")
        return True
    d = json.loads(p.read_text(encoding="utf-8"))
    assert get_nombre(d), "nombre vacío"
    assert get_kpis(d), "kpis vacío"
    assert isinstance(get_perfiles(d), list), "perfiles no es lista"
    assert isinstance(get_posiciones_actuales(d), list), "posiciones no es lista"
    print("  OK: get_nombre/kpis/perfiles/posiciones todos funcionan")
    return True


def test_output_merger():
    """Merger preserva _manual_edits."""
    from tools.output_merger import merge_preserving_manual_edits, mark_manual_edit
    existing = {"nombre": "REAL", "kpis": {"aum": 100}, "_manual_edits": ["nombre", "kpis.aum"]}
    new = {"nombre": "WRONG", "kpis": {"aum": 50, "ter": 0.5}}
    merged = merge_preserving_manual_edits(existing, new)
    assert merged["nombre"] == "REAL", f"nombre no preservado: {merged['nombre']}"
    assert merged["kpis"]["aum"] == 100, f"aum no preservado: {merged['kpis']['aum']}"
    assert merged["kpis"]["ter"] == 0.5, "ter (no manual) no actualizado"
    print("  OK: manual_edits preservados, no-marcados se actualizan")
    return True


def test_regression_guard():
    """Guard rechaza secciones peores."""
    from tools.regression_guard import accept_section, is_regression
    old = {"texto": "Resumen completo de **150 M€** AUM con 5000 partícipes y TER 0.91%."}
    new_worse = {"texto": "Resumen corto."}
    is_reg, reasons = is_regression(new_worse, old, "resumen")
    assert is_reg, "Debería detectar regresión"
    assert reasons, "Debe dar razones"
    final = accept_section("resumen", new_worse, old)
    assert final == old or final.get("texto") == old["texto"], "Debe mantener antiguo"
    print("  OK: guard rechaza nueva versión peor")
    return True


def test_sibling_finder():
    """Sibling finder detecta hermanos por gestora."""
    from tools.sibling_finder import find_siblings, _name_root
    siblings = find_siblings("ES0182527038")  # Cartesio Y
    assert siblings, "Debería encontrar Cartesio X"
    has_x = any(s["isin"] == "ES0116567035" for s in siblings)
    assert has_x, "No encontró Cartesio X como hermano"
    # name_root heuristic
    assert _name_root("DUNAS VALOR PRUDENTE FI") == "DUNAS"
    assert _name_root("CARTESIO X, FI") == "CARTESIO"
    print(f"  OK: Cartesio Y tiene {len(siblings)} hermano(s) detectados, name_root OK")
    return True


def test_cost_tracker():
    """Cost tracker calcula precios actuales."""
    from tools.cost_tracker import calc_cost, track, get_summary, reset
    reset("TEST")
    cost = calc_cost("claude-sonnet-4-5", 1_000_000, 100_000)
    assert abs(cost - 4.5) < 0.01, f"cost calc wrong: {cost}"  # 3 input + 1.5 output = 4.5
    track("TEST", "gemini-2.5-flash", 50_000, 5_000, "test_agent")
    summary = get_summary("TEST")
    assert summary["total_cost_usd"] > 0, "track no registró"
    reset("TEST")
    print("  OK: cost calc + track + summary funcionan")
    return True


if __name__ == "__main__":
    tests = [
        ("Output Accessor", test_output_accessor),
        ("Output Merger", test_output_merger),
        ("Regression Guard", test_regression_guard),
        ("Sibling Finder", test_sibling_finder),
        ("Cost Tracker", test_cost_tracker),
    ]
    print("=" * 60)
    print("Tests de integración tools/ (Fase A/B/C)")
    print("=" * 60)
    failed = 0
    for name, fn in tests:
        print(f"\n[{name}]")
        try:
            fn()
        except AssertionError as exc:
            print(f"  FAIL: {exc}")
            failed += 1
        except Exception as exc:
            print(f"  ERROR: {type(exc).__name__}: {exc}")
            failed += 1
    print()
    print("=" * 60)
    if failed:
        print(f"FAIL: {failed}/{len(tests)} tests")
        sys.exit(1)
    print(f"PASS: {len(tests)}/{len(tests)} tests")
