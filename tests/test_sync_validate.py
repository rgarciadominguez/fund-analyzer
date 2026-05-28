"""
Tests T2.9 (2026-05-28): guard de validación pre-sync en sync_to_supabase.

Bloquea sync cuando output.json tiene fallos críticos (nombre==ISIN/vacío,
gestora==ISIN/vacía, etc.). --force lo bypasa para emergencias.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.sync_to_supabase import _validate_before_sync


@pytest.mark.parametrize("nombre,gestora,expected_ok,expected_in_reason", [
    # Casos OK
    ("Sextant Quality Focus - Action A", "Amiral Gestion", True, None),
    ("DNCA Invest Alpha Bonds", "DNCA Investments", True, None),
    # nombre crítico
    ("", "Amiral Gestion", False, "nombre vacío"),
    ("IE00BDR0JY05", "JPMorgan AM", False, "nombre == ISIN"),
    ("ie00bdr0jy05", "JPMorgan AM", False, "nombre == ISIN"),
    ("LU0123456789", "JPMorgan AM", False, "nombre parece otro ISIN"),
    ("ABC", "JPMorgan AM", False, "nombre muy corto"),
    # gestora crítica
    ("Sextant", "", False, "gestora vacía"),
    ("Sextant", "IE00BDR0JY05", False, "gestora == ISIN"),
    # Múltiples fallos
    ("IE00BDR0JY05", "IE00BDR0JY05", False, None),  # ambos malos → 2 razones
])
def test_validation_cases(nombre, gestora, expected_ok, expected_in_reason):
    data = {"nombre": nombre, "gestora": gestora}
    ok, reasons = _validate_before_sync(data, "IE00BDR0JY05")
    assert ok == expected_ok, f"expected ok={expected_ok}, got {ok}; reasons={reasons}"
    if expected_in_reason:
        assert any(expected_in_reason in r for r in reasons), (
            f"esperaba '{expected_in_reason}' en reasons, got: {reasons}"
        )


def test_validation_multiple_failures():
    data = {"nombre": "IE00BDR0JY05", "gestora": "IE00BDR0JY05"}
    ok, reasons = _validate_before_sync(data, "IE00BDR0JY05")
    assert ok is False
    assert len(reasons) == 2  # nombre==isin Y gestora==isin


def test_validation_whitespace_handled():
    """Whitespace en nombre/gestora se ignora correctamente."""
    data = {"nombre": "   ", "gestora": "   "}
    ok, reasons = _validate_before_sync(data, "ES0123456789")
    assert ok is False
    assert any("nombre vacío" in r for r in reasons)
    assert any("gestora vacía" in r for r in reasons)


def test_sync_fund_aborts_on_bad_data(tmp_path, monkeypatch):
    """sync_fund con output.json corrupto y sin --force aborta sin tocar Supabase."""
    import json
    from tools import sync_to_supabase as mod

    # Crear fixture mínimo
    fund_dir = tmp_path / "data" / "funds" / "FR001400CEK6"
    fund_dir.mkdir(parents=True)
    bad_output = {
        "nombre": "FR001400CEK6",  # == ISIN
        "gestora": "",              # vacío
        "isin": "FR001400CEK6",
    }
    (fund_dir / "output.json").write_text(
        json.dumps(bad_output), encoding="utf-8"
    )
    # Redirigir FUNDS_DIR
    monkeypatch.setattr(mod, "FUNDS_DIR", tmp_path / "data" / "funds")

    res = mod.sync_fund("FR001400CEK6", verbose=False, force=False)
    assert res.get("aborted") is True
    assert res.get("uploaded") == {}
    # 2 razones: nombre==ISIN + gestora vacía
    assert len(res.get("reasons") or []) >= 2


def test_sync_fund_force_bypasses_validation(tmp_path, monkeypatch):
    """Con --force, la validación se ignora pero sí registra warning."""
    import json
    from tools import sync_to_supabase as mod

    fund_dir = tmp_path / "data" / "funds" / "FR001400CEK6"
    fund_dir.mkdir(parents=True)
    (fund_dir / "output.json").write_text(
        json.dumps({"nombre": "FR001400CEK6", "gestora": ""}), encoding="utf-8"
    )
    monkeypatch.setattr(mod, "FUNDS_DIR", tmp_path / "data" / "funds")

    # Stub _safe_read_json para datos opcionales
    # Stub upload + supabase client → todo no-op
    monkeypatch.setattr(mod, "_upload_file_to_storage",
                       lambda *a, **kw: "ok")
    class _FakeRespData:
        data = [{"fund_group_id": "stub-id"}]
    class _FakeTable:
        def update(self, *a, **kw): return self
        def upsert(self, *a, **kw): return self
        def select(self, *a, **kw): return self
        def eq(self, *a, **kw): return self
        def execute(self): return _FakeRespData()
    class _FakeClient:
        def table(self, *a, **kw): return _FakeTable()
    # Mock the supabase_client import (it's imported lazily inside sync_fund)
    import types
    fake_mod = types.ModuleType("tools.supabase_client")
    fake_mod.get_client = lambda: _FakeClient()
    monkeypatch.setitem(sys.modules, "tools.supabase_client", fake_mod)

    res = mod.sync_fund("FR001400CEK6", verbose=False, force=True)
    # Con --force, aborted no se setea → no es la rama de aborto
    assert res.get("aborted") is not True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
