"""Tests de tools/class_links.py — vínculos de clase alias→primary (2026-06-16)."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tools.class_links as cl


def _fresh(tmp_path):
    cl.LINKS_PATH = tmp_path / "fund_class_links.json"
    if cl.LINKS_PATH.exists():
        cl.LINKS_PATH.unlink()


def test_add_resolve_classes_remove(tmp_path):
    _fresh(tmp_path)
    r = cl.add_link("IE00BYV18N80", "IE00B6T42S66", "Clase D EUR")
    assert r["ok"] and r["primary"] == "IE00B6T42S66"
    assert cl.resolve_primary("IE00BYV18N80") == "IE00B6T42S66"
    assert cl.resolve_primary("IE00B6T42S66") == "IE00B6T42S66"   # primario → él mismo
    assert cl.is_alias("IE00BYV18N80") is True
    assert [c["isin"] for c in cl.get_classes("IE00B6T42S66")] == ["IE00BYV18N80"]
    assert cl.remove_link("IE00BYV18N80")["ok"] is True
    assert cl.resolve_primary("IE00BYV18N80") == "IE00BYV18N80"


def test_cycle_and_invalid_blocked(tmp_path):
    _fresh(tmp_path)
    cl.add_link("IE00BYV18N80", "IE00B6T42S66")
    assert cl.add_link("IE00B6T42S66", "IE00BYV18N80")["ok"] is False   # ciclo
    assert cl.add_link("XX", "IE00B6T42S66")["ok"] is False             # alias inválido
    assert cl.add_link("IE00BYV18N80", "ZZ")["ok"] is False             # primary inválido
    assert cl.add_link("IE00B6T42S66", "IE00B6T42S66")["ok"] is False   # iguales


def test_chain_resolves_to_root(tmp_path):
    _fresh(tmp_path)
    # B alias de A; luego C alias de B → C debe resolver a A (raíz)
    cl.add_link("IE00BYV18N80", "IE00B6T42S66")          # B→A
    cl.add_link("IE00BD5CTX77", "IE00BYV18N80")          # C→B (se reapunta a A)
    assert cl.resolve_primary("IE00BD5CTX77") == "IE00B6T42S66"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
