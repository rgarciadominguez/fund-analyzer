"""Tests de tools/reconcile_fund_groups.py — agrupación de clases por prefijo.

Fija el comportamiento que evita corromper el catálogo: clases reales del mismo
fondo se fusionan; fondos distintos que comparten prefijo de nombre NO."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.reconcile_fund_groups import clean_base, _is_prefix, detect


def _f(isin, nombre, gid):
    return {"isin": isin, "nombre_clase": nombre, "fund_group_id": gid,
            "has_qualitative_analysis": False, "dashboard_storage_path": None}


def test_prefix_acepta_clases_reales():
    # Misma cartera, distinta clase → una base es prefijo de la otra.
    a = clean_base('Candriam Long Short Credit Classique')
    b = clean_base('CANDRIAM LONG SHORT CREDIT "R" (EUR)')
    assert _is_prefix(b, a) or _is_prefix(a, b)


def test_prefix_rechaza_falsos_positivos():
    # Fondos DISTINTOS que comparten prefijo de 4 tokens → NO deben emparejar.
    casos = [
        ('Amundi IS Core MSCI Europe AE-C', 'Amundi IS Core MSCI Emerging Markets AE-C'),
        ('Amundi Index Solutions Amundi Core Global Government Bond IHE-C',
         'Amundi Index Solutions Amundi FTSE EPRA NAREIT Global AE-C'),
        ('Vanguard Global Short-Term Corporate Bond Index Fund',
         'Vanguard Global Short-Term Bond Index Fund'),
    ]
    for x, y in casos:
        tx, ty = clean_base(x), clean_base(y)
        assert not (_is_prefix(tx, ty) or _is_prefix(ty, tx)), f"{x!r} != {y!r}"


def test_detect_agrupa_solo_misma_gestora_y_prefijo():
    groups = {
        "g1": {"fund_group_id": "g1", "nombre_base": "candriam long short credit", "gestora": "Candriam"},
        "g2": {"fund_group_id": "g2", "nombre_base": "candriam long short credit", "gestora": "Candriam"},
        "g3": {"fund_group_id": "g3", "nombre_base": "amundi is core msci europe", "gestora": "Amundi"},
        "g4": {"fund_group_id": "g4", "nombre_base": "amundi is core msci emerging markets", "gestora": "Amundi"},
    }
    funds = [
        _f("FR0010760694", "Candriam Long Short Credit Classique", "g1"),
        _f("FR0011510056", 'CANDRIAM LONG SHORT CREDIT "R" (EUR)', "g2"),
        _f("LU0389811885", "Amundi IS Core MSCI Europe AE-C", "g3"),
        _f("LU0996177134", "Amundi IS Core MSCI Emerging Markets AE-C", "g4"),
    ]
    splits, _ = detect(funds, groups)
    keys = {k for s in splits for k in [s["key"][1]]}
    # Candriam: split detectado (2 clases, 2 grupos). Amundi Europe/EM: NO.
    assert any("candriam" in k for k in keys)
    assert not any("emerging" in k or "europe" in k for k in keys)


def test_base_corta_va_a_garbage():
    groups = {"g1": {"fund_group_id": "g1", "nombre_base": "o", "gestora": "Troy"}}
    funds = [_f("IE00B6T42S66", "O EUR Acc", "g1")]
    splits, garbage = detect(funds, groups)
    assert not splits
    assert [g["isin"] for g in garbage] == ["IE00B6T42S66"]


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
