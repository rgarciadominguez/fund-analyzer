"""Tests offline de tools/morningstar_classes (parsers, sin red)."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.morningstar_classes import _divisa, _row


def test_divisa_extrae_iso():
    assert _divisa("CU$$$$$EUR") == "EUR"
    assert _divisa("CU$$$$$USD") == "USD"
    assert _divisa("") == ""


def test_row_mapea_campos():
    r = _row({
        "Isin": "lu0168736675", "Name": "Sifter Fund - Global PA",
        "CurrencyId": "CU$$$$$EUR", "OngoingCharge": 1.65,
        "ManagementFee": 1.4, "InceptionDate": "2003-06-19T00:00:00",
    })
    assert r["isin"] == "LU0168736675"   # uppercased
    assert r["divisa"] == "EUR"
    assert r["ter_pct"] == 1.65
    assert r["comision_gestion_pct"] == 1.4
    assert r["fecha_inicio"] == "2003-06-19"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
