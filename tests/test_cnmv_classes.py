"""Tests offline de tools/cnmv_classes (parsers, sin red)."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.cnmv_classes import _pct, _year


def test_pct_parsea_comision():
    assert _pct("1,750%") == 1.75
    assert _pct("0,500%") == 0.5
    assert _pct("") is None
    assert _pct("una participación") is None


def test_year_extrae_anio_alta():
    assert _year("09/01/2015") == "2015-01-01"
    assert _year("25/04/2025") == "2025-01-01"
    assert _year("") is None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
