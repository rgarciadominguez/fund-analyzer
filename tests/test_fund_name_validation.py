"""Tests del gate anti-basura de nombres de fondo (2026-06-08).

is_valid_fund_name debe ACEPTAR nombres legales (incl. paraguas largos) y
RECHAZAR ISIN, prosa, etiquetas y fragmentos — sin falsos rechazos.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.fund_name_utils import is_valid_fund_name
from tools.name_recovery import needs_recovery


VALID = [
    ("Magallanes European Equity, FI", "ES0159259011"),
    ("DNCA INVEST - Alpha Bonds", "LU1694789451"),
    ("Cobas Selección, FI", "ES0119207001"),
    # Paraguas legal largo (no debe rechazarse)
    ("ROBECO CAPITAL GROWTH FUNDS - ROBECO QI GLOBAL DEVELOPED ENHANCED INDEX EQUITIES", "LU1654173217"),
    ("Sextant Quality Focus", "FR001400CEG4"),
    ("La Française Sub Debt", "FR0010674978"),  # gestora con artículo legítimo
]

INVALID = [
    ("", "ES0159259011", "vacio"),
    ("ES0159259011", "ES0159259011", "es_isin"),
    ("Fund ES0159259011 Class I", "ES0159259011", "contiene_isin"),
    ("AB", "ES0159259011", "muy_corto"),
    ("el fondo invierte en renta variable europea de calidad", "ES0159259011", "conector/minuscula"),
    ("Para inversores que buscan rentas periódicas a largo plazo", "ES0159259011", "conector"),
    ("Información del fondo", "ES0159259011", "stub"),
    ("Nombre del fondo", "ES0159259011", "stub"),
    ("Es un subfondo de la SICAV. Invierte en bonos.", "ES0159259011", "prosa_frases"),
    ("123456", "ES0159259011", "sin_letras"),
]


def test_valid_names_accepted():
    for name, isin in VALID:
        ok, reason = is_valid_fund_name(name, isin)
        assert ok, f"rechazado por error: {name!r} -> {reason}"


def test_invalid_names_rejected():
    for name, isin, _why in INVALID:
        ok, reason = is_valid_fund_name(name, isin)
        assert not ok, f"aceptado por error: {name!r} (deberia fallar: {_why})"


def test_needs_recovery_uses_validator():
    # Antes: needs_recovery solo cazaba vacio/ISIN. Ahora caza prosa/etiqueta.
    assert needs_recovery("el fondo invierte en bonos europeos de corto plazo", "LU123") is True
    assert needs_recovery("Información del fondo", "LU123") is True
    assert needs_recovery("", "LU123") is True
    assert needs_recovery("LU123", "LU123") is True
    # Un nombre bueno NO necesita recovery
    assert needs_recovery("DNCA Invest Alpha Bonds", "LU1694789451") is False
    assert needs_recovery("ROBECO CAPITAL GROWTH FUNDS - ROBECO QI EMERGING MARKETS ACTIVE EQUITIES", "LU0329355670") is False


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
