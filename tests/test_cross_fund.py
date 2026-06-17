"""Audit ALTO #4: red anti-contaminación cross-fund en cartas (tools/cross_fund).

Caza contaminación por fondos YA analizados (hermanos de paraguas) con anchors
umbrella-aware. Conservador: hard (excluir) solo si el fondo NO se menciona y otro
domina ≥4; soft (revisar) si otro domina pero el fondo sí aparece.
"""
from tools.cross_fund import classify, _anchors

KNOWN = {"dnca invest - flex inflation": "LU9999999999"}
THIS = "DNCA INVEST - ALPHA BONDS"
ISIN = "LU1694789378"


def test_anchors_umbrella_aware():
    a = _anchors("DNCA INVEST - ALPHA BONDS")
    assert "alpha bonds" in a and "dnca invest" in a  # sub-fondo + paraguas


def test_hard_when_fund_absent_and_other_dominates():
    t = "Flex Inflation Flex Inflation Flex Inflation Flex Inflation lideró el año."
    level, info = classify(t, THIS, ISIN, KNOWN)
    assert level == "hard", info


def test_soft_when_other_dominates_but_fund_present():
    t = "Alpha Bonds rinde; pero Flex Inflation Flex Inflation Flex Inflation Flex Inflation destacó más."
    level, info = classify(t, THIS, ISIN, KNOWN)
    assert level == "soft", info


def test_clean_when_fund_dominates():
    t = "Alpha Bonds Alpha Bonds Alpha Bonds rinde bien; citamos Flex Inflation una vez."
    level, _ = classify(t, THIS, ISIN, KNOWN)
    assert level is None


def test_isin_in_signal_never_flags():
    t = "Flex Inflation Flex Inflation Flex Inflation Flex Inflation."
    level, _ = classify(t, THIS, ISIN, KNOWN, extra_signal="https://x/LU1694789378.pdf")
    assert level is None


def test_no_shared_umbrella_false_positive():
    # Dos hermanos del mismo paraguas: el prefijo común NO debe contar como "otro".
    t = "DNCA Invest DNCA Invest DNCA Invest. Alpha Bonds bien."
    level, info = classify(t, THIS, ISIN, KNOWN)
    assert level is None, info  # 'dnca invest' es compartido → se excluye del conteo del otro
