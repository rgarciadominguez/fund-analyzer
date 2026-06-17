"""Tests del detector de auto-agrupado de clases (offline, índice sintético)."""
from tools.auto_group_classes import find_same_fund


def _entry(name, stem, classes=(), gestora="", anio=None, specific=True):
    from tools.auto_group_classes import _norm
    return {
        "name": name, "nname": _norm(name), "gestora": _norm(gestora),
        "stem": stem, "class_isins": set(classes), "anio": anio, "specific": specific,
    }


def test_signal1_shared_folleto_isin():
    # CEG4 lista CEK6 en su folleto → mismo fondo aunque los nombres difieran.
    idx = {
        "FR001400CEG4": _entry("Sextant Quality Focus - Action A", "FR001400", ["FR001400CEK6"]),
        "FR001400CEK6": _entry("Sextant Quality Focus - Action F", "FR001400"),
    }
    assert find_same_fund("FR001400CEG4", idx) == {"FR001400CEK6"}
    assert find_same_fund("FR001400CEK6", idx) == {"FR001400CEG4"}  # bidireccional


def test_signal2_exact_name_same_stem():
    # BNY: folleto sin clases, pero nombre EXACTO + mismo stem IE00BD5C → mismo fondo.
    idx = {
        "IE00BD5CTX77": _entry("BNY Mellon Global Short-Dated High Yield Bond Fund", "IE00BD5C"),
        "IE00BD5CV310": _entry("BNY Mellon Global Short-Dated High Yield Bond Fund", "IE00BD5C"),
    }
    assert find_same_fund("IE00BD5CTX77", idx) == {"IE00BD5CV310"}


def test_no_merge_same_name_different_stem():
    # Mismo nombre pero distinto emisor (stem distinto) → NO agrupar.
    idx = {
        "IE00BD5CTX77": _entry("Global Bond Fund", "IE00BD5C"),
        "LU1234567890": _entry("Global Bond Fund", "LU123456"),
    }
    assert find_same_fund("IE00BD5CTX77", idx) == set()


def test_no_merge_different_subfunds_same_umbrella():
    # Sub-fondos distintos del mismo paraguas: nombres distintos, sin ISIN compartido.
    idx = {
        "LU1694789378": _entry("DNCA Invest Alpha Bonds", "LU169478"),
        "LU1694789999": _entry("DNCA Invest Flex Inflation", "LU169478"),
    }
    assert find_same_fund("LU1694789378", idx) == set()


def test_no_merge_when_gestora_differs():
    # Nombre+stem coinciden pero la gestora declarada difiere → homónimos, no agrupar.
    idx = {
        "IE00BD5CTX77": _entry("Income Fund", "IE00BD5C", gestora="BNY Mellon"),
        "IE00BD5CV310": _entry("Income Fund", "IE00BD5C", gestora="Insight Capital"),
    }
    assert find_same_fund("IE00BD5CTX77", idx) == set()
