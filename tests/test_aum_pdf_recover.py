"""Tests para tools/aum_pdf_recover.py (recuperación determinista de AUM)."""

from tools.aum_pdf_recover import _num, pick_best


def test_num_anglo_format():
    assert _num("7,580,260,321") == 7580260321.0
    assert _num("280,753,454.11") == 280753454.11


def test_num_ocr_space_in_number():
    # Artefacto OCR de annual reports: "2 80,753,454.11"
    assert _num("2 80,753,454.11") == 280753454.11
    assert _num("1 94,666,156.04") == 194666156.04


def test_num_european_decimal():
    assert _num("1.234.567,89") == 1234567.89


def test_num_invalid():
    assert _num("abc") is None
    assert _num("") is None


def test_pick_best_prefers_eur_over_fx_converted():
    # Mismo fondo en 3 divisas: debe ganar la cifra EUR (exacta), no la USD/HKD
    cands = [
        {"value_meur": 8234.01, "currency": "USD", "priority": 95, "value": 1},
        {"value_meur": 7875.06, "currency": "HKD", "priority": 95, "value": 1},
        {"value_meur": 7580.26, "currency": "EUR", "priority": 95, "value": 1},
    ]
    best = pick_best(cands)
    assert best["currency"] == "EUR"
    assert best["value_meur"] == 7580.26


def test_pick_best_prefers_higher_priority_label():
    cands = [
        {"value_meur": 100.0, "currency": "EUR", "priority": 80},   # fund size
        {"value_meur": 99.0, "currency": "EUR", "priority": 95},    # total size of fund
    ]
    assert pick_best(cands)["priority"] == 95


def test_pick_best_empty():
    assert pick_best([]) is None
