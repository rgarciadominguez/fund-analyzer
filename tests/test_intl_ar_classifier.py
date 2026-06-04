"""Tests para _classify_pdf_for_task (B-AUM 2026-06-04).

Bug: annual reports de SICAVs mono-fondo se descartaban porque el ISIN no
aparece en las primeras páginas → el fondo quedaba sin AUM. El fix acepta el
AR cuando el NOMBRE del fondo coincide en cabecera.
"""
from pathlib import Path

import pytest

from agents import intl_extractor_v2 as m


@pytest.fixture
def fake_pdf(tmp_path):
    p = tmp_path / "Sifter-Fund-Annual-Report-300820.pdf"
    p.write_bytes(b"%PDF-1.4 fake")
    return p


def _patch_head(monkeypatch, text):
    monkeypatch.setattr(m, "extract_page_range", lambda *a, **k: text, raising=False)
    # extract_page_range se importa dentro de la función; parchear el módulo origen
    import tools.pdf_extractor as pe
    monkeypatch.setattr(pe, "extract_page_range", lambda *a, **k: text)


def test_ar_accepted_when_isin_in_head(monkeypatch, fake_pdf):
    _patch_head(monkeypatch, "Some cover ... ISIN LU0168736675 ... ")
    r = m._classify_pdf_for_task(fake_pdf, "LU0168736675", "Sifter Fund Global")
    assert r is not None and r[0] == "annual_subfund"


def test_ar_accepted_via_fund_name_when_no_isin(monkeypatch, fake_pdf):
    # ISIN ausente en cabecera (caso real SICAV mono-fondo), pero el nombre sí
    _patch_head(monkeypatch, "SIFTER FUND Annual Report 2025. Total net assets...")
    r = m._classify_pdf_for_task(fake_pdf, "LU0168736675", "Sifter Fund Global")
    assert r is not None and r[0] == "annual_subfund"


def test_ar_rejected_when_neither_isin_nor_name(monkeypatch, fake_pdf):
    # AR de otra entidad (p.ej. Tomra holding) — ni ISIN ni nombre del fondo
    _patch_head(monkeypatch, "TOMRA Systems ASA Annual Report 2023. Revenue...")
    r = m._classify_pdf_for_task(fake_pdf, "LU0168736675", "Sifter Fund Global")
    assert r is None


def test_generic_name_tokens_do_not_match(monkeypatch, fake_pdf):
    # Nombre solo con tokens genéricos no debe aceptar un AR ajeno
    _patch_head(monkeypatch, "ACME Global Equity Fund Annual Report")
    r = m._classify_pdf_for_task(fake_pdf, "LU9999999999", "Global Equity Fund")
    assert r is None


def test_ar_schema_includes_aum(monkeypatch, fake_pdf):
    _patch_head(monkeypatch, "SIFTER FUND Annual Report")
    r = m._classify_pdf_for_task(fake_pdf, "LU0168736675", "Sifter Fund")
    assert r is not None
    assert "aum_actual_meur" in r[1].get("kpis", {})
