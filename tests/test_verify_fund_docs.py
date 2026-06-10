"""Tests A.2: verify_fund_docs (rechazo de docs corruptos/ajenos)."""
import pytest
import tools.verify_fund_docs as m
import tools.pdf_extractor as pe


def _patch(monkeypatch, text):
    def fake(*a, **k):
        if text is None:
            raise Exception("Unexpected EOF")
        return text
    monkeypatch.setattr(pe, "extract_page_range", fake)


def test_isin_present_ok(monkeypatch, tmp_path):
    p = tmp_path / "x.pdf"; p.write_bytes(b"%PDF")
    _patch(monkeypatch, "... contiene LU0153585137 ... " + "x" * 300)
    ok, r = m.verify_doc_for_fund(p, "LU0153585137", "Vontobel European Equity", "Vontobel")
    assert ok


def test_corrupt_rejected(monkeypatch, tmp_path):
    p = tmp_path / "x.pdf"; p.write_bytes(b"%PDF")
    _patch(monkeypatch, None)
    ok, r = m.verify_doc_for_fund(p, "LU0153585137", "Vontobel", "Vontobel")
    assert not ok and "corrupto" in r


def test_ajeno_rejected(monkeypatch, tmp_path):
    p = tmp_path / "x.pdf"; p.write_bytes(b"%PDF")
    _patch(monkeypatch, "GMO Quarterly Letter about asset allocation " + "y" * 300)
    ok, r = m.verify_doc_for_fund(p, "LU0153585137", "Vontobel European Equity", "Vontobel")
    assert not ok and "ajeno" in r


def test_subfund_name_ok_not_gestora(monkeypatch, tmp_path):
    p = tmp_path / "x.pdf"; p.write_bytes(b"%PDF")
    # Solo el token de gestora 'robeco' NO basta; necesita 'momentum'
    _patch(monkeypatch, "Robeco monthly market monitor global outlook " + "z" * 300)
    ok, r = m.verify_doc_for_fund(p, "LU1048590381", "Robeco QI Global Momentum Equities", "Robeco")
    assert not ok  # 'momentum' no aparece -> ajeno
    _patch(monkeypatch, "Robeco QI Global Momentum Equities portfolio " + "z" * 300)
    ok2, _ = m.verify_doc_for_fund(p, "LU1048590381", "Robeco QI Global Momentum Equities", "Robeco")
    assert ok2


def test_image_only_rejected(monkeypatch, tmp_path):
    p = tmp_path / "x.pdf"; p.write_bytes(b"%PDF")
    _patch(monkeypatch, "   ")
    ok, r = m.verify_doc_for_fund(p, "LU0153585137", "Vontobel", "Vontobel")
    assert not ok and "texto" in r
