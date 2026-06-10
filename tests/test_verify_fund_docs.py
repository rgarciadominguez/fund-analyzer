"""Tests A.2: verify_fund_docs (rechazo de docs corruptos/truncados/ajenos)."""
import tools.verify_fund_docs as m
import tools.pdf_extractor as pe

_COMPLETE_PDF = b"%PDF-1.4\n" + b"x" * 3000 + b"\n%%EOF\n"   # pasa is_complete_pdf


def _mk(tmp_path, name="x.pdf"):
    p = tmp_path / name
    p.write_bytes(_COMPLETE_PDF)
    return p


def _patch(monkeypatch, text):
    def fake(*a, **k):
        if text is None:
            raise Exception("Unexpected EOF")
        return text
    monkeypatch.setattr(pe, "extract_page_range", fake)


def test_isin_present_ok(monkeypatch, tmp_path):
    _patch(monkeypatch, "... contiene LU0153585137 ... " + "x" * 300)
    ok, r = m.verify_doc_for_fund(_mk(tmp_path), "LU0153585137", "Vontobel European Equity", "Vontobel")
    assert ok


def test_corrupt_parse_rejected(monkeypatch, tmp_path):
    _patch(monkeypatch, None)
    ok, r = m.verify_doc_for_fund(_mk(tmp_path), "LU0153585137", "Vontobel", "Vontobel")
    assert not ok and "corrupto" in r


def test_truncated_pdf_rejected(tmp_path):
    # 1MB sin %%EOF (como los snapshots de Wayback)
    p = tmp_path / "trunc.pdf"
    p.write_bytes(b"%PDF-1.4\n" + b"x" * 1048560)
    ok, r = m.verify_doc_for_fund(p, "LU0153585137", "Vontobel", "Vontobel")
    assert not ok and "truncado" in r


def test_ajeno_rejected(monkeypatch, tmp_path):
    _patch(monkeypatch, "GMO Quarterly Letter about asset allocation " + "y" * 300)
    ok, r = m.verify_doc_for_fund(_mk(tmp_path), "LU0153585137", "Vontobel European Equity", "Vontobel")
    assert not ok and "ajeno" in r


def test_subfund_name_ok_not_gestora(monkeypatch, tmp_path):
    # Solo el token de gestora 'robeco' NO basta; necesita 'momentum'
    _patch(monkeypatch, "Robeco monthly market monitor global outlook " + "z" * 300)
    ok, _ = m.verify_doc_for_fund(_mk(tmp_path), "LU1048590381", "Robeco QI Global Momentum Equities", "Robeco")
    assert not ok
    _patch(monkeypatch, "Robeco QI Global Momentum Equities portfolio " + "z" * 300)
    ok2, _ = m.verify_doc_for_fund(_mk(tmp_path), "LU1048590381", "Robeco QI Global Momentum Equities", "Robeco")
    assert ok2


def test_image_only_rejected(monkeypatch, tmp_path):
    _patch(monkeypatch, "   ")
    ok, r = m.verify_doc_for_fund(_mk(tmp_path), "LU0153585137", "Vontobel", "Vontobel")
    assert not ok and "texto" in r


def test_is_complete_pdf(tmp_path):
    good = tmp_path / "good.pdf"
    good.write_bytes(b"%PDF-1.4\n" + b"x" * 5000 + b"\n%%EOF\n")
    assert m.is_complete_pdf(good)
    trunc = tmp_path / "trunc.pdf"
    trunc.write_bytes(b"%PDF-1.4\n" + b"x" * 1048560)
    assert not m.is_complete_pdf(trunc)
    notpdf = tmp_path / "x.pdf"
    notpdf.write_bytes(b"<html>error</html>")
    assert not m.is_complete_pdf(notpdf)
