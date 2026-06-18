"""Audit fix (2026-06-18): una gestora VACÍA/trivial nunca debe resolver la web de la
gestora vía el registro (el bug `'' in name` matcheaba la 1ª = avantage). Y, si no
está en el registro, `_resolve_gestora_web_url` hace una BÚSQUEDA DIRIGIDA (mockeada
aquí). `_resolve_gestora_domain` (dominio) se mantiene registro-only.
"""
import agents.sources_agent as sa
from agents.sources_agent import SourcesAgent


class _Stub:
    _gestoras_registry = {
        "Avantage Capital": {"web": "https://www.avantagecapital.com"},
        "Magallanes Value Investors": {"web": "https://www.magallanesvalue.com"},
    }
    gestora_domain = ""
    gestora = ""
    isin = "XX0000000000"
    fund_name = ""

    def _extract_domain(self, w):
        return w.replace("https://www.", "").replace("https://", "").split("/")[0]

    def _log(self, *a, **k):
        pass


def _dom(gestora):
    s = _Stub(); s.gestora = gestora
    return SourcesAgent._resolve_gestora_domain(s)


def _url(gestora, monkeypatch, search_result=None):
    # Mock de la búsqueda dirigida para no tocar red en el test.
    import tools.fund_site_finder as fsf
    monkeypatch.setattr(fsf, "find_official_site_sync",
                        lambda *a, **k: (search_result or {}))
    s = _Stub(); s.gestora = gestora
    return SourcesAgent._resolve_gestora_web_url(s)


def test_empty_gestora_no_registry_match(monkeypatch):
    # gestora vacía → NO resuelve avantage por registro (dominio y url)
    assert _dom("") == ""
    assert "avantage" not in _dom("   ").lower()
    assert _url("", monkeypatch) == ""          # búsqueda mockeada vacía → ""


def test_trivial_gestora_resolves_nothing():
    assert _dom("SA") == ""
    assert _dom("FI") == ""


def test_real_gestora_in_registry_matches(monkeypatch):
    assert _dom("Magallanes Value Investors SA") == "magallanesvalue.com"
    # en registro → ni siquiera busca
    assert _url("Magallanes Value Investors SA", monkeypatch) == "https://www.magallanesvalue.com"


def test_unknown_gestora_uses_directed_search(monkeypatch):
    # No en registro → búsqueda dirigida (mock) devuelve la web oficial
    r = _url("Fortune Financial Strategies SA", monkeypatch,
             search_result={"url": "https://www.montlakeucits.com", "domain": "montlakeucits.com"})
    assert r == "https://www.montlakeucits.com"
    # nunca avantage
    assert "avantage" not in _dom("Fortune Financial Strategies SA").lower()
