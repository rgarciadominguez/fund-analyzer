"""Audit fix (2026-06-18): una gestora VACÍA/trivial nunca debe resolver una web de
gestora. El bug era `gestora_lower in name.lower()` con gestora_lower="" → '' está en
TODO → matcheaba la 1ª gestora del registro (Avantage, el fondo de test) y filtraba
avantagecapital.com a fondos ajenos (cross-fund contamination sistémica del discovery).
"""
from agents.sources_agent import SourcesAgent


class _Stub:
    _gestoras_registry = {
        "Avantage Capital": {"web": "https://www.avantagecapital.com"},
        "Magallanes Value Investors": {"web": "https://www.magallanesvalue.com"},
    }
    gestora_domain = ""
    gestora = ""

    def _extract_domain(self, w):
        return w.replace("https://www.", "").replace("https://", "").split("/")[0]


def _dom(gestora):
    s = _Stub(); s.gestora = gestora
    return SourcesAgent._resolve_gestora_domain(s)


def _url(gestora):
    s = _Stub(); s.gestora = gestora
    return SourcesAgent._resolve_gestora_web_url(s)


def test_empty_gestora_resolves_nothing():
    # el bug: gestora vacía devolvía avantagecapital.com (1ª del registro)
    assert _dom("") == ""
    assert _url("") == ""
    assert _dom("   ") == ""


def test_trivial_gestora_resolves_nothing():
    assert _dom("SA") == ""
    assert _dom("FI") == ""


def test_real_gestora_in_registry_matches():
    assert _dom("Magallanes Value Investors SA") == "magallanesvalue.com"
    assert _url("Magallanes Value Investors SA") == "https://www.magallanesvalue.com"


def test_unknown_gestora_returns_empty_not_avantage():
    # Fortune Financial Strategies NO está en el registro → "" (no el default test)
    assert _dom("Fortune Financial Strategies SA") == ""
    assert "avantage" not in _dom("Fortune Financial Strategies SA").lower()
