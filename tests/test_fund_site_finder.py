"""Búsqueda dirigida de la web oficial del gestor/fondo (tools/fund_site_finder).

Valida la LÓGICA (filtra agregadores, puntúa por solapamiento con gestora/nombre)
con resultados de búsqueda simulados — el live depende del tope mensual de Serper.
"""
import asyncio

import pytest

import tools.fund_site_finder as fsf


class _FakeEngine:
    def __init__(self, results): self._r = results
    async def search(self, query, num=8, agent=""): return self._r


def _run(isin, name, gestora, results, monkeypatch):
    import tools.google_search as gs
    monkeypatch.setattr(gs, "SearchEngine", lambda: _FakeEngine(results))
    return asyncio.run(fsf.find_official_site(isin, name, gestora))


def test_picks_official_over_aggregators(monkeypatch):
    results = [
        {"link": "https://www.morningstar.com/funds/xyz", "title": "Magallanes - Morningstar", "snippet": ""},
        {"link": "https://citywire.com/magallanes", "title": "Magallanes Citywire", "snippet": ""},
        {"link": "https://www.magallanesvalue.com/european-equity", "title": "Magallanes Value Investors", "snippet": "Magallanes European Equity"},
    ]
    r = _run("ES0159259011", "Magallanes European Equity", "Magallanes Value Investors", results, monkeypatch)
    assert r.get("domain") == "magallanesvalue.com", r


def test_picks_platform_when_no_gestora_domain(monkeypatch):
    # Fortune Financial no tiene dominio obvio; gana la plataforma del fondo.
    results = [
        {"link": "https://www.ft.com/funds/IE000Z9YV312", "title": "FT", "snippet": ""},
        {"link": "https://www.montlakeucits.com/subfund/alpha-fixed-income-ucits-fund/",
         "title": "Alpha Fixed Income UCITS Fund | MontLake", "snippet": "Fortune Financial Strategies"},
    ]
    r = _run("IE000Z9YV312", "Alpha Fixed Income UCITS Fund", "Fortune Financial Strategies", results, monkeypatch)
    assert r.get("domain") == "montlakeucits.com", r


def test_returns_empty_when_only_aggregators(monkeypatch):
    results = [
        {"link": "https://www.morningstar.com/x", "title": "x", "snippet": ""},
        {"link": "https://finect.com/y", "title": "y", "snippet": ""},
    ]
    r = _run("ES0000000000", "Foo Fund", "Bar Gestora", results, monkeypatch)
    assert r == {}


def test_aggregator_and_domain_helpers():
    assert fsf._is_aggregator("morningstar.com")
    assert fsf._is_aggregator("uk.finect.com")
    assert not fsf._is_aggregator("magallanesvalue.com")
    assert fsf._domain("https://www.dnca-investments.com/en/funds") == "dnca-investments.com"
