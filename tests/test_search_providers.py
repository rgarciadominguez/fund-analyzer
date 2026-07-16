"""Backend de búsqueda multi-proveedor: Brave → Google CSE → Serper (fallback).

Garantiza: (1) si solo hay Serper, se comporta como antes; (2) Brave primero cuando
está; (3) fallback al siguiente proveedor si uno devuelve vacío; (4) cache hits no
llaman a ningún proveedor.
"""
import asyncio

import pytest

import tools.google_search as gs
from tools.google_search import SearchEngine


@pytest.fixture(autouse=True)
def _no_usage_writes(monkeypatch):
    # No tocar el fichero de uso ni dormir en los tests.
    monkeypatch.setattr(gs, "_reserve", lambda p, n=1: 1)
    monkeypatch.setattr(gs, "_RATE_LIMIT_SECONDS", 0.0)


def _setup(monkeypatch, available, results_by_provider):
    monkeypatch.setattr(gs, "_provider_order", lambda: ["brave", "google", "serper"])
    monkeypatch.setattr(gs, "_provider_available", lambda p: p in available)

    async def fake_call(self, provider, query, num):
        return results_by_provider.get(provider, [])
    monkeypatch.setattr(SearchEngine, "_call_provider", fake_call)


_R = [{"title": "t", "url": "https://x.com", "snippet": "s"}]


def test_only_serper_behaves_as_before(monkeypatch):
    _setup(monkeypatch, available={"serper"}, results_by_provider={"serper": _R})
    out = asyncio.run(SearchEngine().search("q"))
    assert out == _R


def test_brave_first_when_available(monkeypatch):
    _setup(monkeypatch, available={"brave", "serper"},
           results_by_provider={"brave": _R, "serper": [{"title": "NO"}]})
    out = asyncio.run(SearchEngine().search("q"))
    assert out == _R  # usó brave, no serper


def test_fallback_when_brave_empty(monkeypatch):
    _setup(monkeypatch, available={"brave", "google", "serper"},
           results_by_provider={"brave": [], "google": [], "serper": _R})
    out = asyncio.run(SearchEngine().search("q"))
    assert out == _R  # brave y google vacíos → cae a serper


def test_returns_empty_when_none_available(monkeypatch):
    _setup(monkeypatch, available=set(), results_by_provider={})
    assert asyncio.run(SearchEngine().search("q")) == []


def test_cache_hit_skips_providers(monkeypatch):
    called = {"n": 0}

    async def fake_call(self, provider, query, num):
        called["n"] += 1
        return _R
    monkeypatch.setattr(gs, "_provider_order", lambda: ["serper"])
    monkeypatch.setattr(gs, "_provider_available", lambda p: True)
    monkeypatch.setattr(SearchEngine, "_call_provider", fake_call)
    eng = SearchEngine()
    eng._cache = {"queries": {"q": {"results": _R}}, "urls": {}}
    out = asyncio.run(eng.search("q"))
    assert out == _R and called["n"] == 0  # cache → no llamó a ningún proveedor


def test_usage_status_all_providers():
    st = gs.search_usage_status()
    # ddg entra en el backend multi-proveedor (2026-07). google sigue reportado
    # aunque NO esté en el orden por defecto: si alguien lo activa, se ve el gasto.
    assert set(st.keys()) == {"brave", "ddg", "google", "serper"}
    for p in st.values():
        assert "remaining" in p and "has_key" in p and "en_uso" in p
