"""
Tests del guardrail de coste Serper (tools/google_search.py).

Garantizan: tope mensual duro, killswitch SERPER_DISABLED, y que los cache hits
NO consumen cuota.
"""
import asyncio
import json

import pytest

from tools import google_search as gs


@pytest.fixture
def isolated_usage(tmp_path, monkeypatch):
    """Redirige el contador a un fichero temporal y limpia env + estado."""
    monkeypatch.setattr(gs, "_USAGE_PATH", tmp_path / "serper_usage.json")
    gs._logged_once.clear()
    monkeypatch.delenv("SERPER_DISABLED", raising=False)
    monkeypatch.delenv("SERPER_MONTHLY_CAP", raising=False)
    monkeypatch.setenv("SERPER_API_KEY", "test-key")
    return tmp_path


def test_status_starts_at_zero(isolated_usage):
    st = gs.serper_usage_status()
    assert st["used"] == 0
    assert st["cap"] == 2000
    assert st["blocked"] is False


def test_reserve_increments_and_persists(isolated_usage):
    assert gs._reserve_usage(1) == 1
    assert gs._reserve_usage(5) == 6
    assert gs._usage_this_month() == 6
    # persistido en disco (formato nuevo multi-proveedor: {provider: {mes: count}})
    data = json.loads((isolated_usage / "serper_usage.json").read_text(encoding="utf-8"))
    assert data["serper"][gs._month_key()] == 6


def test_disabled_blocks_search(isolated_usage, monkeypatch):
    """SERPER_DISABLED es el killswitch de SERPER, no el de buscar.

    Con el backend multi-proveedor (2026-07) otros proveedores gratis pueden
    seguir sirviendo; lo que se garantiza es que Serper no se llama ni se cobra.
    Se fija el orden a solo-serper para aislar esa garantía.
    """
    monkeypatch.setenv("SERPER_DISABLED", "1")
    monkeypatch.setenv("SEARCH_PROVIDER_ORDER", "serper")
    eng = gs.SearchEngine(isin="")
    res = asyncio.run(eng.search("cualquier query"))
    assert res == []
    # no consumió cuota
    assert gs._usage_this_month() == 0


def test_cap_reached_blocks_search(isolated_usage, monkeypatch):
    """Tope duro de Serper: alcanzado, no se llama más (aunque sea el único)."""
    monkeypatch.setenv("SERPER_MONTHLY_CAP", "3")
    monkeypatch.setenv("SEARCH_PROVIDER_ORDER", "serper")
    gs._reserve_usage(3)  # ya en el tope
    eng = gs.SearchEngine(isin="")
    res = asyncio.run(eng.search("otra query"))
    assert res == []
    assert gs.serper_usage_status()["blocked"] is True


def test_cache_hit_does_not_consume_quota(isolated_usage):
    eng = gs.SearchEngine(isin="")
    # Sembrar caché manualmente
    eng._store_results("query cacheada", [{"title": "t", "url": "u", "snippet": "s"}])
    before = gs._usage_this_month()
    res = asyncio.run(eng.search("query cacheada"))
    assert res and res[0]["url"] == "u"
    assert gs._usage_this_month() == before  # 0 consumo


# ── Backend multi-proveedor: brave → ddg (gratis) → serper. Google NUNCA por
# defecto: es de pago (decisión de Rafa, 2026-07). ────────────────────────────

def test_google_no_esta_en_el_orden_por_defecto(isolated_usage, monkeypatch):
    monkeypatch.delenv("SEARCH_PROVIDER_ORDER", raising=False)
    orden = gs._provider_order()
    assert "google" not in orden, "Google CSE es de pago: no puede ir por defecto"
    assert orden == ["brave", "ddg", "serper"]


def test_ddg_no_necesita_key(isolated_usage, monkeypatch):
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    assert gs._provider_available("ddg") is True


def test_ddg_unwrap_devuelve_url_real(isolated_usage):
    u = gs.SearchEngine._ddg_unwrap(
        "//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.es.vanguard%2Ffondo%2F9834&rut=xx")
    assert u == "https://www.es.vanguard/fondo/9834"
    # una URL normal no se toca
    assert gs.SearchEngine._ddg_unwrap("https://a.com/b") == "https://a.com/b"
    assert gs.SearchEngine._ddg_unwrap("") == ""


def test_ddg_sirve_la_query_sin_gastar_serper(isolated_usage, monkeypatch):
    """Si DDG (gratis) responde, Serper no se toca."""
    monkeypatch.setenv("SEARCH_PROVIDER_ORDER", "ddg,serper")

    async def fake_ddg(self, query, num):
        return [{"title": "t", "url": "https://real.com/x", "snippet": "s"}]

    async def fail_serper(self, query, num):
        raise AssertionError("no debe llamarse a Serper si DDG ha respondido")

    monkeypatch.setattr(gs.SearchEngine, "_search_ddg", fake_ddg)
    monkeypatch.setattr(gs.SearchEngine, "_search_serper", fail_serper)
    res = asyncio.run(gs.SearchEngine(isin="").search("q libre"))
    assert res and res[0]["url"] == "https://real.com/x"
    assert gs._usage_count("serper") == 0
    assert gs._usage_count("ddg") == 1


def test_serper_recoge_si_ddg_viene_vacio(isolated_usage, monkeypatch):
    """DDG es frágil (0 sin error): Serper es la red de seguridad."""
    monkeypatch.setenv("SEARCH_PROVIDER_ORDER", "ddg,serper")

    async def empty_ddg(self, query, num):
        return []

    async def ok_serper(self, query, num):
        return [{"title": "t", "url": "https://serper.com/x", "snippet": "s"}]

    monkeypatch.setattr(gs.SearchEngine, "_search_ddg", empty_ddg)
    monkeypatch.setattr(gs.SearchEngine, "_search_serper", ok_serper)
    res = asyncio.run(gs.SearchEngine(isin="").search("q libre 2"))
    assert res and res[0]["url"] == "https://serper.com/x"
    assert gs._usage_count("serper") == 1
