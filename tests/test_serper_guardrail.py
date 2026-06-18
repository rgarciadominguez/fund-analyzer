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
    monkeypatch.setenv("SERPER_DISABLED", "1")
    eng = gs.SearchEngine(isin="")
    res = asyncio.run(eng.search("cualquier query"))
    assert res == []
    # no consumió cuota
    assert gs._usage_this_month() == 0


def test_cap_reached_blocks_search(isolated_usage, monkeypatch):
    monkeypatch.setenv("SERPER_MONTHLY_CAP", "3")
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
