"""
Tests de los canarios de salud (offline, con mocks).

Validan la LÓGICA que detecta el bug "roto en silencio" (2026-03→08): serie vacía/congelada,
firma del redirect a home, cuerpo vacío/HTML, y la aserción de frescura post-análisis.
No tocan la red — mockean fetch_series / httpx.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _ts(days_ago: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days_ago)).timestamp() * 1000)


# ── probe_endpoint: la firma del bug ────────────────────────────────────────
def test_probe_detecta_redirect_a_home(monkeypatch):
    from tools import healthcheck as hc

    class _R:
        status_code = 200
        url = "https://global.morningstar.com/es?marketID=es"  # perdió el path del endpoint
        headers = {"content-type": "text/html"}
        text = "<html>home</html>"
        def json(self): raise ValueError("no json")
    monkeypatch.setattr(hc.httpx, "get", lambda *a, **k: _R())
    p = hc.probe_endpoint("https://tools.morningstar.es/api/rest.svc/timeseries_price/x?id=Y")
    assert p["a_home"] is True
    assert p["es_html"] is True
    assert p["es_json"] is False


def test_probe_detecta_cuerpo_vacio(monkeypatch):
    from tools import healthcheck as hc

    class _R:
        status_code = 200
        url = "https://lt.morningstar.com/api/rest.svc/timeseries_price/x"
        headers = {"content-type": "application/json"}
        text = "   "
        def json(self): raise ValueError("empty")
    monkeypatch.setattr(hc.httpx, "get", lambda *a, **k: _R())
    p = hc.probe_endpoint("https://lt.morningstar.com/api/rest.svc/timeseries_price/x")
    assert p["vacio"] is True
    assert p["a_home"] is False   # conserva el path


def test_probe_json_bueno(monkeypatch):
    from tools import healthcheck as hc

    class _R:
        status_code = 200
        url = "https://lt.morningstar.com/api/rest.svc/timeseries_price/x"
        headers = {"content-type": "application/json"}
        text = "[[1,2],[3,4]]"
        def json(self): return [[1, 2], [3, 4]]
    monkeypatch.setattr(hc.httpx, "get", lambda *a, **k: _R())
    p = hc.probe_endpoint("https://lt.morningstar.com/api/rest.svc/timeseries_price/x")
    assert p["es_json"] is True and p["vacio"] is False and p["a_home"] is False


# ── canary_morningstar_serie: vacía y congelada ─────────────────────────────
def test_canary_serie_vacia(monkeypatch):
    from tools import healthcheck as hc
    import tools.morningstar_daily as md
    monkeypatch.setattr(md, "fetch_series", lambda isin: [])           # host roto → vacío
    monkeypatch.setattr(hc, "probe_endpoint", lambda url, **k: {
        "a_home": True, "vacio": False, "es_html": False})
    r = hc.canary_morningstar_serie()
    assert r["ok"] is False and r["status"] == "SERIE_VACIA" and r["critico"] is True


def test_canary_serie_congelada(monkeypatch):
    from tools import healthcheck as hc
    import tools.morningstar_daily as md
    # 40 puntos pero el último hace 200 días → congelada
    serie = [(_ts(200 + i), 100 + i) for i in range(40)]
    monkeypatch.setattr(md, "fetch_series", lambda isin: serie)
    r = hc.canary_morningstar_serie()
    assert r["ok"] is False and r["status"] == "SERIE_CONGELADA"


def test_canary_serie_fresca(monkeypatch):
    from tools import healthcheck as hc
    import tools.morningstar_daily as md
    serie = [(_ts(60 - i), 100 + i) for i in range(60)]   # último hace 1 día
    monkeypatch.setattr(md, "fetch_series", lambda isin: serie)
    r = hc.canary_morningstar_serie()
    assert r["ok"] is True and r["status"] == "OK"


# ── freshness_guard: solo avisa si CUBIERTO pero vacío ──────────────────────
def test_freshness_no_cubierto_no_avisa(monkeypatch):
    import tools.freshness_guard as fg
    import tools.morningstar_quant as mq
    # testigo (Trojan) SÍ responde, el fondo NO → genuinamente no cubierto (no host caído)
    monkeypatch.setattr(mq, "fetch_quant",
                        lambda isin: {"secid": "TESTIGO"} if isin == "IE00B6T42S66" else {})
    r = fg.check_serie("ES9999999999")
    assert r["ok"] is True and "no cubierto" in r["motivo"]


def test_freshness_host_caido_no_verde(monkeypatch):
    import tools.freshness_guard as fg
    import tools.morningstar_quant as mq
    # ni el fondo ni el testigo responden → host caído → NO verde (el bug que debe cazar)
    monkeypatch.setattr(mq, "fetch_quant", lambda isin: {})
    r = fg.check_serie("ES9999999999")
    assert r["ok"] is False and "caído" in r["motivo"]


def test_freshness_cubierto_pero_vacio_avisa(monkeypatch):
    import tools.freshness_guard as fg
    import tools.morningstar_quant as mq
    import tools.morningstar_daily as md
    monkeypatch.setattr(mq, "fetch_quant", lambda isin: {"secid": "F00000X"})  # cubierto
    monkeypatch.setattr(md, "fetch_series", lambda isin: [])                    # pero serie vacía
    r = fg.check_serie("IE00B6T42S66")
    assert r["ok"] is False and "vacía" in r["motivo"]


# ── dep_autocure: detecta el combo bueno y el redirect ──────────────────────
def test_autocure_try_detecta_redirect(monkeypatch):
    import tools.dep_autocure as ac

    class _R:
        status_code = 200
        url = "https://global.morningstar.com/es?marketID=es"
        text = "<html></html>"
        def json(self): return []
    monkeypatch.setattr(ac.httpx, "get", lambda *a, **k: _R())
    r = ac._try("tools.morningstar.es", "2nhcdckzon", "IE00B6T42S66")
    assert r["ok"] is False and "redirect" in r["nota"]


def test_autocure_try_combo_bueno(monkeypatch):
    import tools.dep_autocure as ac
    puntos = [[1, 2]] * 50

    class _R:
        status_code = 200
        url = "https://lt.morningstar.com/api/rest.svc/timeseries_price/klr5zyak8x?id=X"
        text = "[[1,2]]"
        def json(self): return puntos
    monkeypatch.setattr(ac.httpx, "get", lambda *a, **k: _R())
    r = ac._try("lt.morningstar.com", "klr5zyak8x", "IE00B6T42S66")
    assert r["ok"] is True and r["puntos"] == 50


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
