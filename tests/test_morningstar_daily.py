"""Métricas cuantitativas desde la serie diaria de NAV (tools/morningstar_daily.py).

Fija el fix 2026-07-20: rentab_Na NO debe anualizar una serie más corta que N años
(antes daba una cifra baja y falsa). Se mockea fetch_series para no tocar la red.
"""
from datetime import datetime, timezone

import pytest

from tools import morningstar_daily as md


def _serie(desde: str, hasta: str, v0=100.0, v1=200.0):
    """Serie diaria sintética lineal entre dos fechas (ts_ms, nav)."""
    d0 = datetime.fromisoformat(desde).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(hasta).replace(tzinfo=timezone.utc)
    ndays = (d1 - d0).days
    out = []
    for i in range(ndays + 1):
        ts = int((d0.timestamp() + i * 86400) * 1000)
        out.append((ts, v0 + (v1 - v0) * i / ndays))
    return out


def test_rentab_por_plazo_null_si_serie_mas_corta(monkeypatch):
    # Fondo con ~7 años de histórico: 10a debe ser None; 5a/3a/1a deben salir.
    monkeypatch.setattr(md, "fetch_series", lambda isin: _serie("2019-01-01", "2026-07-16"))
    m = md.compute_metrics("XX0000000000")
    assert m["rentab_10a"] is None, "no hay 10 años de histórico"
    assert m["rentab_5a"] is not None
    assert m["rentab_3a"] is not None
    assert m["rentab_1a"] is not None


def test_rentab_10a_sale_con_historico_largo(monkeypatch):
    monkeypatch.setattr(md, "fetch_series", lambda isin: _serie("2005-01-01", "2026-07-16"))
    m = md.compute_metrics("XX0000000001")
    assert m["rentab_10a"] is not None


def test_metricas_vacias_si_serie_corta(monkeypatch):
    monkeypatch.setattr(md, "fetch_series", lambda isin: _serie("2026-06-01", "2026-06-10"))
    assert md.compute_metrics("XX0000000002") == {}


def test_sharpe_exceso_sobre_rf(monkeypatch):
    """Sharpe = exceso sobre el rf mensual alineado. rf plano a 0 → Sharpe = ret/vol
    del fondo; rf igual al fondo → exceso 0 → Sharpe 0. Y null sin rf."""
    fondo = _serie("2015-01-01", "2026-07-16", v0=100.0, v1=300.0)
    monkeypatch.setattr(md, "fetch_series", lambda isin: fondo)
    # sin rf: no hay claves sharpe
    m0 = md.compute_metrics("XX")
    assert "sharpe_5a" not in m0
    # rf plano (retorno mensual 0 en todos los meses del fondo)
    ym = sorted(md.monthly_returns_by_ym(fondo))
    rf_cero = {k: 0.0 for k in ym}
    m1 = md.compute_metrics("XX", rf_monthly=rf_cero)
    assert m1["sharpe_5a"] is not None
    # rf = mismos retornos que el fondo → exceso 0 en todos los meses → 0/0
    # indefinido → None (no se puede afirmar un Sharpe).
    rf_igual = md.monthly_returns_by_ym(fondo)
    m2 = md.compute_metrics("XX", rf_monthly=rf_igual)
    assert m2["sharpe_5a"] is None


def test_underwater_presente(monkeypatch):
    monkeypatch.setattr(md, "fetch_series", lambda isin: _serie("2020-01-01", "2026-01-01"))
    m = md.compute_metrics("XX")
    assert "underwater" in m and "dias_bajo_agua" in m["underwater"]
