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
