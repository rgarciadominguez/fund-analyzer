"""Tests de tools/quant_enrichment.py — funciones puras (sin red).

El fetch real de Yahoo depende de red/cobertura → no se testea aquí.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.quant_enrichment import (
    decode_style_box, resolve_geo, compute_capture_ratios, _ratio_from_yield,
)


def test_decode_style_box():
    sb = decode_style_box("https://s.yimg.com/lq/i/fi/3_0stylelargeeq4.gif")
    assert sb == {"box": 4, "size": "Mid", "style": "Value", "_source": "yahoo"}
    assert decode_style_box("https://x/eq1.gif")["size"] == "Large"
    assert decode_style_box("https://x/eq9.gif") == {"box": 9, "size": "Small", "style": "Growth", "_source": "yahoo"}
    assert decode_style_box(None) is None
    assert decode_style_box("https://x/sin-codigo.gif") is None


def test_ratio_from_yield():
    # Yahoo da yield (recíproco) -> ratio = 1/yield
    assert _ratio_from_yield(0.05553) == 18.01   # P/E STOXX600
    assert _ratio_from_yield(0.5) == 2.0
    assert _ratio_from_yield(0) is None
    assert _ratio_from_yield(-1) is None
    assert _ratio_from_yield(None) is None
    assert _ratio_from_yield("x") is None


def test_resolve_geo():
    assert resolve_geo("ES0159259011", "Magallanes European Equity") == "europe"
    assert resolve_geo("LU123", "S&P 500 USA Large Cap") == "usa"
    assert resolve_geo("ES999", "Bankinter Índice España IBEX") == "spain"
    assert resolve_geo("LU999", "MSCI World Global") == "global"
    assert resolve_geo("IE999", "Emerging Markets fund") == "emerging"
    # Fallback por prefijo
    assert resolve_geo("ES000", "") == "spain"
    assert resolve_geo("LU000", "") == "global"


def test_compute_capture_ratios():
    import datetime as dt
    # Construye 24 meses: fondo amplifica subidas, amortigua bajadas → up>100, down<100
    base = dt.datetime(2022, 1, 1)
    fund, bench = [], []
    nf = nb = 100.0
    fund.append((int(base.timestamp()), nf)); bench.append((int(base.timestamp()), nb))
    for i in range(1, 25):
        t = int((base + dt.timedelta(days=31 * i)).timestamp())
        br = 0.04 if i % 2 == 0 else -0.02      # benchmark sube/baja alternando
        fr = br * (1.2 if br > 0 else 0.5)        # fondo: 120% subidas, 50% bajadas
        nb *= (1 + br); nf *= (1 + fr)
        bench.append((t, nb)); fund.append((t, nf))
    res = compute_capture_ratios(fund, bench)
    assert res is not None
    assert res["upside_pct"] > 100      # captura más en subidas
    assert res["downside_pct"] < 100    # captura menos en bajadas
    assert res["n_meses"] >= 12


def test_compute_capture_ratios_insufficient():
    # <12 meses comunes → None
    assert compute_capture_ratios([(1, 100), (2, 101)], [(1, 100), (2, 99)]) is None
    assert compute_capture_ratios(None, None) is None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
