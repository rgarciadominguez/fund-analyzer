"""
Tests del motor de reglas de auto-detección de brokers (tools/broker_availability.py).

Cubren la decisión de producto "solo alta confianza + manual":
  - ES registrado CNMV → Ironia (alta) + Mapfre (media-alta)
  - INT UCITS UE       → Ironia + Mapfre (media-alta)
  - Renta 4 Gestora    → Renta4 (alta)
  - No-UCITS-UE (GB/CH/US) → sin auto-detección
  - MyInvestor SIEMPRE excluido (nunca auto)
  - apply_to_output NO pisa broker_disponible (manual)
"""
import json

import pytest

from tools import broker_availability as ba


def _data(gestora="", tipo=""):
    d = {}
    if gestora:
        d["gestora"] = gestora
    if tipo:
        d["tipo"] = tipo
    return d


# ── ES (CNMV) ─────────────────────────────────────────────────────────────────

def test_es_fund_marks_ironia_alta_mapfre_media_alta():
    info = ba.detect("ES0114105036", _data(gestora="Cobas Asset Management", tipo="ES"))
    assert "Ironia" in info["detected"]
    assert "Mapfre" in info["detected"]
    assert info["per_broker"]["Ironia"]["confidence"] == "alta"
    assert info["per_broker"]["Mapfre"]["confidence"] == "media-alta"


def test_es_non_renta4_gestora_no_marca_renta4():
    info = ba.detect("ES0114105036", _data(gestora="Cobas Asset Management", tipo="ES"))
    assert "Renta4" not in info["detected"]


# ── Renta 4 Gestora ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("gestora", [
    "RENTA 4 GESTORA, S.G.I.I.C., S.A.",
    "Renta 4 Gestora",
    "renta4 gestora sgiic",
])
def test_renta4_gestora_marks_renta4_alta(gestora):
    info = ba.detect("ES0173394034", _data(gestora=gestora, tipo="ES"))
    assert "Renta4" in info["detected"]
    assert info["per_broker"]["Renta4"]["confidence"] == "alta"


def test_renta4_via_universe_set(monkeypatch):
    # Si la gestora no es R4 pero el ISIN está en el universo cacheado → Renta4.
    monkeypatch.setattr(ba, "_renta4_universe", lambda: {"ES9999999999"})
    info = ba.detect("ES9999999999", _data(gestora="Otra Gestora", tipo="ES"))
    assert "Renta4" in info["detected"]


# ── INT UCITS UE ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("isin", ["LU1694789378", "IE00B6T42S66", "FR001400CEG4", "DE000ABC1234"])
def test_int_eu_ucits_marks_ironia_mapfre_media_alta(isin):
    info = ba.detect(isin, _data(gestora="Foreign AM", tipo="INT"))
    assert info["per_broker"]["Ironia"]["confidence"] == "media-alta"
    assert info["per_broker"]["Mapfre"]["confidence"] == "media-alta"


@pytest.mark.parametrize("isin", ["GB00B6T42S66", "CH0012345678", "US0231351067"])
def test_non_eu_ucits_no_auto_detect(isin):
    info = ba.detect(isin, _data(gestora="Foreign AM", tipo="INT"))
    assert info["detected"] == []


# ── ETFs excluidos de Ironia/Mapfre (plataformas de fondos, no de ETF) ────────

@pytest.mark.parametrize("isin,nombre,gestora", [
    ("ES0105336038", "IBEX 35 ETF Cotizado Armonizado", "BBVA"),
    ("IE00B4L5Y983", "iShares Core MSCI World UCITS", "iShares"),
    ("IE00B3XXRP09", "Vanguard S&P 500 UCITS", "Vanguard"),
    ("LU0996180864", "Amundi MSCI Japan ESG", "Amundi"),
])
def test_etf_not_auto_marked_ironia_mapfre(isin, nombre, gestora):
    info = ba.detect(isin, {"nombre": nombre, "gestora": gestora,
                            "tipo": "ES" if isin.startswith("ES") else "INT"})
    assert info["is_etf"] is True
    assert "Ironia" not in info["detected"]
    assert "Mapfre" not in info["detected"]
    assert "Ironia" in info["excluded"]


def test_index_fund_not_flagged_as_etf():
    # Un index FUND (no ETF) lleva "Index" en el nombre → NO se excluye.
    info = ba.detect("IE00B000000X", {"nombre": "Amundi Index MSCI World", "gestora": "Amundi", "tipo": "INT"})
    assert info["is_etf"] is False
    assert "Ironia" in info["detected"]


# ── MyInvestor nunca auto ─────────────────────────────────────────────────────

def test_myinvestor_always_excluded_never_detected():
    for isin, tipo in [("ES0114105036", "ES"), ("LU1694789378", "INT")]:
        info = ba.detect(isin, _data(gestora="Renta 4 Gestora", tipo=tipo))
        assert "MyInvestor" not in info["detected"]
        assert "MyInvestor" not in info["per_broker"]
        assert "MyInvestor" in info["excluded"]


# ── Estructura de salida ──────────────────────────────────────────────────────

def test_output_structure_has_method_and_timestamp():
    info = ba.detect("ES0114105036", _data(tipo="ES"))
    assert info["method"] == ba.METHOD
    assert "generated_at" in info and "T" in info["generated_at"]
    for v in info["per_broker"].values():
        assert set(v) == {"available", "confidence", "reason"}


# ── apply_to_output no pisa el marcado manual ─────────────────────────────────

def test_apply_to_output_preserves_manual_broker_disponible(tmp_path, monkeypatch):
    isin = "ES0114105036"
    fund_dir = tmp_path / "funds" / isin
    fund_dir.mkdir(parents=True)
    output = {
        "isin": isin,
        "gestora": "Cobas Asset Management",
        "tipo": "ES",
        "broker_disponible": ["MyInvestor", "Renta4"],  # marcado MANUAL existente
    }
    op = fund_dir / "output.json"
    op.write_text(json.dumps(output), encoding="utf-8")

    # Redirigir FUNDS_DIR y el save_output a tmp para no tocar datos reales.
    monkeypatch.setattr(ba, "FUNDS_DIR", tmp_path / "funds")
    saved = {}

    def fake_save(_isin, data):
        saved["data"] = data
        op.write_text(json.dumps(data), encoding="utf-8")

    monkeypatch.setattr("tools.output_merger.save_output", fake_save)

    info = ba.apply_to_output(isin)
    data = saved["data"]
    # El campo manual sigue intacto…
    assert data["broker_disponible"] == ["MyInvestor", "Renta4"]
    # …y el auto se escribe en un campo SEPARADO.
    assert data["broker_disponible_auto"]["detected"] == info["detected"]
    assert "Ironia" in data["broker_disponible_auto"]["detected"]
