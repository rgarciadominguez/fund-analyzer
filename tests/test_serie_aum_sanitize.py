"""Tests para sanitize_serie_aum (B4 — bug umbrella SICAV DNCA €41B)."""

from agents.intl_extractor_v2 import sanitize_serie_aum


def test_removes_none_periodo_and_rederives_aum():
    out = {
        "kpis": {"aum_actual_meur": 41932.57},
        "cuantitativo": {
            "serie_aum": [
                {"periodo": "2023", "valor_meur": 8859.7},
                {"periodo": "2024", "valor_meur": 14646.92},
                {"periodo": "2025", "valor_meur": 18463.21},
                {"periodo": "None", "valor_meur": 41932.57},  # umbrella artifact
            ]
        },
    }
    removed = sanitize_serie_aum(out)
    assert removed == 1
    periodos = [e["periodo"] for e in out["cuantitativo"]["serie_aum"]]
    assert "None" not in periodos
    # AUM re-derivado al último año válido (2025), no al €41B
    assert out["kpis"]["aum_actual_meur"] == 18463.21


def test_idempotent_on_clean_series():
    out = {
        "kpis": {"aum_actual_meur": 100.0},
        "cuantitativo": {"serie_aum": [{"periodo": "2024", "valor_meur": 100.0}]},
    }
    assert sanitize_serie_aum(out) == 0
    assert out["kpis"]["aum_actual_meur"] == 100.0


def test_handles_missing_fields():
    assert sanitize_serie_aum({}) == 0
    assert sanitize_serie_aum({"cuantitativo": {}}) == 0
    assert sanitize_serie_aum({"cuantitativo": {"serie_aum": []}}) == 0


def test_drops_non_numeric_periodo_variants():
    out = {
        "kpis": {"aum_actual_meur": 999.0},
        "cuantitativo": {
            "serie_aum": [
                {"periodo": "2024", "valor_meur": 50.0},
                {"periodo": "2024-S1", "valor_meur": 999.0},
                {"periodo": "", "valor_meur": 1.0},
            ]
        },
    }
    assert sanitize_serie_aum(out) == 2
    assert out["kpis"]["aum_actual_meur"] == 50.0
