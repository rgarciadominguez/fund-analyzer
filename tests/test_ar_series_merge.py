"""Fase B: statistics/performance del AR -> serie_aum / serie_rentabilidad."""
from agents.orchestrator import merge_ar_statistics_performance


def test_statistics_to_serie_aum():
    out = {}
    data = {"statistics": [
        {"periodo": "2024", "aum_meur": 1200.5},
        {"periodo": "2023", "aum_meur": 1100.0},
        {"periodo": "None", "aum_meur": 99999},   # no-año -> descartado
        {"periodo": "2022", "aum_meur": None},      # sin valor -> descartado
    ]}
    merge_ar_statistics_performance(data, out)
    serie = out["cuantitativo"]["serie_aum"]
    pers = {e["periodo"]: e["valor_meur"] for e in serie}
    assert pers == {"2024": 1200.5, "2023": 1100.0}


def test_statistics_upsert_over_existing():
    out = {"cuantitativo": {"serie_aum": [{"periodo": "2023", "valor_meur": 1.0}]}}
    merge_ar_statistics_performance({"statistics": [{"periodo": "2023", "aum_meur": 1100.0}]}, out)
    serie = {e["periodo"]: e["valor_meur"] for e in out["cuantitativo"]["serie_aum"]}
    assert serie["2023"] == 1100.0  # actualizado


def test_performance_to_serie_rentabilidad():
    out = {}
    data = {"performance": [
        {"periodo": "2024", "clase": "I EUR", "rentabilidad_pct": 12.3, "benchmark_pct": 10.1},
        {"periodo": "2023", "clase": "I EUR", "rentabilidad_pct": -4.0, "benchmark_pct": -3.0},
        {"periodo": "n/a", "clase": "I EUR", "rentabilidad_pct": 1},   # descartado
    ]}
    merge_ar_statistics_performance(data, out)
    sr = out["cuantitativo"]["serie_rentabilidad"]
    assert len(sr) == 2
    assert {e["periodo"] for e in sr} == {"2024", "2023"}


def test_performance_dict_form_ignored():
    # FACTSHEET performance es dict {ytd_pct,...} -> NO debe romper ni crear serie
    out = {}
    merge_ar_statistics_performance({"performance": {"ytd_pct": 5.0}}, out)
    assert "cuantitativo" not in out or not out.get("cuantitativo", {}).get("serie_rentabilidad")


def test_empty_noop():
    out = {}
    merge_ar_statistics_performance({}, out)
    assert out == {}
