"""Tests para tools/analysis_quality.py (gate de calidad del analyst_synthesis)."""

import pytest

from tools.analysis_quality import assess_analysis_quality, MIN_SECTION_CHARS

_LONG = "x" * (MIN_SECTION_CHARS + 10)


def _full_synthesis():
    """6 secciones con contenido real → análisis sano."""
    return {
        "analyst_synthesis": {
            "resumen": {"texto": _LONG},
            "historia": {"texto": _LONG},
            "gestores": {"texto": _LONG, "perfiles": [{"nombre": "Fulano"}]},
            "evolucion": {"texto": _LONG},
            "estrategia": {"texto": _LONG},
            "cartera": {"texto": _LONG},
        }
    }


def test_full_synthesis_ok():
    r = assess_analysis_quality(_full_synthesis())
    assert r["ok"] is True
    assert r["blockers"] == []
    assert r["sections_with_content"] == 6


def test_missing_synthesis_blocks():
    r = assess_analysis_quality({})
    assert r["ok"] is False
    assert any("vacío o ausente" in b for b in r["blockers"])


def test_empty_synthesis_blocks():
    r = assess_analysis_quality({"analyst_synthesis": {}})
    assert r["ok"] is False


def test_all_sections_empty_blocks():
    data = {"analyst_synthesis": {"resumen": {"texto": ""}, "historia": {"texto": "corto"}}}
    r = assess_analysis_quality(data)
    assert r["ok"] is False
    assert any("sección" in b for b in r["blockers"])


def test_hallucination_blocks():
    data = _full_synthesis()
    data["analyst_synthesis"]["resumen"]["texto"] = (
        "Dado que los datos específicos del fondo no han sido proporcionados, "
        "las cifras son ilustrativas y representativas. " + _LONG
    )
    r = assess_analysis_quality(data)
    assert r["ok"] is False
    assert "resumen" in r["hallucinated_sections"]


def test_hallucination_markers_in_raw_text_do_not_block():
    """Una sola palabra suelta ('a modo de ejemplo') NO debe bloquear:
    aparece legítimamente en cartas/análisis de fondos sanos."""
    data = _full_synthesis()
    data["analyst_synthesis"]["estrategia"]["texto"] = (
        "El gestor cita, a modo de ejemplo, su posición en Inditex. " + _LONG
    )
    r = assess_analysis_quality(data)
    assert r["ok"] is True


def test_gestores_empty_is_warning_not_blocker():
    """COBAS RENTA: 5/6 secciones llenas pero gestores vacío → publicable con aviso."""
    data = _full_synthesis()
    data["analyst_synthesis"]["gestores"] = {"texto": "", "perfiles": []}
    r = assess_analysis_quality(data)
    assert r["ok"] is True
    assert any("gestores" in w for w in r["warnings"])


def test_partial_two_sections_is_warning():
    data = {
        "analyst_synthesis": {
            "resumen": {"texto": _LONG},
            "estrategia": {"texto": _LONG},
        }
    }
    r = assess_analysis_quality(data)
    assert r["ok"] is True
    assert r["sections_with_content"] == 2
    assert any("parcial" in w for w in r["warnings"])


def test_gestores_counts_via_perfiles_only():
    data = _full_synthesis()
    data["analyst_synthesis"]["gestores"] = {"texto": "", "perfiles": [{"nombre": "A"}]}
    r = assess_analysis_quality(data)
    assert r["sections_with_content"] == 6
