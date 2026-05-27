"""
Tests T2.5 (branch v2-cowork, 2026-05-27): tools/name_recovery.py.

Cubre:
  - needs_recovery(): detecta nombre == ISIN, vacío, o demasiado corto.
  - _extract_via_regex(): 4 patrones (el fondo X es..., X es un fondo,
    X (ISIN), **X**).
  - recover_name_if_needed(): pipeline end-to-end con Haiku mockeado.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.name_recovery import (
    needs_recovery,
    _extract_via_regex,
    recover_name_if_needed,
)


# ════════════════════════════════════════════════════════════════════
# needs_recovery
# ════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("nombre,isin,expected", [
    ("IE00BDR0JY05", "IE00BDR0JY05", True),       # == ISIN
    ("ie00bdr0jy05", "IE00BDR0JY05", True),       # case-insensitive
    ("", "IE00BDR0JY05", True),                    # vacío
    ("  ", "IE00BDR0JY05", True),                  # whitespace
    ("ABC", "IE00BDR0JY05", True),                 # demasiado corto
    ("LU0123456789", "IE00BDR0JY05", True),       # otro ISIN
    ("Sextant Quality Focus", "IE00BDR0JY05", False),
    ("Magallanes European Equity FI", "ES0159259011", False),
    ("DNCA Invest Alpha Bonds", "LU1694789451", False),
])
def test_needs_recovery(nombre, isin, expected):
    assert needs_recovery(nombre, isin) == expected


def test_needs_recovery_no_isin():
    """Sin ISIN no podemos determinar."""
    assert needs_recovery("Cualquier nombre", "") is False


# ════════════════════════════════════════════════════════════════════
# _extract_via_regex
# ════════════════════════════════════════════════════════════════════


def test_regex_pattern_el_fondo_x_es():
    texts = [
        "El fondo Magallanes European Equity es un vehículo UCITS lanzado en 2014."
    ]
    result = _extract_via_regex(texts, "ES0159259011")
    assert result is not None
    assert "Magallanes" in result


def test_regex_pattern_x_es_un_fondo():
    texts = [
        "Sextant Quality Focus es un sub-fondo de la SICAV Sextant…"
    ]
    result = _extract_via_regex(texts, "FR001400CEK6")
    assert result is not None
    assert "Sextant" in result


def test_regex_pattern_x_paren_isin():
    texts = [
        "El análisis cubre DNCA Invest Alpha Bonds (LU1694789451), un fondo "
        "luxemburgués registrado en CSSF."
    ]
    result = _extract_via_regex(texts, "LU1694789451")
    assert result is not None
    assert "DNCA Invest Alpha Bonds" in result


def test_regex_pattern_markdown_bold():
    texts = [
        "**Cobas Selección FI** es la estrategia value insignia de Cobas AM. "
        "Iván Martín lidera el equipo."
    ]
    result = _extract_via_regex(texts, "ES0119207001")
    assert result is not None
    assert "Cobas" in result


def test_regex_ignores_isin_lookalike():
    """No devolver candidatos que sean a su vez un ISIN."""
    texts = [
        "El fondo IE00BDR0JY05 es muy bueno (no debería matchear)."
    ]
    result = _extract_via_regex(texts, "IE00BDR0JY05")
    assert result is None or "BDR" not in result


def test_regex_ignores_target_isin():
    texts = ["El fondo XYZ123 es bla bla bla"]
    result = _extract_via_regex(texts, "XYZ123ABC123")
    # XYZ123 es demasiado corto (<5 si quitamos) — debería rechazarse o no matchear
    # No es validación estricta, solo aseguramos que no devuelve el ISIN
    if result:
        assert "XYZ123ABC123" not in result


def test_regex_no_match_returns_none():
    texts = ["Texto sin ningún patrón reconocible aquí dentro."]
    result = _extract_via_regex(texts, "ES0123456789")
    assert result is None


def test_regex_picks_most_frequent_candidate():
    """Si el nombre aparece varias veces, debe ser el preferido."""
    texts = [
        "**Sextant Quality Focus** es un fondo de calidad. "
        "El fondo Sextant Quality Focus invierte en compañías sólidas. "
        "Sextant Quality Focus tiene un track-record de 10 años."
    ]
    result = _extract_via_regex(texts, "FR001400CEK6")
    assert result is not None
    assert "Sextant Quality Focus" in result


# ════════════════════════════════════════════════════════════════════
# recover_name_if_needed (end-to-end)
# ════════════════════════════════════════════════════════════════════


def test_recover_skips_when_nombre_ok():
    output_data = {
        "isin": "FR001400CEK6",
        "nombre": "Sextant Quality Focus",
        "analyst_synthesis": {},
    }
    result = recover_name_if_needed(output_data, "FR001400CEK6")
    assert result["applied"] is False
    assert result["reason"] == "nombre_ok"
    # nombre no se toca
    assert output_data["nombre"] == "Sextant Quality Focus"


def test_recover_applies_via_regex(monkeypatch):
    """Caso real reportado: nombre == ISIN, analyst_synthesis con el nombre."""
    output_data = {
        "isin": "IE00BDR0JY05",
        "nombre": "IE00BDR0JY05",
        "analyst_synthesis": {
            "resumen": {
                "texto": "**JPM Income Fund** es un fondo de renta fija "
                         "global gestionado por JPMorgan AM."
            },
            "historia": {
                "texto": "JPM Income Fund fue lanzado en 2017 y desde "
                         "entonces ha acumulado ~1.7B M€ de AUM."
            },
        },
    }
    result = recover_name_if_needed(output_data, "IE00BDR0JY05")
    assert result["applied"] is True
    assert result["method"] == "regex"
    assert "JPM Income Fund" in result["to"]
    assert output_data["nombre"] == result["to"]
    # Marcado como manual edit
    assert "nombre" in (output_data.get("_manual_edits") or [])


def test_recover_via_haiku_fallback(monkeypatch):
    """Si regex no encuentra, Haiku se llama y devuelve un candidato."""
    output_data = {
        "isin": "LU0123456789",
        "nombre": "LU0123456789",
        "gestora": "Test Gestora",
        "analyst_synthesis": {
            "resumen": {
                "texto": ("Texto largo sin patrones regex pero con info "
                          "suficiente para que Haiku deduzca. " * 5)
            },
        },
    }
    # Force ANTHROPIC_API_KEY for the check inside _extract_via_haiku
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy")

    # Mock anthropic.Anthropic
    class _MockContent:
        text = "Mi Fondo Test"

    class _MockResp:
        content = [_MockContent()]

    class _MockMessages:
        def create(self, **kwargs):
            return _MockResp()

    class _MockClient:
        messages = _MockMessages()

    import types
    fake_anthropic = types.ModuleType("anthropic")
    fake_anthropic.Anthropic = lambda: _MockClient()
    monkeypatch.setitem(sys.modules, "anthropic", fake_anthropic)

    result = recover_name_if_needed(output_data, "LU0123456789")
    assert result["applied"] is True
    assert result["method"] == "haiku"
    assert result["to"] == "Mi Fondo Test"
    assert output_data["nombre"] == "Mi Fondo Test"


def test_recover_no_match_returns_unchanged(monkeypatch):
    """Si ni regex ni Haiku encuentran, no modificar."""
    output_data = {
        "isin": "ZZ0123456789",
        "nombre": "ZZ0123456789",
        "analyst_synthesis": {
            "resumen": {"texto": "lorem ipsum dolor sit amet " * 30}
        },
    }
    # Sin ANTHROPIC_API_KEY → haiku no se llama
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    result = recover_name_if_needed(output_data, "ZZ0123456789")
    assert result["applied"] is False
    assert result["reason"] == "no_match"
    assert output_data["nombre"] == "ZZ0123456789"
    assert "nombre" not in (output_data.get("_manual_edits") or [])


def test_recover_haiku_returns_unknown(monkeypatch):
    output_data = {
        "isin": "LU9999999999",
        "nombre": "",
        "analyst_synthesis": {
            "resumen": {"texto": "texto sin info clara " * 30},
        },
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy")

    class _MockContent: text = "UNKNOWN"
    class _MockResp: content = [_MockContent()]
    class _MockMessages:
        def create(self, **kwargs): return _MockResp()
    class _MockClient: messages = _MockMessages()
    import types
    fake = types.ModuleType("anthropic")
    fake.Anthropic = lambda: _MockClient()
    monkeypatch.setitem(sys.modules, "anthropic", fake)

    result = recover_name_if_needed(output_data, "LU9999999999")
    assert result["applied"] is False


def test_recover_logger_invoked(monkeypatch):
    """El log_fn se llama si se pasa."""
    output_data = {
        "isin": "IE00ABC",
        "nombre": "IE00ABC",
        "analyst_synthesis": {
            "resumen": {"texto": "**Test Fund Name** es un fondo lorem ipsum."},
        },
    }
    log_calls = []
    def log_fn(agent, level, msg):
        log_calls.append((agent, level, msg))
    result = recover_name_if_needed(output_data, "IE00ABC", log_fn=log_fn)
    assert result["applied"] is True
    # Al menos un log de NAME_RECOVERY (INFO + OK)
    assert any(c[0] == "NAME_RECOVERY" for c in log_calls)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
