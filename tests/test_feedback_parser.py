"""Tests T3.2 (2026-05-28): tools/feedback_parser.py."""
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.feedback_parser import (
    parse_feedback,
    extract_urls_from_text,
    _validate_items,
    _parse_via_fallback,
)


# ════════════════════════════════════════════════════════════════════
# extract_urls_from_text
# ════════════════════════════════════════════════════════════════════


def test_extract_urls_basic():
    text = "Mira esta entrevista: https://example.com/khemka y este PDF http://x.com/y.pdf"
    urls = extract_urls_from_text(text)
    assert "https://example.com/khemka" in urls
    assert "http://x.com/y.pdf" in urls
    assert len(urls) == 2


def test_extract_urls_handles_trailing_punctuation():
    text = "Ver https://example.com/page (relevante)"
    urls = extract_urls_from_text(text)
    # URL parser puede incluir o no el paréntesis; lo importante es que detecte
    assert any("example.com" in u for u in urls)


def test_extract_urls_empty_text():
    assert extract_urls_from_text("") == []
    assert extract_urls_from_text(None) == []


# ════════════════════════════════════════════════════════════════════
# _parse_via_fallback (sin LLM)
# ════════════════════════════════════════════════════════════════════


def test_fallback_creates_url_items_only():
    items = _parse_via_fallback("", ["https://x.com"])
    assert len(items) == 1
    assert items[0]["action"] == "consultar_fuente"
    assert items[0]["source_urls"] == ["https://x.com"]


def test_fallback_adds_global_revisar_if_text():
    items = _parse_via_fallback(
        "El nombre del fondo está mal, debería ser Ashoka",
        ["https://x.com"]
    )
    # 1 consultar_fuente + 1 revisar global
    assert len(items) == 2
    actions = {it["action"] for it in items}
    assert actions == {"consultar_fuente", "revisar"}


def test_fallback_skips_revisar_if_text_too_short():
    items = _parse_via_fallback("x", ["https://x.com"])
    assert len(items) == 1
    assert items[0]["action"] == "consultar_fuente"


# ════════════════════════════════════════════════════════════════════
# _validate_items (filtros y normalización)
# ════════════════════════════════════════════════════════════════════


def test_validate_filters_invalid_action():
    items = [
        {"target_path": "nombre", "action": "invalid_action", "value": "X"},
        {"target_path": "nombre", "action": "set", "value": "Y"},
    ]
    valid = _validate_items(items, [])
    assert len(valid) == 1
    assert valid[0]["action"] == "set"


def test_validate_filters_invalid_section():
    items = [
        {"target_section": "section_inexistente", "action": "revisar"},
        {"target_section": "resumen", "action": "revisar"},
    ]
    valid = _validate_items(items, [])
    # El primero pierde target_section pero como no tiene target_path tampoco,
    # se filtra (acción revisar sin section es inválida).
    assert len(valid) == 1
    assert valid[0]["target_section"] == "resumen"


def test_validate_consultar_fuente_without_path_or_section_OK():
    items = [
        {"action": "consultar_fuente", "source_urls": ["https://x.com"]},
    ]
    valid = _validate_items(items, [])
    assert len(valid) == 1


def test_validate_adds_orphan_url_as_consultar_fuente():
    items = [
        {"target_path": "nombre", "action": "set", "value": "X", "source_urls": []},
    ]
    valid = _validate_items(items, ["https://orphan.com"])
    # set normal + consultar_fuente orphan
    assert len(valid) == 2
    orphan = [i for i in valid if i["action"] == "consultar_fuente"][0]
    assert orphan["source_urls"] == ["https://orphan.com"]


def test_validate_doesnt_duplicate_used_urls():
    items = [
        {"target_path": "gestores.equipo", "action": "add", "value": "Khemka",
         "source_urls": ["https://x.com"]},
    ]
    valid = _validate_items(items, ["https://x.com"])
    # No se añade consultar_fuente porque la URL ya está usada
    assert len(valid) == 1


def test_validate_normalizes_confidence():
    items = [
        {"target_path": "nombre", "action": "set", "value": "X", "confidence": "ALTA"},
        {"target_path": "nombre", "action": "set", "value": "Y"},  # sin confidence
    ]
    valid = _validate_items(items, [])
    # confidence inválido → fallback a 'medium'
    assert valid[0]["confidence"] == "medium"
    assert valid[1]["confidence"] == "medium"


# ════════════════════════════════════════════════════════════════════
# parse_feedback end-to-end (con Haiku mockeado)
# ════════════════════════════════════════════════════════════════════


def _install_fake_anthropic(monkeypatch, json_payload: str):
    class _Content:
        def __init__(self, t): self.text = t
    class _Resp:
        def __init__(self, t): self.content = [_Content(t)]
    class _Messages:
        def __init__(self, t): self._t = t
        def create(self, **kwargs): return _Resp(self._t)
    class _Client:
        def __init__(self): self.messages = _Messages(json_payload)
    fake = types.ModuleType("anthropic")
    fake.Anthropic = lambda: _Client()
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy")


def test_parse_feedback_haiku_path(monkeypatch):
    payload = json.dumps([
        {"target_path": "nombre", "target_section": None, "action": "set",
         "value": "Ashoka WhiteOak India Opp", "confidence": "high",
         "source_urls": [], "rationale": "usuario menciona el nombre"},
        {"target_path": "gestores.equipo", "target_section": None, "action": "add",
         "value": "Prashant Khemka", "confidence": "high",
         "source_urls": ["https://x.com/y"],
         "rationale": "usuario añade gestor con fuente"},
    ])
    _install_fake_anthropic(monkeypatch, payload)
    result = parse_feedback(
        "El nombre es Ashoka WhiteOak India Opp. Añade Prashant Khemka, mira https://x.com/y",
        raw_urls=["https://x.com/y"],
        isin="IE00BDR0JY05",
        fund_name="Test",
        gestora="WhiteOak",
    )
    assert result["parse_meta"]["method"] == "haiku"
    items = result["structured_items"]
    assert len(items) == 2
    actions = [i["action"] for i in items]
    assert "set" in actions
    assert "add" in actions


def test_parse_feedback_empty_returns_empty():
    result = parse_feedback("", raw_urls=[])
    assert result["parse_meta"]["method"] == "empty"
    assert result["structured_items"] == []


def test_parse_feedback_haiku_strips_markdown_fences(monkeypatch):
    payload = "```json\n" + json.dumps([
        {"target_path": "nombre", "action": "set", "value": "X",
         "confidence": "high", "source_urls": [], "rationale": "x"}
    ]) + "\n```"
    _install_fake_anthropic(monkeypatch, payload)
    result = parse_feedback("nombre es X", isin="ES0123456789")
    assert result["parse_meta"]["method"] == "haiku"
    assert len(result["structured_items"]) == 1


def test_parse_feedback_haiku_failure_falls_back(monkeypatch):
    """Si Haiku raisea, debe caer al fallback heurístico."""
    class _Messages:
        def create(self, **kwargs):
            raise RuntimeError("API down")
    class _Client:
        messages = _Messages()
    fake = types.ModuleType("anthropic")
    fake.Anthropic = lambda: _Client()
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "dummy")

    result = parse_feedback(
        "El nombre es Ashoka",
        raw_urls=["https://x.com"],
        isin="IE00ABC",
    )
    assert result["parse_meta"]["method"] == "fallback"
    assert "haiku failed" in result["parse_meta"]["error"]
    items = result["structured_items"]
    # consultar_fuente (URL) + revisar global (texto>10)
    actions = {i["action"] for i in items}
    assert "consultar_fuente" in actions
    assert "revisar" in actions


def test_parse_feedback_no_api_key_uses_fallback(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    result = parse_feedback("El nombre es X", raw_urls=["https://x.com"])
    assert result["parse_meta"]["method"] == "fallback"
    assert result["parse_meta"]["error"] is not None


def test_parse_feedback_extracts_urls_from_text(monkeypatch):
    """Si el user escribe URLs en el texto sin ponerlas en raw_urls, las pillamos."""
    _install_fake_anthropic(monkeypatch, json.dumps([]))
    result = parse_feedback(
        "Mira https://entrevista.com",
        raw_urls=[],  # ← vacío
        isin="ES0123456789",
    )
    # El validador debe añadir consultar_fuente para la URL del texto
    urls_in_items = {
        u for item in result["structured_items"]
        for u in item.get("source_urls", [])
    }
    assert "https://entrevista.com" in urls_in_items


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
