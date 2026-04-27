"""
LLM dispatcher: encamina extracción a Gemini Flash (fast) o Claude Sonnet
(reasoning), con cache on-disk y fallback automático.

Uso:
    from tools.llm_dispatcher import llm_extract
    out = llm_extract(text, schema, mode="fast", context="...", cache_dir=Path("..."))

- mode="fast":     Gemini 2.5 Flash. Barato. Tablas, KPIs, holdings, mix.
- mode="reasoning": Claude Sonnet 4.5. Narrativa, tesis, consistencia, síntesis.

Cache: key = sha256(mode + text_hash + schema) → guarda JSON en cache_dir.
Fallback: si mode=fast falla (JSON inválido o error API), reintenta con reasoning.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from rich.console import Console

console = Console()


def _cache_key(mode: str, text: str, schema: dict) -> str:
    text_hash = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:16]
    schema_str = json.dumps(schema, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(f"{mode}|{text_hash}|{schema_str}".encode()).hexdigest()[:24]


def llm_extract(
    text: str,
    schema: dict,
    mode: Literal["fast", "reasoning"] = "fast",
    context: str = "",
    cache_dir: Path | None = None,
) -> Any:
    """
    Extrae JSON siguiendo `schema`. Cachea por (mode, text, schema).
    Si mode='fast' falla, fallback a reasoning.
    """
    if not text or not text.strip():
        return None

    key = _cache_key(mode, text, schema)
    cache_file = (cache_dir / f"{key}.json") if cache_dir else None

    if cache_file and cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass  # cache corrupto → reextract

    # Ambos modos usan Gemini como primario. Claude solo si Gemini falla Y
    # hay credit de Anthropic (fallback silencioso si no).
    out: Any = None
    try:
        from tools.gemini_wrapper import extract_fast
        out = extract_fast(text, schema, context=context)
    except (ValueError, json.JSONDecodeError) as e:
        console.log(f"[yellow]Gemini falló ({e}); intentando Claude fallback")
        try:
            from tools.claude_extractor import extract_structured_data
            out = extract_structured_data(text, schema, context=context)
        except Exception as e2:
            console.log(f"[red]Claude fallback también falló: {e2}")
            out = None
    except Exception as e:
        console.log(f"[red]LLM dispatcher error: {e}")
        out = None

    if out is not None and cache_file:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    return out
