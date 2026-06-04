"""
Gemini Flash 2.0 wrapper para extracción estructurada JSON.

Usa el SDK moderno `google.genai`. Configurado para responder en JSON estricto.

Firma:
    extract_fast(text: str, schema: dict, context: str = "") -> dict | list

Se usa desde `tools/llm_dispatcher.py` cuando mode="fast". Apto para extracciones
directas (AUM, TER, holdings, mix porcentuales, fechas). NO usar para síntesis
narrativa (eso va a Claude via claude_extractor.py).
"""
from __future__ import annotations

import json
import os
from typing import Any

from rich.console import Console

from tools.claude_extractor import _parse_json_response

console = Console()

MODEL_FLASH = "gemini-2.5-flash"
MODEL_PRO = "gemini-2.5-pro"
_MODEL = MODEL_FLASH  # default back-compat

# Lazy client (evita fallar si falta API key y no se usa)
_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        from dotenv import load_dotenv
        from pathlib import Path
        load_dotenv(Path(__file__).parent.parent / ".env")
        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY no configurada en .env")
    from google import genai
    _client = genai.Client(api_key=api_key)
    return _client


def extract_fast(
    text: str,
    schema: dict,
    context: str = "",
    model: str = MODEL_FLASH,
    custom_prompt: str | None = None,
    max_chars: int = 800_000,
) -> Any:
    """
    Extrae JSON según schema usando Gemini.

    Args:
        text: texto del documento
        schema: dict que describe la estructura esperada
        context: info adicional que el llamador quiera pasar
        model: MODEL_FLASH (default, ~$0.15/MTok) o MODEL_PRO (~$1.25/MTok,
               mejor reasoning para tareas de mapeo semántico largo)
        custom_prompt: si el caller quiere reemplazar la instrucción base por
                       una propia (el mapper la usa). Si None, usa la
                       instrucción estándar de extracción por schema.
        max_chars: cap defensivo del texto enviado. Flash soporta 1M tokens
                   (~4M chars); Pro idem. Default 800k cubre ARs grandes.

    Returns:
        dict o list según el schema. ValueError si JSON inválido.
    """
    # Kill switch (2026-05-28): si GEMINI_DISABLED=1, redirigir a Claude Haiku.
    from tools.gemini_killswitch import is_gemini_disabled, claude_json_fallback_with_schema
    if is_gemini_disabled():
        console.log(f"[cyan][KillSwitch] Gemini OFF → Claude Haiku ({context[:40]}...)")
        return claude_json_fallback_with_schema(text, schema, context=context, max_tokens=32768)

    client = _get_client()

    if custom_prompt is not None:
        # El caller aporta el prompt completo (el mapper pasa su propia
        # instrucción que describe la taxonomía entera); schema solo se
        # adjunta como indicación de formato de respuesta.
        prompt = (
            f"{custom_prompt}\n\n"
            f"SCHEMA de respuesta (estructura JSON esperada):\n"
            f"{json.dumps(schema, ensure_ascii=False, indent=2)}\n\n"
            f"TEXTO DEL DOCUMENTO:\n{text[:max_chars]}"
        )
    else:
        prompt = (
            f"Extrae la información del texto siguiendo EXACTAMENTE el schema JSON.\n"
            f"Si un campo no aparece, usa null. Devuelve SOLO el JSON, sin explicaciones.\n\n"
            f"CONTEXTO: {context}\n\n"
            f"SCHEMA:\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n\n"
            f"TEXTO:\n{text[:max_chars]}"
        )

    try:
        resp = client.models.generate_content(
            model=model,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
                # Subir output budget: algunos extractores (cartera completa)
                # pueden devolver 50-100 holdings o series largas. Pro/Flash
                # soportan output hasta ~65k tokens.
                "max_output_tokens": 32768,
            },
        )
        # Cost-Opt Fase 2 (2026-05-02): instrumentar coste del extractor.
        # ISIN se infiere del environment (orchestrator lo pone en ENV antes
        # de llamar al pipeline INT). Si no hay ISIN → log con isin="?".
        try:
            import os as _os
            from tools.cost_tracker import track as _track
            isin = _os.environ.get("CURRENT_FUND_ISIN", "?")
            inp = getattr(getattr(resp, "usage_metadata", None), "prompt_token_count", 0) or 0
            out = getattr(getattr(resp, "usage_metadata", None), "candidates_token_count", 0) or 0
            if inp or out:
                _track(isin, model, inp, out, agent="gemini_wrapper")
        except Exception:
            pass
    except Exception as e:
        # Refactor L2 (2026-05-05): NO fallback a Anthropic API. Si Gemini
        # falla, lanza limpio. La rama cowork (modo default) no llama a este
        # wrapper. La rama legacy (--api-fallback) requiere Gemini real.
        console.log(f"[yellow]Gemini ({model}) request error: {e}")
        raise ValueError(f"Gemini request failed: {e}") from e

    raw = getattr(resp, "text", None) or ""
    if not raw.strip():
        raise ValueError("Gemini devolvió respuesta vacía")

    try:
        return _parse_json_response(raw)
    except Exception as e:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise ValueError(f"Gemini JSON inválido: {e}") from e


def extract_with_pro(
    text: str,
    schema: dict,
    context: str = "",
    custom_prompt: str | None = None,
) -> Any:
    """Shortcut para llamadas con Gemini 2.5 Pro (reasoning más fuerte)."""
    return extract_fast(
        text=text, schema=schema, context=context,
        model=MODEL_PRO, custom_prompt=custom_prompt,
    )
