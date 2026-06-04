"""
Gemini kill switch — desactiva 100% las llamadas a Google Cloud Gemini API.

Motivo: cargo inesperado de €10 en mayo 2026 en Google Cloud.
Decisión: forzar uso exclusivo de Anthropic (Sonnet/Haiku) en todo el pipeline.

Activación: variable de entorno `GEMINI_DISABLED=1` en `.env` (default ON).

Cómo lo usan los wrappers Gemini:

    from tools.gemini_killswitch import is_gemini_disabled, claude_text_fallback, claude_json_fallback

    def _gemini_text(prompt, max_tokens):
        if is_gemini_disabled():
            return claude_text_fallback(prompt, max_tokens)
        # ... resto del código original Gemini

Para reactivar Gemini en el futuro: poner `GEMINI_DISABLED=0` en `.env` y
restaurar `GOOGLE_API_KEY` (comentado en `.env` como referencia).
"""
from __future__ import annotations

import json
import os
from typing import Any

from rich.console import Console

console = Console()

# Modelo Haiku 4.5 — fallback default (rápido y barato)
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Modelo Sonnet — fallback opcional si Haiku no está disponible / falla
_SONNET_MODEL = "claude-sonnet-4-5"


def is_gemini_disabled() -> bool:
    """True si Gemini está desactivado (kill switch ON).

    Default: True (Gemini OFF). Para reactivar setear `GEMINI_DISABLED=0`.
    """
    return os.getenv("GEMINI_DISABLED", "1").lower() in ("1", "true", "yes", "on")


def is_google_cloud_disabled() -> bool:
    """True si TODA la API de Google Cloud está desactivada (alias de seguridad).

    Cubre Gemini Y Custom Search Engine. Default: True (todo OFF).
    Para reactivar setear `GOOGLE_CLOUD_DISABLED=0` en .env.

    Cualquier wrapper futuro que llame APIs Google debe chequear esta función
    al inicio. Si devuelve True → skip / raise / fallback.
    """
    # Si GEMINI_DISABLED=1, también consideramos toda Google Cloud OFF.
    if is_gemini_disabled():
        return True
    return os.getenv("GOOGLE_CLOUD_DISABLED", "1").lower() in ("1", "true", "yes", "on")


def _get_anthropic_client():
    """Lazy init del cliente Anthropic. Cachea en módulo."""
    if not hasattr(_get_anthropic_client, "_client"):
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY no configurada — no se puede usar fallback Claude. "
                "Setea la key en .env o desactiva GEMINI_DISABLED."
            )
        _get_anthropic_client._client = anthropic.Anthropic(
            api_key=api_key, timeout=180.0
        )
    return _get_anthropic_client._client


def claude_text_fallback(prompt: str, max_tokens: int = 8000, retries: int = 2) -> str:
    """Reemplazo de `_gemini_text` usando Haiku 4.5.

    Devuelve string vacío si todos los intentos fallan (mismo contrato que
    `_gemini_text`).
    """
    try:
        import time
        client = _get_anthropic_client()
    except Exception as exc:
        console.log(f"[red][KillSwitch] Anthropic init falló: {exc}")
        return ""

    for attempt in range(retries + 1):
        try:
            resp = client.messages.create(
                model=_HAIKU_MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip() if resp.content else ""
            if not text:
                raise ValueError("Empty Haiku response")
            return text
        except Exception as exc:
            if attempt < retries:
                import time
                time.sleep(3)
            else:
                console.log(f"[red][KillSwitch] Haiku text falló: {str(exc)[:200]}")
                return ""
    return ""


def claude_json_fallback(prompt: str, max_tokens: int = 8000, retries: int = 2) -> dict | None:
    """Reemplazo de `_gemini_call` (JSON mode) usando Haiku 4.5.

    Devuelve dict o None si falla. Mismo contrato que `_gemini_call`.
    """
    try:
        client = _get_anthropic_client()
    except Exception as exc:
        console.log(f"[red][KillSwitch] Anthropic init falló: {exc}")
        return None

    # Forzar JSON añadiendo instrucción explícita al prompt
    json_prompt = (
        prompt + "\n\nIMPORTANTE: Responde SOLO con un objeto JSON válido, "
        "sin markdown, sin texto adicional, sin ```json fences."
    )

    for attempt in range(retries + 1):
        try:
            resp = client.messages.create(
                model=_HAIKU_MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": json_prompt}],
            )
            raw = resp.content[0].text.strip() if resp.content else ""
            if not raw:
                raise ValueError("Empty Haiku response")
            # Strip code fences si los hay
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.startswith("json"):
                    raw = raw[4:].strip()
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                # Reparación simple
                import re
                m = re.search(r"\{[\s\S]+\}", raw)
                if m:
                    try:
                        return json.loads(m.group(0))
                    except json.JSONDecodeError:
                        pass
                if attempt < retries:
                    import time
                    time.sleep(2)
                else:
                    console.log(f"[red][KillSwitch] Haiku JSON inválido tras {retries+1} intentos")
                    return None
        except Exception as exc:
            if attempt < retries:
                import time
                time.sleep(3)
            else:
                console.log(f"[red][KillSwitch] Haiku JSON falló: {str(exc)[:200]}")
                return None
    return None


def claude_json_fallback_with_schema(
    text: str, schema: dict, context: str = "", max_tokens: int = 8000
) -> Any:
    """Reemplazo de `extract_fast` (gemini_wrapper) usando Haiku 4.5.

    Mismo contrato: extrae JSON según schema. Devuelve dict|list o lanza
    ValueError si JSON inválido (idéntico a Gemini wrapper).
    """
    prompt = (
        f"Extrae la información del texto siguiendo EXACTAMENTE el schema JSON.\n"
        f"Si un campo no aparece, usa null. Devuelve SOLO el JSON, sin explicaciones.\n\n"
        f"CONTEXTO: {context}\n\n"
        f"SCHEMA:\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n\n"
        f"TEXTO:\n{text[:800_000]}"
    )
    result = claude_json_fallback(prompt, max_tokens=max_tokens, retries=2)
    if result is None:
        raise ValueError("Claude fallback (Haiku) devolvió None — no se pudo extraer JSON")
    return result
