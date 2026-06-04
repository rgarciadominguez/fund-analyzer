"""
Unified LLM extractor — Gemini first, Claude as fallback.

Provides two functions:
  - extract_structured(text, schema) → dict (JSON mode)
  - extract_text(prompt) → str (free text mode)

Provider selection:
  - "auto" (default): Gemini → Claude fallback
  - "gemini": only Gemini
  - "claude": only Claude
"""
import json
import os
import re
import time

from rich.console import Console

console = Console()

GEMINI_MODEL = "gemini-2.5-flash"
CLAUDE_MODEL = "claude-sonnet-4-5"


# ═══════════════════════════════════════════════════════════════
# JSON repair (from analyst_agent, reused)
# ═══════════════════════════════════════════════════════════════

def _repair_json(raw: str) -> dict | None:
    """Try to repair truncated/malformed JSON."""
    # Strip markdown fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    # Try extracting first { ... } block
    m = re.search(r"\{[\s\S]+\}", cleaned)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # Try adding closing braces
    for suffix in ["}", "}}", "]}}", '"]}']:
        try:
            return json.loads(cleaned + suffix)
        except json.JSONDecodeError:
            continue
    return None


# ═══════════════════════════════════════════════════════════════
# Gemini
# ═══════════════════════════════════════════════════════════════

_gemini_client = None


def _get_gemini():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))
    return _gemini_client


def _gemini_structured(prompt: str, max_tokens: int = 4000, retries: int = 2) -> dict | None:
    # Kill switch (2026-05-28): si GEMINI_DISABLED=1, devuelve None para que
    # el fallback Claude en extract_structured() se active automáticamente.
    from tools.gemini_killswitch import is_gemini_disabled
    if is_gemini_disabled():
        console.log("[cyan][KillSwitch] Gemini structured OFF → Claude fallback (auto)")
        return None

    from google.genai import types
    client = _get_gemini()
    for attempt in range(retries + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                    max_output_tokens=max_tokens,
                ),
            )
            raw = resp.text.strip() if resp.text else ""
            if not raw:
                raise ValueError("Empty Gemini response")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                repaired = _repair_json(raw)
                if repaired:
                    return repaired
                raise
        except Exception as exc:
            if "429" in str(exc) or "ResourceExhausted" in str(exc):
                wait = 30 * (attempt + 1)
                console.log(f"[yellow][LLM] Gemini rate limit — waiting {wait}s")
                time.sleep(wait)
            elif attempt < retries:
                time.sleep(3)
            else:
                console.log(f"[red][LLM] Gemini structured failed: {str(exc)[:150]}")
                return None
    return None


def _gemini_text(prompt: str, max_tokens: int = 8000, retries: int = 2) -> str:
    # Kill switch (2026-05-28): si GEMINI_DISABLED=1, devuelve "" para que
    # el fallback Claude en extract_text() se active automáticamente.
    from tools.gemini_killswitch import is_gemini_disabled
    if is_gemini_disabled():
        console.log("[cyan][KillSwitch] Gemini text OFF → Claude fallback (auto)")
        return ""

    from google.genai import types
    client = _get_gemini()
    for attempt in range(retries + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    max_output_tokens=max_tokens,
                ),
            )
            text = resp.text.strip() if resp.text else ""
            if not text:
                raise ValueError("Empty Gemini response")
            return text
        except Exception as exc:
            if "429" in str(exc) or "ResourceExhausted" in str(exc):
                wait = 30 * (attempt + 1)
                console.log(f"[yellow][LLM] Gemini rate limit — waiting {wait}s")
                time.sleep(wait)
            elif attempt < retries:
                time.sleep(3)
            else:
                console.log(f"[red][LLM] Gemini text failed: {str(exc)[:150]}")
                return ""
    return ""


# ═══════════════════════════════════════════════════════════════
# Claude
# ═══════════════════════════════════════════════════════════════

def _claude_available() -> bool:
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def _claude_structured(prompt: str, max_tokens: int = 4000) -> dict | None:
    if not _claude_available():
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            repaired = _repair_json(raw)
            if repaired:
                return repaired
            return None
    except Exception as exc:
        console.log(f"[red][LLM] Claude structured failed: {str(exc)[:150]}")
        return None


def _claude_text(prompt: str, max_tokens: int = 8000) -> str:
    if not _claude_available():
        return ""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as exc:
        console.log(f"[red][LLM] Claude text failed: {str(exc)[:150]}")
        return ""


# ═══════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════

def extract_structured(
    text: str,
    schema: dict | str,
    context: str = "",
    provider: str = "auto",
    max_tokens: int = 4000,
) -> dict | None:
    """Extract structured data from text using LLM.

    Args:
        text: Source text to extract from
        schema: JSON schema (dict) or description (str) of expected output
        context: Additional context for the LLM
        provider: "auto" (Gemini→Claude), "gemini", or "claude"
        max_tokens: Max output tokens

    Returns:
        Parsed dict, or None if all providers fail
    """
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2) if isinstance(schema, dict) else schema
    prompt = (
        f"Extrae datos estructurados del siguiente texto. Responde SOLO JSON.\n"
        f"Schema esperado:\n{schema_str}\n"
        f"{f'Contexto: {context}' if context else ''}\n\n"
        f"TEXTO:\n{text[:12000]}"
    )

    if provider in ("auto", "gemini"):
        result = _gemini_structured(prompt, max_tokens)
        if result:
            return result

    if provider in ("auto", "claude"):
        result = _claude_structured(prompt, max_tokens)
        if result:
            return result

    return None


def extract_text(
    prompt: str,
    max_tokens: int = 8000,
    provider: str = "auto",
) -> str:
    """Generate free text using LLM.

    Args:
        prompt: Full prompt for the LLM
        provider: "auto" (Gemini→Claude), "gemini", or "claude"
        max_tokens: Max output tokens

    Returns:
        Generated text, or "" if all providers fail
    """
    if provider in ("auto", "gemini"):
        result = _gemini_text(prompt, max_tokens)
        if result:
            return result

    if provider in ("auto", "claude"):
        result = _claude_text(prompt, max_tokens)
        if result:
            return result

    return ""
