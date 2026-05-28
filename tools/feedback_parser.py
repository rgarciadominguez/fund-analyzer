"""Parser de feedback humano libre → structured items (T3.2, 2026-05-28).

Toma el texto libre que el usuario escribe en el modal "📝 Mejorar este
análisis" + opcional lista de URLs, y devuelve una lista de items
estructurados que el orchestrator puede aplicar al próximo re-run.

Estrategia:
  1. Haiku 4.5 con prompt detallado y vocabulario controlado (preferido).
  2. Fallback heurístico si no hay ANTHROPIC_API_KEY o la API falla
     (genera consultar_fuente per URL + 1 item "revisar" global).

Coste estimado: ~$0.001 por feedback (Haiku, ~300 input + 400 output tokens).

API pública:
    from tools.feedback_parser import parse_feedback
    result = parse_feedback(raw_text, raw_urls, isin, fund_name, gestora)
    # → {"structured_items": [...], "parse_meta": {"method": "haiku"|"fallback", ...}}
"""
from __future__ import annotations

import json
import os
import re
from typing import Iterable

# ════════════════════════════════════════════════════════════════════
# Vocabularios controlados
# ════════════════════════════════════════════════════════════════════

VALID_ACTIONS = {"set", "add", "replace", "revisar", "consultar_fuente"}
VALID_SECTIONS = {
    "resumen", "historia", "gestores", "evolucion",
    "estrategia", "cartera", "fuentes_externas", "documentos",
}
VALID_CONFIDENCES = {"high", "medium", "low"}

URL_REGEX = re.compile(r"https?://[^\s\"'<>]+")


def extract_urls_from_text(text: str) -> list[str]:
    """Extrae URLs http(s) del texto libre."""
    if not text:
        return []
    return URL_REGEX.findall(text)


# ════════════════════════════════════════════════════════════════════
# API pública
# ════════════════════════════════════════════════════════════════════


def parse_feedback(
    raw_text: str,
    raw_urls: Iterable[str] = (),
    isin: str = "",
    fund_name: str = "",
    gestora: str = "",
) -> dict:
    """Estructura el texto libre del usuario en items aplicables.

    Args:
        raw_text: texto libre del usuario describiendo qué mejorar.
        raw_urls: URLs adicionales del input (campo separado del modal).
        isin/fund_name/gestora: contexto para el parser.

    Returns:
        {
          "structured_items": [...],
          "parse_meta": {
            "method": "haiku" | "fallback" | "empty",
            "n_items": int,
            "error": str | None,    # solo si fallback por error
          }
        }
    """
    raw_text = (raw_text or "").strip()
    raw_urls_list = [u.strip() for u in (raw_urls or []) if u and u.strip()]

    urls_in_text = extract_urls_from_text(raw_text)
    all_urls = list({*raw_urls_list, *urls_in_text})

    if not raw_text and not all_urls:
        return {
            "structured_items": [],
            "parse_meta": {"method": "empty", "n_items": 0, "error": None},
        }

    # Intentar Haiku
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            items = _parse_via_haiku(raw_text, all_urls, isin, fund_name, gestora)
            items = _validate_items(items, all_urls)
            return {
                "structured_items": items,
                "parse_meta": {"method": "haiku", "n_items": len(items), "error": None},
            }
        except Exception as e:
            err = str(e)[:200]
            items = _parse_via_fallback(raw_text, all_urls)
            return {
                "structured_items": items,
                "parse_meta": {
                    "method": "fallback", "n_items": len(items),
                    "error": f"haiku failed: {err}",
                },
            }

    # No hay API key
    items = _parse_via_fallback(raw_text, all_urls)
    return {
        "structured_items": items,
        "parse_meta": {
            "method": "fallback", "n_items": len(items),
            "error": "ANTHROPIC_API_KEY no configurada",
        },
    }


# ════════════════════════════════════════════════════════════════════
# Haiku parser
# ════════════════════════════════════════════════════════════════════


_PROMPT_TEMPLATE = """Fondo: {fund_name} (ISIN: {isin}, gestora: {gestora})

Feedback del usuario sobre el análisis:
\"\"\"
{raw_text}
\"\"\"

URLs proporcionadas: {urls_str}

Tarea: extraer items estructurados que el sistema pueda aplicar al próximo análisis.

Cada item es un OBJETO con:
- target_path: campo concreto en output.json (ej. "nombre", "gestora", "kpis.aum_actual_meur", "kpis.ter_pct", "gestores.equipo") o null si es sobre una sección narrativa.
- target_section: sección del analyst (una de: resumen, historia, gestores, evolucion, estrategia, cartera, fuentes_externas, documentos) o null si es campo concreto.
- action:
  * "set" → reemplazar un valor concreto (ej. nombre, gestora, kpi único)
  * "add" → añadir un elemento a una lista (ej. añadir un gestor que falta)
  * "replace" → reemplazar una lista entera (ej. lista de gestores completa nueva)
  * "revisar" → la sección/campo está mal o falta detalle, pero el user no da valor concreto. Requiere que el analyst la re-analice considerando el rationale.
  * "consultar_fuente" → el user proporciona una URL como fuente extra a consultar. target_path/section = null. source_urls = [esa URL].
- value: SOLO si el user dice el valor exacto. null en otro caso.
- confidence: "high" si el user es explícito y específico, "medium" si es ambiguo o requiere interpretación, "low" si poco claro o demasiado vago.
- source_urls: URLs relacionadas con este item específico (subconjunto de las URLs proporcionadas).
- rationale: una frase breve explicando qué dice el user y por qué se mapea así.

REGLAS:
1. Un solo feedback puede generar VARIOS items (ej. "el nombre es X y faltan gestores" → 2 items).
2. Para CADA URL proporcionada, crear al menos un item que la incluya en source_urls (asignada a un campo concreto o a un consultar_fuente genérico).
3. Si el user dice "el resumen no menciona X" → action=revisar, target_section="resumen", value=null, rationale="usuario dice que falta X en el resumen".
4. Si dice "los gestores son A y B" (lista completa) → action=replace, target_path="gestores.equipo", value=["A","B"].
5. Si dice "añade gestor C" o "falta el gestor C" → action=add, target_path="gestores.equipo", value="C".
6. Si dice "el AUM debería ser 350 M" → action=set, target_path="kpis.aum_actual_meur", value=350.
7. NO inventes: si el user no dice el valor, action=revisar con value=null.

Devuelve SOLO un JSON array de items, sin texto adicional ni markdown:

[
  {{"target_path": ..., "target_section": ..., "action": ..., "value": ..., "confidence": ..., "source_urls": [...], "rationale": ...}},
  ...
]
"""


def _parse_via_haiku(
    raw_text: str, urls: list[str], isin: str, fund_name: str, gestora: str,
) -> list[dict]:
    """Llama a Haiku con el prompt estructurado. Lanza excepción si falla."""
    from anthropic import Anthropic
    client = Anthropic()

    urls_str = ", ".join(urls) if urls else "(ninguna)"
    prompt = _PROMPT_TEMPLATE.format(
        fund_name=fund_name or "?",
        isin=isin or "?",
        gestora=gestora or "?",
        raw_text=raw_text,
        urls_str=urls_str,
    )

    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = (resp.content[0].text or "").strip()

    # Strip markdown fences si las hubiera
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1] if "```" in raw[3:] else raw[3:]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    items = json.loads(raw)
    if not isinstance(items, list):
        raise ValueError(f"Haiku no devolvió un array: {type(items).__name__}")
    return items


# ════════════════════════════════════════════════════════════════════
# Validación / normalización
# ════════════════════════════════════════════════════════════════════


def _validate_items(items: list, urls_input: list[str]) -> list[dict]:
    """Filtra items que no cumplen schema mínimo. Añade URLs orphan como
    consultar_fuente si no aparecen en ningún source_urls."""
    valid: list[dict] = []
    urls_used: set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        action = (it.get("action") or "").lower().strip()
        if action not in VALID_ACTIONS:
            continue
        target_section = it.get("target_section")
        if target_section and target_section not in VALID_SECTIONS:
            target_section = None
        target_path = it.get("target_path")
        if isinstance(target_path, str):
            target_path = target_path.strip() or None
        # OK si tiene path, section, o action=consultar_fuente
        if not target_path and not target_section and action != "consultar_fuente":
            continue
        confidence = (it.get("confidence") or "medium").lower()
        if confidence not in VALID_CONFIDENCES:
            confidence = "medium"
        srcs = it.get("source_urls") or []
        if not isinstance(srcs, list):
            srcs = []
        srcs = [str(u).strip() for u in srcs if u and str(u).strip()]
        for u in srcs:
            urls_used.add(u)
        valid.append({
            "target_path": target_path,
            "target_section": target_section,
            "action": action,
            "value": it.get("value"),
            "confidence": confidence,
            "source_urls": srcs,
            "rationale": str(it.get("rationale") or "")[:300],
        })

    # URLs orphan (no asignadas a ningún item) → crear consultar_fuente
    for u in urls_input:
        if u and u not in urls_used:
            valid.append({
                "target_path": None,
                "target_section": None,
                "action": "consultar_fuente",
                "value": None,
                "confidence": "high",
                "source_urls": [u],
                "rationale": "URL proporcionada por el usuario (no asignada a item específico)",
            })

    return valid


# ════════════════════════════════════════════════════════════════════
# Fallback heurístico
# ════════════════════════════════════════════════════════════════════


def _parse_via_fallback(raw_text: str, urls: list[str]) -> list[dict]:
    """Sin LLM: genera consultar_fuente por cada URL + 1 item revisar global
    si hay texto. Confidence siempre low (sin estructura real)."""
    items: list[dict] = []
    for u in urls:
        items.append({
            "target_path": None,
            "target_section": None,
            "action": "consultar_fuente",
            "value": None,
            "confidence": "high",
            "source_urls": [u],
            "rationale": "URL proporcionada (parser fallback sin LLM)",
        })
    if raw_text and len(raw_text) >= 10:
        items.append({
            "target_path": None,
            "target_section": None,
            "action": "revisar",
            "value": None,
            "confidence": "low",
            "source_urls": [],
            "rationale": (
                "Feedback textual sin parser Haiku. El analyst recibirá el "
                "texto como instrucción general. Texto: "
                f"{raw_text[:200]!r}"
            ),
        })
    return items


__all__ = [
    "parse_feedback", "extract_urls_from_text",
    "VALID_ACTIONS", "VALID_SECTIONS", "VALID_CONFIDENCES",
]
