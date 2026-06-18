"""Auto-recuperación del campo `nombre` en output.json (2026-05-27, T2.5).

Problema: cuando un agente fuente (regulator_router CBI/AMF, cnmv_agent INT)
no consigue extraer el nombre oficial del fondo, `output.json["nombre"]` queda
igual al ISIN. El analyst_synthesis sí menciona el nombre real en sus
párrafos, pero por la regla zona-RAW vs zona-PROCESADA del schema NO se
propaga back al campo top-level.

Resultado: el catálogo público muestra el ISIN en la columna "Nombre del
fondo", aunque el análisis cualitativo esté completo y correcto.

Solución (2 niveles):
  A) Regex local sobre analyst_synthesis.{resumen,historia,estrategia}.texto.
     Cubre patrones típicos ("El fondo X es...", "X invierte en...",
     "**X**", "X (ISIN)"). Coste cero, ~70-80% de los casos.
  B) Fallback Haiku (1 call, ~$0.001/fondo) cuando A no encuentra.
     Prompt corto: extraer nombre comercial o "UNKNOWN".

Si encuentra un candidato, mark_manual_edit("nombre") evita que próximas
regeneraciones del analyst lo sobrescriban.

API:
    from tools.name_recovery import recover_name_if_needed
    result = recover_name_if_needed(output_data, isin, log_fn=log)
    # result: {"applied": bool, "method": str|None, "from": str, "to": str}
"""
from __future__ import annotations

import os
import re
from collections import Counter
from typing import Callable, Optional

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# Patrones regex ordenados de más específico (high precision) a menos.
# Cada uno tiene un único grupo de captura = candidate name.
_REGEX_PATTERNS_TEMPLATE = (
    # 1. "El fondo X es/invierte/sigue/persigue/aplica"
    r"(?:[Ee]l\s+fondo|[Ee]l\s+sub-?fondo|[Ll]a\s+sicav|[Ee]l\s+vehículo)\s+"
    r"([A-Z][^\.\,\(\)\n]{3,80}?)\s+"
    r"(?:es\b|invierte\b|sigue\b|gestiona\b|consist|tiene\b|aplica\b|busca\b|persigue\b|invertir\b)",
    # 2. "X es un (sub-)fondo / UCITS / vehículo"
    r"^([A-Z][^\.\,\(\)\n]{3,80}?)\s+"
    r"(?:es\s+un\s+(?:sub-?)?fondo|es\s+un\s+vehículo|es\s+un\s+UCITS)",
    # 3. "X (ISIN)" — nombre seguido del ISIN entre paréntesis
    r"([A-Z][^\.\,\(\)\n]{{3,80}}?)\s*\(\s*{isin_escaped}\s*\)",
    # 4. **Nombre** en bold markdown
    r"\*\*([A-Z][^\*\n]{3,80}?)\*\*",
)

# Filtros: si el "candidato" arranca con una de estas palabras, lo
# descartamos (suele ser una frase, no un nombre).
_BLACKLIST_STARTS = {
    "para", "como", "esta", "este", "estos", "estas", "desde", "durante",
    "según", "según", "tras", "antes", "después", "the", "this", "these",
}


def needs_recovery(nombre: str, isin: str) -> bool:
    """¿El campo `nombre` está claramente mal? Usa el validador central, así
    detecta NO solo vacío/ISIN sino también prosa, etiquetas y fragmentos
    (antes se colaban porque needs_recovery solo miraba vacío/ISIN/corto)."""
    if not isin:
        return False
    try:
        from tools.fund_name_utils import is_valid_fund_name
        return not is_valid_fund_name(nombre, isin)[0]
    except Exception:
        # Fallback al chequeo mínimo histórico
        n = (nombre or "").strip()
        return (not n) or n.upper() == isin.upper() or bool(_ISIN_RE.match(n.upper())) or len(n) < 5


def _candidate_is_valid(name: str, isin: str) -> bool:
    """Filtros para descartar matches espurios (mismo validador central)."""
    try:
        from tools.fund_name_utils import is_valid_fund_name
        return is_valid_fund_name(name, isin)[0]
    except Exception:
        name = (name or "").strip()
        if len(name) < 5 or name.upper() == isin.upper() or _ISIN_RE.match(name.upper()):
            return False
        words = name.split()
        return len(words) <= 10 and words[0].lower().rstrip(",.;:") not in _BLACKLIST_STARTS


def _extract_via_regex(texts: list[str], isin: str) -> Optional[str]:
    """Intenta extraer el nombre del fondo de los textos narrativos.

    Devuelve el candidato más frecuente entre todos los textos, o None.
    """
    if not texts:
        return None
    isin_escaped = re.escape(isin)
    patterns = []
    for tmpl in _REGEX_PATTERNS_TEMPLATE:
        try:
            # El template del patrón 3 tiene {isin_escaped}; el resto {{ }} ya están
            # como llaves literales en el regex (cuantificadores).
            patterns.append(tmpl.format(isin_escaped=isin_escaped))
        except (IndexError, KeyError):
            patterns.append(tmpl)

    candidates: list[str] = []
    for text in texts:
        if not text or not isinstance(text, str):
            continue
        for pat in patterns:
            try:
                for m in re.finditer(pat, text, re.MULTILINE):
                    name = m.group(1).strip().rstrip(".,;:")
                    if _candidate_is_valid(name, isin):
                        candidates.append(name)
            except re.error:
                continue

    if not candidates:
        return None
    most_common = Counter(candidates).most_common(1)
    return most_common[0][0] if most_common else None


def _extract_via_haiku(
    texts: list[str], isin: str, gestora: str = ""
) -> Optional[str]:
    """Fallback Haiku: 1 call con los primeros 2000 chars de los textos.

    Devuelve None si la API no está disponible, el modelo dice "UNKNOWN",
    o el resultado parece basura.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        from anthropic import Anthropic
    except Exception:
        return None

    combined = ""
    for text in texts:
        if not text or not isinstance(text, str):
            continue
        remaining = 2000 - len(combined)
        if remaining <= 100:
            break
        combined += text[:remaining] + "\n---\n"
    if len(combined.strip()) < 100:
        return None

    gestora_hint = f", gestora: {gestora}" if gestora else ""
    prompt = (
        f"Texto sobre un fondo de inversión (ISIN: {isin}{gestora_hint}):\n\n"
        f"{combined}\n\n"
        "Extrae el NOMBRE COMERCIAL completo del fondo "
        "(ejemplos: 'Magallanes European Equity FI', 'DNCA Invest Alpha Bonds'). "
        "Devuelve SOLO el nombre, sin el ISIN ni la clase de acción. "
        "Si no aparece claramente en el texto, responde EXACTAMENTE 'UNKNOWN'."
    )

    try:
        client = Anthropic()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        name = (resp.content[0].text or "").strip().strip('"').strip("'")
    except Exception:
        return None

    if not name or name.upper() == "UNKNOWN":
        return None
    if not _candidate_is_valid(name, isin):
        return None
    return name


def recover_name_if_needed(
    output_data: dict,
    isin: str,
    log_fn: Optional[Callable[[str, str, str], None]] = None,
) -> dict:
    """Recupera output_data["nombre"] si está claramente mal.

    Mutates `output_data` in-place si encuentra un candidato.
    Marca `_manual_edits["nombre"]` para que el próximo analyst no lo
    sobrescriba.

    Args:
        output_data: el dict de output.json (modificado in-place).
        isin: ISIN del fondo.
        log_fn: opcional, función (agent, level, msg) para logging.

    Returns:
        {"applied": bool, "method": "regex"|"haiku"|None,
         "from": str, "to": str, "reason": str}
    """
    nombre_actual = (output_data.get("nombre") or "").strip()
    if not needs_recovery(nombre_actual, isin):
        return {"applied": False, "reason": "nombre_ok",
                "from": nombre_actual, "to": nombre_actual, "method": None}

    # Recoger textos del analyst_synthesis para parsear
    synth = output_data.get("analyst_synthesis", {}) or {}
    texts: list[str] = []
    for sec_name in ("resumen", "historia", "estrategia"):
        sec = synth.get(sec_name) or {}
        t = sec.get("texto") or ""
        if t and isinstance(t, str):
            texts.append(t)

    if log_fn:
        log_fn("NAME_RECOVERY", "INFO",
               f"nombre='{nombre_actual}' inválido para {isin}, "
               f"intentando recuperar de {len(texts)} textos…")

    # A) Morningstar por ISIN — fuente AUTORITATIVA y fiable (nombre oficial del
    # fondo, validado por ISIN). PRIMERO: la extracción de la narrativa (regex)
    # coge títulos de sección/gráfico ("Fondo vs mercado año a año", "Origen: …"),
    # así que solo es fallback cuando Morningstar no cubre el fondo.
    name = None
    method = None
    try:
        from tools.morningstar_quant import fetch_identity
        mn = (fetch_identity(isin) or {}).get("name", "").strip()
        if mn and _candidate_is_valid(mn, isin):
            name = mn
            method = "morningstar"
    except Exception:
        pass

    # B) regex local (validado) si Morningstar no cubre el fondo
    if not name:
        name = _extract_via_regex(texts, isin)
        method = "regex" if name else None

    # C) fallback Haiku
    if not name:
        gestora = (output_data.get("gestora") or "").strip()
        name = _extract_via_haiku(texts, isin, gestora)
        method = "haiku" if name else None

    if not name:
        if log_fn:
            log_fn("NAME_RECOVERY", "WARN",
                   f"no se pudo recuperar nombre para {isin}")
        return {"applied": False, "reason": "no_match",
                "from": nombre_actual, "to": nombre_actual, "method": None}

    # Aplicar
    output_data["nombre"] = name
    try:
        from tools.output_merger import mark_manual_edit
        mark_manual_edit(output_data, "nombre")
    except Exception:
        edits = output_data.setdefault("_manual_edits", [])
        if isinstance(edits, list) and "nombre" not in edits:
            edits.append("nombre")

    if log_fn:
        log_fn("NAME_RECOVERY", "OK",
               f"nombre {isin}: '{nombre_actual}' → '{name}' (via {method})")

    return {"applied": True, "method": method,
            "from": nombre_actual, "to": name, "reason": "recovered"}


__all__ = ["needs_recovery", "recover_name_if_needed"]
