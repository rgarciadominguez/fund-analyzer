"""
Extracción robusta de cartera completa (top_holdings).

Problema que resuelve: Gemini Flash trunca listas largas a pesar del prompt y
`max_output_tokens` altos — típicamente devuelve 10/15/20 posiciones cuando el
fondo tiene 40-80. Este módulo implementa una cascada de fallbacks:

  1. Flash normal (ya en concept_extractor) — si da resultado razonable, fin.
  2. Flash con chunking del texto — divide el Schedule of Investments en N
     bloques, extrae cada uno y hace merge deduplicado por nombre.
  3. Claude Haiku 4.5 — modelo alternativo con mejor recall en listas largas
     y ventana contextual amplia. Max output 16k tokens.

También incluye la heurística para detectar truncación a partir del número de
posiciones devueltas vs el número de líneas sospechosas de contener una
posición en el texto original.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from rich.console import Console

# Cargar .env al import para garantizar que ANTHROPIC_API_KEY esté disponible
# también cuando se invoca desde subprocesos / módulos sin main harness.
try:
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).parent.parent / ".env")
except Exception:
    pass

console = Console()

HAIKU_MODEL = "claude-haiku-4-5-20251001"
HAIKU_MAX_TOKENS = 16000


# ─────────────────────────────────────────────────────────────────────
# Heurística de truncación
# ─────────────────────────────────────────────────────────────────────

# Patrones que suelen marcar una posición en Schedule of Investments:
#  - peso entre 0.01% y 15% con coma o punto decimal
#  - shares/units con formato "1,234,567" o "1.234.567"
#  - valores nominales grandes "10,000,000"
_POS_LINE_PATTERNS = [
    re.compile(r"\b\d{1,2}[.,]\d{1,4}\s*%"),              # pesos %
    re.compile(r"\b\d{1,3}(?:[,.]\d{3}){2,}\b"),           # números grandes (nominal / valor)
    re.compile(r"\b(?:USD|EUR|GBP|CHF|JPY)\s*[\d,.]+", re.I),
]


def count_position_like_lines(text: str) -> int:
    """Cuenta líneas del texto que parecen representar una posición de cartera."""
    if not text:
        return 0
    n = 0
    for line in text.split("\n"):
        s = line.strip()
        if len(s) < 15:
            continue
        for pat in _POS_LINE_PATTERNS:
            if pat.search(s):
                n += 1
                break
    return n


def looks_truncated(holdings: list, raw_text: str, claimed_total: int | None = None) -> bool:
    """True si parece que la extracción dejó fuera posiciones.

    Reglas:
      - Si el fondo reporta `claimed_total` y los holdings devueltos son <70% de ese número.
      - Si los holdings devueltos son ≤20 y hay al menos 2.5x líneas sospechosas en el texto.
      - Si los holdings devueltos suman <85% (cuando el fondo no está 100% invertido
        pero suele acercarse; <85% es sospechoso de corte).
    """
    n = len(holdings or [])
    if claimed_total and claimed_total > 0:
        if n < 0.7 * claimed_total:
            return True

    pos_lines = count_position_like_lines(raw_text)
    if n <= 20 and pos_lines > n * 2.5 and pos_lines >= 30:
        return True

    try:
        total_weight = sum(float(h.get("weight_pct") or h.get("peso_pct") or 0) for h in holdings)
    except Exception:
        total_weight = 0.0
    if n >= 5 and total_weight and total_weight < 85:
        return True

    return False


# ─────────────────────────────────────────────────────────────────────
# Chunking del texto manteniendo líneas enteras
# ─────────────────────────────────────────────────────────────────────

def chunk_text_by_lines(text: str, n_chunks: int = 2, overlap_lines: int = 8) -> list[str]:
    """Divide el texto en `n_chunks` partes sin cortar líneas. Cada chunk solapa
    `overlap_lines` con el siguiente para no perder posiciones que queden
    justo en la frontera.
    """
    lines = text.split("\n")
    if len(lines) < n_chunks * 5:
        return [text]
    size = len(lines) // n_chunks + 1
    out: list[str] = []
    i = 0
    while i < len(lines):
        end = min(len(lines), i + size + overlap_lines)
        out.append("\n".join(lines[i:end]))
        i += size
    return out


def chunk_text_by_size(text: str, max_chars: int = 60_000, overlap_chars: int = 1_500) -> list[str]:
    """Divide por nº de caracteres cortando en saltos de línea cercanos.

    Garantiza terminación: cuando el chunk llega al final del texto, corta
    el loop aunque el overlap sugeriría retroceder. El último chunk siempre
    termina en `len(text)`.
    """
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        end = min(n, i + max_chars)
        # Buscar salto de línea cercano para no partir una posición
        if end < n:
            nl = text.rfind("\n", i + max_chars - 2000, end)
            if nl > i:
                end = nl
        chunks.append(text[i:end])
        if end >= n:
            break
        new_i = end - overlap_chars
        # Asegurar progreso mínimo para evitar loops con overlap excesivo
        if new_i <= i:
            new_i = i + max(1, max_chars - overlap_chars)
        i = new_i
    return chunks


# ─────────────────────────────────────────────────────────────────────
# Merge deduplicado de listas de holdings
# ─────────────────────────────────────────────────────────────────────

def _normalize_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip().lower()
    # Quitar sufijos técnicos comunes que pueden variar entre chunks
    s = re.sub(r"\s+(inc|plc|ag|s\.?a\.?|corp|corporation|ltd|co\.?|holdings?|group|n\.?v\.?)\b\.?", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s[:60]


def merge_holdings_lists(*lists: list) -> list:
    """Une listas de holdings deduplicando por nombre normalizado.
    Cuando hay duplicado, se queda con el que tiene más campos completos.
    """
    seen: dict[str, dict] = {}
    for lst in lists:
        for h in (lst or []):
            if not isinstance(h, dict):
                continue
            name_raw = h.get("name") or h.get("nombre") or ""
            key = _normalize_name(name_raw)
            if not key:
                continue
            if key not in seen:
                seen[key] = h
            else:
                # Quedarse con el que tenga más campos no vacíos
                existing = seen[key]
                score_new = sum(1 for v in h.values() if v not in (None, "", 0))
                score_old = sum(1 for v in existing.values() if v not in (None, "", 0))
                if score_new > score_old:
                    seen[key] = h
    # Ordenar por peso desc
    out = list(seen.values())
    out.sort(key=lambda h: float(h.get("weight_pct") or h.get("peso_pct") or 0), reverse=True)
    return out


# ─────────────────────────────────────────────────────────────────────
# Fallback Claude Haiku 4.5
# ─────────────────────────────────────────────────────────────────────

def _parse_json_haiku(raw: str) -> Any:
    """Parser tolerante para respuesta JSON de Haiku."""
    if not raw:
        return None
    # Intento directo
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        pass
    # Bloque markdown ```json...```
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", raw)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Primer {...} o [...]
    for pat in (r"\{[\s\S]+\}", r"\[[\s\S]+\]"):
        m = re.search(pat, raw)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                continue
    return None


def extract_holdings_with_haiku(
    text: str,
    schema: dict,
    base_prompt: str,
    isin: str = "",
    fund_name: str = "",
    max_chars: int = 180_000,
) -> Any:
    """Llama a Claude Haiku 4.5 para extraer la cartera completa.
    Haiku tiene mejor recall en listas largas y 16k tokens de output.

    Devuelve dict con forma compatible con el schema del concept_extractor
    (`{value: {holdings: [...], ...}, extracted_from, extraction_notes}`)
    o None si falla.
    """
    try:
        import anthropic
    except ImportError:
        console.log("[yellow]anthropic SDK no disponible — skip Haiku fallback")
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        console.log("[yellow]ANTHROPIC_API_KEY no configurada — skip Haiku fallback")
        return None

    client = anthropic.Anthropic(api_key=api_key)

    schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
    prompt_full = (
        f"{base_prompt}\n\n"
        f"FONDO OBJETIVO: {fund_name} (ISIN {isin})\n\n"
        f"INSTRUCCIÓN CRÍTICA: Extrae TODAS las posiciones del Schedule of "
        f"Investments / Securities Portfolio / Top Holdings sin truncar. "
        f"Si el documento lista 40 posiciones, devuelve 40. Si lista 80, "
        f"devuelve 80. No resumas, no agrupes. El receptor cuenta cada item.\n\n"
        f"SCHEMA DE RESPUESTA:\n{schema_str}\n\n"
        f"TEXTO DEL DOCUMENTO:\n{text[:max_chars]}\n\n"
        f"Responde ÚNICAMENTE con el JSON solicitado, sin markdown ni "
        f"explicaciones. Cifras como número (ej. 4.83 no \"4.83%\"). "
        f"null para campos ausentes."
    )

    try:
        console.log(f"[cyan]Haiku fallback: {len(text)} chars → claude-haiku-4-5")
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=HAIKU_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt_full}],
        )
        raw = resp.content[0].text if resp.content else ""
        parsed = _parse_json_haiku(raw)
        if parsed:
            console.log(
                f"[green]Haiku fallback ok "
                f"(in={resp.usage.input_tokens}/out={resp.usage.output_tokens})"
            )
        return parsed
    except Exception as e:
        console.log(f"[red]Haiku fallback error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# Haiku-first extractor: chunking siempre + merge dedup
# ─────────────────────────────────────────────────────────────────────

def extract_holdings_haiku_chunked(
    raw_text: str,
    schema: dict,
    base_prompt: str,
    isin: str = "",
    fund_name: str = "",
    chunk_size: int = 45_000,
    overlap: int = 2_000,
) -> Any:
    """Extrae holdings usando Haiku como modelo primario con chunking SIEMPRE
    aplicado (salvo que el texto entero quepa en un solo chunk).

    Filosofía: preferir seguridad (más llamadas, menos coste/riesgo de
    truncación) sobre coste mínimo. Cada chunk es suficientemente pequeño
    para que Haiku pueda devolver todas sus posiciones dentro de su límite
    de 16k tokens output, y el merge posterior deduplica por nombre.

    Returns: dict compatible con el wrapper {value, extracted_from,
    extraction_notes} usado por concept_extractor, o None si Haiku no está
    disponible / falla.
    """
    if not raw_text:
        return None

    # Dividir siempre que el texto supere chunk_size. Chunks solapan para
    # no perder posiciones que caigan en la frontera.
    chunks = chunk_text_by_size(raw_text, max_chars=chunk_size, overlap_chars=overlap)
    console.log(
        f"[cyan]Haiku-primary holdings: {len(raw_text)} chars → "
        f"{len(chunks)} chunk(s) de ~{chunk_size} chars"
    )

    all_results: list[Any] = []
    for i, ch in enumerate(chunks):
        r = extract_holdings_with_haiku(
            text=ch, schema=schema, base_prompt=base_prompt,
            isin=isin, fund_name=fund_name,
        )
        if r:
            all_results.append(r)

    if not all_results:
        return None

    # Merge deduplicado por nombre normalizado
    def _holdings_of(result: Any) -> list:
        if not isinstance(result, dict):
            return []
        val = result.get("value")
        if isinstance(val, dict):
            return val.get("holdings") or []
        if isinstance(val, list):
            return val
        return result.get("holdings") or []

    merged = merge_holdings_lists(*(_holdings_of(r) for r in all_results))
    if not merged:
        return None

    # Construir resultado final con forma wrapper
    base = all_results[0] if isinstance(all_results[0], dict) else {}
    val = dict(base.get("value") or {})
    val["holdings"] = merged
    # Preservar as_of_date del primer chunk no vacío
    if not val.get("as_of_date"):
        for r in all_results:
            v = (r or {}).get("value") or {}
            if isinstance(v, dict) and v.get("as_of_date"):
                val["as_of_date"] = v["as_of_date"]
                break
    base["value"] = val
    base["extraction_notes"] = (
        (base.get("extraction_notes") or "")
        + f" haiku_primary(chunks={len(chunks)}, holdings={len(merged)})"
    ).strip()
    console.log(
        f"[green]Haiku-primary holdings ok: {len(merged)} posiciones "
        f"(chunks={len(chunks)})"
    )
    return base


# ─────────────────────────────────────────────────────────────────────
# Entry point: cascada completa
# ─────────────────────────────────────────────────────────────────────

def extract_holdings_primary(
    raw_text: str,
    schema: dict,
    base_prompt: str,
    run_flash_fn,
    isin: str = "",
    fund_name: str = "",
    claimed_total: int | None = None,
) -> Any:
    """Extractor PRIMARIO de cartera (reemplaza la cascada Flash-first).

    Orden:
      1. Haiku 4.5 con chunking siempre — modelo con mejor recall en listas.
      2. Heurística post-extracción: si aún detecta truncación, reintenta
         con Flash (chunking) como complemento y hace merge final.
      3. Si Haiku NO está disponible (sin API key / SDK), fallback completo
         a Flash chunking (nivel de seguridad).

    Returns: resultado con forma {value, extracted_from, extraction_notes}
    compatible con concept_extractor.
    """
    # Detectar si Haiku está disponible
    import importlib.util
    has_anthropic = importlib.util.find_spec("anthropic") is not None
    has_key = bool(os.getenv("ANTHROPIC_API_KEY"))

    def _holdings_of(result: Any) -> list:
        if not isinstance(result, dict):
            return []
        val = result.get("value")
        if isinstance(val, dict):
            return val.get("holdings") or []
        if isinstance(val, list):
            return val
        return result.get("holdings") or []

    haiku_result = None
    if has_anthropic and has_key:
        haiku_result = extract_holdings_haiku_chunked(
            raw_text=raw_text, schema=schema, base_prompt=base_prompt,
            isin=isin, fund_name=fund_name,
        )

    haiku_holdings = _holdings_of(haiku_result)

    # Si Haiku cubrió bien, devolver sin tocar nada más
    if haiku_holdings and not looks_truncated(haiku_holdings, raw_text, claimed_total):
        return haiku_result

    # Caso A: sin Haiku disponible (sin API key / SDK) → Flash chunking
    if not haiku_result:
        console.log(
            "[yellow]Haiku no disponible — fallback Flash chunking"
        )
        chunks = chunk_text_by_size(raw_text, max_chars=60_000, overlap_chars=2_000)
        flash_results: list[Any] = []
        for i, ch in enumerate(chunks):
            try:
                r = run_flash_fn(ch)
                if r:
                    flash_results.append(r)
            except Exception as e:
                console.log(f"[yellow]Flash chunk {i+1} error: {e}")
        merged = merge_holdings_lists(*(_holdings_of(r) for r in flash_results))
        if not merged:
            return None
        base = flash_results[0] if flash_results and isinstance(flash_results[0], dict) else {}
        val = dict(base.get("value") or {})
        val["holdings"] = merged
        base["value"] = val
        base["extraction_notes"] = (
            (base.get("extraction_notes") or "")
            + f" flash_fallback(chunks={len(chunks)}, holdings={len(merged)})"
        ).strip()
        return base

    # Caso B: Haiku dio resultado pero parece truncado → complementar con Flash
    console.log(
        f"[yellow]Haiku result parece incompleto ({len(haiku_holdings)} pos) "
        f"— complemento con Flash chunking"
    )
    chunks = chunk_text_by_size(raw_text, max_chars=55_000, overlap_chars=2_000)
    flash_results: list[Any] = []
    for ch in chunks:
        try:
            r = run_flash_fn(ch)
            if r:
                flash_results.append(r)
        except Exception as e:
            console.log(f"[yellow]Flash complement error: {e}")

    merged = merge_holdings_lists(
        haiku_holdings,
        *(_holdings_of(r) for r in flash_results),
    )
    if not merged:
        return haiku_result

    base = haiku_result if isinstance(haiku_result, dict) else {}
    val = dict(base.get("value") or {})
    val["holdings"] = merged
    base["value"] = val
    base["extraction_notes"] = (
        (base.get("extraction_notes") or "")
        + f" haiku+flash_complement(final={len(merged)})"
    ).strip()
    console.log(f"[green]Holdings final (Haiku+Flash): {len(merged)} posiciones")
    return base


def robust_holdings_extraction(
    raw_text: str,
    schema: dict,
    base_prompt: str,
    first_attempt: Any,
    run_flash_fn,
    isin: str = "",
    fund_name: str = "",
    claimed_total: int | None = None,
) -> Any:
    """Cascada completa de extracción robusta de holdings.

    Args:
        raw_text: texto de las páginas de cartera (ya filtrado por sub-fondo)
        schema: schema del concepto (con wrapper `{value, extracted_from}`)
        base_prompt: prompt base usado en el primer intento de Flash
        first_attempt: resultado del primer intento de Flash (puede venir truncado)
        run_flash_fn: callable `(text: str) -> dict|None` — re-ejecuta Flash
                      sobre un texto arbitrario devolviendo el mismo wrapper
        isin, fund_name: identificación del sub-fondo para el fallback
        claimed_total: si el extractor conoce `num_holdings_total` del fondo,
                       pasarlo ayuda a la heurística de truncación

    Returns:
        El mejor resultado encontrado (puede ser el primer intento si era OK).
    """
    def _holdings_of(result: Any) -> list:
        if not isinstance(result, dict):
            return []
        val = result.get("value")
        if isinstance(val, dict):
            return val.get("holdings") or []
        if isinstance(val, list):
            return val
        return result.get("holdings") or []

    first_holdings = _holdings_of(first_attempt)

    # Heurística inicial: ¿el primer intento parece completo?
    truncated = looks_truncated(first_holdings, raw_text, claimed_total)
    if not truncated:
        return first_attempt

    console.log(
        f"[yellow]top_holdings: primer intento parece truncado "
        f"({len(first_holdings)} posiciones, líneas sospechosas={count_position_like_lines(raw_text)})"
    )

    # Nivel 2 — Chunking Flash
    all_results = [first_attempt] if first_attempt else []
    chunks = chunk_text_by_size(raw_text, max_chars=60_000, overlap_chars=2_000)
    if len(chunks) > 1:
        console.log(f"[dim]chunking Flash: {len(chunks)} bloques")
        for i, ch in enumerate(chunks):
            try:
                r = run_flash_fn(ch)
                if r:
                    all_results.append(r)
                    console.log(f"[dim]  chunk {i+1}/{len(chunks)}: {len(_holdings_of(r))} pos")
            except Exception as e:
                console.log(f"[yellow]chunk {i+1} flash error: {e}")

    merged = merge_holdings_lists(*(_holdings_of(r) for r in all_results))
    if merged and not looks_truncated(merged, raw_text, claimed_total):
        # Construir resultado con forma wrapper
        base = first_attempt if isinstance(first_attempt, dict) else {}
        val = dict(base.get("value") or {})
        val["holdings"] = merged
        base["value"] = val
        base.setdefault("extraction_notes", "")
        if "chunked" not in (base.get("extraction_notes") or ""):
            base["extraction_notes"] = (base.get("extraction_notes") or "") + f" chunked_flash({len(chunks)})"
        console.log(f"[green]chunking Flash ok: {len(merged)} posiciones totales")
        return base

    # Nivel 3 — Haiku fallback
    haiku_result = extract_holdings_with_haiku(
        text=raw_text, schema=schema, base_prompt=base_prompt,
        isin=isin, fund_name=fund_name,
    )
    haiku_holdings = _holdings_of(haiku_result)
    all_results.append(haiku_result) if haiku_result else None

    merged_all = merge_holdings_lists(
        *(_holdings_of(r) for r in all_results), haiku_holdings
    )

    if merged_all:
        # Preferir la forma del haiku si vino con value/extracted_from
        base = haiku_result if isinstance(haiku_result, dict) else (first_attempt or {})
        if not isinstance(base, dict):
            base = {}
        val = dict(base.get("value") or {})
        val["holdings"] = merged_all
        base["value"] = val
        notes = base.get("extraction_notes") or ""
        base["extraction_notes"] = (notes + " robust_cascade(flash+chunks+haiku)").strip()
        console.log(f"[green]cascada robusta final: {len(merged_all)} posiciones")
        return base

    # Último recurso: devolver primer intento aunque trunque
    return first_attempt
