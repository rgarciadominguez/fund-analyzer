"""
ConceptExtractor — Stage 2 del pipeline extractor v3.

Recibe un mapa del ConceptMapper y, para cada concepto con location
conocida, hace una llamada Gemini Flash dirigida únicamente a las páginas
indicadas. Los prompts usan la descripción conceptual de la taxonomía sin
asumir estructura específica del documento.

Devuelve un dict `{concept_name: extracted_value_or_list}` + metadata de
extracción (cuántos conceptos, confianza media, etc.).

Filosofía:
- Cero conocimiento del dominio fuera de `agents/concepts.TAXONOMY`.
- Cada valor extraído lleva `extracted_from` (cita literal breve del PDF)
  como evidencia verificable para detectar alucinaciones.
- Si el primer intento devuelve todo null, retry con ±5 pp de contexto.

Modelo: Gemini 2.5 Flash (task simple con contexto ya pre-filtrado).
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from rich.console import Console

from agents.concepts import TAXONOMY
from tools.gemini_wrapper import extract_fast, MODEL_FLASH

console = Console()


# Cuántas páginas de contexto añadir en el retry si primer intento devuelve null
_RETRY_MARGIN = 5


def _read_pages(pdf_path: Path, pages_1indexed: list[int]) -> str:
    """Extrae texto de las páginas indicadas (1-indexed). Preserva orden."""
    if not pages_1indexed:
        return ""
    import pdfplumber
    uniq = sorted(set(pages_1indexed))
    parts: list[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        total = len(pdf.pages)
        for p_1 in uniq:
            idx = p_1 - 1
            if 0 <= idx < total:
                t = (pdf.pages[idx].extract_text() or "").strip()
                if t:
                    parts.append(f"--- PAGE {p_1} ---\n{t}")
    return "\n\n".join(parts)


def _widen_pages(pages: list[int], margin: int, max_page: int) -> list[int]:
    """Amplía la lista con ±margin páginas (clampeado a [1, max_page])."""
    out: set[int] = set()
    for p in pages:
        for d in range(-margin, margin + 1):
            q = p + d
            if 1 <= q <= max_page:
                out.add(q)
    return sorted(out)


def _filter_by_delimiter(text: str, delimiter_value: str) -> str:
    """
    Si hay delimiter_signal (un nombre/header repetido del sub-fondo), aísla bloques
    del texto que empiezan con ese marker y terminan al siguiente marker
    distinto. Esto reduce ruido de sub-fondos hermanos.

    Heurística simple: buscar ocurrencias del delimiter y mantener los
    siguientes ~3000 chars tras cada ocurrencia, cortando si aparece OTRO
    bloque tipo "fund_name" (detectado por frecuencia de strings similares).

    Si el delimiter no se encuentra, devuelve el texto completo.
    """
    if not delimiter_value or len(delimiter_value) < 4:
        return text
    delimiter_re = re.compile(re.escape(delimiter_value), re.IGNORECASE)
    matches = list(delimiter_re.finditer(text))
    if not matches:
        return text

    # Cortar cada match en un bloque: desde el match hasta ~3000 chars
    # después (o hasta otro sub-fund header — mejor aproximación con
    # palabras capitalized+parens que suelen marcar sub-fondos en umbrellas).
    # Pragmático: mantener los primeros 8000 chars post-match.
    blocks: list[str] = []
    for m in matches:
        start = max(0, m.start() - 100)  # un poco de contexto previo
        end = min(len(text), m.end() + 8000)
        blocks.append(text[start:end])
    # Deduplicar bloques muy solapados
    if not blocks:
        return text
    merged = [blocks[0]]
    for b in blocks[1:]:
        # Si overlap >60% con el último, skip
        if len(set(b.split()) & set(merged[-1].split())) > 0.6 * len(b.split()):
            continue
        merged.append(b)
    return "\n\n".join(merged)


def _is_empty_result(result: Any) -> bool:
    """True si el resultado no tiene ningún valor útil."""
    if result is None:
        return True
    if isinstance(result, dict):
        if not result:
            return True
        # Si todos los valores son None/empty
        def _all_empty(v):
            if v is None or v == "" or v == [] or v == {}:
                return True
            if isinstance(v, (list, tuple)):
                return all(_all_empty(x) for x in v)
            if isinstance(v, dict):
                return all(_all_empty(x) for x in v.values())
            return False
        return all(_all_empty(v) for v in result.values())
    if isinstance(result, list):
        return all(_is_empty_result(x) for x in result)
    return False


def _build_schema_with_evidence(output_shape: dict | list) -> dict:
    """
    Envuelve el output_shape del concepto con un campo adicional
    `extracted_from` (cita literal breve del PDF) para evidencia.
    """
    return {
        "value": output_shape,
        "extracted_from": "cita literal breve (50-200 chars) del texto del documento que soporta el valor extraído. Debe existir textualmente en el documento.",
        "extraction_notes": "cualquier advertencia útil: ej. 'solo se reporta a 2 años', 'valor derivado por suma de clases', etc.",
    }


def _build_prompt(
    concept_name: str,
    description: str,
    computation_hints: str | None,
    format_clue: str,
    isin: str,
    fund_name: str,
) -> str:
    return (
        f"Extrae el siguiente concepto financiero del texto del documento.\n\n"
        f"FONDO OBJETIVO: {fund_name or '(desconocido)'} (ISIN {isin})\n\n"
        f"CONCEPTO A EXTRAER: {concept_name}\n"
        f"DESCRIPCIÓN: {description}\n"
        + (f"NOTAS DE CÓMPUTO: {computation_hints}\n" if computation_hints else "")
        + f"FORMATO EN ESTE DOCUMENTO (según mapeo previo): {format_clue or '(no especificado)'}\n\n"
        f"REGLAS:\n"
        f"1. Devuelve ÚNICAMENTE información del sub-fondo objetivo. Si el "
        f"texto mezcla varios sub-fondos del paraguas, aísla las cifras "
        f"correspondientes al objetivo.\n"
        f"2. Si un campo del output_shape no aparece en el texto, déjalo "
        f"null. No inventes.\n"
        f"3. Incluye en `extracted_from` una cita corta (50-200 chars) del "
        f"documento que soporte tu respuesta. La cita debe existir LITERAL "
        f"en el texto (verificable con búsqueda exacta).\n"
        f"4. Si encuentras múltiples snapshots temporales para el concepto, "
        f"devuelve todos (no solo el más reciente).\n"
        f"5. Preserva las divisas originales en que aparecen los valores; "
        f"no conviertas tú — el pipeline lo hace luego.\n"
    )


def extract_concept(
    pdf_path: Path,
    concept_name: str,
    concept_location: dict,
    concept_entry: dict,
    isin: str,
    fund_name: str,
    delimiter_value: str = "",
    total_pages: int = 0,
    cache_dir: Path | None = None,
) -> dict[str, Any] | None:
    """
    Extrae un solo concepto del documento siguiendo su location en el mapa.

    Args:
        pdf_path: ruta al PDF
        concept_name: nombre del concepto (ej. 'fund_size_history')
        concept_location: entry del `concept_locations` del mapa
        concept_entry: entry de la taxonomía
        isin: ISIN del sub-fondo
        fund_name: nombre del sub-fondo
        delimiter_value: si el mapa lo detectó, filtra bloques del target
        total_pages: número total de páginas del PDF (para widening)
        cache_dir: para cachear el resultado

    Returns:
        dict con {"value": ..., "extracted_from": "...", ...} o None si no
        se pudo extraer.
    """
    pages = concept_location.get("pages_1indexed") or []
    if not pages:
        return None
    format_clue = concept_location.get("format_clue", "")
    covers_target_only = concept_location.get("covers_target_only", True)

    description = concept_entry["description"]
    computation_hints = concept_entry.get("computation_hints")
    output_shape = concept_entry["output_shape"]

    # Cache key: pages + isin + concept + pdf_hash (de concept_location es
    # estable porque viene del mapa cacheado)
    cache_key = None
    if cache_dir:
        key_src = f"{concept_name}|{isin}|{sorted(pages)}|{delimiter_value}"
        cache_key = hashlib.sha256(key_src.encode()).hexdigest()[:24]
        cf = cache_dir / f"{cache_key}.json"
        if cf.exists():
            try:
                return json.loads(cf.read_text(encoding="utf-8"))
            except Exception:
                pass

    def _run(pp: list[int]) -> Any:
        text = _read_pages(pdf_path, pp)
        if not text:
            return None
        # Filtrar por delimiter solo si el bloque NO es target-only
        # (porque target-only ya está limpio; si es global, filtramos ruido
        # de otros sub-fondos)
        if delimiter_value and not covers_target_only:
            text = _filter_by_delimiter(text, delimiter_value)
        prompt = _build_prompt(
            concept_name, description, computation_hints,
            format_clue, isin, fund_name,
        )
        schema = _build_schema_with_evidence(output_shape)
        try:
            return extract_fast(
                text=text, schema=schema,
                custom_prompt=prompt, model=MODEL_FLASH,
            )
        except Exception as e:
            console.log(f"[yellow]extractor {concept_name} error: {e}")
            return None

    result = _run(pages)

    # Retry con margen si empty
    if _is_empty_result(result) and total_pages:
        widened = _widen_pages(pages, _RETRY_MARGIN, total_pages)
        if widened != pages:
            console.log(f"[dim]retry {concept_name} con ±{_RETRY_MARGIN}pp (n={len(widened)})")
            result = _run(widened)

    if _is_empty_result(result):
        return None

    if cache_key and cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / f"{cache_key}.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    return result


def extract_all(
    pdf_path: str | Path,
    doc_map: dict,
    isin: str,
    fund_name: str = "",
    cache_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Itera sobre `doc_map.concept_locations` y extrae todos los conceptos.

    Returns:
        {
            "by_concept": {concept_name: {"value": ..., "extracted_from": "..."} | None},
            "stats": {"concepts_attempted": N, "concepts_extracted": M, "empty": [...]},
        }
    """
    pdf_path = Path(pdf_path)
    concept_locations = doc_map.get("concept_locations") or {}
    delimiter_signal = doc_map.get("target_fund_delimiter_signal") or {}
    delimiter_value = delimiter_signal.get("value", "") if isinstance(delimiter_signal, dict) else ""
    total_pages = (doc_map.get("_meta") or {}).get("total_pages", 0)

    by_concept: dict[str, Any] = {}
    empty: list[str] = []
    for name, loc in concept_locations.items():
        entry = TAXONOMY.get(name)
        if not entry:
            console.log(f"[yellow]concepto desconocido en mapa: {name}")
            continue
        result = extract_concept(
            pdf_path=pdf_path,
            concept_name=name,
            concept_location=loc,
            concept_entry=entry,
            isin=isin,
            fund_name=fund_name,
            delimiter_value=delimiter_value,
            total_pages=total_pages,
            cache_dir=cache_dir,
        )
        if result is None:
            empty.append(name)
        by_concept[name] = result
        # Log breve
        status = "ok" if result else "empty"
        console.log(f"[cyan]{name:40} [{status}]")

    stats = {
        "concepts_attempted": len(concept_locations),
        "concepts_extracted": len(concept_locations) - len(empty),
        "empty": empty,
    }
    return {"by_concept": by_concept, "stats": stats}
