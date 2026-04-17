"""
ConceptMapper — Stage 1 del pipeline extractor v3.

Dado un PDF y la identidad del sub-fondo objetivo, devuelve un MAPA que
indica, para cada concepto de la taxonomía, en qué páginas aparece la
información que lo responde, qué formato encuentra, y cómo delimitar
las secciones que hablan SOLO del sub-fondo (ignorando otros sub-fondos
del paraguas umbrella).

Filosofía: el mapper razona sobre CONCEPTOS financieros. No hace
asunciones sobre estructura concreta del documento (ni menciona "Note N",
"Statistics", etiquetas específicas). Todo el conocimiento del dominio
vive en `agents/concepts.TAXONOMY`.

Modelo: Gemini 2.5 Pro (1M context + reasoning fuerte).

Caching: por sha256(PDF bytes + ISIN + fund_name) en
`data/funds/{ISIN}/cache/mapper/`.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from rich.console import Console

from agents.concepts import TAXONOMY
from tools.gemini_wrapper import extract_with_pro

console = Console()


# Shape del mapa esperado (el mapper rellena este schema)
_MAP_SHAPE = {
    "doc_language": "idioma dominante del documento (en, fr, es, de, it, pt, multi)",
    "target_fund_in_this_doc": "bool — ¿aparece el sub-fondo objetivo en este documento?",
    "target_fund_delimiter_signal": {
        "type": "header_pattern | section_title | isin_marker | unique_name | none",
        "value": "texto literal (o vacío si no aplica) que permite reconocer dónde empieza y acaba un bloque del sub-fondo objetivo",
    },
    "umbrella_context_detected": "bool — ¿el documento cubre múltiples sub-fondos de un paraguas?",
    "concept_locations": {
        "<concept_name>": {
            "pages_1indexed": "lista de páginas (1-indexed) donde aparece la información del concepto",
            "format_clue": "descripción breve del formato en que aparece (columnas, tabla, párrafo, lista) sin asumir un formato concreto",
            "covers_target_only": "bool — si ese contenido es específico del sub-fondo objetivo (True) o contiene info mezclada con otros sub-fondos (False)",
            "evidence_quote": "primeras 100-200 caracteres del bloque, copia literal del documento, para verificar",
            "confidence": "float 0.0-1.0 — tu confianza en que has localizado bien el concepto",
        }
    },
    "concepts_not_found": "lista de nombres de conceptos de la taxonomía que no has podido localizar en el documento",
    "global_context_pages": "lista de páginas con contexto GENERAL del paraguas / gestora (market outlook, carta del chairman, strategy) que enriquece los conceptos cualitativos aunque no sean específicas del sub-fondo",
}


def _pdf_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _read_full_pdf_text(path: Path) -> tuple[str, int]:
    """Lee el PDF completo con marcadores de página. Devuelve (texto, num_paginas)."""
    import pdfplumber
    parts: list[str] = []
    with pdfplumber.open(str(path)) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, start=1):
            t = (page.extract_text() or "").strip()
            if t:
                parts.append(f"--- PAGE {i} ---\n{t}")
    return "\n\n".join(parts), total


def _deterministic_sample(full_text: str, total_pages: int, max_chars: int = 700_000) -> str:
    """
    Si el texto completo excede max_chars, hace sampling determinista
    preservando first/last de cada página (no usa keywords — totalmente
    agnóstico al contenido).
    """
    if len(full_text) <= max_chars:
        return full_text
    # Sampling: tomar primera N líneas de cada página hasta caber
    lines = full_text.splitlines()
    out: list[str] = []
    char_budget = max_chars
    current_page_lines: list[str] = []
    page_char_budget = max(500, max_chars // max(1, total_pages))
    for line in lines:
        if line.startswith("--- PAGE "):
            if current_page_lines:
                chunk = "\n".join(current_page_lines)
                out.append(chunk[:page_char_budget])
                char_budget -= len(chunk[:page_char_budget])
            current_page_lines = [line]
            if char_budget <= 0:
                break
        else:
            current_page_lines.append(line)
    if current_page_lines and char_budget > 0:
        chunk = "\n".join(current_page_lines)
        out.append(chunk[:page_char_budget])
    return "\n".join(out)


def _build_prompt(isin: str, fund_name: str, gestora: str) -> str:
    """
    Construye el prompt para el mapper. Deliberadamente:
    - No menciona 'Note 18', 'Statistics', ni ninguna etiqueta observada en
      documentos concretos.
    - Describe qué hay que encontrar en términos conceptuales.
    - La taxonomía (descriptions) provee el detalle semántico por concepto.
    """
    # Serializar taxonomía de forma compacta para el prompt
    taxonomy_snippet = []
    for name, entry in TAXONOMY.items():
        taxonomy_snippet.append(
            f"- {name} (prioridad: {entry.get('priority','nice_to_have')}): "
            f"{entry['description']}"
        )
    tax_str = "\n".join(taxonomy_snippet)

    return (
        f"Eres un analista financiero especializado en fondos de inversión. "
        f"Recibes un documento (annual report, semi-annual, factsheet, KID, "
        f"prospectus, carta del gestor…) y tu tarea es MAPEAR en qué páginas "
        f"del documento aparece la información necesaria para responder a "
        f"cada concepto financiero de la lista dada. NO extraigas valores: "
        f"solo localiza.\n\n"

        f"FONDO OBJETIVO:\n"
        f"  ISIN: {isin}\n"
        f"  Nombre: {fund_name or '(desconocido)'}\n"
        f"  Gestora: {gestora or '(desconocida)'}\n\n"

        f"CONCEPTOS FINANCIEROS A LOCALIZAR:\n"
        f"{tax_str}\n\n"

        f"INSTRUCCIONES:\n"
        f"1. Lee el documento completo e identifica en qué páginas aparece "
        f"cada concepto. El documento puede estar en cualquier idioma, usar "
        f"cualquier numeración de notas (o ninguna), cualquier orden de "
        f"secciones, cualquier layout de tablas. Adapta tu búsqueda al "
        f"documento concreto que tienes enfrente.\n"
        f"2. Si el documento cubre múltiples sub-fondos de un paraguas, "
        f"identifica cómo se delimitan los bloques del sub-fondo objetivo "
        f"(un header recurrente, un título de sección, su ISIN repetido, "
        f"su nombre único). Devuélvelo en target_fund_delimiter_signal. "
        f"Marca covers_target_only=true para páginas que hablan SOLO del "
        f"sub-fondo objetivo, false para tablas globales del paraguas "
        f"donde el sub-fondo es una fila/columna.\n"
        f"3. Para cada concepto localizado, describe el FORMATO que "
        f"encuentras (una tabla por clase, un párrafo narrativo, una "
        f"lista, una matriz) sin asumir ningún formato de antemano.\n"
        f"4. Cita los primeros 100-200 caracteres del bloque como "
        f"evidencia. La cita debe existir LITERAL en el documento.\n"
        f"5. Si un concepto NO aparece en este documento, inclúyelo en "
        f"concepts_not_found. No inventes páginas.\n"
        f"6. Identifica también páginas de CONTEXTO GENERAL (outlook de "
        f"mercado, carta del chairman, filosofía de la gestora) que "
        f"enriquezcan los conceptos cualitativos aunque no sean "
        f"exclusivas del sub-fondo.\n\n"

        f"Devuelve el resultado siguiendo exactamente el schema JSON que "
        f"se indica a continuación."
    )


def map_document(
    pdf_path: str | Path,
    isin: str,
    fund_name: str = "",
    gestora: str = "",
    cache_dir: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Devuelve el mapa del documento. Cachea por sha256(pdf)+isin.

    Args:
        pdf_path: ruta al PDF
        isin: ISIN del sub-fondo objetivo
        fund_name: nombre del sub-fondo (mejor extracción si se provee)
        gestora: nombre de la gestora (context para el mapper)
        cache_dir: directorio para cachear el mapa. Si None, no cachea.
        force: saltar cache

    Returns:
        dict con estructura `_MAP_SHAPE`
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF no existe: {pdf_path}")

    pdf_h = _pdf_hash(pdf_path)
    cache_key = hashlib.sha256(f"{pdf_h}|{isin}|{fund_name}".encode()).hexdigest()[:24]

    if cache_dir and not force:
        cache_file = cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                cached = json.loads(cache_file.read_text(encoding="utf-8"))
                console.log(f"[dim]mapper cache hit: {pdf_path.name}")
                return cached
            except Exception:
                pass

    console.log(f"[cyan]mapper: leyendo {pdf_path.name}")
    full_text, total_pages = _read_full_pdf_text(pdf_path)
    console.log(f"[cyan]mapper: {total_pages} pp, {len(full_text):,} chars")

    text_to_send = _deterministic_sample(full_text, total_pages)
    if len(text_to_send) < len(full_text):
        console.log(f"[yellow]mapper: sampling a {len(text_to_send):,} chars")

    prompt = _build_prompt(isin, fund_name, gestora)

    try:
        result = extract_with_pro(
            text=text_to_send,
            schema=_MAP_SHAPE,
            custom_prompt=prompt,
        )
    except Exception as e:
        console.log(f"[red]mapper failed: {e}")
        raise

    if not isinstance(result, dict):
        raise ValueError(f"mapper devolvió tipo inesperado: {type(result)}")

    # Enriquece el mapa con metadata técnica (no va al prompt)
    result["_meta"] = {
        "pdf_hash": pdf_h,
        "total_pages": total_pages,
        "chars_sent": len(text_to_send),
        "truncated": len(text_to_send) < len(full_text),
    }

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / f"{cache_key}.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    return result
