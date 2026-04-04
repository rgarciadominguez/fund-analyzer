"""
PDF extraction utilities for annual reports (SICAV luxemburguesas) and CNMV semestrales.

Patrón documentado: DNCA INVEST Annual Report (522 págs, 25 fondos)
- TOC en páginas 2-8 del PDF (0-indexed: 1-7)
- Offset TOC→PDF: +2 (TOC dice 134 → PDF página real 136)
- 3 secciones por fondo: Directors' Report, Statistics, Financial Statements
"""
import re
from pathlib import Path
from difflib import SequenceMatcher

import pdfplumber
from rich.console import Console

console = Console()

# Páginas del PDF donde suele estar el TOC en annual reports SICAV luxemburguesas
TOC_SEARCH_RANGE = (1, 8)  # 0-indexed


# ── TOC ─────────────────────────────────────────────────────────────────────

def parse_toc(pdf_path: str) -> dict:
    """
    Parsea el TOC de un annual report SICAV luxemburguesa.

    Extrae texto de páginas 2-8, busca líneas con patrón "NOMBRE FONDO  123".
    Retorna: {fondo_nombre: {"doc_page": int, "sections": {}}}
    """
    toc: dict = {}
    # Patrón: nombre en mayúsculas (puede incluir guiones, &, puntos) seguido de número de página
    pattern = re.compile(r"^([A-Z][A-Z0-9 \-&\.\/\(\)]+?)\s{2,}(\d+)\s*$")

    with pdfplumber.open(pdf_path) as pdf:
        end = min(TOC_SEARCH_RANGE[1], len(pdf.pages))
        for page_idx in range(TOC_SEARCH_RANGE[0], end):
            page = pdf.pages[page_idx]
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                match = pattern.match(line)
                if match:
                    name = match.group(1).strip()
                    doc_page = int(match.group(2))
                    toc[name] = {"doc_page": doc_page, "sections": {}}

    console.log(f"[green]TOC parseado: {len(toc)} fondos encontrados")
    return toc


def calculate_pdf_offset(pdf_path: str, toc: dict) -> int:
    """
    Calcula el offset entre página indicada en el TOC y página real en el PDF.

    Estrategia: toma la primera entrada del TOC, busca en un rango ±3
    la página real donde aparece el nombre del fondo.
    Retorna el offset (normalmente +2 para SICAVs luxemburguesas).
    """
    if not toc:
        return 0

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)

        for fund_name, info in toc.items():
            doc_page = info["doc_page"]
            search_start = max(0, doc_page - 2)
            search_end = min(total_pages, doc_page + 5)

            # Buscar el nombre del fondo en las páginas candidatas
            name_words = fund_name.split()[:3]  # primeras 3 palabras son suficientes
            search_term = " ".join(name_words)

            for pdf_page_idx in range(search_start, search_end):
                text = pdf.pages[pdf_page_idx].extract_text() or ""
                if search_term.upper() in text.upper():
                    offset = pdf_page_idx - doc_page
                    console.log(
                        f"[green]Offset calculado: {offset:+d} "
                        f"('{fund_name}' TOC p.{doc_page} → PDF p.{pdf_page_idx})"
                    )
                    return offset

    console.log("[yellow]No se pudo calcular offset, usando 0")
    return 0


# ── Extracción de páginas ────────────────────────────────────────────────────

def extract_page_range(pdf_path: str, start_page: int, end_page: int) -> str:
    """
    Extrae texto de rango de páginas (0-indexed, end_page exclusivo).
    """
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        start = max(0, start_page)
        end = min(total, end_page)
        texts = []
        for idx in range(start, end):
            page_text = pdf.pages[idx].extract_text() or ""
            if page_text.strip():
                texts.append(f"--- Página {idx + 1} ---\n{page_text}")
        return "\n\n".join(texts)


def extract_pages_by_keyword(
    pdf_path: str,
    keywords: list[str],
    context_pages: int = 1,
    search_range: tuple[int, int] | None = None,
) -> str:
    """
    Extrae texto de páginas donde aparece alguna de las keywords,
    más `context_pages` páginas de contexto antes y después.

    Args:
        search_range: (start, end) 0-indexed para limitar la búsqueda.
                      Si es None, busca en todo el documento.
    """
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        start_idx = search_range[0] if search_range else 0
        end_idx = search_range[1] if search_range else total
        end_idx = min(end_idx, total)

        # Identificar páginas con matches
        hit_pages: set[int] = set()
        for page_idx in range(start_idx, end_idx):
            text = pdf.pages[page_idx].extract_text() or ""
            if any(kw.lower() in text.lower() for kw in keywords):
                # Añadir la página y su contexto
                for ctx in range(
                    max(0, page_idx - context_pages),
                    min(total, page_idx + context_pages + 1),
                ):
                    hit_pages.add(ctx)

        if not hit_pages:
            console.log(f"[yellow]Keywords no encontradas: {keywords}")
            return ""

        texts = []
        for idx in sorted(hit_pages):
            page_text = pdf.pages[idx].extract_text() or ""
            if page_text.strip():
                texts.append(f"--- Página {idx + 1} ---\n{page_text}")

        console.log(f"[green]Extraídas {len(hit_pages)} páginas con keywords {keywords}")
        return "\n\n".join(texts)


def extract_text_section(pdf_path: str, start_kw: str, end_kw: str) -> str:
    """
    Extrae el texto entre la primera aparición de start_kw y end_kw
    (buscando en todo el documento, línea a línea).
    """
    with pdfplumber.open(pdf_path) as pdf:
        all_lines: list[str] = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_lines.extend(text.splitlines())

    full_text = "\n".join(all_lines)
    start_pos = full_text.lower().find(start_kw.lower())
    if start_pos == -1:
        console.log(f"[yellow]Keyword de inicio no encontrada: '{start_kw}'")
        return ""

    end_pos = full_text.lower().find(end_kw.lower(), start_pos + len(start_kw))
    if end_pos == -1:
        console.log(f"[yellow]Keyword de fin no encontrada: '{end_kw}'")
        return full_text[start_pos:]

    return full_text[start_pos:end_pos]


# ── Metadata y búsqueda en TOC ───────────────────────────────────────────────

def get_pdf_metadata(pdf_path: str) -> dict:
    """Retorna num_pages, file_size_mb, title, author del PDF."""
    path = Path(pdf_path)
    with pdfplumber.open(pdf_path) as pdf:
        meta = pdf.metadata or {}
        return {
            "num_pages": len(pdf.pages),
            "file_size_mb": round(path.stat().st_size / 1_048_576, 2),
            "title": meta.get("Title", ""),
            "author": meta.get("Author", ""),
            "creator": meta.get("Creator", ""),
        }


def find_fund_in_toc(
    toc: dict,
    fund_name: str | None = None,
    isin: str | None = None,
) -> dict:
    """
    Busca un fondo en el TOC por nombre (fuzzy) o ISIN.

    Retorna el dict del fondo o {} si no se encuentra.
    Prioridad: coincidencia exacta > coincidencia parcial > fuzzy ratio > 0.6.
    """
    if not toc:
        return {}

    if fund_name:
        query = fund_name.strip().upper()

        # 1. Coincidencia exacta
        if query in toc:
            return {"toc_name": query, **toc[query]}

        # 2. Coincidencia parcial (el nombre del TOC contiene el query o viceversa)
        for name, info in toc.items():
            if query in name.upper() or name.upper() in query:
                console.log(f"[green]Fondo encontrado por coincidencia parcial: '{name}'")
                return {"toc_name": name, **info}

        # 3. Fuzzy matching
        best_ratio = 0.0
        best_name = ""
        for name in toc:
            ratio = SequenceMatcher(None, query, name.upper()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_name = name

        if best_ratio >= 0.6:
            console.log(
                f"[green]Fondo encontrado por fuzzy match: '{best_name}' (ratio={best_ratio:.2f})"
            )
            return {"toc_name": best_name, **toc[best_name]}

    if isin:
        # El ISIN puede aparecer en la clave (poco común) o no estar en el TOC
        isin_upper = isin.strip().upper()
        for name, info in toc.items():
            if isin_upper in name.upper():
                return {"toc_name": name, **info}

    console.log(f"[yellow]Fondo no encontrado en TOC (name={fund_name}, isin={isin})")
    return {}
