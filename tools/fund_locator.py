"""
Localizador robusto de páginas de un sub-fondo en un PDF umbrella SICAV.

Combina 5 anchors (unión, no intersección):
  A. TOC + offset — parse_toc / find_fund_in_toc / calculate_pdf_offset
  B. ISIN directo — regex en cada página
  C. Nombre del fondo + aliases — match case-insensitive de palabras distintivas
  D. Sub-fund boundary — detectar inicio/fin leyendo otros ISINs
  E. Canonical headings — cabeceras estándar de sección cercanas a A/B/C

Devuelve listas de páginas 0-indexed separando "fund_pages" (específicas del
sub-fondo) y "general_pages" (estrategia, mercado, filosofía generales al SICAV).

Firma:
    locate_fund_pages(pdf_path, isin, fund_name, aliases=None,
                      margin=2, include_general=True) -> dict
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pdfplumber
from rich.console import Console

from tools.pdf_extractor import (
    calculate_pdf_offset,
    extract_page_range,
    find_fund_in_toc,
    get_pdf_metadata,
    parse_toc,
)

console = Console()


CANONICAL_HEADINGS = [
    "Directors' Report",
    "Investment Manager's Report",
    "Fund Manager's Report",
    "Management Report",
    "Statement of Net Assets",
    "Statement of Operations",
    "Financial Position",
    "Schedule of Investments",
    "Portfolio of Investments",
    "Securities Portfolio",
    "Top Ten Holdings",
    "Top 10 Holdings",
    "Portfolio Breakdown",
    "Statement of Changes",
]

GENERAL_KEYWORDS = [
    "Market Commentary", "Market Review", "Investment Environment",
    "Macroeconomic", "Macro Outlook", "Economic Review",
    "Outlook", "Chairman", "Chairman's Letter",
    "Notes to the Financial Statements",
    "Investment Strategy", "Investment Philosophy",
]

MAX_FUND_BLOCK = 20   # pp. máximas por sub-fondo tras anchor TOC
MAX_GENERAL = 25      # cap pp. generales
SHORT_PDF_THRESHOLD = 30  # PDFs más pequeños → texto completo


def _isin_pattern(prefix: str = r"[A-Z]{2}") -> re.Pattern:
    return re.compile(rf"\b{prefix}\d{{9}}\d\b")


def _read_pages_text(pdf_path: str) -> list[str]:
    """Devuelve lista con el texto de cada página (0-indexed)."""
    with pdfplumber.open(pdf_path) as pdf:
        return [(p.extract_text() or "") for p in pdf.pages]


def _expand_with_margin(pages: set[int], margin: int, total: int) -> set[int]:
    out: set[int] = set()
    for p in pages:
        for d in range(-margin, margin + 1):
            q = p + d
            if 0 <= q < total:
                out.add(q)
    return out


def _derive_aliases(fund_name: str) -> list[str]:
    """Palabras distintivas del nombre (evita tokens genéricos)."""
    if not fund_name:
        return []
    NOISE = {
        "fund", "funds", "plc", "ltd", "sa", "sicav", "ucits", "company",
        "the", "of", "and", "for", "in", "a", "an",
        "acc", "inc", "eur", "usd", "gbp", "chf", "jpy",
        "class", "share", "shares", "accumulation", "distribution",
        "investment", "investments", "capital", "asset", "management",
    }
    tokens = re.findall(r"[A-Za-z]{3,}", fund_name)
    out = []
    for t in tokens:
        low = t.lower()
        if low not in NOISE and t not in out:
            out.append(t)
    # Añade nombre completo como alias
    if fund_name not in out:
        out.insert(0, fund_name)
    return out[:6]


def _anchor_toc(pdf_path: str, fund_name: str) -> tuple[set[int], int | None]:
    """Anchor A. Devuelve páginas 0-indexed + doc_page del TOC (para blocks)."""
    try:
        toc = parse_toc(pdf_path)
    except Exception:
        return set(), None
    if not toc:
        return set(), None
    try:
        offset = calculate_pdf_offset(pdf_path, toc)
    except Exception:
        offset = 0
    try:
        fund_info = find_fund_in_toc(toc, fund_name=fund_name)
    except Exception:
        fund_info = None
    if not fund_info:
        return set(), None
    doc_page = fund_info.get("doc_page")
    if doc_page is None:
        return set(), None
    start = doc_page + offset - 1  # TOC es 1-indexed, pdfplumber 0-indexed
    return set(range(max(0, start), start + MAX_FUND_BLOCK)), doc_page


def _anchor_isin(pages_text: list[str], isin: str) -> set[int]:
    """Anchor B."""
    hits: set[int] = set()
    pat = re.compile(rf"\b{re.escape(isin)}\b")
    for i, txt in enumerate(pages_text):
        if pat.search(txt):
            hits.add(i)
    return hits


def _anchor_name(pages_text: list[str], aliases: list[str]) -> set[int]:
    """Anchor C. Requiere match del nombre completo O de ≥2 alias distintivos."""
    if not aliases:
        return set()
    fullname = aliases[0]
    distinctive = aliases[1:]  # tokens
    patterns = [re.compile(re.escape(fullname), re.I)]
    for tok in distinctive:
        if len(tok) >= 4:
            patterns.append(re.compile(rf"\b{re.escape(tok)}\b", re.I))
    hits: set[int] = set()
    for i, txt in enumerate(pages_text):
        # Match del nombre completo cuenta solo; o ≥2 tokens distintivos
        if patterns[0].search(txt):
            hits.add(i)
            continue
        if len(distinctive) >= 2:
            n = sum(1 for p in patterns[1:] if p.search(txt))
            if n >= 2:
                hits.add(i)
    return hits


def _anchor_boundary(pages_text: list[str], isin: str, seed: set[int]) -> set[int]:
    """
    Anchor D. Dado un seed de páginas relevantes, amplía hasta encontrar
    límite de sub-fondo (página con OTRO ISIN del mismo país que NO sea el
    nuestro en contexto de título/header).
    """
    if not seed:
        return set()
    country = isin[:2]
    isin_pat = _isin_pattern(country)
    hits = set(seed)
    total = len(pages_text)

    for start in sorted(seed):
        # Ampliar hacia adelante hasta otro ISIN distinto
        for j in range(start + 1, min(total, start + MAX_FUND_BLOCK)):
            txt = pages_text[j]
            others = [m.group(0) for m in isin_pat.finditer(txt) if m.group(0) != isin]
            if len(others) >= 2:
                break  # otro sub-fondo empieza
            hits.add(j)
    return hits


def _anchor_headings(pages_text: list[str], seed: set[int], window: int = 3) -> set[int]:
    """Anchor E. Páginas con canonical headings a ±window de un hit del seed."""
    if not seed:
        return set()
    heads_pat = re.compile(
        "(" + "|".join(re.escape(h) for h in CANONICAL_HEADINGS) + ")",
        re.IGNORECASE,
    )
    seed_sorted = sorted(seed)
    min_s, max_s = seed_sorted[0], seed_sorted[-1]
    hits: set[int] = set()
    for i, txt in enumerate(pages_text):
        if not heads_pat.search(txt):
            continue
        # Cercanía: dentro del rango del seed o ±window de algún hit
        if min_s - window <= i <= max_s + window:
            hits.add(i)
    return hits


def _general_pages(pages_text: list[str], exclude: set[int]) -> list[int]:
    """Páginas con keywords generales (mercado, estrategia, outlook)."""
    total = len(pages_text)
    pat = re.compile(
        "(" + "|".join(re.escape(k) for k in GENERAL_KEYWORDS) + ")",
        re.IGNORECASE,
    )
    # Primeras 15 páginas (intro, chairman, directors' report global)
    out = [i for i in range(min(15, total)) if i not in exclude]
    # Keywords
    for i in range(15, total):
        if i in exclude or i in out:
            continue
        if pat.search(pages_text[i]):
            out.append(i)
        if len(out) >= MAX_GENERAL:
            break
    return out[:MAX_GENERAL]


def locate_fund_pages(
    pdf_path: str,
    isin: str,
    fund_name: str | None = None,
    aliases: list[str] | None = None,
    margin: int = 2,
    include_general: bool = True,
) -> dict[str, Any]:
    """
    Devuelve:
      {
        "fund_pages": [int],     # 0-indexed
        "general_pages": [int],
        "total": int,
        "strategy": str,         # anchors usados, debug
      }

    Si el PDF tiene <30 páginas, devuelve todas las páginas como fund_pages
    (y general_pages vacío) — no merece la pena recortar.
    """
    meta = get_pdf_metadata(pdf_path)
    total = meta.get("num_pages", 0)
    if total == 0:
        return {"fund_pages": [], "general_pages": [], "total": 0, "strategy": "empty"}

    if total < SHORT_PDF_THRESHOLD:
        return {
            "fund_pages": list(range(total)),
            "general_pages": [],
            "total": total,
            "strategy": "short_pdf_full",
        }

    if aliases is None and fund_name:
        aliases = _derive_aliases(fund_name)
    aliases = aliases or []

    pages_text = _read_pages_text(pdf_path)

    strategies = []
    fund_pages: set[int] = set()

    # Anchor A — TOC
    a_toc, _doc_page = _anchor_toc(pdf_path, fund_name or "")
    if a_toc:
        fund_pages |= a_toc
        strategies.append(f"toc:{len(a_toc)}")

    # Anchor B — ISIN
    a_isin = _anchor_isin(pages_text, isin)
    if a_isin:
        fund_pages |= a_isin
        strategies.append(f"isin:{len(a_isin)}")

    # Anchor C — name + aliases
    a_name = _anchor_name(pages_text, aliases)
    if a_name:
        fund_pages |= a_name
        strategies.append(f"name:{len(a_name)}")

    # Anchor D — boundary (amplía seed ABC)
    if fund_pages:
        a_boundary = _anchor_boundary(pages_text, isin, fund_pages)
        added = a_boundary - fund_pages
        if added:
            fund_pages |= a_boundary
            strategies.append(f"boundary:+{len(added)}")

    # Anchor E — canonical headings cerca del seed
    a_heads = _anchor_headings(pages_text, fund_pages, window=3)
    added = a_heads - fund_pages
    if added:
        fund_pages |= a_heads
        strategies.append(f"headings:+{len(added)}")

    # Si muy pocas páginas → fallback: usar solo headings globales
    if len(fund_pages) < 4:
        all_heads = _anchor_headings(pages_text, set(range(total)), window=0)
        fund_pages |= all_heads
        strategies.append(f"all_heads:+{len(all_heads)}")

    # Margen ±
    fund_pages = _expand_with_margin(fund_pages, margin, total)

    general_pages_sorted: list[int] = []
    if include_general:
        general_pages_sorted = _general_pages(pages_text, exclude=fund_pages)
        strategies.append(f"general:{len(general_pages_sorted)}")

    return {
        "fund_pages": sorted(fund_pages),
        "general_pages": general_pages_sorted,
        "total": total,
        "strategy": "+".join(strategies) or "no_anchor",
    }


def render_pages(pdf_path: str, pages: list[int]) -> str:
    """
    Extrae texto de una lista de páginas 0-indexed preservando orden.
    Agrupa en rangos contiguos para minimizar llamadas a pdfplumber.
    """
    if not pages:
        return ""
    pages = sorted(set(pages))
    # Agrupa en rangos contiguos
    ranges = []
    start = pages[0]
    prev = start
    for p in pages[1:]:
        if p == prev + 1:
            prev = p
            continue
        ranges.append((start, prev + 1))
        start = p
        prev = p
    ranges.append((start, prev + 1))

    parts: list[str] = []
    for s, e in ranges:
        parts.append(extract_page_range(pdf_path, s, e))
    return "\n\n".join(parts)
