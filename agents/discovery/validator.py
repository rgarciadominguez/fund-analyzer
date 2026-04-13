"""
Validator + content indexer.

Un archivo descargado es VÁLIDO si:
  1. Contiene el ISIN o el nombre oficial del fondo en el texto
  2. Es parseable (PDF: pdfplumber lo abre; HTML: bs4 lo abre; XML: ElementTree)
  3. Tamaño razonable (>20KB, <100MB)

Además: content-index para saber QUÉ tipos de doc contiene un archivo
(ej. un AR que incluye la carta anual del gestor). Los otros tracks lo
consultan para no re-buscar lo que ya está dentro.

Coste: 0 tokens (solo pdfplumber + regex).
"""
from __future__ import annotations

import re
from pathlib import Path


# Patrones de detección por tipo de documento dentro del texto extraído
CONTENT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(annual\s*report|rapport\s+annuel|jahresbericht|informe\s+anual|memoria\s+anual)\b", re.I), "annual_report"),
    (re.compile(r"\b(semi[-\s]?annual\s*report|rapport\s+semestriel|halbjahresbericht|informe\s+semestral)\b", re.I), "semi_annual_report"),
    (re.compile(r"\b(quarterly\s+letter|letter\s+to\s+(share|unit)holders?|carta\s+trimestral|carta\s+a\s+inversor|lettre\s+trimestrielle)\b", re.I), "quarterly_letter"),
    (re.compile(r"\b(factsheet|fact\s+sheet|monthly\s*report|ficha\s+(mensual|del\s+fondo)|reporting\s+mensuel)\b", re.I), "factsheet"),
    (re.compile(r"\b(prospectus|prospekt|folleto\s+informativo)\b", re.I), "prospectus"),
    (re.compile(r"\b(kid|kiid|key\s+information\s+document|wesentliche\s+anlegerinformationen|datos\s+fundamentales)\b", re.I), "kid"),
    (re.compile(r"\b(presentation|pitch\s+deck|fund\s+overview|investor\s+day)\b", re.I), "manager_presentation"),
]


def _extract_text_for_validation(path: Path) -> str:
    """Extrae hasta ~5000 chars de texto relevante del fichero para validar."""
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(path) as pdf:
                chunks = []
                # Leer primeras 8 págs (cover + TOC + cualitativo inicial)
                for i, page in enumerate(pdf.pages[:8]):
                    t = page.extract_text() or ""
                    chunks.append(t[:800])
                return "\n".join(chunks)
        except Exception:
            return ""
    if suffix in (".html", ".htm"):
        try:
            from bs4 import BeautifulSoup
            html = path.read_text(encoding="utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            return soup.get_text(" ", strip=True)[:8000]
        except Exception:
            return ""
    if suffix == ".xml":
        try:
            return path.read_text(encoding="utf-8", errors="ignore")[:8000]
        except Exception:
            return ""
    return ""


def validate_file(
    path: Path, isin: str, fund_name: str = "",
) -> tuple[bool, set[str]]:
    """
    Devuelve (is_valid, contains_set).

    is_valid = True si el archivo contiene el ISIN o el nombre del fondo,
    es parseable y tiene tamaño razonable.

    contains = conjunto de doc_types detectados dentro del archivo,
    útil para que otros tracks sepan que no hace falta re-buscar.
    """
    # 1. Tamaño
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return False, set()
    if size < 20_000 or size > 100_000_000:
        return False, set()

    # 2. Parseo + extracción de texto
    text = _extract_text_for_validation(path)
    if not text.strip():
        return False, set()

    # 3. Match del fondo
    isin_present = isin.upper() in text.upper()
    name_present = False
    if fund_name:
        # comparamos tokens significativos (longitud ≥4) del nombre
        tokens = [t for t in re.findall(r"[A-Za-z]{4,}", fund_name)]
        if tokens:
            score = sum(1 for t in tokens if t.lower() in text.lower())
            name_present = score >= max(2, len(tokens) // 2)
    if not (isin_present or name_present):
        return False, set()

    # 4. Content-index
    contains = set()
    for pat, kind in CONTENT_PATTERNS:
        if pat.search(text):
            contains.add(kind)

    return True, contains


def guess_fecha_publicacion(path: Path) -> str:
    """
    Intenta extraer la fecha del documento (publishDate o periodo de referencia).
    Devuelve ISO YYYY-MM-DD si lo encuentra, "" si no.
    """
    text = _extract_text_for_validation(path)
    # Patrones típicos en primeras páginas:
    # "as at 31 December 2024", "au 31 décembre 2024", "zum 30. September 2025"
    months_en = {"january": "01", "february": "02", "march": "03", "april": "04",
                 "may": "05", "june": "06", "july": "07", "august": "08",
                 "september": "09", "october": "10", "november": "11", "december": "12"}
    m = re.search(
        r"\b(\d{1,2})\s+("
        r"january|february|march|april|may|june|july|august|september|october|november|december"
        r")\s+(20\d{2})", text, re.I,
    )
    if m:
        d = m.group(1).zfill(2)
        mo = months_en[m.group(2).lower()]
        y = m.group(3)
        return f"{y}-{mo}-{d}"
    # ISO directo YYYY-MM-DD
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if m:
        return "-".join(m.groups())
    # DD.MM.YYYY (típico alemán)
    m = re.search(r"\b(\d{2})\.(\d{2})\.(20\d{2})\b", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # Solo año
    m = re.search(r"\b(20\d{2})\b", text)
    if m:
        return f"{m.group(1)}-12-31"
    return ""


def detect_language(path: Path) -> str:
    """
    Heurística simple para detectar idioma del documento (en/es/fr/de).
    """
    text = _extract_text_for_validation(path).lower()
    if not text:
        return ""
    scores = {
        "en": sum(1 for w in ["the", "and", "of", "fund", "report"] if f" {w} " in text),
        "es": sum(1 for w in ["del", "los", "las", "fondo", "informe"] if f" {w} " in text),
        "fr": sum(1 for w in ["du", "des", "les", "fonds", "rapport"] if f" {w} " in text),
        "de": sum(1 for w in ["der", "die", "das", "fonds", "bericht"] if f" {w} " in text),
    }
    return max(scores.items(), key=lambda kv: kv[1])[0]
