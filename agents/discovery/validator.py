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


# Patrones de detección por tipo de documento dentro del texto extraído.
# Usados con CONTEO de ocurrencias: el tipo dominante (más repetido) gana.
# Ej: un Prospectus menciona "annual report" 1 vez (referencia legal) pero
# "prospectus" 30 veces — clasificación correcta = prospectus.
CONTENT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(annual\s*report|rapport\s+annuel|jahresbericht|rechenschaftsbericht|informe\s+anual|memoria\s+anual)\b", re.I), "annual_report"),
    (re.compile(r"\b(semi[-\s]?annual\s*report|rapport\s+semestriel|halbjahresbericht|halbjahres|informe\s+semestral)\b", re.I), "semi_annual_report"),
    (re.compile(r"\b(quarterly\s+letter|letter\s+to\s+(share|unit)holders?|investor\s+letter|carta\s+trimestral|carta\s+a\s+inversor|lettre\s+trimestrielle)\b", re.I), "quarterly_letter"),
    (re.compile(r"\b(factsheet|fact\s+sheet|monthly\s*report|monatsbericht|ficha\s+(mensual|del\s+fondo)|reporting\s+mensuel)\b", re.I), "factsheet"),
    (re.compile(r"\b(prospectus|prospekt|verkaufsprospekt|folleto\s+informativo)\b", re.I), "prospectus"),
    (re.compile(r"\b(kid|kiid|key\s+information\s+document|wesentliche\s+anlegerinformationen|datos\s+fundamentales)\b", re.I), "kid"),
    (re.compile(r"\b(presentation|pitch\s+deck|investor\s+day|investor\s+presentation)\b", re.I), "manager_presentation"),
]


def classify_content(text: str) -> tuple[set[str], str]:
    """
    Devuelve (contains_set, dominant_type).
    contains = todos los tipos detectados (≥1 ocurrencia).
    dominant_type = el de MAYOR número de ocurrencias en el texto.
    """
    counts: dict[str, int] = {}
    for pat, kind in CONTENT_PATTERNS:
        n = len(pat.findall(text))
        if n > 0:
            counts[kind] = counts.get(kind, 0) + n
    if not counts:
        return set(), ""
    # Empate: prefiere el tipo más específico (orden CONTENT_PATTERNS, semi antes
    # que annual, prospectus antes que annual, etc.) — invertimos para empate
    PRIORITY_TIE = ["semi_annual_report", "kid", "factsheet", "prospectus",
                    "quarterly_letter", "manager_presentation", "annual_report"]
    max_n = max(counts.values())
    tied = [k for k, n in counts.items() if n == max_n]
    if len(tied) == 1:
        dominant = tied[0]
    else:
        for k in PRIORITY_TIE:
            if k in tied:
                dominant = k
                break
        else:
            dominant = tied[0]
    return set(counts.keys()), dominant


def _extract_text_for_validation(path: Path) -> str:
    """Extrae texto relevante para validar. Cover + TOC (donde suelen
    aparecer todos los ISINs del SICAV) + primeras secciones."""
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(path) as pdf:
                chunks = []
                n_pages = len(pdf.pages)
                # Estrategia: primeras 5 págs completas + 25 págs siguientes
                # con ~500 chars (suficiente para detectar ISINs en TOC).
                for i, page in enumerate(pdf.pages[:5]):
                    t = page.extract_text() or ""
                    chunks.append(t[:1500])
                for i, page in enumerate(pdf.pages[5:30]):
                    t = page.extract_text() or ""
                    chunks.append(t[:500])
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
    path: Path,
    isin: str,
    fund_name: str = "",
    sicav_paraguas: str = "",
) -> tuple[bool, set[str]]:
    """
    Devuelve (is_valid, contains_set).

    Validación RELAJADA: en gestoras grandes, el AR es del SICAV completo y
    contiene 20-50 sub-fondos. Aceptamos cualquiera de:
      a) ISIN del fondo objetivo aparece en el texto
      b) Nombre del SICAV-paraguas aparece (≥1 palabra significativa)
      c) Nombre del sub-fondo aparece (≥30% tokens significativos)

    El extractor decidirá luego qué páginas son las del fondo.
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
    text_low = text.lower()
    text_up = text.upper()

    # 3. Múltiples vías de match
    isin_present = isin.upper() in text_up
    sicav_present = False
    name_present = False

    # Para SICAV/nombre exigimos al menos 2 tokens significativos coincidentes,
    # PARA EVITAR falsos positivos cuando solo queda 1 token genérico (ej. "Storm").
    if sicav_paraguas:
        sicav_tokens = [t for t in re.findall(r"[A-Za-z]{4,}", sicav_paraguas) if t.lower() not in _STOP]
        if len(sicav_tokens) >= 2:
            hits = sum(1 for t in sicav_tokens if t.lower() in text_low)
            sicav_present = hits >= 2
        elif len(sicav_tokens) == 1:
            # Solo 1 token significativo: requerimos también que aparezca ISIN o
            # alguna pista del nombre del subfondo
            sicav_present = sicav_tokens[0].lower() in text_low and isin_present

    if fund_name:
        name_tokens = [t for t in re.findall(r"[A-Za-z]{4,}", fund_name) if t.lower() not in _STOP]
        if len(name_tokens) >= 2:
            hits = sum(1 for t in name_tokens if t.lower() in text_low)
            name_present = hits >= 2  # al menos 2 tokens
        elif len(name_tokens) == 1:
            name_present = name_tokens[0].lower() in text_low and isin_present

    if not (isin_present or sicav_present or name_present):
        return False, set()

    # 4. Content-index (con conteo, ya devuelve set + dominant)
    contains, _dominant = classify_content(text)
    return True, contains


# Patrones típicos de "commentary" / "manager view" dentro de cualquier doc.
# AMPLIADOS para cubrir factsheets típicos (más coloquiales que ARs).
_COMMENTARY_PATTERNS = [
    # Headings tipo "Commentary", "Manager's report"
    re.compile(r"\b(manager[''`]?s?\s+)?commentary\b", re.I),
    re.compile(r"\bmanager[''`]?s?\s+(report|view|comment|letter|note)\b", re.I),
    re.compile(r"\b(market|investment|monthly|quarterly|annual)\s+(overview|outlook|commentary|review|update)\b", re.I),
    re.compile(r"\bdirectors?[''`]?\s+report\b", re.I),
    re.compile(r"\bfund\s+manager[''`]?s?\s+(notes|comments|view|outlook)\b", re.I),
    re.compile(r"\bperformance\s+(review|attribution|analysis|drivers)\b", re.I),
    re.compile(r"\b(year|month|quarter|period)\s+in\s+review\b", re.I),
    re.compile(r"\boutlook\s+(for|on)\s+(20\d{2}|the\s+(year|quarter|month))\b", re.I),
    # Typical factsheet section headers
    re.compile(r"\b(market|fund)\s+update\b", re.I),
    re.compile(r"\bportfolio\s+(positioning|changes|activity|review)\b", re.I),
    # ES
    re.compile(r"\b(comentario|comentarios?)\s+del?\s+gestor\b", re.I),
    re.compile(r"\bvisi[oó]n\s+del?\s+(gestor|mercado)\b", re.I),
    re.compile(r"\bperspectivas?\s+(del?\s+mercado|para\s+el\s+(a[ñn]o|trimestre))\b", re.I),
    re.compile(r"\bcomentario\s+(de\s+mercado|mensual|trimestral|anual)\b", re.I),
    # FR
    re.compile(r"\blettre\s+(du|des)\s+g[eé]rant\b", re.I),
    re.compile(r"\bcommentaire\s+(de\s+gestion|du\s+g[eé]rant|mensuel|trimestriel)\b", re.I),
    # DE
    re.compile(r"\bmanagerbericht|anlagemanager[-\s]+kommentar", re.I),
    re.compile(r"\bmarktkommentar|fondsmanagerkommentar", re.I),
    # NL / NO / SV (Storm es nórdico)
    re.compile(r"\bbeheerdersbrief|kommentar\s+fra\s+forvalter", re.I),
]


def detect_manager_commentary(path: Path) -> bool:
    """
    ¿El documento contiene commentary del gestor?

    Estrategia en 3 fases:
      1. Texto base (primeras 5 págs completas + 25 págs con 500 chars)
      2. Para factsheets (PDFs <2MB con pocas páginas): leer TODAS las páginas
         completas (suelen tener commentary en pg. 2 o 3, no en cover)
      3. Para ARs grandes (>500KB, >30 págs): escaneo extendido pp. 30-100
    """
    suffix = path.suffix.lower()
    if suffix not in (".pdf", ".html", ".htm"):
        return False

    # Fase 1: chequeo barato
    text = _extract_text_for_validation(path)
    if text and any(p.search(text) for p in _COMMENTARY_PATTERNS):
        return True

    if suffix != ".pdf":
        return False

    try:
        size = path.stat().st_size
    except Exception:
        return False

    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            n_pages = len(pdf.pages)

            # Fase 2: factsheets (chicos, pocas páginas) — escaneo COMPLETO
            if size < 2_000_000 and n_pages <= 12:
                full = "\n".join(
                    (p.extract_text() or "") for p in pdf.pages
                )
                if any(p.search(full) for p in _COMMENTARY_PATTERNS):
                    return True

            # Fase 3: ARs grandes — escaneo extendido pp. 30-100
            elif size >= 500_000 and n_pages >= 30:
                for i, page in enumerate(pdf.pages[30:100]):
                    chunk = (page.extract_text() or "")[:1500]
                    if any(p.search(chunk) for p in _COMMENTARY_PATTERNS):
                        return True
    except Exception:
        pass
    return False


# Stop words que NO cuentan para matching de nombres (son demasiado genéricas)
_STOP = {
    "fund", "funds", "fondo", "fondos", "fonds", "invest", "investment",
    "investments", "asset", "management", "global", "europe", "european",
    "international", "capital", "company", "limited", "ltd", "sicav",
    "fcp", "ucits", "select", "selection", "class", "classe", "icav",
}


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


def detect_isins_in_doc(path: Path) -> set[str]:
    """
    Extrae todos los ISINs distintos presentes en el documento.
    Útil para detectar si es un annual report umbrella del SICAV
    (que cubre 20-50 sub-fondos).
    """
    text = _extract_text_for_validation(path)
    if not text:
        return set()
    # ISINs ISO: 2 letras país + 10 alfanuméricos. Filtramos prefijos típicos.
    matches = re.findall(r"\b(LU|IE|FR|DE|GB|ES|AT|NL|BE|IT|FI|SE|DK|NO|CH)([0-9A-Z]{10})\b", text)
    return {p + s for p, s in matches}


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
