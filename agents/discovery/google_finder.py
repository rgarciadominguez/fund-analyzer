"""
Google finder: queries dirigidas para cubrir gaps.

Reglas de construcción de queries:
  - Preferimos búsquedas exactas con comillas
  - ISIN es el anchor más fiable: `"{ISIN}" annual report filetype:pdf`
  - Si falta un año concreto: añadir el año a la query
  - Para cartas trimestrales: inglés primero (`"quarterly letter" "{fund}"`)
  - Limitar a filetype:pdf cuando buscamos reports formales
  - Filtrar resultados: descartar dominios ruidosos (forums, blogs) y
    priorizar dominios de gestora/Natixis/fundinfo
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

from agents.discovery.state import SharedState


# Query templates por doc_type. {isin} y {fund} se sustituyen.
QUERY_TEMPLATES: dict[str, list[str]] = {
    "annual_report": [
        '"{isin}" "annual report" filetype:pdf',
        '"{isin}" "rapport annuel" filetype:pdf',
        '"{fund}" "annual report" {year} filetype:pdf',
    ],
    "semi_annual_report": [
        '"{isin}" "semi-annual report" filetype:pdf',
        '"{isin}" "rapport semestriel" filetype:pdf',
        '"{fund}" "semi-annual" {year} filetype:pdf',
    ],
    "quarterly_letter": [
        '"{fund}" "quarterly letter" filetype:pdf',
        '"{fund}" "letter to shareholders" {year} filetype:pdf',
        '"{isin}" "carta trimestral" OR "quarterly letter"',
    ],
    "factsheet": [
        '"{isin}" factsheet filetype:pdf',
        '"{fund}" "monthly report" filetype:pdf',
    ],
    "prospectus": [
        '"{isin}" prospectus filetype:pdf',
    ],
    "kid": [
        '"{isin}" KID OR KIID OR PRIIPS filetype:pdf',
    ],
    "manager_presentation": [
        '"{fund}" presentation filetype:pdf',
        '"{fund}" "investor" OR "webinar" filetype:pdf',
    ],
}


# Dominios que preferimos (score alto) — gestoras, CDNs de parent groups,
# fundinfo, bundesanzeiger (directo)
PREFERRED_DOMAIN_HINTS = [
    "-am.", "-investments.", "investments.", "funds.", "fund.",
    ".im.natixis.com",  "api.fundinfo.com", "fefundinfo.com",
    "bundesanzeiger.de",
    "amundi", "blackrock", "ishares", "vanguard", "carmignac",
    "pictet", "jpmorgan", "pimco", "dnca", "groupama", "allianz",
    "fidelity", "schroders", "mfs", "mfsinvestments", "ubs",
    "dws", "deka", "invesco", "natixis",
]

# Dominios que descartamos (ruido)
BAD_DOMAIN_HINTS = [
    "wikipedia.", "investopedia.", "reddit.com", "quora.com",
    "tradingview.com", "stocktwits.com", "youtube.com",
    "facebook.com", "linkedin.com", "twitter.com", "x.com",
]


def _year_from_periodo(periodo: str) -> str:
    """Extrae el año principal del periodo."""
    if not periodo:
        return ""
    m = re.search(r"\b(20\d{2})\b", periodo)
    return m.group(1) if m else ""


def build_queries(doc_type: str, periodo: str, isin: str, fund_name: str) -> list[str]:
    """Genera queries Google ordenadas (del más general al más específico)."""
    templates = QUERY_TEMPLATES.get(doc_type, [])
    year = _year_from_periodo(periodo)
    queries = []
    for t in templates:
        try:
            q = t.format(isin=isin, fund=fund_name or isin, year=year).strip()
        except KeyError:
            continue
        # Si no hay año y la plantilla lo usa, limpiar "  filetype" → "filetype"
        q = re.sub(r"\s{2,}", " ", q).strip()
        queries.append(q)
    return queries


def score_url(url: str) -> int:
    """Scoring: más alto = más confiable."""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return 0
    if any(bad in host for bad in BAD_DOMAIN_HINTS):
        return -100
    score = 0
    for pref in PREFERRED_DOMAIN_HINTS:
        if pref in host:
            score += 10
    if url.lower().endswith(".pdf"):
        score += 5
    return score


async def search_google(
    state: SharedState, query: str, web_search_fn,
) -> list[dict]:
    """
    Ejecuta una query en Google via web_search_fn (inyectada).
    web_search_fn(query: str) -> list[{title, url}]
    Dedup vs state.google_queries_done + budget.
    """
    if state.google_done(query):
        return []
    if not state.budget.try_google():
        return []
    await state.mark_google_done(query)
    try:
        results = await web_search_fn(query) or []
    except Exception:
        return []
    # Filtrar y puntuar
    scored = []
    for r in results:
        url = r.get("url") or r.get("href") or ""
        if not url:
            continue
        s = score_url(url)
        if s < 0:
            continue
        scored.append({**r, "url": url, "_score": s})
    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored
