"""
intl_url_filter — pre-filtro de URLs HTML para el fallback INT.

Fix G13 (branch v2-cowork, 2026-05-19): gestoras como Amiral Gestion NO publican
annual reports descargables; toda la información del sub-fondo vive en HTML.
Antes de gastar tokens Gemini Flash en cada URL harvested por discovery,
descartamos las que claramente NO son páginas del sub-fondo (corporate boilerplate,
journal de bord generales, etc.).

Uso:
    from tools.intl_url_filter import is_fund_page_url, rank_fund_page_urls

    if is_fund_page_url(url, isin="FR001400CEK6", fund_name="Sextant Quality Focus"):
        # vale la pena fetchear + extraer

    ranked = rank_fund_page_urls(urls, isin, fund_name)  # ordenadas best-first
"""
from __future__ import annotations

import re
from urllib.parse import urlparse


# Paths que NUNCA son del sub-fondo (corporate / noticias generales)
_BLACKLIST_PATHS = (
    "/notre-vision", "/nuestra-vision", "/our-vision", "/unsere-vision",
    "/investissement-responsable", "/responsible-investment",
    "/inversion-responsable", "/investimento-responsavel",
    "/equipe", "/team", "/equipo", "/about-us", "/about", "/qui-sommes-nous",
    "/carrieres", "/careers", "/carreras", "/jobs",
    "/informations-reglementaires", "/regulatory", "/legal", "/mentions-legales",
    "/privacy", "/cookies", "/contact", "/contacto",
    "/journal-de-bord", "/diario-de-abordo", "/captains-log",
    "/press-release", "/press-releases", "/comunicados", "/news/", "/noticias/",
    "/gestion-privee", "/gestion-privada", "/private-management",
    "/podcast", "/podcasts",
)

# Sub-strings que sugieren contenido temporal genérico (noticias sectoriales)
# Los keywords del fondo en la URL pueden anular esta blacklist.
_BLACKLIST_KEYWORDS = (
    "actualites/journal", "actualites/l-aparte",
    "actualidades/journal", "actualidades/l-aparte",
    "negocios-tv", "podcast", "webinar",
    "press-release", "comunicado-prensa",
)

# Sub-strings que SÍ son páginas del fondo (boost en ranking)
_WHITELIST_KEYWORDS = (
    "factsheet", "fact-sheet", "ficha",
    "fund-page", "fonds", "fondo",
    "performance", "encours", "aum",
    "kid", "kiid", "prip",
    "prospectus", "folleto", "prospecto",
    "annual-report", "annualreport", "semi-annual",
    "informe-anual", "informe-semestral",
    "publications-adminmenu",  # patrón Amiral: /publications-adminmenu/<fund-slug>-<doc>
)

_ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")


def _slug_tokens(s: str, min_len: int = 4) -> list[str]:
    """Convierte 'Sextant Quality Focus' en ['sextant', 'quality', 'focus']."""
    if not s:
        return []
    tokens = re.split(r"[^a-zA-Z0-9]+", s.lower())
    return [t for t in tokens if len(t) >= min_len]


def is_fund_page_url(
    url: str,
    isin: str = "",
    fund_name: str = "",
    gestora: str = "",
) -> bool:
    """¿Vale la pena fetchear esta URL para extraer datos del sub-fondo?

    Política:
      1. Si la URL contiene el ISIN exacto → SIEMPRE pasa (señal fuerte).
      2. Si la path está en blacklist → fuera (a menos que vaya con whitelist
         override por ISIN, ya cubierto en regla 1).
      3. Si la URL contiene un blacklist keyword → fuera.
      4. Si la URL contiene un slug del fondo (>=4 chars) Y un whitelist
         keyword → pasa.
      5. Si la URL contiene la mayoría de los slug tokens del fondo → pasa.
      6. En cualquier otro caso → fuera (conservador para ahorrar tokens).
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    path_lc = (parsed.path or "").lower()
    url_lc = url.lower()
    isin_lc = (isin or "").lower()

    # Regla 1: ISIN explícito en la URL → confianza máxima
    if isin_lc and isin_lc in url_lc:
        return True

    # Regla 2: blacklist de paths corporate
    for blocked in _BLACKLIST_PATHS:
        if path_lc.startswith(blocked) or path_lc == blocked.rstrip("/"):
            return False

    # Regla 3: blacklist de keywords compuestos
    for kw in _BLACKLIST_KEYWORDS:
        if kw in url_lc:
            return False

    # Tokens del fondo
    fund_tokens = _slug_tokens(fund_name)
    gestora_tokens = _slug_tokens(gestora)
    # Quitar tokens de la gestora del set de fondo (un slug que solo coincide
    # con el nombre de la gestora no implica que la URL sea del sub-fondo).
    fund_only_tokens = [t for t in fund_tokens if t not in gestora_tokens]

    matched_fund_tokens = sum(1 for t in fund_only_tokens if t in url_lc)
    has_whitelist_kw = any(kw in url_lc for kw in _WHITELIST_KEYWORDS)

    # Regla 4: slug del fondo + whitelist keyword
    if matched_fund_tokens >= 1 and has_whitelist_kw:
        return True

    # Regla 5: la mayoría de tokens del fondo presentes
    if fund_only_tokens and matched_fund_tokens >= max(2, len(fund_only_tokens) - 1):
        return True

    # Regla 6 (default): fuera
    return False


def _score_url(url: str, isin: str, fund_name: str, gestora: str) -> float:
    """Score 0..3 para ordenar URLs candidatas (mayor = mejor)."""
    if not url:
        return 0.0
    url_lc = url.lower()
    isin_lc = (isin or "").lower()
    score = 0.0
    if isin_lc and isin_lc in url_lc:
        score += 3.0
    fund_tokens = [t for t in _slug_tokens(fund_name)
                   if t not in _slug_tokens(gestora)]
    matched = sum(1 for t in fund_tokens if t in url_lc)
    if fund_tokens:
        score += matched / len(fund_tokens)
    if any(kw in url_lc for kw in _WHITELIST_KEYWORDS):
        score += 1.0
    # Penalty leve si la URL es muy larga (suele ser slug de noticia)
    if len(url) > 150:
        score -= 0.2
    return score


def rank_fund_page_urls(
    urls: list[str],
    isin: str = "",
    fund_name: str = "",
    gestora: str = "",
    max_urls: int = 10,
) -> list[str]:
    """Devuelve hasta `max_urls` URLs que pasan el filtro, ordenadas best-first."""
    kept = [u for u in urls if is_fund_page_url(u, isin, fund_name, gestora)]
    kept.sort(key=lambda u: _score_url(u, isin, fund_name, gestora), reverse=True)
    return kept[:max_urls]


__all__ = ["is_fund_page_url", "rank_fund_page_urls"]
