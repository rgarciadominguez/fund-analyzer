"""
Adapter Serper.dev → web_search_fn requerido por el discovery.

Uso:
    from agents.discovery.serper_adapter import make_web_search_fn
    fn = make_web_search_fn(isin)
    results = await fn("query string")
    # → [{"title": "...", "url": "...", "snippet": "..."}]

El adapter:
  - Reusa el SearchEngine de tools/google_search.py (con caché por ISIN)
  - Permite localización (gl/hl) por país del fondo según prefijo ISIN
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tools.google_search import SearchEngine


# País por prefijo ISIN para localizar Serper (gl/hl)
_LOCALE_BY_PREFIX = {
    "LU": ("lu", "en"),
    "IE": ("ie", "en"),
    "FR": ("fr", "fr"),
    "DE": ("de", "de"),
    "GB": ("gb", "en"),
    "ES": ("es", "es"),
}


def make_web_search_fn(isin: str):
    """
    Devuelve un callable async (query: str) -> list[{title, url, snippet}]
    listo para inyectar en IntlDiscoveryAgent.
    """
    engine = SearchEngine(isin=isin)

    async def web_search(query: str) -> list[dict]:
        results = await engine.search(query, num=8, agent="intl_discovery")
        # Devuelve como lo espera google_finder.search_google: dict con url
        return results

    return web_search
