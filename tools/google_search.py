"""
Google Search tool — búsqueda centralizada via Serper.dev

Todos los agentes usan esta herramienta en vez de DDG scraping.
Serper.dev: 2500 búsquedas gratis, resultados reales de Google.

Incluye caché de búsquedas por ISIN: evita duplicar búsquedas y
comparte resultados relevantes entre agentes.

Usage:
    from tools.google_search import SearchEngine

    engine = SearchEngine(isin="ES0112231008")  # con caché por fondo

    results = await engine.search("juan gomez bada citywire", num=3)
    # → [{"title": "...", "url": "...", "snippet": "..."}]

    pages = await engine.search_and_fetch("avantage fund morningstar", num=3)
    # → [{"title": "...", "url": "...", "text": "..."}]

    # Obtener todos los resultados previos que interesan a un agente
    relevant = engine.get_cached_for_agent("manager_deep")
    # → resultados previos etiquetados como relevantes para ese agente
"""
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.http_client import get_with_headers

# ── Config ───────────────────────────────────────────────────────────────────

_SERPER_URL = "https://google.serper.dev/search"
_HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}
_RATE_LIMIT_SECONDS = 1.0
_last_search_time = 0.0

# Keywords que indican relevancia para cada agente
_AGENT_KEYWORDS = {
    "manager_deep": ["gestor", "manager", "equipo", "citywire", "trustnet", "morningstar equipo",
                      "entrevista", "biografía", "trayectoria", "curriculum", "linkedin", "compromiso"],
    "readings": ["análisis", "opinión", "reseña", "rankia", "finect", "substack", "astralis",
                  "morningstar", "salud financiera", "masdividendos"],
    "letters": ["carta", "informe", "trimestral", "semestral", "anual", "pdf", "letter"],
    "sources": ["morningstar", "rankia", "finect", "citywire", "youtube", "podcast", "cnmv"],
}


def _get_serper_key() -> str:
    key = os.getenv("SERPER_API_KEY", "")
    if not key:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).parent.parent / ".env")
        key = os.getenv("SERPER_API_KEY", "")
    return key


# ── Search Engine with cache ────────────────────────────────────────────────

class SearchEngine:
    """
    Google search via Serper.dev with per-fund caching.
    Avoids duplicate searches and shares results between agents.
    """

    def __init__(self, isin: str = ""):
        self.isin = isin
        root = Path(__file__).parent.parent
        if isin:
            self._cache_path = root / "data" / "funds" / isin / "search_cache.json"
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            self._cache_path = None
        self._cache: dict = self._load_cache()

    def _load_cache(self) -> dict:
        if self._cache_path and self._cache_path.exists():
            try:
                return json.loads(self._cache_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"queries": {}, "urls": {}}

    def _save_cache(self):
        if self._cache_path:
            self._cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    def _is_cached(self, query: str) -> bool:
        return query.lower().strip() in self._cache.get("queries", {})

    def _get_cached(self, query: str) -> list[dict]:
        return self._cache.get("queries", {}).get(query.lower().strip(), {}).get("results", [])

    def _store_results(self, query: str, results: list[dict], agent: str = ""):
        q_key = query.lower().strip()
        self._cache.setdefault("queries", {})[q_key] = {
            "results": results,
            "timestamp": datetime.now().isoformat(),
            "agent": agent,
        }
        # Also index by URL for dedup
        for r in results:
            url = r.get("url", "")
            if url:
                self._cache.setdefault("urls", {})[url] = {
                    "title": r.get("title", ""),
                    "snippet": r.get("snippet", ""),
                    "queries": list(set(
                        self._cache.get("urls", {}).get(url, {}).get("queries", []) + [q_key]
                    )),
                }
        self._save_cache()

    # ── Public API ───────────────────────────────────────────────────────────

    async def search(self, query: str, num: int = 5, agent: str = "") -> list[dict]:
        """
        Search Google. Returns cached results if query already done.
        Returns: [{"title": str, "url": str, "snippet": str}]
        """
        # Check cache first
        if self._is_cached(query):
            return self._get_cached(query)

        global _last_search_time
        key = _get_serper_key()
        if not key:
            return []

        # Rate limit
        now = asyncio.get_event_loop().time()
        wait = _RATE_LIMIT_SECONDS - (now - _last_search_time)
        if wait > 0:
            await asyncio.sleep(wait)

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    _SERPER_URL,
                    headers={"X-API-KEY": key, "Content-Type": "application/json"},
                    json={"q": query, "num": num, "gl": "es", "hl": "es"},
                )
            _last_search_time = asyncio.get_event_loop().time()
            data = resp.json()
            results = []
            for item in data.get("organic", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                })
            self._store_results(query, results, agent)
            return results
        except Exception as exc:
            print(f"[SEARCH] Error: {exc}")
            return []

    async def search_multiple(self, queries: list[str], num_per_query: int = 3,
                               agent: str = "") -> list[dict]:
        """
        Multiple searches, deduplicate by URL.
        Returns: [{"title": str, "url": str, "snippet": str, "query": str}]
        """
        seen_urls: set[str] = set()
        all_results: list[dict] = []

        for query in queries:
            results = await self.search(query, num=num_per_query, agent=agent)
            for r in results:
                url = r["url"]
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    r["query"] = query
                    all_results.append(r)

        return all_results

    def get_cached_for_agent(self, agent_name: str) -> list[dict]:
        """
        Get all cached URLs that are relevant for a specific agent.
        Uses keyword matching to determine relevance.
        """
        keywords = _AGENT_KEYWORDS.get(agent_name, [])
        if not keywords:
            return []

        relevant: list[dict] = []
        seen: set[str] = set()

        for url, info in self._cache.get("urls", {}).items():
            if url in seen:
                continue
            combined = (
                info.get("title", "") + " " +
                info.get("snippet", "") + " " +
                " ".join(info.get("queries", []))
            ).lower()
            if any(kw in combined for kw in keywords):
                seen.add(url)
                relevant.append({
                    "title": info.get("title", ""),
                    "url": url,
                    "snippet": info.get("snippet", ""),
                })

        return relevant

    def get_all_cached_urls(self) -> list[dict]:
        """Get all unique URLs found across all searches."""
        return [
            {"title": info.get("title", ""), "url": url, "snippet": info.get("snippet", "")}
            for url, info in self._cache.get("urls", {}).items()
        ]


# ── Fetch page text ──────────────────────────────────────────────────────────

async def fetch_page_text(url: str, max_chars: int = 5000) -> str:
    """
    Fetch a URL and extract clean text content.
    Handles trailing slash issues (some servers return 500 with slash but 200 without).
    Returns empty string on error.
    """
    skip_domains = ("linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com")
    if any(d in url for d in skip_domains):
        return ""

    async def _try_fetch(u: str) -> str:
        try:
            html = await get_with_headers(u, _HEADERS_WEB)
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            return text[:max_chars] if max_chars else text
        except Exception:
            return ""

    # Try original URL first
    text = await _try_fetch(url)
    if text and len(text) > 100:
        return text

    # If failed and URL ends with /, try without trailing slash (and vice versa)
    if url.endswith("/"):
        alt = url.rstrip("/")
    else:
        alt = url + "/"
    text = await _try_fetch(alt)
    return text


async def search_and_fetch(
    query: str, num: int = 3, max_chars_per_page: int = 4000
) -> list[dict]:
    """
    Search Google + fetch content from each result.
    Returns: [{"title": str, "url": str, "snippet": str, "text": str}]
    Only includes results where text was successfully extracted.
    """
    results = await search(query, num=num)
    fetched = []

    for r in results:
        text = await fetch_page_text(r["url"], max_chars=max_chars_per_page)
        if text and len(text) > 200:
            r["text"] = text
            fetched.append(r)

    return fetched


async def search_fetch_multiple(
    queries: list[str], num_per_query: int = 3, max_pages: int = 10, max_chars: int = 4000
) -> list[dict]:
    """
    Multiple searches + fetch, deduplicate, limit total pages.
    Returns: [{"title": str, "url": str, "snippet": str, "text": str, "query": str}]
    """
    all_urls = await search_multiple(queries, num_per_query)
    fetched: list[dict] = []

    for r in all_urls[:max_pages]:
        text = await fetch_page_text(r["url"], max_chars=max_chars)
        if text and len(text) > 200:
            r["text"] = text
            fetched.append(r)

    return fetched


# ── CLI test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    query = sys.argv[1] if len(sys.argv) > 1 else "avantage fund morningstar"

    async def main():
        results = await search(query, num=5)
        print(f"Query: {query}")
        print(f"Results: {len(results)}")
        for r in results:
            print(f"  {r['title'][:55]:55s} {r['url'][:80]}")

    asyncio.run(main())
