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

# ── Guardrail de coste Serper (2026-06-03) ───────────────────────────────────
# Serper.dev cobra más allá del plan gratuito (~2500 búsquedas). Discovery hace
# ~86 búsquedas/fondo y NO había tope → con 170 fondos se dispara el coste.
# Este chokepoint (todas las búsquedas pasan por SearchEngine.search) impone:
#   - SERPER_DISABLED=1            → corta TODA búsqueda (coste 0).
#   - SERPER_MONTHLY_CAP (def 2000) → tope mensual duro; al alcanzarlo, search()
#     devuelve [] y degrada (discovery encuentra menos, pero no gasta).
# Solo cuentan las llamadas REALES a la API (los cache hits NO cuentan).
import threading

_USAGE_PATH = Path(__file__).parent.parent / "data" / "serper_usage.json"
_usage_lock = threading.Lock()
_logged_once: set = set()


def _log_once(msg: str) -> None:
    if msg not in _logged_once:
        _logged_once.add(msg)
        print(f"[SERPER-GUARD] {msg}")


def _month_key() -> str:
    return datetime.now().strftime("%Y-%m")


def _serper_cap() -> int:
    try:
        return int(os.getenv("SERPER_MONTHLY_CAP", "2000"))
    except (TypeError, ValueError):
        return 2000


def _serper_disabled() -> bool:
    return os.getenv("SERPER_DISABLED", "0").lower() in ("1", "true", "yes", "on")


def _load_usage() -> dict:
    """Uso por proveedor: {provider: {period_key: count}}. Migra el formato viejo
    {YYYY-MM: int} (solo Serper) → {"serper": {YYYY-MM: int}}."""
    try:
        u = json.loads(_USAGE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if u and all(isinstance(v, int) for v in u.values()):
        u = {"serper": u}
    return u


# ── Proveedores de búsqueda: Brave (gratis $5/mes) → DDG (gratis) → Serper ────
# Mismo interfaz para el pipeline. Si solo hay SERPER_API_KEY, se comporta como antes.
# - Brave: $5 crédito gratis/mes ≈ 1000 búsquedas a $5/1k → tope 1000 = NUNCA paga.
# - DDG: gratis total sin tarjeta (scraping html.duckduckgo.com), frágil → desborde.
# - Google CSE: de pago (se descartó) → no en el orden por defecto.
_PROVIDER_CAP_ENV = {"brave": ("BRAVE_MONTHLY_CAP", 1000),
                     "ddg": ("DDG_DAILY_CAP", 2000),
                     "google": ("GOOGLE_DAILY_CAP", 100),
                     "serper": ("SERPER_MONTHLY_CAP", 2000)}
_PROVIDER_KEY_ENV = {"brave": "BRAVE_API_KEY", "google": "GOOGLE_CSE_API_KEY",
                     "serper": "SERPER_API_KEY"}   # ddg no necesita key


def _provider_period_key(provider: str) -> str:
    # Google CSE y DDG cuentan por DÍA; Brave/Serper por MES.
    return datetime.now().strftime("%Y-%m-%d" if provider in ("google", "ddg") else "%Y-%m")


def _provider_cap(provider: str) -> int:
    env, default = _PROVIDER_CAP_ENV.get(provider, ("", 0))
    try:
        return int(os.getenv(env, str(default)))
    except (TypeError, ValueError):
        return default


def _env_or_dotenv(name: str) -> str:
    if not name:
        return ""
    v = os.getenv(name, "")
    if not v:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).parent.parent / ".env")
        v = os.getenv(name, "")
    return v


def _provider_key(provider: str) -> str:
    return _env_or_dotenv(_PROVIDER_KEY_ENV.get(provider, ""))


def _google_cx() -> str:
    return _env_or_dotenv("GOOGLE_CSE_CX")


def _usage_count(provider: str) -> int:
    return int(_load_usage().get(provider, {}).get(_provider_period_key(provider), 0))


def _reserve(provider: str, n: int = 1) -> int:
    """Reserva n usos del proveedor en su periodo (mes/día). Atómico. Se llama ANTES
    de la llamada para garantizar que nunca se supera el tope del proveedor."""
    with _usage_lock:
        u = _load_usage()
        pk = _provider_period_key(provider)
        pu = u.setdefault(provider, {})
        pu[pk] = int(pu.get(pk, 0)) + n
        if len(pu) > 60:                       # poda periodos viejos
            for k in sorted(pu)[:-60]:
                pu.pop(k, None)
        try:
            _USAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = _USAGE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(u, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(_USAGE_PATH)
        except Exception:
            pass
        return pu[pk]


def _provider_order() -> list:
    raw = os.getenv("SEARCH_PROVIDER_ORDER", "brave,google,serper")
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def _provider_available(provider: str) -> bool:
    """True si el proveedor tiene key/cx, no está disabled y está bajo su tope."""
    if provider == "serper" and _serper_disabled():
        return False
    if not _provider_key(provider):
        return False
    if provider == "google" and not _google_cx():
        return False
    if _usage_count(provider) >= _provider_cap(provider):
        _log_once(f"{provider}: tope alcanzado ({_provider_cap(provider)}, "
                  f"{_provider_period_key(provider)}) — se prueba el siguiente proveedor")
        return False
    return True


# ── Back-compat: funciones serper_* (callsites que llaman a Serper directo) ───
def _reserve_usage(n: int = 1) -> int:
    return _reserve("serper", n)


def _usage_this_month() -> int:
    return _usage_count("serper")


def serper_allowed_and_reserve() -> bool:
    """Guardrail para callsites que llaman a Serper DIRECTAMENTE (no vía SearchEngine)."""
    if not _provider_available("serper"):
        return False
    _reserve("serper", 1)
    return True


def serper_usage_status() -> dict:
    """Estado del guardrail Serper (back-compat)."""
    used = _usage_count("serper")
    cap = _provider_cap("serper")
    return {
        "month": _provider_period_key("serper"),
        "used": used,
        "cap": cap,
        "remaining": max(0, cap - used),
        "disabled": _serper_disabled(),
        "blocked": _serper_disabled() or used >= cap,
    }


def search_usage_status() -> dict:
    """Estado de TODOS los proveedores de búsqueda (brave/google/serper)."""
    return {p: {"period": _provider_period_key(p), "used": _usage_count(p),
                "cap": _provider_cap(p),
                "remaining": max(0, _provider_cap(p) - _usage_count(p)),
                "has_key": bool(_provider_key(p))}
            for p in ("brave", "google", "serper")}

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
        """Búsqueda web MULTI-PROVEEDOR: Brave → Google CSE → Serper (fallback).
        Mismo retorno [{title,url,snippet}]. Los cache hits NO cuentan. Cada proveedor
        tiene su key + tope; si uno no tiene key/está al tope/devuelve vacío, se prueba
        el siguiente. Si SOLO hay SERPER_API_KEY, se comporta EXACTAMENTE como antes.
        """
        # Check cache first (los cache hits NO cuentan para el tope)
        if self._is_cached(query):
            return self._get_cached(query)

        global _last_search_time
        for provider in _provider_order():
            if not _provider_available(provider):
                continue
            _reserve(provider, 1)   # reserva ANTES → nunca se supera el tope
            # Rate limit (global entre llamadas reales)
            now = asyncio.get_event_loop().time()
            wait = _RATE_LIMIT_SECONDS - (now - _last_search_time)
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                results = await self._call_provider(provider, query, num)
                _last_search_time = asyncio.get_event_loop().time()
            except Exception as exc:
                print(f"[SEARCH] {provider} error: {str(exc)[:80]}")
                results = []
            if results:
                self._store_results(query, results, agent)
                return results
            # vacío/error → probar el siguiente proveedor (fallback de cobertura)
        return []

    async def _call_provider(self, provider: str, query: str, num: int) -> list[dict]:
        if provider == "brave":
            return await self._search_brave(query, num)
        if provider == "google":
            return await self._search_google(query, num)
        return await self._search_serper(query, num)

    async def _search_brave(self, query: str, num: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": min(num, 20)},
                headers={"X-Subscription-Token": _provider_key("brave"),
                         "Accept": "application/json"})
        resp.raise_for_status()
        return [{"title": it.get("title", ""), "url": it.get("url", ""),
                 "snippet": it.get("description", "")}
                for it in (resp.json().get("web", {}) or {}).get("results", [])]

    async def _search_google(self, query: str, num: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": _provider_key("google"), "cx": _google_cx(),
                        "q": query, "num": min(num, 10)})
        resp.raise_for_status()
        return [{"title": it.get("title", ""), "url": it.get("link", ""),
                 "snippet": it.get("snippet", "")}
                for it in resp.json().get("items", [])]

    async def _search_serper(self, query: str, num: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                _SERPER_URL,
                headers={"X-API-KEY": _provider_key("serper"), "Content-Type": "application/json"},
                json={"q": query, "num": num, "gl": "es", "hl": "es"})
        return [{"title": it.get("title", ""), "url": it.get("link", ""),
                 "snippet": it.get("snippet", "")}
                for it in resp.json().get("organic", [])]

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


async def find_pdfs_in_page(url: str) -> list[dict]:
    """
    Enter a URL and extract all PDF links found in the page.
    Returns: [{"url": "https://.../doc.pdf", "titulo": "link text"}]
    """
    try:
        html = await get_with_headers(url, _HEADERS_WEB)
    except Exception:
        # Try without trailing slash
        alt = url.rstrip("/") if url.endswith("/") else url + "/"
        try:
            html = await get_with_headers(alt, _HEADERS_WEB)
        except Exception:
            return []

    soup = BeautifulSoup(html, "html.parser")
    from urllib.parse import urljoin
    pdfs = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        full_url = urljoin(url, href)
        if full_url.lower().endswith(".pdf") and full_url not in seen:
            seen.add(full_url)
            titulo = a.get_text(strip=True) or full_url.split("/")[-1]
            pdfs.append({"url": full_url, "titulo": titulo})
    return pdfs


async def find_links_by_keywords(url: str, keywords: list[str]) -> list[dict]:
    """
    Navigate a page and extract internal links that match any keyword.
    Returns: [{"url": "...", "titulo": "...", "matched_keyword": "carta"}]
    """
    try:
        html = await get_with_headers(url, _HEADERS_WEB)
    except Exception:
        alt = url.rstrip("/") if url.endswith("/") else url + "/"
        try:
            html = await get_with_headers(alt, _HEADERS_WEB)
        except Exception:
            return []

    soup = BeautifulSoup(html, "html.parser")
    from urllib.parse import urljoin, urlparse
    base_domain = urlparse(url).netloc
    results = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        # Only internal links or PDFs
        if parsed.netloc and parsed.netloc != base_domain:
            continue
        if full_url in seen:
            continue

        link_text = (a.get_text(strip=True) + " " + href).lower()
        for kw in keywords:
            if kw.lower() in link_text:
                seen.add(full_url)
                results.append({
                    "url": full_url,
                    "titulo": a.get_text(strip=True) or href.split("/")[-1],
                    "matched_keyword": kw,
                })
                break

    return results


async def crawl_for_documents(
    start_url: str, keywords: list[str], max_depth: int = 2, max_pages: int = 20
) -> list[dict]:
    """
    Web crawling: start at start_url, follow internal links matching keywords,
    find PDFs and document pages. Max depth levels.
    Returns: [{"url": "...", "titulo": "...", "tipo": "pdf|html"}]
    """
    from urllib.parse import urljoin, urlparse
    base_domain = urlparse(start_url).netloc
    visited: set[str] = set()
    documents: list[dict] = []
    to_visit: list[tuple[str, int]] = [(start_url, 0)]  # (url, depth)

    while to_visit and len(visited) < max_pages:
        current_url, depth = to_visit.pop(0)
        if current_url in visited:
            continue
        visited.add(current_url)

        try:
            html = await get_with_headers(current_url, _HEADERS_WEB)
        except Exception:
            # Try without/with trailing slash
            alt = current_url.rstrip("/") if current_url.endswith("/") else current_url + "/"
            try:
                html = await get_with_headers(alt, _HEADERS_WEB)
            except Exception:
                continue

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(current_url, href)
            parsed = urlparse(full_url)

            # Only same domain
            if parsed.netloc and parsed.netloc != base_domain:
                continue
            if full_url in visited:
                continue

            link_text = (a.get_text(strip=True) + " " + href).lower()

            # Found a PDF
            if full_url.lower().endswith(".pdf"):
                if any(kw.lower() in link_text for kw in keywords):
                    documents.append({
                        "url": full_url,
                        "titulo": a.get_text(strip=True) or full_url.split("/")[-1],
                        "tipo": "pdf",
                    })
                continue

            # Found a page link matching keywords → add to visit queue
            if depth < max_depth and any(kw.lower() in link_text for kw in keywords):
                to_visit.append((full_url, depth + 1))

    return documents


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

    if "--usage" in sys.argv[1:]:
        st = serper_usage_status()
        print(f"Serper {st['month']}: {st['used']}/{st['cap']} usadas "
              f"| restantes {st['remaining']} | disabled={st['disabled']} | blocked={st['blocked']}")
        sys.exit(0)

    query = sys.argv[1] if len(sys.argv) > 1 else "avantage fund morningstar"

    async def main():
        results = await search(query, num=5)
        print(f"Query: {query}")
        print(f"Results: {len(results)}")
        for r in results:
            print(f"  {r['title'][:55]:55s} {r['url'][:80]}")

    asyncio.run(main())
