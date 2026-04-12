"""
Discovery Agent — unified URL discovery + content fetching + cache.

Replaces duplicated search logic across manager_deep, letters, readings agents.
Uses Serper API (Google) for search, web_fetcher for content download,
llm_extractor for structured extraction when needed.

Cache: data/cache/discovery/{ISIN}/ — TTL 365 days.

Usage:
    discovery = DiscoveryAgent(isin, fund_name, gestora, manager_names)
    managers = await discovery.find_manager_info()
    letters  = await discovery.find_letters(years_range=(2018, 2025))
    readings = await discovery.find_readings()
    all_     = await discovery.find_all()
"""
import asyncio
import json
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path

from rich.console import Console

console = Console()

ROOT = Path(__file__).parent.parent
CACHE_TTL_DAYS = 365


@dataclass
class FetchedSource:
    url: str
    titulo: str = ""
    tipo: str = ""        # "gestor" | "carta" | "articulo" | "podcast" | "video" | "ficha"
    text: str = ""        # extracted content (clean text)
    fecha: str | None = None
    fuente: str = ""      # "citywire" | "rankia" | "finect" | "morningstar" | "web_gestora" | etc.
    metadata: dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════
# Search helpers
# ═══════════════════════════════════════════════════════════════

async def _serper_search(query: str, num: int = 8) -> list[dict]:
    """Google search via Serper API. Returns [{title, url, snippet}]."""
    import os, httpx
    api_key = os.getenv("SERPER_API_KEY", "")
    if not api_key:
        console.log("[yellow][DISCOVERY] SERPER_API_KEY not set — falling back to DDG")
        return await _ddg_search(query, num)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
                json={"q": query, "num": num, "gl": "es", "hl": "es"},
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in data.get("organic", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                })
            return results
    except Exception as exc:
        console.log(f"[yellow][DISCOVERY] Serper failed: {exc} — fallback DDG")
        return await _ddg_search(query, num)


async def _ddg_search(query: str, num: int = 8) -> list[dict]:
    """DuckDuckGo HTML scraping fallback."""
    import httpx
    from bs4 import BeautifulSoup
    try:
        url = f"https://html.duckduckgo.com/html/?q={query}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "es-ES,es;q=0.9",
        }
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for a in soup.select(".result__a")[:num]:
            href = a.get("href", "")
            title = a.get_text(strip=True)
            snippet_el = a.find_parent("div", class_="result")
            snippet = ""
            if snippet_el:
                s = snippet_el.select_one(".result__snippet")
                if s:
                    snippet = s.get_text(strip=True)
            if href and title:
                results.append({"title": title, "url": href, "snippet": snippet})
        return results
    except Exception as exc:
        console.log(f"[red][DISCOVERY] DDG search failed: {exc}")
        return []


# ═══════════════════════════════════════════════════════════════
# URL dedup + domain detection
# ═══════════════════════════════════════════════════════════════

def _normalize_url(url: str) -> str:
    return url.rstrip("/").split("?")[0].split("#")[0]


def _detect_fuente(url: str) -> str:
    """Detect source type from URL domain."""
    domain = url.lower()
    if "citywire" in domain: return "citywire"
    if "rankia" in domain: return "rankia"
    if "finect" in domain: return "finect"
    if "morningstar" in domain: return "morningstar"
    if "youtube" in domain or "youtu.be" in domain: return "youtube"
    if "ivoox" in domain or "spotify" in domain: return "podcast"
    if "substack" in domain: return "substack"
    if "cnmv.es" in domain: return "cnmv"
    if "inversis" in domain: return "inversis"
    if "expansi" in domain or "cincodias" in domain: return "prensa"
    if "elconfidencial" in domain: return "prensa"
    if "estrategiasdeinversion" in domain: return "prensa"
    return "web"


# ═══════════════════════════════════════════════════════════════
# Discovery Agent
# ═══════════════════════════════════════════════════════════════

class DiscoveryAgent:

    def __init__(
        self,
        isin: str,
        fund_name: str = "",
        gestora: str = "",
        manager_names: list[str] | None = None,
    ):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name
        self.fund_short = fund_name.split(",")[0].strip() if fund_name else ""
        self.gestora = gestora
        self.gestora_short = gestora.split(",")[0].split("S.G.I.I.C")[0].strip() if gestora else ""
        self.manager_names = [n for n in (manager_names or []) if n and not n.startswith("Equipo")]
        self.cache_dir = ROOT / "data" / "cache" / "discovery" / self.isin
        self._seen_urls: set[str] = set()

    def _log(self, level: str, msg: str):
        console.log(f"[{'green' if level == 'OK' else 'yellow' if level == 'INFO' else 'red'}][DISCOVERY] [{level}] {msg}")

    # ── Cache ────────────────────────────────────────────────────────────

    def _cache_path(self, category: str) -> Path:
        return self.cache_dir / f"{category}.json"

    def _cache_read(self, category: str) -> list[FetchedSource] | None:
        p = self._cache_path(category)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            meta = data.get("_meta", {})
            cached_at = meta.get("cached_at", "")
            if cached_at:
                age = (datetime.now() - datetime.fromisoformat(cached_at)).days
                if age > CACHE_TTL_DAYS:
                    return None
            return [FetchedSource(**item) for item in data.get("sources", [])]
        except Exception:
            return None

    def _cache_write(self, category: str, sources: list[FetchedSource]):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        p = self._cache_path(category)
        data = {
            "_meta": {"isin": self.isin, "category": category, "cached_at": datetime.now().isoformat()},
            "sources": [asdict(s) for s in sources],
        }
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Search + Fetch ───────────────────────────────────────────────────

    async def _search_and_fetch(
        self,
        queries: list[str],
        tipo: str,
        use_browser: bool = False,
        max_results_per_query: int = 5,
        max_total: int = 20,
    ) -> list[FetchedSource]:
        """Search multiple queries, dedup URLs, fetch content for each."""
        from tools.web_fetcher import fetch_url

        # Search phase
        all_results = []
        for q in queries:
            results = await _serper_search(q, num=max_results_per_query)
            all_results.extend(results)
            await asyncio.sleep(0.5)  # rate limit

        # Dedup
        unique = []
        for r in all_results:
            url = r.get("url", "")
            if not url:
                continue
            norm = _normalize_url(url)
            if norm in self._seen_urls:
                continue
            self._seen_urls.add(norm)
            self._seen_urls.add(url)
            unique.append(r)

        self._log("INFO", f"Tipo '{tipo}': {len(queries)} queries → {len(all_results)} results → {len(unique)} unique URLs")

        # Fetch phase (parallel, max 5 concurrent)
        semaphore = asyncio.Semaphore(5)

        async def _fetch_one(r: dict) -> FetchedSource | None:
            async with semaphore:
                url = r["url"]
                result = await fetch_url(url, use_browser=use_browser)
                if not result.ok or len(result.text) < 100:
                    return None
                return FetchedSource(
                    url=url,
                    titulo=r.get("title", ""),
                    tipo=tipo,
                    text=result.text,
                    fuente=_detect_fuente(url),
                    metadata={"snippet": r.get("snippet", ""), "fetch_method": result.method},
                )

        tasks = [_fetch_one(r) for r in unique[:max_total]]
        fetched = await asyncio.gather(*tasks)
        sources = [s for s in fetched if s is not None]
        self._log("OK", f"Tipo '{tipo}': {len(sources)}/{len(unique)} URLs fetched con contenido")
        return sources

    # ── Public: find_manager_info ─────────────────────────────────────────

    async def find_manager_info(self, force_refresh: bool = False) -> list[FetchedSource]:
        """Find and fetch content about fund managers."""
        if not force_refresh:
            cached = self._cache_read("manager")
            if cached is not None:
                self._log("INFO", f"Manager info from cache ({len(cached)} sources)")
                return cached

        queries = []
        for name in self.manager_names[:5]:
            queries.extend([
                f'"{name}" "{self.gestora_short}" entrevista filosofia inversion',
                f'"{name}" citywire',
                f'"{name}" rankia',
                f'"{name}" gestor fondo',
            ])
        # Gestora team page
        if self.gestora_short:
            queries.append(f'"{self.gestora_short}" equipo gestor')
            queries.append(f'site:{self._guess_domain()} equipo OR team')

        sources = await self._search_and_fetch(
            queries, tipo="gestor", use_browser=True, max_total=25,
        )
        self._cache_write("manager", sources)
        return sources

    # ── Public: find_letters ──────────────────────────────────────────────

    async def find_letters(
        self,
        years_range: tuple[int, int] | None = None,
        force_refresh: bool = False,
    ) -> list[FetchedSource]:
        """Find and fetch quarterly/semiannual letters."""
        if not force_refresh:
            cached = self._cache_read("letters")
            if cached is not None:
                self._log("INFO", f"Letters from cache ({len(cached)} sources)")
                return cached

        start_year = years_range[0] if years_range else 2020
        end_year = years_range[1] if years_range else datetime.now().year

        queries = [
            f'"{self.fund_short}" carta trimestral',
            f'"{self.fund_short}" carta semestral',
            f'"{self.fund_short}" informe inversores',
            f'"{self.gestora_short}" carta inversores',
            f'"{self.fund_short}" comentario gestor',
        ]
        # Year-specific searches
        for year in range(max(start_year, end_year - 3), end_year + 1):
            queries.append(f'"{self.fund_short}" carta {year}')

        # Gestora documents page
        domain = self._guess_domain()
        if domain:
            queries.append(f'site:{domain} carta OR informe OR semestral filetype:pdf')
            queries.append(f'site:{domain} documentos OR informes')

        # Additional: inversis, morningstar docs
        queries.append(f'"{self.isin}" informe site:inversis.com')
        queries.append(f'"{self.isin}" site:doc.morningstar.com')

        sources = await self._search_and_fetch(
            queries, tipo="carta", use_browser=False, max_total=20,
        )

        # Also try to find PDFs specifically
        pdf_sources = await self._find_pdfs_in_sources(sources)
        all_sources = sources + pdf_sources

        self._cache_write("letters", all_sources)
        return all_sources

    # ── Public: find_readings ─────────────────────────────────────────────

    async def find_readings(self, force_refresh: bool = False) -> list[FetchedSource]:
        """Find external analysis, articles, podcasts, videos."""
        if not force_refresh:
            cached = self._cache_read("readings")
            if cached is not None:
                self._log("INFO", f"Readings from cache ({len(cached)} sources)")
                return cached

        queries = [
            f'"{self.fund_short}" analisis',
            f'"{self.fund_short}" opinion',
            f'"{self.fund_short}" rankia',
            f'"{self.fund_short}" finect',
            f'"{self.fund_short}" substack',
            f'"{self.fund_short}" podcast',
            f'"{self.fund_short}" youtube entrevista',
        ]
        for name in self.manager_names[:3]:
            queries.append(f'"{name}" entrevista podcast')
            queries.append(f'"{name}" youtube')

        # Prensa
        queries.extend([
            f'"{self.fund_short}" El Confidencial',
            f'"{self.fund_short}" Expansion OR "Cinco Dias"',
            f'"{self.fund_short}" estrategiasdeinversion',
        ])

        sources = await self._search_and_fetch(
            queries, tipo="articulo", use_browser=False, max_total=30,
        )

        # Classify: youtube/podcast/articulo
        for s in sources:
            if s.fuente == "youtube":
                s.tipo = "video"
            elif s.fuente == "podcast":
                s.tipo = "podcast"

        self._cache_write("readings", sources)
        return sources

    # ── Public: find_all ──────────────────────────────────────────────────

    async def find_all(self, force_refresh: bool = False) -> dict[str, list[FetchedSource]]:
        """Run all discovery categories in parallel."""
        managers, letters, readings = await asyncio.gather(
            self.find_manager_info(force_refresh=force_refresh),
            self.find_letters(force_refresh=force_refresh),
            self.find_readings(force_refresh=force_refresh),
        )
        total = len(managers) + len(letters) + len(readings)
        self._log("OK", f"Total: {total} sources (managers={len(managers)}, letters={len(letters)}, readings={len(readings)})")
        return {"manager": managers, "letters": letters, "readings": readings}

    # ── Helpers ───────────────────────────────────────────────────────────

    def _guess_domain(self) -> str:
        """Guess gestora domain from name."""
        name = self.gestora_short.lower().replace(" ", "")
        # Known domains
        known = {
            "dunascapital": "dunascapital.com",
            "avantage": "avantage-capital.es",
            "myinvestor": "myinvestor.es",
            "renta4": "renta4.es",
        }
        for key, domain in known.items():
            if key in name:
                return domain
        # Heuristic: first word + .com or .es
        first = self.gestora_short.split()[0].lower() if self.gestora_short else ""
        return f"{first}.com" if first else ""

    async def _find_pdfs_in_sources(self, sources: list[FetchedSource]) -> list[FetchedSource]:
        """From fetched pages that might be document listings, extract PDF links."""
        from tools.web_fetcher import fetch_url
        from bs4 import BeautifulSoup

        pdf_sources = []
        for s in sources:
            if not s.metadata.get("snippet", ""):
                continue
            # Check if the page HTML has PDF links
            try:
                # Re-parse HTML from the fetch (stored in cache)
                result = await fetch_url(s.url)
                if not result.html:
                    continue
                soup = BeautifulSoup(result.html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if ".pdf" in href.lower():
                        # Make absolute
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed = urlparse(s.url)
                            href = f"{parsed.scheme}://{parsed.netloc}{href}"
                        elif not href.startswith("http"):
                            continue
                        norm = _normalize_url(href)
                        if norm in self._seen_urls:
                            continue
                        self._seen_urls.add(norm)
                        pdf_sources.append(FetchedSource(
                            url=href,
                            titulo=a.get_text(strip=True)[:120] or "PDF",
                            tipo="carta",
                            fuente=_detect_fuente(href),
                            text="",  # PDFs need separate extraction
                            metadata={"is_pdf": True, "found_in": s.url},
                        ))
            except Exception:
                continue

        if pdf_sources:
            self._log("INFO", f"Found {len(pdf_sources)} PDF links in fetched pages")
        return pdf_sources[:10]  # cap


# ═══════════════════════════════════════════════════════════════
# CLI for testing
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0175316001"

    # Load fund metadata
    fund_name, gestora, managers = "", "", []
    for fname in ["output.json", "cnmv_data.json"]:
        p = ROOT / "data" / "funds" / isin / fname
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            fund_name = fund_name or data.get("nombre", "")
            gestora = gestora or data.get("gestora", "")
            if not managers:
                eq = data.get("gestores", {}).get("equipo", [])
                if not eq:
                    mgr = ROOT / "data" / "funds" / isin / "manager_profile.json"
                    if mgr.exists():
                        eq = json.loads(mgr.read_text(encoding="utf-8")).get("equipo_gestor", [])
                managers = eq

    print(f"Discovery for: {fund_name} ({isin})")
    print(f"Gestora: {gestora}")
    print(f"Managers: {managers[:5]}")

    agent = DiscoveryAgent(isin, fund_name, gestora, managers)
    result = asyncio.run(agent.find_all())

    for cat, sources in result.items():
        print(f"\n{'='*60}")
        print(f"{cat}: {len(sources)} sources")
        for s in sources[:5]:
            print(f"  [{s.fuente}] {s.titulo[:60]} ({len(s.text)} chars)")
            print(f"    {s.url[:80]}")
