"""
Sources Discovery Agent — Encuentra y valida TODAS las fuentes de información
disponibles para un fondo ANTES de que comience el análisis.

Para cada fondo (dado ISIN + nombre + gestora) busca en:
  - Web gestora (cartas semestrales, rentabilidad, entrevistas)
  - Morningstar
  - Rankia
  - Finect
  - Citywire (perfiles de gestores)
  - YouTube (conferencias, entrevistas)
  - Podcasts (Value Investing FM, iVoox, etc.)
  - Artículos de prensa financiera (El Confidencial, Estrategias de Inversión, etc.)
  - CNMV (documentos oficiales para fondos ES)

Output:
  data/funds/{ISIN}/sources.json
"""
import asyncio
import json
import re
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_with_headers

console = Console(highlight=False, force_terminal=False)

DDG_HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "Chrome/120 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://duckduckgo.com/",
}

# Delay between DDG searches to avoid rate-limiting
DDG_DELAY_S = 1.0

# Known gestora domains for site:-scoped searches (ES)
KNOWN_GESTORA_DOMAINS = {
    "avantage": "avantage-capital.es",
    "cobas": "cobasam.com",
    "azvalor": "azvalor.com",
    "bestinver": "bestinver.es",
    "magallanes": "magallanesinversion.com",
    "horos": "horosam.com",
    "valentum": "valentum.es",
    "dnca": "dnca-investments.com",
    "renta 4": "renta4.es",
    "buy & hold": "buyandhold.es",
    "numantia": "numantiapatrimonioglobal.com",
    "metagestion": "metagestion.com",
    "abante": "abanteasesores.com",
    "cartesio": "cartesio.com",
    "narval": "narvalinvest.com",
}

# Dominios internacionales para buscar analisis, entrevistas y perfiles
# de gestores de fondos INT.
INT_ANALYSIS_DOMAINS = [
    "citywire.com", "citywire.co.uk",     # Perfiles gestores + ratings
    "fundspeople.com",                     # Entrevistas, analisis EU
    "trustnet.com",                        # Fund analysis UK
    "morningstar.co.uk", "morningstar.com",# Analyst reports
    "ft.com",                              # Press + fund profiles
    "institutionalinvestor.com",           # Entrevistas institutional
    "allfunds.com",                        # Platform analysis
    "youtube.com",                         # Videos gestores, conferencias
]

# Keywords multi-idioma para buscar cartas e info de gestores INT
INT_SEARCH_KEYWORDS = {
    "cartas": [
        "quarterly letter", "investor letter", "fund commentary",
        "investment report", "carta trimestral", "lettre trimestrielle",
        "Quartalsbericht", "manager commentary", "market outlook",
    ],
    "entrevistas_gestor": [
        "interview", "Q&A", "entrevista", "entretien",
        "fund manager", "portfolio manager", "CIO",
    ],
    "analisis_fondo": [
        "fund review", "fund analysis", "analyst report", "opinion",
        "fund rating", "due diligence",
    ],
    "perfil_gestor": [
        "manager profile", "track record", "biography", "background",
        "perfil gestor", "Citywire rating",
    ],
    "conferencias": [
        "conference", "webinar", "presentation", "investor day",
        "annual meeting", "AGM",
    ],
}


class SourcesAgent:
    """
    Agente de descubrimiento y validación de fuentes.
    async def run() -> dict según convenio del proyecto.

    Searches DuckDuckGo HTML for multiple source categories, validates each
    URL with an HTTP GET, deduplicates by URL, and saves structured output
    to data/funds/{ISIN}/sources.json.
    """

    def __init__(
        self,
        isin: str,
        fund_name: str = "",
        gestora: str = "",
        gestores: list[str] | None = None,
        gestora_domain: str = "",
    ):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name
        self.gestora = gestora
        self.gestores = gestores or []
        self.gestora_domain = gestora_domain

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

        self._log_path = root / "progress.log"
        self._gestoras_registry_path = root / "data" / "gestoras_registry.json"
        self._gestoras_registry: dict = {}

        self._sources: list[dict] = []
        self._seen_urls: set[str] = set()
        self._current_year = datetime.now().year

    # ══════════════════════════════════════════════════════════════════════════
    # Logging
    # ══════════════════════════════════════════════════════════════════════════

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [SOURCES] [{level}] {msg}"
        # Use print with cp1252-safe encoding for Windows console
        safe_line = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe_line, flush=True)
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════════════
    # Helpers
    # ══════════════════════════════════════════════════════════════════════════

    def _short_fund_name(self) -> str:
        """Strip legal suffixes for better search queries."""
        s = re.sub(
            r"\b(FI|SICAV|FP|SIL|FUND|FONDO|S\.A\.|SA)\b",
            "",
            self.fund_name,
            flags=re.IGNORECASE,
        )
        return s.strip().strip(",").strip()

    def _extract_domain(self, url: str) -> str:
        """Extract bare domain from URL (strips www.)."""
        try:
            return urllib.parse.urlparse(url).netloc.lstrip("www.")
        except Exception:
            return ""

    def _add_source(
        self,
        url: str,
        tipo: str,
        titulo: str = "",
        fecha: str = "",
        accesible: bool | None = None,
    ):
        """Add a source to the internal list if URL not already seen."""
        if not url or url in self._seen_urls:
            return
        # Normalize trailing slashes for dedup
        normalized = url.rstrip("/")
        if normalized in self._seen_urls:
            return
        self._seen_urls.add(url)
        self._seen_urls.add(normalized)
        self._sources.append({
            "url": url,
            "tipo": tipo,
            "fecha": fecha,
            "titulo": titulo,
            "accesible": accesible,
            "agente_origen": "sources_agent",
        })

    def _resolve_gestora_domain(self) -> str:
        """
        Resolve the gestora's web domain from:
          1. Explicit gestora_domain parameter
          2. gestoras_registry.json
          3. KNOWN_GESTORA_DOMAINS hardcoded map
        Returns domain string (e.g. 'avantage-capital.es') or ''.
        """
        # Explicit parameter takes priority
        if self.gestora_domain:
            return self.gestora_domain

        # Check gestoras_registry.json
        for name, info in self._gestoras_registry.items():
            gestora_lower = (self.gestora or "").lower()
            if name.lower() in gestora_lower or gestora_lower in name.lower():
                web = info.get("web", "")
                if web:
                    return self._extract_domain(web)

        # Fallback: hardcoded known domains
        gestora_lower = (self.gestora or "").lower()
        for keyword, domain in KNOWN_GESTORA_DOMAINS.items():
            if keyword in gestora_lower:
                return domain

        return ""

    def _resolve_gestora_web_url(self) -> str:
        """Get full URL for gestora website."""
        for name, info in self._gestoras_registry.items():
            gestora_lower = (self.gestora or "").lower()
            if name.lower() in gestora_lower or gestora_lower in name.lower():
                return info.get("web", "")
        return ""

    def _load_gestoras_registry(self):
        """Load gestoras_registry.json for known gestora URLs."""
        try:
            if self._gestoras_registry_path.exists():
                data = json.loads(
                    self._gestoras_registry_path.read_text(encoding="utf-8")
                )
                self._gestoras_registry = data.get("gestoras", {})
                self._log(
                    "INFO",
                    f"gestoras_registry loaded: {len(self._gestoras_registry)} gestoras",
                )
        except Exception as exc:
            self._log("WARN", f"Error loading gestoras_registry: {exc}")

    # ══════════════════════════════════════════════════════════════════════════
    # DuckDuckGo search
    # ══════════════════════════════════════════════════════════════════════════

    async def _ddg_search(self, query: str, max_results: int = 5) -> list[dict]:
        """
        Search DuckDuckGo HTML (no API key needed).
        Returns list of [{titulo, url, snippet}].
        """
        enc_q = urllib.parse.quote_plus(query)
        ddg_url = f"https://html.duckduckgo.com/html/?q={enc_q}"
        try:
            html = await get_with_headers(ddg_url, DDG_HEADERS)
        except Exception as exc:
            self._log("WARN", f"DDG error for '{query}': {exc}")
            return []

        soup = BeautifulSoup(html, "lxml")
        results: list[dict] = []
        for a_tag in soup.select(".result__a"):
            titulo = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")
            url = self._unwrap_ddg_url(href)
            if not url:
                continue
            snippet = ""
            parent = a_tag.find_parent(class_="result")
            if parent:
                snip_el = parent.select_one(".result__snippet")
                if snip_el:
                    snippet = snip_el.get_text(strip=True)
            results.append({"titulo": titulo, "url": url, "snippet": snippet})
            if len(results) >= max_results:
                break

        self._log("INFO", f"DDG '{query[:70]}' -> {len(results)} results")
        return results

    def _unwrap_ddg_url(self, href: str) -> str:
        """Extract real URL from DuckDuckGo redirect wrapper."""
        if href.startswith("http") and "duckduckgo" not in href:
            return href
        m = re.search(r"uddg=([^&]+)", href)
        if m:
            return urllib.parse.unquote(m.group(1))
        m2 = re.search(r"\bu=([^&]+)", href)
        if m2:
            return urllib.parse.unquote(m2.group(1))
        return ""

    # ══════════════════════════════════════════════════════════════════════════
    # Accessibility check
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_accessible(self, url: str) -> bool:
        """Quick HTTP GET with 10s timeout to verify URL is reachable."""
        import httpx

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=10.0,
                headers={"User-Agent": DDG_HEADERS["User-Agent"]},
            ) as client:
                resp = await client.get(url)
                return resp.status_code < 400
        except Exception:
            return False

    async def _check_all_accessible(self):
        """Check accessibility for all sources that haven't been checked yet."""
        tasks = []
        indices = []
        for i, src in enumerate(self._sources):
            if src["accesible"] is None:
                tasks.append(self._check_accessible(src["url"]))
                indices.append(i)

        if not tasks:
            return

        self._log("INFO", f"Checking accessibility for {len(tasks)} URLs...")

        # Process in batches of 10 to avoid overwhelming connections
        batch_size = 10
        for batch_start in range(0, len(tasks), batch_size):
            batch_end = min(batch_start + batch_size, len(tasks))
            batch_tasks = tasks[batch_start:batch_end]
            batch_indices = indices[batch_start:batch_end]

            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            for idx, result in zip(batch_indices, results):
                if isinstance(result, Exception):
                    self._sources[idx]["accesible"] = False
                else:
                    self._sources[idx]["accesible"] = result

        accessible = sum(1 for s in self._sources if s.get("accesible") is True)
        inaccessible = sum(1 for s in self._sources if s.get("accesible") is False)
        self._log("INFO", f"Accessibility: {accessible} OK, {inaccessible} failed")

    # ══════════════════════════════════════════════════════════════════════════
    # Source category searches
    # ══════════════════════════════════════════════════════════════════════════

    # ── CNMV (ES funds only) ─────────────────────────────────────────────────

    def _add_cnmv_sources(self):
        """Add CNMV direct URLs for ES-prefixed funds."""
        if not self.isin.startswith("ES"):
            return
        # Ficha del fondo en CNMV
        url = f"https://www.cnmv.es/portal/Consultas/IIC/Fondo.aspx?isin={self.isin}"
        self._add_source(url, "cnmv_doc", titulo=f"CNMV ficha fondo {self.isin}")
        # Bulk data download page
        self._add_source(
            "https://www.cnmv.es/portal/publicaciones/descarga-informacion-individual",
            "cnmv_doc",
            titulo="CNMV descarga informacion individual (XML bulk)",
        )

    # ── Web Gestora (PRIMARY) ────────────────────────────────────────────────

    async def _search_web_gestora(self):
        """
        Search for fund-specific pages on the gestora's website.
        Looks for: cartas semestrales, rentabilidad, entrevistas pages.
        """
        domain = self._resolve_gestora_domain()
        web_url = self._resolve_gestora_web_url()
        fund_short = self._short_fund_name()

        # Add the gestora home page
        if web_url:
            self._add_source(
                web_url, "web_gestora", titulo=f"Web gestora - {self.gestora}"
            )

        if not domain and not fund_short:
            self._log("WARN", "No gestora domain or fund name; skipping web gestora search")
            return

        # Search gestora site for fund-related pages
        if domain and fund_short:
            queries = [
                (f'"{fund_short}" site:{domain}', "web_gestora"),
                (f'"carta" OR "cartas semestrales" site:{domain}', "carta_gestor"),
                (f'"rentabilidad" site:{domain}', "web_gestora"),
                (f'"entrevista" OR "conferencia" site:{domain}', "web_gestora"),
                (f'"ficha mensual" OR "informe" site:{domain}', "web_gestora"),
            ]
            for query, tipo in queries:
                try:
                    results = await self._ddg_search(query, max_results=5)
                    for r in results:
                        self._add_source(r["url"], tipo, titulo=r["titulo"])
                    await asyncio.sleep(DDG_DELAY_S)
                except Exception as exc:
                    self._log("WARN", f"Web gestora search error: {exc}")

        # If no domain known, do a broader search to discover the gestora site
        elif fund_short and self.gestora:
            query = f'"{fund_short}" "{self.gestora}" sitio oficial'
            try:
                results = await self._ddg_search(query, max_results=5)
                for r in results:
                    self._add_source(r["url"], "web_gestora", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Gestora discovery search error: {exc}")

        # Check known successful PDF URLs from registry
        for name, info in self._gestoras_registry.items():
            if self.isin in info.get("funds", []):
                for pdf_url in info.get("successful_pdf_urls", []):
                    self._add_source(
                        pdf_url, "carta_gestor", titulo=f"PDF descargado - {name}"
                    )
                for lp in info.get("letters_pages", []):
                    self._add_source(
                        lp, "web_gestora", titulo=f"Pagina cartas - {name}"
                    )

    # ── Morningstar ──────────────────────────────────────────────────────────

    async def _search_morningstar(self):
        """Search Morningstar for fund pages (ES + international)."""
        fund_short = self._short_fund_name() or self.isin
        queries = [
            f'"{fund_short}" morningstar',
            f'"{self.isin}" site:morningstar.es',
            f'"{self.isin}" site:morningstar.com',
        ]
        for q in queries:
            try:
                results = await self._ddg_search(q, max_results=3)
                for r in results:
                    if "morningstar" in r["url"].lower():
                        self._add_source(r["url"], "morningstar", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Morningstar search error: {exc}")

    # ── Rankia ───────────────────────────────────────────────────────────────

    async def _search_rankia(self):
        """Search Rankia for fund analysis and forum discussions."""
        fund_short = self._short_fund_name() or self.isin
        queries = [
            f'"{fund_short}" rankia',
            f'"{self.isin}" rankia',
        ]
        for q in queries:
            try:
                results = await self._ddg_search(q, max_results=5)
                for r in results:
                    if "rankia" in r["url"].lower():
                        self._add_source(r["url"], "rankia", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Rankia search error: {exc}")

    # ── Finect ───────────────────────────────────────────────────────────────

    async def _search_finect(self):
        """Search Finect for fund profile and analysis."""
        fund_short = self._short_fund_name() or self.isin
        # Direct URL (deterministic)
        self._add_source(
            f"https://www.finect.com/fondos-inversion/{self.isin}",
            "finect",
            titulo=f"Finect - {self.fund_name or self.isin}",
        )
        # DDG search for additional Finect content
        query = f'"{fund_short}" finect'
        try:
            results = await self._ddg_search(query, max_results=5)
            for r in results:
                if "finect" in r["url"].lower():
                    self._add_source(r["url"], "finect", titulo=r["titulo"])
            await asyncio.sleep(DDG_DELAY_S)
        except Exception as exc:
            self._log("WARN", f"Finect search error: {exc}")

    # ── Citywire (manager profiles) ──────────────────────────────────────────

    async def _search_citywire(self):
        """Search Citywire for fund manager profiles."""
        for gestor in self.gestores[:3]:
            query = f'"{gestor}" citywire'
            try:
                results = await self._ddg_search(query, max_results=3)
                for r in results:
                    if "citywire" in r["url"].lower():
                        self._add_source(r["url"], "citywire", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Citywire search error for {gestor}: {exc}")

            # Also try direct Citywire profile URL
            slug = re.sub(r"[^a-z0-9]+", "-", gestor.lower().strip()).strip("-")
            direct_url = f"https://citywire.com/selector/manager/profile/{slug}"
            self._add_source(
                direct_url, "citywire", titulo=f"Citywire profile - {gestor}"
            )

    # ── YouTube ──────────────────────────────────────────────────────────────

    async def _search_youtube(self):
        """Search YouTube for fund conferences and manager interviews."""
        fund_short = self._short_fund_name()
        queries: list[tuple[str, str]] = []

        # Fund-level searches
        if fund_short:
            queries.append(
                (f'"{fund_short}" conferencia site:youtube.com', "youtube")
            )
            queries.append(
                (f'"{fund_short}" presentacion site:youtube.com', "youtube")
            )

        # Manager-level searches
        for gestor in self.gestores[:2]:
            queries.append(
                (f'"{gestor}" entrevista site:youtube.com', "youtube")
            )

        for q, tipo in queries:
            try:
                results = await self._ddg_search(q, max_results=4)
                for r in results:
                    if "youtube.com" in r["url"] or "youtu.be" in r["url"]:
                        self._add_source(r["url"], tipo, titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"YouTube search error: {exc}")

    # ── Podcasts ─────────────────────────────────────────────────────────────

    async def _search_podcasts(self):
        """Search for podcast episodes mentioning the fund or managers."""
        fund_short = self._short_fund_name()
        queries: list[str] = []

        # Fund-level podcast searches
        if fund_short:
            queries.append(f'"{fund_short}" podcast')
            queries.append(f'"{fund_short}" value investing fm')

        # Manager-level podcast searches
        for gestor in self.gestores[:2]:
            queries.append(f'"{gestor}" podcast value investing fm')
            queries.append(f'"{gestor}" podcast inversion')

        for q in queries:
            try:
                results = await self._ddg_search(q, max_results=4)
                for r in results:
                    self._add_source(r["url"], "podcast", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Podcast search error: {exc}")

    # ── Articles / Press ─────────────────────────────────────────────────────

    async def _search_articles(self):
        """
        Search for articles mentioning the fund or managers in financial press.
        Covers: El Confidencial, Estrategias de Inversion, Expansion, Cinco Dias,
                general press with current year.
        """
        fund_short = self._short_fund_name()
        queries: list[str] = []

        # Manager interviews current year
        for gestor in self.gestores[:2]:
            queries.append(f'"{gestor}" entrevista {self._current_year}')
            queries.append(f'"{gestor}" El Confidencial')

        # Fund in press
        if fund_short:
            queries.append(f'"{fund_short}" El Confidencial')
            queries.append(f'"{fund_short}" estrategias inversion')
            queries.append(f'"{fund_short}" Expansion OR "Cinco Dias"')

        # Estrategias de Inversion site-scoped
        if fund_short:
            queries.append(
                f'"{fund_short}" site:estrategiasdeinversion.com'
            )

        for q in queries:
            try:
                results = await self._ddg_search(q, max_results=3)
                for r in results:
                    url_lower = r["url"].lower()
                    # Skip search engine result pages
                    if any(
                        d in url_lower
                        for d in ("google.com", "duckduckgo.com", "bing.com")
                    ):
                        continue
                    self._add_source(r["url"], "articulo", titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"Articles search error: {exc}")

    # ══════════════════════════════════════════════════════════════════════════
    # INT Ultra-detail: cartas, entrevistas gestores, analisis especializados
    # ══════════════════════════════════════════════════════════════════════════

    async def _search_int_ultra_detail(self):
        """
        Para fondos INT: busca ultra-detalle que discovery v2 no cubre:
        - Entrevistas del gestor en medios especializados
        - Cartas trimestrales en blogs/insights de la gestora
        - Analisis de terceros (Citywire, FundsPeople, Trustnet)
        - Perfiles de gestores (Citywire manager ratings)
        - Videos/conferencias del gestor (YouTube, Vimeo)
        """
        fund_short = self._short_fund_name()
        queries: list[str] = []

        # ── Cartas y commentary del gestor (multi-idioma) ──
        for kw in INT_SEARCH_KEYWORDS.get("cartas", [])[:4]:
            if fund_short:
                queries.append(f'"{fund_short}" "{kw}"')

        # ── Entrevistas del gestor en medios INT ──
        for gestor in self.gestores[:2]:
            for kw in INT_SEARCH_KEYWORDS.get("entrevistas_gestor", [])[:3]:
                queries.append(f'"{gestor}" "{kw}"')
            # Citywire manager profile
            queries.append(f'site:citywire.com "{gestor}"')
            queries.append(f'site:citywire.co.uk "{gestor}"')

        # ── Analisis del fondo en medios INT ──
        for domain in INT_ANALYSIS_DOMAINS[:6]:
            if fund_short:
                queries.append(f'site:{domain} "{fund_short}"')

        # ── Perfil gestores en dominios especializados ──
        for gestor in self.gestores[:2]:
            for kw in INT_SEARCH_KEYWORDS.get("perfil_gestor", [])[:2]:
                queries.append(f'"{gestor}" "{kw}"')

        # ── Videos / conferencias ──
        if fund_short:
            queries.append(f'site:youtube.com "{fund_short}" OR "{self.gestora}"')
        for gestor in self.gestores[:2]:
            queries.append(f'site:youtube.com "{gestor}" {self._current_year}')

        # Ejecutar queries (limitar para no saturar DDG)
        for q in queries[:25]:
            try:
                results = await self._ddg_search(q, max_results=3)
                for r in results:
                    url_lower = r["url"].lower()
                    if any(d in url_lower for d in ("google.com", "duckduckgo.com", "bing.com")):
                        continue
                    tipo = "analisis_int"
                    if "youtube" in url_lower or "vimeo" in url_lower:
                        tipo = "video"
                    elif "citywire" in url_lower:
                        tipo = "perfil_gestor"
                    elif any(kw in url_lower for kw in ("interview", "entrevista", "entretien")):
                        tipo = "entrevista"
                    elif any(kw in url_lower for kw in ("letter", "commentary", "carta")):
                        tipo = "carta_gestor"
                    self._add_source(r["url"], tipo, titulo=r["titulo"])
                await asyncio.sleep(DDG_DELAY_S)
            except Exception as exc:
                self._log("WARN", f"INT ultra-detail query error: {exc}")

        # ── Reuse discovery v2 URLs as sources (si existen) ──
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                for doc in disc.get("documents", []):
                    url = doc.get("url", "")
                    if url and "manual://" not in url:
                        self._add_source(url, f"doc_discovery_{doc.get('doc_type', 'unknown')}")
            except Exception:
                pass

    # ══════════════════════════════════════════════════════════════════════════
    # Main run
    # ══════════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        """
        Execute all source discovery searches, validate URLs, and save output.
        Returns the full sources.json structure.
        """
        self._log(
            "INFO",
            f"SourcesAgent starting for {self.isin} - {self.fund_name} "
            f"(gestora: {self.gestora})",
        )

        # Load known gestora URLs from registry
        self._load_gestoras_registry()

        # ── STEP 1: Direct/known URLs (no search needed) ────────────────────
        self._log("INFO", "Step 1: Adding direct/known source URLs")
        self._add_cnmv_sources()

        # ── STEP 2: Web gestora (PRIMARY source) ────────────────────────────
        self._log("INFO", "Step 2: Searching gestora website")
        try:
            await self._search_web_gestora()
        except Exception as exc:
            self._log("ERROR", f"Web gestora search failed: {exc}")

        # ── STEP 3: Morningstar ─────────────────────────────────────────────
        self._log("INFO", "Step 3: Searching Morningstar")
        try:
            await self._search_morningstar()
        except Exception as exc:
            self._log("ERROR", f"Morningstar search failed: {exc}")

        # ── STEP 4: Rankia ──────────────────────────────────────────────────
        self._log("INFO", "Step 4: Searching Rankia")
        try:
            await self._search_rankia()
        except Exception as exc:
            self._log("ERROR", f"Rankia search failed: {exc}")

        # ── STEP 5: Finect ──────────────────────────────────────────────────
        self._log("INFO", "Step 5: Searching Finect")
        try:
            await self._search_finect()
        except Exception as exc:
            self._log("ERROR", f"Finect search failed: {exc}")

        # ── STEP 6: Citywire (manager profiles) ────────────────────────────
        if self.gestores:
            self._log("INFO", "Step 6: Searching Citywire manager profiles")
            try:
                await self._search_citywire()
            except Exception as exc:
                self._log("ERROR", f"Citywire search failed: {exc}")
        else:
            self._log("INFO", "Step 6: Skipped Citywire (no gestores provided)")

        # ── STEP 7: YouTube ─────────────────────────────────────────────────
        self._log("INFO", "Step 7: Searching YouTube")
        try:
            await self._search_youtube()
        except Exception as exc:
            self._log("ERROR", f"YouTube search failed: {exc}")

        # ── STEP 8: Podcasts ────────────────────────────────────────────────
        self._log("INFO", "Step 8: Searching podcasts")
        try:
            await self._search_podcasts()
        except Exception as exc:
            self._log("ERROR", f"Podcast search failed: {exc}")

        # ── STEP 9: Articles / Press ────────────────────────────────────────
        self._log("INFO", "Step 9: Searching articles and press")
        try:
            await self._search_articles()
        except Exception as exc:
            self._log("ERROR", f"Articles search failed: {exc}")

        # ── STEP 10: INT Ultra-detail (cartas, entrevistas, perfiles gestores) ──
        if not self.isin.startswith("ES"):
            self._log("INFO", "Step 10: INT ultra-detail search (cartas, manager profiles, analysis)")
            try:
                await self._search_int_ultra_detail()
            except Exception as exc:
                self._log("ERROR", f"INT ultra-detail failed: {exc}")

        # ── STEP 11: Accessibility check ────────────────────────────────────
        self._log("INFO", "Step 10: Verifying URL accessibility")
        try:
            await self._check_all_accessible()
        except Exception as exc:
            self._log("ERROR", f"Accessibility check failed: {exc}")

        # ── Summary and save ────────────────────────────────────────────────
        accessible_count = sum(
            1 for s in self._sources if s.get("accesible") is True
        )
        inaccessible_count = sum(
            1 for s in self._sources if s.get("accesible") is False
        )
        self._log(
            "INFO",
            f"Done. Total sources: {len(self._sources)} "
            f"({accessible_count} accessible, {inaccessible_count} inaccessible)",
        )

        # Build output matching the required schema
        output = {
            "isin": self.isin,
            "generated": datetime.now().isoformat(),
            "sources": self._sources,
        }

        output_path = self.fund_dir / "sources.json"
        output_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self._log("INFO", f"Saved {len(self._sources)} sources to {output_path}")

        return output


# ══════════════════════════════════════════════════════════════════════════════
# CLI standalone
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Sources Discovery Agent - find all available sources for a fund"
    )
    parser.add_argument("--isin", required=True, help="Fund ISIN code")
    parser.add_argument("--fund-name", default="", help="Fund name")
    parser.add_argument("--gestora", default="", help="Gestora name")
    parser.add_argument(
        "--gestores",
        default="",
        help="Manager names separated by semicolons",
    )
    parser.add_argument(
        "--gestora-domain",
        default="",
        help="Gestora website domain (e.g. avantage-capital.es)",
    )
    args = parser.parse_args()

    gestores_list = (
        [g.strip() for g in args.gestores.split(";") if g.strip()]
        if args.gestores
        else []
    )
    agent = SourcesAgent(
        isin=args.isin,
        fund_name=args.fund_name,
        gestora=args.gestora,
        gestores=gestores_list,
        gestora_domain=args.gestora_domain,
    )
    result = asyncio.run(agent.run())
    print(f"\nSources found: {len(result.get('sources', []))}")
    for src in result.get("sources", []):
        status = "OK" if src.get("accesible") else "FAIL" if src.get("accesible") is False else "?"
        print(f"  [{status}] [{src['tipo']}] {src['url'][:80]}")
