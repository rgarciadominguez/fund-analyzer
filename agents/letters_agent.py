"""
Letters Agent v2 — Cartas trimestrales de gestores

Estrategia de descubrimiento en dos fases:
  Fase 1: Descubrimiento de fuentes (DDG broad search -> dominios relevantes, cacheados 30 dias)
  Fase 2: Busqueda año a año en paralelo con fuentes descubiertas (asyncio.gather + Semaphore)

Para cada URL candidata:
  - BLOG_INDEX  -> recorrer links de articulos + paginacion (hasta 5 paginas)
  - ARTICLE     -> extraer texto + Claude
  - PDF         -> descargar + pdfplumber + Claude

Output:
  data/funds/{ISIN}/letters_data.json
  data/funds/{ISIN}/letters_sources.json  (cache fuentes, TTL 30 dias)
"""
import asyncio
import json
import re
import sys
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

from tools.http_client import get_bytes, get_with_headers
from tools.pdf_extractor import extract_pages_by_keyword, extract_page_range, get_pdf_metadata
from tools.claude_extractor import extract_structured_data


async def _fetch_no_retry(url: str, headers: dict, timeout: float = 15.0) -> str:
    """Single GET with no retry — for probing paths that may not exist (e.g. sitemaps)."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.text

# ── Constants ─────────────────────────────────────────────────────────────────

DDG_HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://duckduckgo.com/",
}

# ISIN-specific confirmed sources (user-validated)
_ISIN_LETTERS_URLS: dict[str, list[str]] = {
    "ES0156572002": [  # MyInvestor Cartera Permanente
        "https://www.riverpatrimonio.com/tag/myinvestor",
        "https://www.riverpatrimonio.com/blog",
    ],
    "ES0175316001": [  # Dunas Valor Flexible
        "https://dunascapital.com/publicaciones/",
        "https://www.dunas.es/publicaciones/",
    ],
}

# Generic sources by ISIN prefix
_GESTORA_LETTERS_URLS: dict[str, list[str]] = {
    "ES": [],
    "LU": ["https://www.dnca-investments.com/en/news/management-letters"],
    "IE": [], "FR": [], "GB": [], "DE": [],
}

# URL path patterns that indicate a blog/publication INDEX page
BLOG_INDEX_PATHS = re.compile(
    r"/tag/|/category/|/blog/?$|/publicaciones/?$|/cartas/?$|"
    r"/newsletters?/?$|/comentarios?/?$|/archivo/?$|/news/?$|/noticias/?$",
    re.IGNORECASE,
)

# Keywords to filter relevant article links on index pages
LETTER_LINK_KW = re.compile(
    r"carta|letter|trimestral|quarterly|comment|semestral|semestr|informe|"
    r"newsletter|perspectiv|gestion|gestor|mercado|cartera|portfolio",
    re.IGNORECASE,
)

# Text keywords for PDF section extraction (pdfplumber)
LETTER_TEXT_KEYWORDS = [
    "posiciones", "cartera", "portfolio", "holdings",
    "perspectivas", "outlook", "tesis", "rentabilidad",
    "performance", "trimestre", "quarter",
]

# Known Spanish fund commentary domains (seeded into source discovery)
KNOWN_COMMENTARY_SITES = [
    "riverpatrimonio.com",
    "inversor-tranquilo.com",
    "rankia.com",
    "finect.com",
    "selfbank.es",
    "myinvestor.es",
]

MAX_LETTERS         = 12   # max cartas a procesar por run
MAX_PAGES_PAGINATE  = 5    # max paginas de paginacion a seguir
CACHE_TTL_DAYS      = 30   # dias antes de re-descubrir fuentes
DDG_SEM_SIZE        = 3    # max consultas DDG en paralelo
EXTRACT_SEM_SIZE    = 2    # max extracciones Claude en paralelo


class LettersAgent:
    """
    Agente de cartas trimestrales v2.

    Descubrimiento en dos fases:
    1. Source discovery: DDG broad queries -> dominios relevantes (cacheados)
    2. Year-by-year: busqueda paralela con fuentes descubiertas

    async def run() -> dict segun convenio del proyecto.
    """

    def __init__(self, isin: str, config: dict = None, gestora_url: str = "",
                 fund_name: str = "", gestora: str = "", anio_creacion: int | None = None):
        self.isin          = isin.strip().upper()
        self.config        = config or {"fuentes": "1"}
        self.gestora_url   = gestora_url
        self.prefix        = self.isin[:2].upper()
        self.fund_name     = fund_name
        self.gestora       = gestora
        self.anio_creacion = anio_creacion or (datetime.now().year - 5)
        self.current_year  = datetime.now().year
        # Short name without legal suffixes — better DDG results
        self.fund_short    = re.sub(
            r'\b(FI|SICAV|FP|SIL|FUND|FONDO)\b', '',
            fund_name, flags=re.IGNORECASE,
        ).strip().strip(",").strip()

        root = Path(__file__).parent.parent
        self.fund_dir      = root / "data" / "funds" / self.isin
        self.letters_dir   = self.fund_dir / "raw" / "letters"
        self._sources_path = self.fund_dir / "letters_sources.json"
        self._log_path     = root / "progress.log"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.letters_dir.mkdir(parents=True, exist_ok=True)

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [LETTERS] [{level}] {msg}"
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self) -> dict:
        result = {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(),
            "cartas": [],
            "fuentes": {"cartas_gestores": [], "urls_consultadas": []},
        }

        if self.config.get("fuentes") == "2":
            self._log("INFO", "Config fuentes=2: saltar cartas trimestrales")
            self._save(result)
            return result

        self._log("START", f"LettersAgent v2 — {self.isin} ({self.fund_short or self.fund_name})")

        candidate_urls: list[dict] = []
        seen_urls: set[str] = set()

        def add_candidates(links: list[dict]):
            for link in links:
                url = link.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    candidate_urls.append(link)

        # ── Phase 1a: Sitemap discovery from all seed domains ─────────────
        seed_domains = self._seed_domains()
        for domain in seed_domains:
            sitemap_links = await self._discover_via_sitemap(domain)
            add_candidates(sitemap_links)
            result["fuentes"]["urls_consultadas"].append(f"sitemap:{domain}")

        # ── Phase 1b: Expand seed URLs (blog index / standalone article) ──
        for seed_url in self._build_seed_urls():
            result["fuentes"]["urls_consultadas"].append(seed_url)
            try:
                links = await self._expand_url(seed_url)
                add_candidates(links)
            except Exception as exc:
                self._log("WARN", f"Seed URL error {seed_url[:60]}: {exc}")

        # ── Phase 1c: Source discovery + parallel DDG (fallback) ──────────
        if len(candidate_urls) < 3 and (self.fund_short or self.fund_name):
            self._log("INFO", "Pocos candidatos — activando DDG discovery")
            discovered_domains = await self._discover_sources()
            # Try sitemaps from discovered domains too
            for domain in discovered_domains[:4]:
                if domain not in seed_domains:
                    sitemap_links = await self._discover_via_sitemap(domain)
                    add_candidates(sitemap_links)
            ddg_links = await self._search_all_years_parallel(discovered_domains)
            add_candidates(ddg_links)

        if not candidate_urls:
            self._log("WARN", "Sin cartas encontradas. Guardando JSON vacio.")
            self._save(result)
            return result

        self._log("INFO", f"Candidatas totales: {len(candidate_urls)} URLs")

        # ── Phase 2: Process — extract content in parallel ─────────────────
        candidates = self._prioritize(candidate_urls)[:MAX_LETTERS]
        sem = asyncio.Semaphore(EXTRACT_SEM_SIZE)
        tasks = [self._safe_process(entry, sem) for entry in candidates]
        processed = await asyncio.gather(*tasks)

        for carta in processed:
            if carta:
                result["cartas"].append(carta)
                result["fuentes"]["cartas_gestores"].append(
                    carta.get("archivo", "") or carta.get("url_fuente", "")
                )

        self._log("OK", f"{len(result['cartas'])} cartas extraidas de {len(candidates)} candidatas")
        self._save(result)
        return result

    # ── Seed URL building ─────────────────────────────────────────────────────

    def _build_seed_urls(self) -> list[str]:
        """Build ordered list of seed URLs: ISIN-specific > config > gestora > prefix."""
        urls: list[str] = []
        if self.config.get("gestora_url"):
            urls.append(self.config["gestora_url"])
        urls.extend(_ISIN_LETTERS_URLS.get(self.isin, []))
        if self.gestora_url:
            urls.append(self.gestora_url)
        urls.extend(_GESTORA_LETTERS_URLS.get(self.prefix, []))
        # Deduplicate preserving order
        seen: set[str] = set()
        return [u for u in urls if u and not (u in seen or seen.add(u))]

    # ── Seed domain extraction ────────────────────────────────────────────────

    def _seed_domains(self) -> list[str]:
        """Extract unique domains from all known seed URLs for this ISIN."""
        domains: list[str] = []
        seen: set[str] = set()

        def add(url: str):
            d = self._domain_of(url)
            if d and d not in seen:
                seen.add(d)
                domains.append(d)

        for url in _ISIN_LETTERS_URLS.get(self.isin, []):
            add(url)
        if self.gestora_url:
            add(self.gestora_url)
        for url in _GESTORA_LETTERS_URLS.get(self.prefix, []):
            add(url)
        return domains

    # ── Sitemap-based discovery ───────────────────────────────────────────────

    async def _discover_via_sitemap(self, domain: str) -> list[dict]:
        """
        Discover letter URLs from a domain's sitemap.
        Strategy:
          1. Fetch /sitemap.xml -> look for sub-sitemap links
          2. Also try common Wix/WordPress blog sitemap paths directly
          3. Parse each sitemap XML for <url><loc> entries
          4. Filter strictly by distinctive fund keywords (prevents false positives
             from other funds on the same site)
        """
        base = f"https://{domain}"
        EXTRA_SITEMAP_PATHS = [
            "/blog-posts-sitemap.xml",    # Wix
            "/post-sitemap.xml",          # WordPress
            "/sitemap_index.xml",         # WordPress Yoast index
            "/sitemap-posts-post-1.xml",  # All-in-One SEO
            "/page_sitemap.xml",
            "/news-sitemap.xml",
        ]

        candidate_sitemaps: list[str] = []
        tried: set[str] = set()
        found_entries: list[dict] = []

        # Step 1: Fetch main sitemap.xml and look for sub-sitemaps
        main_sitemap_url = f"{base}/sitemap.xml"
        tried.add(main_sitemap_url)
        try:
            xml = await _fetch_no_retry(main_sitemap_url, DDG_HEADERS)
            # Extract sub-sitemap locs
            sub_locs = re.findall(r"<loc>([^<]+sitemap[^<]*)</loc>", xml, re.IGNORECASE)
            for loc in sub_locs:
                loc = loc.strip()
                if loc not in tried:
                    candidate_sitemaps.append(loc)
            # Also try to parse as a URL sitemap directly
            found_entries.extend(self._parse_sitemap_urls(xml))
        except Exception as exc:
            self._log("WARN", f"Sitemap {main_sitemap_url}: {exc}")

        # Step 2: Add common CMS-specific paths (only if not already found via main)
        for path in EXTRA_SITEMAP_PATHS:
            url = base + path
            if url not in tried:
                candidate_sitemaps.append(url)

        # Step 3: Fetch all candidate sitemaps
        for sitemap_url in candidate_sitemaps:
            if sitemap_url in tried:
                continue
            tried.add(sitemap_url)
            try:
                xml = await _fetch_no_retry(sitemap_url, DDG_HEADERS)
                entries = self._parse_sitemap_urls(xml)
                if entries:
                    self._log("INFO", f"Sitemap {sitemap_url.split('/')[-1]}: {len(entries)} matches")
                    found_entries.extend(entries)
                    await asyncio.sleep(0.5)
            except Exception:
                pass  # silently skip missing sitemap paths

        # Dedup by URL
        seen_urls: set[str] = set()
        result: list[dict] = []
        for e in found_entries:
            if e["url"] not in seen_urls:
                seen_urls.add(e["url"])
                result.append(e)

        if result:
            self._log("INFO", f"Sitemap [{domain}]: {len(result)} candidatas")
        return result

    def _parse_sitemap_urls(self, xml: str) -> list[dict]:
        """
        Parse sitemap XML and return entries whose URL contains a distinctive
        keyword from the fund name (no false positives from other funds on same site).
        """
        entries: list[dict] = []
        gestora_kw = self.gestora.lower()[:12] if self.gestora else ""
        isin_lower = self.isin.lower()

        def is_match(text: str) -> bool:
            return self._is_letter_url(text, "", gestora_kw, isin_lower)

        url_blocks = re.findall(r"<url>(.*?)</url>", xml, re.DOTALL | re.IGNORECASE)

        if not url_blocks:
            # Flat sitemap: just extract <loc> that aren't sub-sitemaps
            locs = re.findall(r"<loc>([^<]+)</loc>", xml)
            for loc in locs:
                loc = loc.strip()
                if "sitemap" in loc.lower():
                    continue
                if not is_match(loc.lower()):
                    continue
                fecha = self._extract_date_hint(loc)
                entries.append({
                    "url": loc,
                    "titulo": loc.split("/")[-1].replace("-", " ").replace("_", " ")[:100],
                    "fecha_estimada": fecha,
                    "is_html": not loc.lower().endswith(".pdf"),
                })
            return entries

        for block in url_blocks:
            loc_m = re.search(r"<loc>([^<]+)</loc>", block)
            if not loc_m:
                continue
            url = loc_m.group(1).strip()
            lastmod_m = re.search(r"<lastmod>([^<]+)</lastmod>", block)
            lastmod = lastmod_m.group(1).strip() if lastmod_m else ""

            combined = (url + " " + lastmod).lower()
            if not is_match(combined):
                continue

            fecha = self._extract_date_hint(url) or self._extract_date_hint(lastmod) or lastmod[:7]
            title = url.split("/")[-1].replace("-", " ").replace("_", " ")[:100]
            entries.append({
                "url": url,
                "titulo": title,
                "fecha_estimada": fecha,
                "is_html": not url.lower().endswith(".pdf"),
            })

        return entries

    # Generic finance words that are too common to use as fund identifiers
    _GENERIC_FINANCE_WORDS = frozenset({
        "valor", "fondo", "renta", "fija", "variable", "global", "fund",
        "invest", "capital", "asset", "growth", "bonds", "equity", "multi",
        "cartera", "activos", "flexible", "total", "return", "income",
        "permanente", "mixto", "fixed", "dynamic", "balance", "balanced",
    })

    def _fund_keywords(self) -> list[str]:
        """
        Extract distinctive keywords from the fund name for URL matching.
        Rules:
          - ALL-CAPS acronyms >= 3 chars (brand names like DNCA, ISIN prefix) → always include
          - Words >4 chars that are NOT generic finance terms → include
        Falls back to ISIN if nothing distinctive found.
        """
        words = (self.fund_short or self.fund_name or "").split()
        kws: list[str] = []
        for w in words:
            wl = w.lower()
            if wl in self._GENERIC_FINANCE_WORDS:
                continue
            # ALL-CAPS brand acronyms (e.g. DNCA, BBVA, ING) — min 3 chars
            if w.isupper() and len(w) >= 3:
                kws.append(wl)
            # Normal words — must be >4 chars to avoid noise
            elif len(w) > 4:
                kws.append(wl)
        return kws if kws else [self.isin.lower()]

    def _is_letter_url(self, text: str, fund_kw: str, gestora_kw: str, isin: str) -> bool:
        """
        Check if a URL/text is relevant to this fund's letters.
        Requires ALL distinctive fund keywords to appear (AND-logic), which prevents
        false positives on sites that write about generic fund concepts (e.g. 'cartera permanente').
        Falls back to gestora or ISIN if no fund keywords found.
        """
        kws = self._fund_keywords()
        if kws and kws != [self.isin.lower()]:
            # All keywords must appear (with hyphen fallback for URL slugs)
            if all(kw in text or kw.replace("-", " ") in text for kw in kws):
                return True
        if gestora_kw:
            gestora_kw_h = gestora_kw.replace(" ", "-")
            if gestora_kw in text or gestora_kw_h in text:
                return True
        if isin and isin in text:
            return True
        return False

    # ── URL expansion ─────────────────────────────────────────────────────────

    async def _expand_url(self, url: str) -> list[dict]:
        """
        Classify a URL and expand it into individual letter entries.
        - PDF -> return directly
        - BLOG_INDEX -> scrape article links + follow pagination
        - ARTICLE -> return directly as single entry
        """
        if url.lower().endswith(".pdf"):
            return [{"url": url, "titulo": url.split("/")[-1],
                     "fecha_estimada": self._extract_date_hint(url), "is_html": False}]

        try:
            html = await get_with_headers(url, DDG_HEADERS)
        except Exception as exc:
            self._log("WARN", f"No accesible: {url[:60]} — {exc}")
            return []

        soup = BeautifulSoup(html, "lxml")
        url_type = self._classify_url(url, soup)
        self._log("INFO", f"[{url_type}] {url[:70]}")

        if url_type == "blog_index":
            return await self._scrape_blog_index(url, soup)

        if url_type == "article":
            h1 = soup.find("h1") or soup.find("title")
            titulo = h1.get_text(strip=True)[:120] if h1 else url
            fecha = self._extract_date_hint(titulo + " " + url)
            return [{"url": url, "titulo": titulo, "fecha_estimada": fecha, "is_html": True}]

        return []

    # ── URL classification ────────────────────────────────────────────────────

    def _classify_url(self, url: str, soup: BeautifulSoup) -> Literal["article", "blog_index", "unknown"]:
        """
        Classify a fetched URL as a blog index, individual article, or unknown.
        Order of checks: path patterns -> structural HTML signals -> content length.
        """
        path = urlparse(url).path.lower()

        # 1. Path-based: definitive index patterns
        if BLOG_INDEX_PATHS.search(path):
            return "blog_index"

        # 2. Structural: multiple <article> elements → index
        articles = soup.find_all("article")
        if len(articles) >= 3:
            return "blog_index"

        # 3. Structural: multiple <time> elements → list of dated posts
        times = soup.find_all("time")
        if len(times) >= 3:
            return "blog_index"

        # 4. Structural: explicit pagination element
        pagination = soup.find(attrs={"class": re.compile(
            r"paginat|next-page|page-link|wp-pagenavi|page-numbers", re.I
        )})
        if pagination:
            return "blog_index"

        # 5. Content-based: long article (4+ substantial paragraphs)
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")
                      if len(p.get_text(strip=True)) > 60]
        if len(paragraphs) >= 4:
            return "article"

        return "unknown"

    # ── Blog index scraping ───────────────────────────────────────────────────

    async def _scrape_blog_index(self, base_url: str, initial_soup: BeautifulSoup) -> list[dict]:
        """
        Scrape a blog/publication index: extract article links + follow pagination
        up to MAX_PAGES_PAGINATE pages.
        """
        all_entries: list[dict] = []
        seen: set[str] = set()
        soup = initial_soup
        current_url = base_url

        for page_num in range(MAX_PAGES_PAGINATE):
            entries = self._extract_article_links(soup, current_url)
            for e in entries:
                if e["url"] not in seen:
                    seen.add(e["url"])
                    all_entries.append(e)

            next_url = self._find_next_page(soup, current_url)
            if not next_url or next_url == current_url:
                break
            self._log("INFO", f"  Paginacion [{page_num + 2}/{MAX_PAGES_PAGINATE}]: {next_url[:60]}")
            try:
                html = await get_with_headers(next_url, DDG_HEADERS)
                soup = BeautifulSoup(html, "lxml")
                current_url = next_url
                await asyncio.sleep(1)
            except Exception as exc:
                self._log("WARN", f"Error paginacion {next_url[:50]}: {exc}")
                break

        self._log("INFO", f"Blog index: {len(all_entries)} articulos encontrados en {base_url[:50]}")
        return all_entries

    def _extract_article_links(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """
        Extract individual article links from a blog index page using 3 strategies:
        1. <article> elements with <a> links
        2. <h2>/<h3> headings with <a> links
        3. PDF <a> links anywhere on the page
        """
        entries: list[dict] = []
        parsed_base = urlparse(base_url)
        base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
        gestora_kw = self.gestora.lower()[:12] if self.gestora else ""
        isin_lower = self.isin.lower()

        def is_relevant(text: str, href: str) -> bool:
            combined = (text + " " + href).lower()
            return self._is_letter_url(combined, "", gestora_kw, isin_lower)

        # Strategy 1: <article> elements
        for article in soup.find_all("article"):
            # Prefer heading link, fallback to first link
            heading = article.find(["h2", "h3"])
            link = (heading.find("a", href=True) if heading else None) or article.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            text = (heading.get_text(strip=True) if heading else link.get_text(strip=True))
            if not is_relevant(text, href):
                continue
            full_url = self._abs_url(href, base_url, base_domain)
            if not full_url:
                continue
            fecha = self._extract_date_hint(text + " " + href)
            time_tag = article.find("time")
            if time_tag:
                fecha = fecha or self._extract_date_hint(
                    (time_tag.get("datetime") or "") + " " + time_tag.get_text(strip=True)
                )
            entries.append({"url": full_url, "titulo": text[:120],
                            "fecha_estimada": fecha, "is_html": not full_url.lower().endswith(".pdf")})

        # Strategy 2: <h2>/<h3> with <a> (used when no <article> wrapper)
        if not entries:
            for heading in soup.find_all(["h2", "h3"]):
                link = heading.find("a", href=True)
                if not link:
                    continue
                href = link["href"]
                text = heading.get_text(strip=True)
                if not is_relevant(text, href):
                    continue
                full_url = self._abs_url(href, base_url, base_domain)
                if not full_url:
                    continue
                fecha = self._extract_date_hint(text + " " + href)
                entries.append({"url": full_url, "titulo": text[:120],
                                "fecha_estimada": fecha, "is_html": True})

        # Strategy 3: PDF links (publication indexes often list PDFs directly)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            text = a.get_text(strip=True)
            if not is_relevant(text, href):
                continue
            full_url = self._abs_url(href, base_url, base_domain)
            if not full_url:
                continue
            fecha = self._extract_date_hint(text + " " + href)
            entries.append({"url": full_url, "titulo": text[:120] or href.split("/")[-1],
                            "fecha_estimada": fecha, "is_html": False})

        return entries

    def _find_next_page(self, soup: BeautifulSoup, current_url: str) -> str | None:
        """Detect and return the next pagination page URL, or None."""
        parsed = urlparse(current_url)
        base_domain = f"{parsed.scheme}://{parsed.netloc}"

        # CSS selector patterns (most reliable)
        for sel in [
            "a[rel='next']", "a.next", ".pagination a.next",
            ".nav-previous a", "a[aria-label='Next page']",
            ".wp-pagenavi a.nextpostslink",
        ]:
            try:
                link = soup.select_one(sel)
                if link and link.get("href"):
                    return self._abs_url(link["href"], current_url, base_domain)
            except Exception:
                continue

        # Text-based detection
        NEXT_TEXTS = {"siguiente", "next", "older posts", "mas antiguo", "anterior", ">>", "›"}
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True).lower() in NEXT_TEXTS:
                return self._abs_url(a["href"], current_url, base_domain)

        # Pattern: /page/N/ -> /page/N+1/
        m = re.search(r"/page/(\d+)/?$", current_url)
        if m:
            next_page = int(m.group(1)) + 1
            return re.sub(r"/page/\d+/?$", f"/page/{next_page}/", current_url)

        return None

    def _abs_url(self, href: str, base_url: str, base_domain: str) -> str | None:
        """Convert any href to an absolute URL."""
        if not href or href.startswith("#") or href.startswith("javascript:"):
            return None
        if href.startswith("http"):
            return href
        if href.startswith("//"):
            return urlparse(base_url).scheme + ":" + href
        if href.startswith("/"):
            return base_domain + href
        return base_url.rstrip("/") + "/" + href

    # ── Source discovery (DDG broad, cached) ──────────────────────────────────

    async def _discover_sources(self) -> list[str]:
        """
        Run 3 broad DDG queries to find domains that publish content about this fund.
        Returns ordered list of domain strings for year-specific queries.
        Cached for CACHE_TTL_DAYS days in letters_sources.json.
        """
        cache = self._load_sources_cache()
        if cache and self._cache_fresh(cache):
            domains = cache.get("discovered_domains", [])
            self._log("INFO", f"Fuentes desde cache: {domains[:4]}")
            return domains

        self._log("INFO", "Descubriendo fuentes (DDG broad)...")
        fund_q = self.fund_short or self.fund_name
        if not fund_q:
            return list(KNOWN_COMMENTARY_SITES)

        discovery_queries = [
            f'"{fund_q}" carta gestores',
            f'"{fund_q}" informe trimestral',
            f'"{self.gestora}" cartas gestores inversión' if self.gestora else f'"{fund_q}" carta',
        ]

        domain_hits: dict[str, int] = {}
        for query in discovery_queries:
            results = await self._ddg_search(query, max_results=8)
            for r in results:
                domain = self._domain_of(r.get("url", ""))
                if domain and domain not in ("duckduckgo.com", "google.com", "bing.com"):
                    domain_hits[domain] = domain_hits.get(domain, 0) + 1
            await asyncio.sleep(2)

        # Seed known commentary sites with weight 0 (always included, low priority)
        for site in KNOWN_COMMENTARY_SITES:
            if site not in domain_hits:
                domain_hits[site] = 0

        discovered = [d for d, _ in sorted(domain_hits.items(), key=lambda x: x[1], reverse=True)]
        self._save_sources_cache({
            "isin": self.isin,
            "discovered_domains": discovered,
            "last_discovery": datetime.now().isoformat(),
        })
        self._log("INFO", f"Fuentes descubiertas: {discovered[:5]}")
        return discovered

    def _load_sources_cache(self) -> dict | None:
        if not self._sources_path.exists():
            return None
        try:
            return json.loads(self._sources_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _cache_fresh(self, cache: dict) -> bool:
        try:
            ts = datetime.fromisoformat(cache.get("last_discovery", "2000-01-01"))
            return (datetime.now() - ts) < timedelta(days=CACHE_TTL_DAYS)
        except Exception:
            return False

    def _save_sources_cache(self, cache: dict):
        self._sources_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ── DDG year-by-year search (parallel) ───────────────────────────────────

    async def _search_all_years_parallel(self, discovered_domains: list[str]) -> list[dict]:
        """
        Search all years from anio_creacion to current_year in parallel,
        using asyncio.gather + Semaphore to avoid DDG rate limits.
        """
        sem = asyncio.Semaphore(DDG_SEM_SIZE)
        years = list(range(self.anio_creacion, self.current_year + 1))
        tasks = [self._search_year_sem(year, discovered_domains, sem) for year in years]
        year_results = await asyncio.gather(*tasks)

        all_links: list[dict] = []
        seen: set[str] = set()
        for links in year_results:
            for link in links:
                url = link.get("url", "")
                if url and url not in seen:
                    seen.add(url)
                    all_links.append(link)

        self._log("INFO", f"DDG paralelo: {len(all_links)} URLs unicas en {len(years)} anos")
        return all_links

    async def _search_year_sem(self, year: int, domains: list[str], sem: asyncio.Semaphore) -> list[dict]:
        async with sem:
            result = await self._search_year(year, domains)
            await asyncio.sleep(1.5)
            return result

    async def _search_year(self, year: int, domains: list[str]) -> list[dict]:
        """Build 3 DDG queries for a year and collect matching letter URLs."""
        fund_q = self.fund_short or self.fund_name
        queries: list[str] = []

        # 1. Generic fund + year
        if fund_q:
            queries.append(f'"{fund_q}" carta {year}')

        # 2. Domain-specific for top 2 discovered domains
        for domain in domains[:2]:
            if fund_q:
                queries.append(f'site:{domain} "{fund_q}" {year}')

        # 3. ISIN-based fallback
        queries.append(f'"{self.isin}" carta informe {year}')

        found: list[dict] = []
        seen: set[str] = set()
        for query in queries[:3]:
            results = await self._ddg_search(query, max_results=4)
            for r in results:
                url = r.get("url", "")
                if not url or url in seen:
                    continue
                seen.add(url)
                combined = (url + " " + r.get("titulo", "")).lower()
                if not re.search(r"\.pdf|carta|letter|trimestral|quarterly|comentario|informe", combined):
                    continue
                found.append({
                    "url": url,
                    "titulo": r.get("titulo", ""),
                    "fecha_estimada": self._extract_date_hint(r.get("titulo", "") + " " + url) or str(year),
                    "is_html": not url.lower().endswith(".pdf"),
                })

        if found:
            self._log("INFO", f"  DDG {year}: {len(found)} resultado(s) -> {found[0]['url'][:60]}")
        return found

    async def _ddg_search(self, query: str, max_results: int = 5) -> list[dict]:
        """Execute a DuckDuckGo HTML search; return [{titulo, url}]."""
        enc_q = urllib.parse.quote_plus(query)
        ddg_url = f"https://html.duckduckgo.com/html/?q={enc_q}"
        try:
            html = await get_with_headers(ddg_url, DDG_HEADERS)
            soup = BeautifulSoup(html, "lxml")
            results: list[dict] = []
            for a_tag in soup.select(".result__a"):
                href = a_tag.get("href", "")
                titulo = a_tag.get_text(strip=True)
                url = self._extract_ddg_url(href)
                if url:
                    results.append({"titulo": titulo, "url": url})
                    if len(results) >= max_results:
                        break
            return results
        except Exception as exc:
            self._log("WARN", f"DDG error '{query[:40]}': {exc}")
            return []

    def _extract_ddg_url(self, href: str) -> str:
        if href.startswith("http") and "duckduckgo" not in href:
            return href
        m = re.search(r"uddg=([^&]+)", href)
        if m:
            return urllib.parse.unquote(m.group(1))
        m2 = re.search(r"\bu=([^&]+)", href)
        if m2:
            return urllib.parse.unquote(m2.group(1))
        return ""

    # ── Prioritization ────────────────────────────────────────────────────────

    def _prioritize(self, candidates: list[dict]) -> list[dict]:
        """Sort candidates: most recent year first; PDFs preferred within same year."""
        def key(e: dict) -> tuple:
            fecha = e.get("fecha_estimada", "")
            m = re.search(r"(20\d{2})", fecha)
            year = int(m.group(1)) if m else 0
            pdf_first = 0 if not e.get("is_html", True) else 1
            return (-year, pdf_first)
        return sorted(candidates, key=key)

    # ── Letter processing ─────────────────────────────────────────────────────

    async def _safe_process(self, entry: dict, sem: asyncio.Semaphore) -> dict | None:
        async with sem:
            return await self._process_letter(entry)

    async def _process_letter(self, entry: dict) -> dict | None:
        url = entry.get("url", "")
        is_html = entry.get("is_html", True) if "is_html" in entry else not url.lower().endswith(".pdf")
        if is_html:
            return await self._process_html_letter(entry)
        return await self._process_pdf_letter(entry)

    async def _process_html_letter(self, entry: dict) -> dict | None:
        url = entry.get("url", "")
        self._log("INFO", f"HTML: {url[:70]}")
        try:
            html = await get_with_headers(url, DDG_HEADERS)
            soup = BeautifulSoup(html, "lxml")
            for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
                tag.decompose()
            paragraphs = [
                p.get_text(strip=True)
                for p in soup.find_all(["p", "h2", "h3"])
                if len(p.get_text(strip=True)) > 40
            ]
            text = " ".join(paragraphs)
            if len(text) < 200:
                self._log("WARN", f"HTML insuficiente ({len(text)} chars): {url[:60]}")
                return None
            return await self._extract_letter_content(text[:5000], entry)
        except Exception as exc:
            self._log("WARN", f"Error HTML {url[:60]}: {exc}")
            return None

    async def _process_pdf_letter(self, entry: dict) -> dict | None:
        url = entry.get("url", "")
        safe_name = re.sub(r"[^\w\-]", "_", entry.get("titulo", "carta"))[:50]
        filename = f"letter_{safe_name}.pdf"
        target = self.letters_dir / filename

        if not (target.exists() and target.stat().st_size > 1000):
            try:
                data = await get_bytes(url)
                if b"%PDF" not in data[:20]:
                    self._log("WARN", f"No es PDF: {url[:60]}")
                    return None
                target.write_bytes(data)
                self._log("OK", f"PDF descargado: {filename} ({len(data) // 1024} KB)")
            except Exception as exc:
                self._log("WARN", f"Error PDF {url[:60]}: {exc}")
                return None
        else:
            self._log("INFO", f"PDF existente: {filename}")

        try:
            meta = get_pdf_metadata(str(target))
            text = extract_pages_by_keyword(str(target), LETTER_TEXT_KEYWORDS, context_pages=1)
            if not text.strip():
                text = extract_page_range(str(target), 0, min(5, meta["num_pages"]))
        except Exception as exc:
            self._log("WARN", f"Error extrayendo PDF {filename}: {exc}")
            return None

        if not text.strip():
            return None
        return await self._extract_letter_content(text[:4000], {**entry, "archivo": filename})

    async def _extract_letter_content(self, text: str, entry: dict) -> dict | None:
        """Pass text to Claude to extract structured letter data."""
        schema = {
            "fecha": "fecha de la carta o periodo (ej. 'Q1 2024', '1T2024', '1S 2025')",
            "periodo": "formato normalizado (ej. '2024-Q1', '2025-S1')",
            "resumen_mercado": "contexto de mercado descrito en la carta (2-4 frases)",
            "posiciones_comentadas": [
                {"nombre": "activo o empresa",
                 "accion": "entrada/salida/aumento/reduccion/mantener",
                 "racional": "razon de la decision"}
            ],
            "tesis_inversion": "tesis principal de inversion expuesta en la carta",
            "perspectivas": "outlook o perspectivas para el proximo periodo",
            "decisiones_cartera": "resumen de cambios realizados en la cartera",
        }
        url = entry.get("url", "")
        archivo = entry.get("archivo", "")
        try:
            extracted = extract_structured_data(
                text, schema,
                context=f"Carta trimestral/semestral del fondo {self.fund_name} (ISIN {self.isin})",
            )
            extracted["archivo"] = archivo
            extracted["url_fuente"] = url
            self._log("OK", f"Extraida: {url[:60]}")
            return extracted
        except Exception as exc:
            self._log("WARN", f"Claude error {url[:60]}: {exc}")
            return {
                "archivo": archivo, "url_fuente": url,
                "fecha": entry.get("fecha_estimada", ""),
                "periodo": entry.get("fecha_estimada", ""),
                "resumen_mercado": None, "posiciones_comentadas": [],
                "tesis_inversion": None, "perspectivas": None, "decisiones_cartera": None,
            }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_date_hint(self, text: str) -> str:
        """Extract quarter/semester/year hint from text."""
        for pat in [
            r'\b(Q[1-4])\s*(20\d{2})\b',
            r'\b([1-4][TtQ])\s*(20\d{2})\b',
            r'\b([12][Ss])\s*(20\d{2})\b',
            r'\b(20\d{2})[/\-](0[1-9]|1[0-2])\b',
            r'\b(20\d{2})\b',
        ]:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(0)
        return ""

    def _domain_of(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lstrip("www.")
        except Exception:
            return ""

    def _save(self, result: dict) -> None:
        out = self.fund_dir / "letters_data.json"
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log("OK", f"Guardado: {out}")


# ── Standalone CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--fund-name", default="")
    parser.add_argument("--gestora", default="")
    parser.add_argument("--anio-creacion", type=int, default=None)
    args = parser.parse_args()

    agent = LettersAgent(
        isin=args.isin,
        fund_name=args.fund_name,
        gestora=args.gestora,
        anio_creacion=args.anio_creacion,
    )
    result = asyncio.run(agent.run())
    print(f"Cartas encontradas: {len(result['cartas'])}")
    for c in result["cartas"]:
        print(f"  [{c.get('periodo','?')}] {c.get('url_fuente','')[:70]}")
