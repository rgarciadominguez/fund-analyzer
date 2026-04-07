"""
Letters Agent — Encuentra y descarga cartas del gestor

Busca cartas trimestrales, semestrales y anuales del fondo desde el año de inicio
hasta la actualidad. Usa el SearchEngine centralizado para búsqueda Google + navegación web.

Pipeline:
  1. Buscar fuentes de cartas (Google + caché de otros agentes)
  2. Navegar webs de gestora para encontrar PDFs
  3. Buscar año por año si faltan cartas
  4. Validar cada URL (es realmente una carta de este fondo?)
  5. Descargar PDFs + extraer texto con pdfplumber (sin API)
  6. Output: texto raw completo al analyst_agent

Se ejecuta DESPUÉS de cnmv_agent (necesita anio_creacion y nombre del fondo).

Output:
  data/funds/{ISIN}/letters_data.json
"""
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.google_search import (
    SearchEngine, fetch_page_text, find_pdfs_in_page,
    find_links_by_keywords, crawl_for_documents,
)
from tools.http_client import get_bytes
from tools.pdf_extractor import extract_page_range, get_pdf_metadata

console = Console(highlight=False, force_terminal=False)

# Keywords that indicate a letter/report
LETTER_KEYWORDS = [
    "carta", "letter", "informe", "report", "trimestral", "semestral",
    "anual", "quarterly", "semiannual", "annual", "mensual", "monthly",
    "inversores", "coinversores", "partícipes",
]

# Keywords to filter PDFs (must match at least one)
PDF_LETTER_PATTERNS = re.compile(
    r"carta|informe|report|letter|trimestral|semestral|anual|mensual|quarterly",
    re.IGNORECASE,
)


class LettersAgent:

    def __init__(self, isin: str, config: dict = None, fund_name: str = "",
                 gestora: str = "", anio_creacion: int | None = None, **kwargs):
        self.isin = isin.strip().upper()
        self.config = config or {}
        self.fund_name = fund_name
        self.fund_short = fund_name.split(",")[0].strip() if fund_name else ""
        self.gestora = gestora
        self.anio_creacion = anio_creacion or 2015
        self.current_year = datetime.now().year

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.letters_dir = self.fund_dir / "raw" / "letters"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.letters_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = root / "progress.log"

        self.search = SearchEngine(isin=self.isin)

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [LETTERS] [{level}] {msg}"
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"Letters Agent — {self.isin} ({self.fund_short})")

        result = {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(),
            "cartas": [],
            "anos_sin_carta": [],
            "fuentes_consultadas": [],
        }

        if self.config.get("fuentes") == "2":
            self._log("INFO", "Config fuentes=2: saltar cartas")
            self._save(result)
            return result

        # ── Paso 1: Buscar fuentes de cartas ─────────────────────────────────
        candidate_urls = await self._find_letter_sources()
        self._log("INFO", f"Paso 1: {len(candidate_urls)} URLs candidatas")

        # ── Paso 2: Navegar webs de gestora ──────────────────────────────────
        web_pdfs = await self._navigate_gestora_web()
        candidate_urls.extend(web_pdfs)
        self._log("INFO", f"Paso 2: {len(web_pdfs)} PDFs de navegación web")

        # Dedup
        seen = set()
        deduped = []
        for c in candidate_urls:
            url = c.get("url", "")
            if url and url not in seen:
                seen.add(url)
                deduped.append(c)
        candidate_urls = deduped

        # ── Paso 3: Buscar año por año si faltan ─────────────────────────────
        found_years = self._detect_years(candidate_urls)
        all_years = set(range(self.anio_creacion, self.current_year + 1))
        missing_years = sorted(all_years - found_years)

        if missing_years:
            self._log("INFO", f"Paso 3: Buscando {len(missing_years)} años sin carta: {missing_years}")
            extra = await self._search_missing_years(missing_years)
            for e in extra:
                url = e.get("url", "")
                if url and url not in seen:
                    seen.add(url)
                    candidate_urls.append(e)

        # ── Paso 4: Validar URLs ─────────────────────────────────────────────
        validated = self._validate_urls(candidate_urls)
        self._log("INFO", f"Paso 4: {len(validated)} URLs validadas (de {len(candidate_urls)})")

        # ── Paso 5: Descargar y extraer texto ────────────────────────────────
        all_cartas = await self._download_and_extract(validated)

        # ── Paso 5b: Filter by periodicity preference ────────────────────────
        # Prefer: trimestral > semestral > mensual (only mar/jun/sep/dic if monthly)
        cartas = self._filter_by_periodicity(all_cartas)
        result["cartas"] = sorted(cartas, key=lambda c: c.get("periodo", ""))

        # Final: report missing years
        final_years = {c.get("periodo", "")[:4] for c in cartas if c.get("periodo")}
        result["anos_sin_carta"] = sorted(all_years - {int(y) for y in final_years if y.isdigit()})
        result["fuentes_consultadas"] = list(seen)[:50]

        self._log("OK", f"Cartas: {len(cartas)} | Sin carta: {result['anos_sin_carta']}")
        self._save(result)
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 1: BUSCAR FUENTES
    # ═══════════════════════════════════════════════════════════════════════════

    async def _find_letter_sources(self) -> list[dict]:
        """Google search for letter/report sources."""
        queries = []
        if self.fund_short:
            queries.extend([
                f'"{self.fund_short}" carta semestral',
                f'"{self.fund_short}" carta trimestral',
                f'"{self.fund_short}" informe inversores',
                f'"{self.fund_short}" carta anual',
                f'"{self.fund_short}" cartas semestrales históricas',
            ])
        if self.gestora:
            queries.extend([
                f'"{self.gestora}" cartas inversores informes',
                f'"{self.gestora}" cartas semestrales',
                f'site:{self.gestora.lower().replace(" ", "")}.com cartas',
            ])
        if not queries:
            queries.append(f'{self.isin} carta informe')

        results = await self.search.search_multiple(queries, num_per_query=5, agent="letters")

        # Also get cached results from other agents
        cached = self.search.get_cached_for_agent("letters")
        results.extend(cached)

        # Filter: only URLs that look like letters/documents
        filtered = []
        for r in results:
            url = r.get("url", "").lower()
            title = r.get("title", "").lower()
            snippet = r.get("snippet", "").lower()
            combined = url + " " + title + " " + snippet
            if any(kw in combined for kw in LETTER_KEYWORDS) or url.endswith(".pdf"):
                filtered.append(r)

        return filtered

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 2: NAVEGAR WEB GESTORA
    # ═══════════════════════════════════════════════════════════════════════════

    async def _navigate_gestora_web(self) -> list[dict]:
        """Navigate gestora website to find ALL letters (PDFs + HTML pages).
        PRIORITY: when a listing page is found, exhaust ALL its pagination before searching elsewhere."""
        docs_found = []

        # Strategy 1: Find listing/archive pages on the gestora website
        doc_queries = []
        if self.gestora:
            doc_queries.extend([
                f'"{self.gestora}" cartas semestrales',
                f'"{self.gestora}" informes cartas inversores',
            ])
        if self.fund_short:
            doc_queries.extend([
                f'"{self.fund_short}" cartas semestrales',
                f'"{self.fund_short}" informes mensuales pdf',
            ])

        doc_results = await self.search.search_multiple(doc_queries, num_per_query=5, agent="letters")

        # PRIORITY: Find listing/archive pages and exhaust ALL pagination first
        listing_keywords = ["category", "cartas", "informes", "publicaciones", "archivo",
                           "documentos", "reports", "letters"]
        for r in doc_results:
            url = r.get("url", "")
            combined = (url + " " + r.get("title", "")).lower()
            is_listing = any(kw in combined for kw in listing_keywords)
            if is_listing:
                self._log("INFO", f"Página de listado encontrada: {url[:60]}")
                # Exhaust ALL pagination
                paginated_links = await self._follow_pagination(url, max_pages=10)
                self._log("INFO", f"Paginación agotada: {len(paginated_links)} links totales")
                for link in paginated_links:
                    docs_found.append({
                        "url": link["url"],
                        "titulo": link.get("titulo", ""),
                        "tipo": "html",
                    })
                # Also extract PDFs from the listing page itself
                pdfs = await find_pdfs_in_page(url)
                for pdf in pdfs:
                    docs_found.append(pdf)
                if paginated_links:
                    break  # Found the archive — don't search more

        # Strategy 2: Visit remaining results and extract PDFs
        for r in doc_results[:5]:
            url = r.get("url", "")
            if url in {d.get("url") for d in docs_found}:
                continue
            pdfs = await find_pdfs_in_page(url)
            for pdf in pdfs:
                titulo_lower = (pdf.get("titulo", "") + " " + pdf.get("url", "")).lower()
                if PDF_LETTER_PATTERNS.search(titulo_lower):
                    docs_found.append(pdf)

        # Strategy 3: Crawl from gestora home (only if few results so far)
        if self.gestora and len(docs_found) < 5:
            gestora_results = await self.search.search(
                f'"{self.gestora}" site oficial', num=3, agent="letters"
            )
            for r in gestora_results[:2]:
                url = r.get("url", "")
                if self.gestora.lower().split()[0] in url.lower():
                    docs = await crawl_for_documents(
                        url, keywords=LETTER_KEYWORDS, max_depth=2, max_pages=10
                    )
                    docs_found.extend(docs)
                    break

        self._log("INFO", f"Navegación web: {len(docs_found)} documentos")
        return docs_found

    async def _follow_pagination(self, start_url: str, max_pages: int = 10) -> list[dict]:
        """Follow ALL paginated pages of a listing (WordPress category, blog index, etc.)
        EXHAUSTS all pages before returning.
        When on a category page, ALL article links are relevant (no keyword filtering needed)."""
        all_links: list[dict] = []
        seen_urls: set[str] = set()
        visited_pages: set[str] = set()

        base_url = re.sub(r'/page/\d+/?$', '/', start_url)
        is_category = "category" in base_url.lower()

        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = base_url
            else:
                url = base_url.rstrip("/") + f"/page/{page_num}/"

            if url in visited_pages:
                continue
            visited_pages.add(url)

            # Fetch raw HTML to extract ALL links (not just keyword-matched)
            try:
                from tools.http_client import get_with_headers as _fetch_html
                html = await _fetch_html(url, _HEADERS_WEB)
            except Exception:
                alt_url = url.rstrip("/") if url.endswith("/") else url + "/"
                try:
                    html = await _fetch_html(alt_url, _HEADERS_WEB)
                except Exception:
                    break

            from urllib.parse import urljoin, urlparse
            soup = BeautifulSoup(html, "html.parser")
            base_domain = urlparse(base_url).netloc

            new_count = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full_url = urljoin(url, href)
                parsed = urlparse(full_url)

                # Only same domain
                if parsed.netloc and parsed.netloc != base_domain:
                    continue
                # Skip pagination, category self-links, anchors
                if "/page/" in full_url or full_url.rstrip("/") == base_url.rstrip("/"):
                    continue
                if full_url in seen_urls:
                    continue

                link_text = a.get_text(strip=True)
                # Skip pure navigation (numbers, "Anterior", "Siguiente", category names)
                if not link_text or link_text.strip().isdigit():
                    continue
                if link_text.lower() in ("anterior", "siguiente", "next", "prev"):
                    continue
                # Skip links to other categories
                if "/category/" in full_url and full_url != base_url:
                    continue

                # On category pages: accept ALL article links (they belong to this category)
                # On other pages: filter by keywords
                if is_category or any(kw in (link_text + " " + full_url).lower() for kw in LETTER_KEYWORDS):
                    seen_urls.add(full_url)
                    all_links.append({"url": full_url, "titulo": link_text})
                    new_count += 1

            self._log("INFO", f"Paginación {page_num}: {new_count} nuevos links (total: {len(all_links)})")

            if new_count == 0:
                break

        return all_links

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 3: BUSCAR AÑOS FALTANTES
    # ═══════════════════════════════════════════════════════════════════════════

    async def _search_missing_years(self, missing: list[int]) -> list[dict]:
        """Search for specific years where no letter was found.
        Strategy: 1) Google per year, 2) Find gestora cartas archive pages with pagination."""
        results = []

        # Strategy 1: Find the archive/category page and exhaust ALL its pagination
        # First: collect candidate URLs from Google
        archive_url = None
        if self.gestora or self.fund_short:
            archive_queries = [
                f'"{self.gestora}" cartas semestrales' if self.gestora else f'"{self.fund_short}" cartas semestrales',
                f'"{self.fund_short}" category cartas' if self.fund_short else None,
            ]
            for q in [q for q in archive_queries if q]:
                found = await self.search.search(q, num=5, agent="letters_archive")
                for r in found:
                    url = r.get("url", "")
                    # If this IS a listing/category page → use directly
                    if "category" in url.lower() or "archivo" in url.lower():
                        archive_url = url
                        break
                    # If this is a carta individual → look for category link inside
                    if any(kw in url.lower() for kw in ["carta", "semestral", "informe"]):
                        category_links = await find_links_by_keywords(
                            url, ["cartas semestrales", "category", "archivo", "todas las cartas"]
                        )
                        for cl in category_links:
                            cl_url = cl.get("url", "")
                            if "category" in cl_url.lower() or "archivo" in cl_url.lower():
                                archive_url = cl_url
                                self._log("INFO", f"Archivo descubierto desde carta: {archive_url[:60]}")
                                break
                    if archive_url:
                        break
                if archive_url:
                    break

        # Now exhaust ALL pages of the archive
        if archive_url:
            self._log("INFO", f"Agotando paginación de: {archive_url[:60]}")
            paginated = await self._follow_pagination(archive_url, max_pages=10)
            results.extend(paginated)
            self._log("INFO", f"Archivo: {len(paginated)} cartas encontradas en total")

        # Strategy 3: Google per missing year (fallback)
        still_missing = [y for y in missing if not any(str(y) in r.get("url", "") for r in results)]
        for year in still_missing[:5]:
            queries = [
                f'"{self.fund_short}" carta {year}' if self.fund_short else f'{self.isin} carta {year}',
            ]
            for q in queries:
                found = await self.search.search(q, num=3, agent="letters")
                for r in found:
                    url = r.get("url", "").lower()
                    combined = url + " " + r.get("title", "").lower()
                    if any(kw in combined for kw in LETTER_KEYWORDS) or url.endswith(".pdf"):
                        results.append(r)

        return results

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 4: VALIDAR URLs
    # ═══════════════════════════════════════════════════════════════════════════

    def _validate_urls(self, candidates: list[dict]) -> list[dict]:
        """Validate: is this URL really a letter of THIS fund?"""
        validated = []
        fund_terms = set()
        if self.fund_short:
            for w in self.fund_short.lower().split():
                if len(w) > 3:
                    fund_terms.add(w)
        if self.gestora:
            for w in self.gestora.lower().split():
                if len(w) > 3:
                    fund_terms.add(w)
        fund_terms.add(self.isin.lower())

        # Gestora domains — PDFs from these domains are trusted
        gestora_domains = set()
        if self.gestora:
            # Extract likely domain from gestora name
            gestora_domains.add(self.gestora.lower().replace(" ", ""))
            gestora_domains.add(self.gestora.lower().split()[0])

        for c in candidates:
            url = c.get("url", "").lower()
            title = c.get("titulo", c.get("title", "")).lower()
            combined = url + " " + title

            # Skip search engines
            if any(d in url for d in ("google.com", "bing.com", "duckduckgo.com")):
                continue

            # Trust PDFs from gestora domain (don't need fund name in filename)
            is_gestora_domain = any(d in url for d in gestora_domains)
            if is_gestora_domain and (url.endswith(".pdf") or PDF_LETTER_PATTERNS.search(combined)):
                validated.append(c)
                continue

            # For other URLs: must mention fund or gestora
            if any(term in combined for term in fund_terms):
                validated.append(c)

        return validated

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 5: DESCARGAR Y EXTRAER
    # ═══════════════════════════════════════════════════════════════════════════

    async def _download_and_extract(self, urls: list[dict]) -> list[dict]:
        """Download PDFs/HTML and extract raw text with pdfplumber."""
        cartas = []

        for entry in urls[:50]:  # Max 50 cartas
            url = entry.get("url", "")
            titulo = entry.get("titulo", entry.get("title", ""))

            if url.lower().endswith(".pdf"):
                carta = await self._process_pdf(url, titulo)
            else:
                carta = await self._process_html(url, titulo)

            if carta and carta.get("texto_completo"):
                cartas.append(carta)

        return cartas

    async def _process_pdf(self, url: str, titulo: str) -> dict | None:
        """Download PDF and extract text with pdfplumber."""
        # Generate safe filename
        safe_name = re.sub(r'[^\w\-.]', '_', url.split("/")[-1])[:60]
        if not safe_name.endswith(".pdf"):
            safe_name += ".pdf"
        target = self.letters_dir / f"letter_{safe_name}"

        # Download if not cached
        if not target.exists() or target.stat().st_size < 1000:
            try:
                data = await get_bytes(url)
                if not data or not data[:5].startswith(b"%PDF"):
                    return None
                target.write_bytes(data)
                self._log("INFO", f"Descargado: {safe_name} ({len(data)//1024}KB)")
            except Exception as exc:
                self._log("WARN", f"Error descargando {url[:60]}: {exc}")
                return None

        # Extract text with pdfplumber
        try:
            meta = get_pdf_metadata(str(target))
            text = extract_page_range(str(target), 0, meta["num_pages"])
            text = re.sub(r'\(cid:\d+\)', ' ', text)  # Clean ligatures
        except Exception as exc:
            self._log("WARN", f"Error extrayendo {safe_name}: {exc}")
            return None

        if not text or len(text) < 100:
            return None

        periodo = self._infer_period(url, titulo, text[:500])
        tipo = self._infer_tipo(url, titulo)

        return {
            "periodo": periodo,
            "tipo": tipo,
            "url_fuente": url,
            "archivo": safe_name,
            "titulo": titulo,
            "texto_completo": text,  # Sin límite de caracteres
            "num_paginas": meta.get("num_pages", 0),
            "fecha_inferida": periodo,
        }

    async def _process_html(self, url: str, titulo: str) -> dict | None:
        """Fetch HTML page and extract text."""
        text = await fetch_page_text(url, max_chars=0)  # Sin límite
        if not text or len(text) < 200:
            return None

        periodo = self._infer_period(url, titulo, text[:500])
        tipo = self._infer_tipo(url, titulo)

        return {
            "periodo": periodo,
            "tipo": tipo,
            "url_fuente": url,
            "archivo": "",
            "titulo": titulo,
            "texto_completo": text,
            "num_paginas": 0,
            "fecha_inferida": periodo,
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _get_known_carta_urls(self) -> list[str]:
        """Get URLs of cartas already found (from search cache or previous results)."""
        urls = []
        # From search cache
        for url_info in self.search.get_all_cached_urls():
            url = url_info.get("url", "")
            if any(kw in url.lower() for kw in ["carta", "semestral", "informe"]):
                urls.append(url)
        return urls[:5]

    def _filter_by_periodicity(self, cartas: list[dict]) -> list[dict]:
        """Filter cartas by periodicity preference:
        trimestral > semestral > mensual (only mar/jun/sep/dic)."""
        # Group by year
        by_year: dict[str, list[dict]] = {}
        for c in cartas:
            year = c.get("periodo", "")[:4]
            if year:
                by_year.setdefault(year, []).append(c)

        filtered = []
        quarterly_months = {"03", "06", "09", "12"}

        for year, year_cartas in by_year.items():
            tipos = {c.get("tipo", "") for c in year_cartas}

            if "trimestral" in tipos:
                # Keep all trimestral + semestral + anual
                filtered.extend(c for c in year_cartas if c.get("tipo") in ("trimestral", "semestral", "anual", "carta"))
            elif "semestral" in tipos:
                # Keep semestral + anual
                filtered.extend(c for c in year_cartas if c.get("tipo") in ("semestral", "anual", "carta"))
            elif "mensual" in tipos:
                # Keep only quarterly months (mar, jun, sep, dic) + non-mensual
                for c in year_cartas:
                    if c.get("tipo") != "mensual":
                        filtered.append(c)
                    else:
                        # Check if month is quarterly
                        periodo = c.get("periodo", "")
                        month = periodo[5:7] if len(periodo) >= 7 else ""
                        if month in quarterly_months:
                            filtered.append(c)
            else:
                # Keep everything (cartas, anual, unknown)
                filtered.extend(year_cartas)

        # Also keep cartas without year
        filtered.extend(c for c in cartas if not c.get("periodo", "")[:4])
        return filtered

    def _detect_years(self, candidates: list[dict]) -> set[int]:
        """Detect which years are covered by found URLs."""
        years = set()
        for c in candidates:
            combined = c.get("url", "") + " " + c.get("titulo", c.get("title", ""))
            for m in re.finditer(r'\b(20[12]\d)\b', combined):
                years.add(int(m.group(1)))
        return years

    def _infer_period(self, url: str, titulo: str, text_start: str = "") -> str:
        """Infer period (YYYY or YYYY-MM) from URL, title, or text."""
        combined = url + " " + titulo + " " + text_start

        # Month names in Spanish
        months_es = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        for name, num in months_es.items():
            m = re.search(rf'{name}\s*[\-_\s]*(20[12]\d)', combined, re.IGNORECASE)
            if m:
                return f"{m.group(1)}-{num}"
            m = re.search(rf'(20[12]\d)\s*[\-_\s]*{name}', combined, re.IGNORECASE)
            if m:
                return f"{m.group(1)}-{num}"

        # URL patterns: /2025/01/ or _2025_01
        m = re.search(r'/(20[12]\d)/(\d{2})/', combined)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

        # Quarter/semester
        m = re.search(r'(20[12]\d)[_\-\s]*[QqTt]([1-4])', combined)
        if m:
            return f"{m.group(1)}-Q{m.group(2)}"
        m = re.search(r'(20[12]\d)[_\-\s]*[SsHh]([12])', combined)
        if m:
            return f"{m.group(1)}-S{m.group(2)}"

        # Just year
        m = re.search(r'(20[12]\d)', combined)
        return m.group(1) if m else ""

    def _infer_tipo(self, url: str, titulo: str) -> str:
        """Infer letter type from URL/title."""
        combined = (url + " " + titulo).lower()
        if "semestral" in combined:
            return "semestral"
        if "trimestral" in combined or "quarterly" in combined:
            return "trimestral"
        if "anual" in combined or "annual" in combined:
            return "anual"
        if "mensual" in combined or "monthly" in combined:
            return "mensual"
        return "carta"

    def _save(self, result: dict):
        out = self.fund_dir / "letters_data.json"
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log("OK", f"Guardado: {out}")


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="ES0112231008")
    parser.add_argument("--fund-name", default="Avantage Fund")
    parser.add_argument("--gestora", default="Avantage Capital")
    parser.add_argument("--anio-creacion", type=int, default=2014)
    args = parser.parse_args()

    agent = LettersAgent(
        args.isin, fund_name=args.fund_name, gestora=args.gestora,
        anio_creacion=args.anio_creacion,
    )
    result = asyncio.run(agent.run())
    cartas = result.get("cartas", [])
    sin = result.get("anos_sin_carta", [])
    print(f"\nCartas: {len(cartas)} | Sin carta: {sin}")
    for c in cartas[:5]:
        print(f"  [{c.get('periodo','')}] {c.get('tipo',''):10s} {c.get('titulo','')[:50]}")
