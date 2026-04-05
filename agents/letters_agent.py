"""
Letters Agent — Cartas trimestrales de gestores

Para cada fondo busca en la web de la gestora:
  - Cartas a inversores / comentarios de gestores / management letters
  - Descarga PDFs, extrae con claude_extractor:
      * fecha y periodo cubierto
      * posiciones comentadas y racional
      * perspectivas y tesis de inversión
      * decisiones de cartera del trimestre

Si la web no es accesible → guarda JSON vacío y continúa sin bloquear.
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
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_bytes, get_with_headers
from tools.pdf_extractor import extract_pages_by_keyword, get_pdf_metadata
from tools.claude_extractor import extract_structured_data

console = Console()

# URLs específicas por ISIN (fuentes confirmadas por el usuario)
_ISIN_LETTERS_URLS: dict[str, list[str]] = {
    "ES0156572002": [  # MyInvestor Cartera Permanente
        "https://www.riverpatrimonio.com/post/carta-1s-2025-myinvestor-cartera-permanente-fi",
        "https://www.riverpatrimonio.com/tag/myinvestor",
        "https://www.riverpatrimonio.com/blog",
    ],
    "ES0175316001": [  # Dunas Valor Flexible
        "https://dunascapital.com/publicaciones/",
        "https://www.dunas.es/publicaciones/",
        "https://dunascapital.com/",
    ],
}

# URLs conocidas por prefijo ISIN / gestora
_GESTORA_LETTERS_URLS: dict[str, list[str]] = {
    # Avantage Capital / Renta 4 (ES0112231008)
    "ES": [
        "https://www.avantage-capital.es/informacion-para-inversores/",
        "https://www.r4.com/fondos/avantage",
        "https://www.renta4.com/es/fondos/avantage",
    ],
    # DNCA (LU)
    "LU": [
        "https://www.dnca-investments.com/en/news/management-letters",
        "https://www.dnca-investments.com/en/news/publications",
        "https://www.dnca-investments.com/fr/actualites/lettres-de-gestion",
    ],
    # Genérico IE, FR, GB, DE
    "IE": [],
    "FR": [],
    "GB": [],
    "DE": [],
}

# Keywords para identificar links de cartas en HTML
LETTER_LINK_KEYWORDS = re.compile(
    r"carta|letter|trimestral|quarterly|comment|commentaire|gestora|manager|"
    r"informe|newsletter|update|outlook|perspectiv",
    re.IGNORECASE,
)

# Keywords para extracción del texto de las cartas
LETTER_TEXT_KEYWORDS = [
    "posiciones", "cartera", "portfolio", "holdings",
    "perspectivas", "outlook", "tesis", "thesis",
    "rentabilidad", "performance", "trimestre", "quarter",
]

MAX_LETTERS = 8  # máximo de cartas a descargar por fondo


class LettersAgent:
    """
    Agente de cartas trimestrales.
    Clase con async def run() -> dict según convenio del proyecto.
    """

    DDG_HEADERS = {
        "Accept-Language": "es-ES,es;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://duckduckgo.com/",
    }

    def __init__(self, isin: str, config: dict = None, gestora_url: str = "",
                 fund_name: str = "", gestora: str = "", anio_creacion: int | None = None):
        self.isin         = isin.strip().upper()
        self.config       = config or {"fuentes": "1"}
        self.gestora_url  = gestora_url
        self.prefix       = self.isin[:2].upper()
        self.fund_name    = fund_name
        self.gestora      = gestora
        self.anio_creacion = anio_creacion or (datetime.now().year - 5)
        self.current_year = datetime.now().year

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.letters_dir = self.fund_dir / "raw" / "letters"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.letters_dir.mkdir(parents=True, exist_ok=True)

        self._log = self._make_logger()

    def _make_logger(self):
        """Devuelve función de log que escribe en progress.log y rich."""
        log_path = Path(__file__).parent.parent / "progress.log"
        def _log(level: str, msg: str):
            ts = datetime.now().strftime("%H:%M:%S")
            line = f"[{ts}] [LETTERS] [{level}] {msg}"
            console.log(line)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        return _log

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> dict:
        result = {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(),
            "cartas": [],
            "fuentes": {
                "cartas_gestores": [],
                "urls_consultadas": [],
            },
        }

        # Si config dice solo informes oficiales, saltar cartas
        if self.config.get("fuentes") == "2":
            self._log("INFO", "Config fuentes=2: saltar cartas trimestrales")
            self._save(result)
            return result

        self._log("START", f"Buscando cartas para {self.isin}")

        urls_to_try = self._build_url_list()
        letters_found: list[dict] = []

        for url in urls_to_try:
            try:
                self._log("FETCH", f"Probando {url}")
                result["fuentes"]["urls_consultadas"].append(url)
                pdf_links = await self._scrape_letter_links(url)
                if pdf_links:
                    self._log("OK", f"{len(pdf_links)} cartas encontradas en {url}")
                    letters_found.extend(pdf_links)
                    if len(letters_found) >= MAX_LETTERS:
                        break
            except Exception as exc:
                self._log("WARN", f"URL no accesible {url}: {exc}")

        # DuckDuckGo search por año si no hay cartas de URLs conocidas
        if len(letters_found) < 2 and (self.fund_name or self.gestora):
            self._log("INFO", f"Sin cartas desde URLs, buscando en DDG año a año ({self.anio_creacion}-{self.current_year})")
            ddg_links = await self._search_letters_ddg_all_years()
            letters_found.extend(ddg_links)
            result["fuentes"]["urls_consultadas"].extend(
                [f"DDG:{l['titulo']}" for l in ddg_links[:5]]
            )

        if not letters_found:
            self._log("WARN", "No se encontraron cartas. Guardando JSON vacío.")
            self._save(result)
            return result

        # Ordenar por fecha desc, limitar
        letters_found = letters_found[:MAX_LETTERS]

        # Descargar y extraer
        for entry in letters_found:
            carta = await self._process_letter(entry)
            if carta:
                result["cartas"].append(carta)
                result["fuentes"]["cartas_gestores"].append(carta.get("archivo", ""))

        self._log("OK", f"{len(result['cartas'])} cartas procesadas")
        self._save(result)
        return result

    # ── DuckDuckGo search por año ─────────────────────────────────────────────

    async def _search_letters_ddg_all_years(self) -> list[dict]:
        """Busca cartas de gestores en DDG para cada año desde anio_creacion."""
        all_results: list[dict] = []
        seen_urls: set[str] = set()

        for year in range(self.anio_creacion, self.current_year + 1):
            year_results = await self._search_letters_ddg(year)
            for r in year_results:
                url = r.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(r)
            await asyncio.sleep(1.5)  # respetar rate limit

        self._log("INFO", f"DDG total: {len(all_results)} links únicos encontrados")
        return all_results[:MAX_LETTERS * 2]

    async def _search_letters_ddg(self, year: int) -> list[dict]:
        """Busca cartas de gestores en DDG para un año concreto."""
        fund_short = re.sub(r'\b(FI|SICAV|FP|SIL)\b', '', self.fund_name, flags=re.IGNORECASE).strip()
        queries = []
        if fund_short:
            queries.append(f'"{fund_short}" carta {year} site:riverpatrimonio.com')
            queries.append(f'"{fund_short}" carta trimestral {year}')
        if self.gestora:
            queries.append(f'"{self.gestora}" carta gestores {year}')

        found: list[dict] = []
        for query in queries[:2]:
            enc_q = urllib.parse.quote_plus(query)
            ddg_url = f"https://html.duckduckgo.com/html/?q={enc_q}"
            try:
                html = await get_with_headers(ddg_url, self.DDG_HEADERS)
                soup = BeautifulSoup(html, "lxml")
                for a_tag in soup.select(".result__a"):
                    href = a_tag.get("href", "")
                    titulo = a_tag.get_text(strip=True)
                    url = self._extract_ddg_url(href)
                    if not url:
                        continue
                    combined = (url + " " + titulo).lower()
                    if not re.search(r"\.pdf|carta|letter|trimestral|quarterly|comentario", combined):
                        continue
                    fecha_hint = self._extract_date_hint(titulo + " " + url)
                    found.append({"url": url, "titulo": titulo,
                                  "fecha_estimada": fecha_hint or str(year),
                                  "is_html": not url.lower().endswith(".pdf")})
                    if len(found) >= 3:
                        break
                await asyncio.sleep(1)
            except Exception as exc:
                self._log("WARN", f"DDG error year {year} query '{query[:40]}': {exc}")

        if found:
            self._log("INFO", f"DDG {year}: {len(found)} cartas -> {found[0].get('titulo','')[:50]}")
        else:
            self._log("INFO", f"DDG {year}: sin cartas encontradas")
        return found

    def _extract_ddg_url(self, href: str) -> str:
        """Extrae URL real del redirect de DuckDuckGo."""
        if href.startswith("http") and "duckduckgo" not in href:
            return href
        m = re.search(r"uddg=([^&]+)", href)
        if m:
            return urllib.parse.unquote(m.group(1))
        m2 = re.search(r"\bu=([^&]+)", href)
        if m2:
            return urllib.parse.unquote(m2.group(1))
        return ""

    # ── URL building ─────────────────────────────────────────────────────────

    def _build_url_list(self) -> list[str]:
        """Construye lista de URLs a intentar (ISIN-specific first, then prefix)."""
        urls = []

        # ISIN-specific URLs (highest priority — user-confirmed sources)
        isin_urls = _ISIN_LETTERS_URLS.get(self.isin, [])
        urls.extend(isin_urls)

        if self.gestora_url:
            urls.append(self.gestora_url)

        # Config URL
        if self.config.get("gestora_url"):
            urls.insert(0, self.config["gestora_url"])

        # URLs conocidas por prefijo
        prefix_urls = _GESTORA_LETTERS_URLS.get(self.prefix, [])
        urls.extend(prefix_urls)

        return urls

    # ── Scraping ─────────────────────────────────────────────────────────────

    async def _scrape_letter_links(self, base_url: str) -> list[dict]:
        """
        Scrapea una página buscando links a PDFs o artículos HTML de cartas.
        Si la URL es en sí misma un artículo (no un índice), lo devuelve directamente.
        Devuelve lista de {url, title, fecha_estimada, is_html}.
        """
        html = await get_with_headers(base_url, self.DDG_HEADERS)
        soup = BeautifulSoup(html, "lxml")
        links: list[dict] = []

        # Check if this URL is itself an article (has substantial content)
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 80]
        if len(paragraphs) >= 3:
            # Looks like a standalone article — treat the URL itself as the carta
            title_tag = soup.find("h1") or soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else base_url
            fecha = self._extract_date_hint(title + " " + base_url)
            return [{"url": base_url, "titulo": title[:120], "fecha_estimada": fecha, "is_html": True}]

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)

            # Filtrar por keywords en href o texto
            combined = (href + " " + text).lower()
            if not LETTER_LINK_KEYWORDS.search(combined):
                continue

            # PDF, HTML article, or doc link
            is_pdf = ".pdf" in href.lower()
            is_html_article = any(kw in combined for kw in
                                  ("carta", "letter", "trimestral", "quarterly", "comment", "post"))
            is_doc_link = any(kw in href.lower() for kw in
                              ("download", "document", "verdocumento", "fichero"))

            if not (is_pdf or is_html_article or is_doc_link):
                continue

            # Construir URL absoluta
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
            else:
                full_url = base_url.rstrip("/") + "/" + href

            fecha = self._extract_date_hint(text + " " + href)
            links.append({
                "url": full_url,
                "titulo": text[:120],
                "fecha_estimada": fecha,
                "is_html": not is_pdf,
            })

        return links

    def _extract_date_hint(self, text: str) -> str:
        """Extrae pista de fecha del texto (trimestre/año)."""
        # Patrón: Q1 2024, 1T 2024, primer trimestre 2024, etc.
        patterns = [
            r'\b(Q[1-4])\s*(20\d{2})\b',
            r'\b([1-4][TtQ])\s*(20\d{2})\b',
            r'\b(20\d{2})[/\-](0[1-9]|1[0-2])\b',
            r'\b(20\d{2})\b',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(0)
        return ""

    # ── Procesamiento de cada carta ───────────────────────────────────────────

    async def _process_letter(self, entry: dict) -> dict | None:
        """Descarga y extrae contenido de una carta trimestral (PDF o HTML)."""
        url = entry.get("url", "")
        is_html = entry.get("is_html", False) or not url.lower().endswith(".pdf")

        if is_html:
            return await self._process_html_letter(entry)
        return await self._process_pdf_letter(entry)

    async def _process_html_letter(self, entry: dict) -> dict | None:
        """Extrae contenido de una carta en formato HTML (blog post, artículo web)."""
        url = entry.get("url", "")
        self._log("INFO", f"Procesando carta HTML: {url[:60]}")
        try:
            html = await get_with_headers(url, self.DDG_HEADERS)
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
                self._log("WARN", f"Contenido HTML insuficiente: {url[:60]}")
                return None
            return await self._extract_letter_content(text[:5000], entry)
        except Exception as exc:
            self._log("WARN", f"Error procesando HTML {url[:60]}: {exc}")
            return None

    async def _process_pdf_letter(self, entry: dict) -> dict | None:
        """Descarga y extrae una carta en PDF."""
        url = entry["url"]
        safe_name = re.sub(r'[^\w\-]', '_', entry.get("titulo", entry.get("title", "carta")))[:50]
        filename = f"letter_{safe_name}.pdf"
        target = self.letters_dir / filename

        # Descargar si no existe
        if not (target.exists() and target.stat().st_size > 1000):
            try:
                data = await get_bytes(url)
                if b"%PDF" not in data[:20]:
                    self._log("WARN", f"No es PDF: {url[:60]}")
                    return None
                target.write_bytes(data)
                self._log("OK", f"Carta descargada: {filename} ({len(data)//1024} KB)")
            except Exception as exc:
                self._log("WARN", f"Error descargando {url[:60]}: {exc}")
                return None
        else:
            self._log("INFO", f"Carta ya existe: {filename}")

        # Extraer texto relevante
        try:
            meta = get_pdf_metadata(str(target))
            text = extract_pages_by_keyword(
                str(target),
                keywords=LETTER_TEXT_KEYWORDS,
                context_pages=1,
            )
            if not text.strip():
                from tools.pdf_extractor import extract_page_range
                text = extract_page_range(str(target), 0, min(5, meta["num_pages"]))
        except Exception as exc:
            self._log("WARN", f"Error extrayendo texto de {filename}: {exc}")
            return None

        if not text.strip():
            return None

        entry_with_file = {**entry, "archivo": filename}
        return await self._extract_letter_content(text[:4000], entry_with_file)

    async def _extract_letter_content(self, text: str, entry: dict) -> dict | None:
        """Usa Claude para extraer la estructura de una carta (PDF o HTML)."""
        url = entry.get("url", "")
        schema = {
            "fecha": "fecha de la carta o periodo que cubre (ej. 'Q1 2024', '1T2024')",
            "periodo": "trimestre y año en formato normalizado (ej. '2024-Q1')",
            "resumen_mercado": "contexto de mercado descrito en la carta",
            "posiciones_comentadas": [
                {
                    "nombre": "nombre del activo o empresa",
                    "accion": "entrada/salida/aumento/reduccion/mantener",
                    "racional": "razón de la decisión según el gestor",
                }
            ],
            "tesis_inversion": "tesis principal de inversión expuesta",
            "perspectivas": "outlook o perspectivas para el próximo periodo",
            "decisiones_cartera": "resumen de cambios realizados en la cartera",
        }

        archivo = entry.get("archivo", "")
        try:
            extracted = extract_structured_data(
                text,
                schema,
                context=f"Carta trimestral del fondo {self.isin}",
            )
            extracted["archivo"] = archivo
            extracted["url_fuente"] = url
            self._log("OK", f"Carta extraída: {url[:60]}")
            return extracted
        except Exception as exc:
            self._log("WARN", f"Claude no disponible para {url[:60]}: {exc}")
            return {
                "archivo": archivo,
                "url_fuente": url,
                "fecha": entry.get("fecha_estimada", ""),
                "periodo": entry.get("fecha_estimada", ""),
                "resumen_mercado": None,
                "posiciones_comentadas": [],
                "tesis_inversion": None,
                "perspectivas": None,
                "decisiones_cartera": None,
            }

    def _save(self, result: dict) -> None:
        out = self.fund_dir / "letters_data.json"
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log("OK", f"Guardado: {out}")


# ── Standalone ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    config = {"fuentes": "1"}
    for isin in ["ES0112231008", "LU1694789451"]:
        agent = LettersAgent(isin, config)
        result = asyncio.run(agent.run())
        console.print(f"[green]{isin}: {len(result['cartas'])} cartas procesadas")
