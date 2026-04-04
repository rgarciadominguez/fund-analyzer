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
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_bytes
from tools.pdf_extractor import extract_pages_by_keyword, get_pdf_metadata
from tools.claude_extractor import extract_structured_data

console = Console()

# URLs conocidas por ISIN o gestora
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

    def __init__(self, isin: str, config: dict, gestora_url: str = ""):
        self.isin = isin.strip().upper()
        self.config = config
        self.gestora_url = gestora_url
        self.prefix = self.isin[:2].upper()

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

    # ── URL building ─────────────────────────────────────────────────────────

    def _build_url_list(self) -> list[str]:
        """Construye lista de URLs a intentar."""
        urls = []
        if self.gestora_url:
            urls.append(self.gestora_url)

        # URLs conocidas por prefijo
        prefix_urls = _GESTORA_LETTERS_URLS.get(self.prefix, [])
        urls.extend(prefix_urls)

        # Si hay config con URL de la gestora
        if self.config.get("gestora_url"):
            urls.insert(0, self.config["gestora_url"])

        return urls

    # ── Scraping ─────────────────────────────────────────────────────────────

    async def _scrape_letter_links(self, base_url: str) -> list[dict]:
        """
        Scrapea una página buscando links a PDFs de cartas trimestrales.
        Devuelve lista de {url, title, fecha_estimada}.
        """
        html = await get(base_url)
        soup = BeautifulSoup(html, "lxml")
        links: list[dict] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)

            # Filtrar por keywords en href o texto
            combined = (href + " " + text).lower()
            if not LETTER_LINK_KEYWORDS.search(combined):
                continue

            # PDF o link a página de descarga
            is_pdf = ".pdf" in href.lower()
            is_doc_link = any(kw in href.lower() for kw in
                              ("download", "document", "verdocumento", "fichero"))

            if not (is_pdf or is_doc_link):
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

            # Extraer fecha del texto o href (patrón Q1 2024, 1T2024, etc.)
            fecha = self._extract_date_hint(text + " " + href)

            links.append({
                "url": full_url,
                "title": text[:120],
                "fecha_estimada": fecha,
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
        """Descarga y extrae contenido de una carta trimestral."""
        url = entry["url"]
        safe_name = re.sub(r'[^\w\-]', '_', entry.get("title", "carta"))[:50]
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
                # Si no hay keywords, extraer primeras páginas
                from tools.pdf_extractor import extract_page_range
                text = extract_page_range(str(target), 0, min(5, meta["num_pages"]))
        except Exception as exc:
            self._log("WARN", f"Error extrayendo texto de {filename}: {exc}")
            return None

        if not text.strip():
            return None

        # Extraer estructura con Claude (si hay API key)
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

        try:
            extracted = extract_structured_data(
                text[:4000],  # limitar tokens
                schema,
                context=f"Carta trimestral del fondo {self.isin}",
            )
            extracted["archivo"] = filename
            extracted["url_fuente"] = url
            self._log("OK", f"Carta extraída: {filename}")
            return extracted
        except Exception as exc:
            self._log("WARN", f"Claude no disponible para {filename}: {exc}")
            # Devolver estructura mínima sin Claude
            return {
                "archivo": filename,
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
