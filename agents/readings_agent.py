"""
Readings Agent — Curador de análisis externos y contenido sobre el fondo

NO busca info del gestor (eso es manager_deep_agent).
NO busca cartas del gestor (eso es letters_agent).
SÍ busca: análisis de terceros, opiniones, entrevistas/podcasts/vídeos SOBRE EL FONDO,
reviews de la comunidad, artículos en blogs especializados.

Pipeline:
  1. Definir búsquedas: análisis en fuentes clave (Substack, Astralis, Rankia, blogs)
  2. Pedir URLs al SearchEngine (usa caché compartido — no duplica búsquedas)
  3. Validar: ¿la URL tiene contenido relevante sobre este fondo?
  4. Descargar texto de URLs validadas
  5. Output: todo el contenido raw al analyst_agent

Se ejecuta DESPUÉS de cnmv_agent y letters_agent (para tener nombre del fondo).

Output:
  data/funds/{ISIN}/readings_data.json
"""
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.google_search import SearchEngine, fetch_page_text

console = Console(highlight=False, force_terminal=False)

# Priority sources for fund analysis in Spain
PRIORITY_SOURCES = [
    # ── Medios financieros españoles ──
    ("saludfinanciera.substack.com", "Substack Salud Financiera"),
    ("astralisfundsacademy.com", "Astralis Funds Academy"),
    ("astralis.es", "Astralis"),
    ("rankia.com", "Rankia"),
    ("finect.com", "Finect"),
    ("masdividendos.com", "Mas Dividendos"),
    ("valueschool.es", "Value School"),
    ("quenoteloinviertan.com", "Que No Te Lo Inviertan"),
    ("inversor-tranquilo.com", "Inversor Tranquilo"),
    # ── Medios financieros internacionales ──
    ("morningstar.es", "Morningstar ES"),
    ("morningstar.co.uk", "Morningstar UK"),
    ("morningstar.com", "Morningstar US"),
    ("citywire.com", "Citywire Global"),
    ("citywire.co.uk", "Citywire UK"),
    ("trustnet.com", "Trustnet"),
    ("fundspeople.com", "FundsPeople"),
    ("allfunds.com", "AllFunds"),
    ("ft.com", "Financial Times"),
    ("institutionalinvestor.com", "Institutional Investor"),
    ("seekingalpha.com", "Seeking Alpha"),
    ("youtube.com", "YouTube"),
]

# Content types to find (multi-idioma)
CONTENT_TYPES = {
    "analisis": ["analysis", "review", "opinion", "rating", "outlook",
                 "analisis", "resena", "valoracion", "analyse", "bewertung"],
    "entrevista": ["interview", "Q&A", "conversation", "entrevista",
                   "entretien", "gesprach"],
    "podcast": ["podcast", "audio", "episode", "episodio"],
    "video": ["youtube", "video", "conference", "webinar", "presentation",
              "conferencia", "presentacion"],
    "articulo": ["article", "blog", "post", "column", "commentary",
                 "articulo", "columna", "beitrag"],
    "comunidad": ["forum", "thread", "discussion", "comments",
                  "foro", "hilo", "debate", "comentarios"],
}


class ReadingsAgent:

    def __init__(self, isin: str, fund_name: str = "", gestora: str = "",
                 gestores: list[str] = None, **kwargs):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name
        self.fund_short = fund_name.split(",")[0].strip() if fund_name else ""
        self.gestora = gestora
        self.gestores = gestores or []

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = root / "progress.log"

        self.search = SearchEngine(isin=self.isin)

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [READINGS] [{level}] {msg}"
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"Readings Agent — {self.isin} ({self.fund_short})")

        result = {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(),
            "analisis": [],
            "lecturas": [],
            "fuentes_consultadas": [],
        }

        # ── Paso 1: Definir búsquedas ────────────────────────────────────────
        queries = self._build_queries()
        self._log("INFO", f"Paso 1: {len(queries)} búsquedas definidas")

        # ── Paso 2: Pedir URLs al SearchEngine ───────────────────────────────
        search_results = await self.search.search_multiple(
            queries, num_per_query=5, agent="readings"
        )
        # Also get cached results from other agents that might be relevant
        cached = self.search.get_cached_for_agent("readings")
        all_results = search_results + cached
        self._log("INFO", f"Paso 2: {len(search_results)} nuevas + {len(cached)} cacheadas = {len(all_results)} URLs")

        # ── Paso 3: Validar URLs ─────────────────────────────────────────────
        validated = self._validate_urls(all_results)
        self._log("INFO", f"Paso 3: {len(validated)} URLs validadas (de {len(all_results)})")

        # ── Paso 4: Descargar y clasificar ───────────────────────────────────
        for entry in validated[:25]:  # Max 25 artículos
            url = entry.get("url", "")
            title = entry.get("title", entry.get("titulo", ""))
            snippet = entry.get("snippet", "")

            text = await fetch_page_text(url, max_chars=0)  # Sin límite
            if not text or len(text) < 300:
                continue

            content_type = self._classify_content(url, title, snippet, text[:500])
            source = self._identify_source(url)

            item = {
                "fuente": source,
                "tipo": content_type,
                "titulo": title,
                "url": url,
                "snippet": snippet,
                "texto_completo": text,
                "fecha": self._extract_date(url, title, text[:300]),
            }

            if content_type in ("analisis", "articulo", "comunidad"):
                result["analisis"].append(item)
            else:
                result["lecturas"].append(item)

            self._log("INFO", f"[{content_type:10s}] {source:20s} {title[:40]}")

        result["fuentes_consultadas"] = list({r.get("url", "") for r in validated})[:50]

        n_analisis = len(result["analisis"])
        n_lecturas = len(result["lecturas"])
        self._log("OK", f"Análisis: {n_analisis} | Lecturas: {n_lecturas}")

        self._save(result)
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 1: DEFINIR BÚSQUEDAS
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_queries(self) -> list[str]:
        """Build targeted search queries for fund analysis content."""
        queries = []
        fund_q = self.fund_short or self.isin

        # Priority sources — direct queries
        queries.extend([
            f'"{fund_q}" salud financiera',
            f'"{fund_q}" astralis',
            f'"{fund_q}" rankia análisis',
            f'"{fund_q}" rankia opinión',
            f'"{fund_q}" morningstar',
            f'"{fund_q}" finect',
            f'"{fund_q}" masdividendos',
        ])

        # Content type queries
        queries.extend([
            f'"{fund_q}" análisis fondo inversión',
            f'"{fund_q}" opinión cartera',
            f'"{fund_q}" entrevista fondo',
            f'"{fund_q}" podcast fondo',
            f'"{fund_q}" youtube',
            f'"{fund_q}" conferencia inversores',
        ])

        # Gestora-specific
        if self.gestora:
            queries.append(f'"{self.gestora}" análisis fondos')

        # ISIN fallback
        queries.append(f'{self.isin} análisis')

        return queries

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 3: VALIDAR URLs
    # ═══════════════════════════════════════════════════════════════════════════

    def _validate_urls(self, results: list[dict]) -> list[dict]:
        """Filter: keep only URLs that contain relevant fund analysis content."""
        validated = []
        seen_urls: set[str] = set()

        # Terms that must appear in title/snippet/URL
        fund_terms = set()
        if self.fund_short:
            for w in self.fund_short.lower().split():
                if len(w) > 3:
                    fund_terms.add(w)
        if self.gestora:
            fund_terms.add(self.gestora.lower().split()[0])
        fund_terms.add(self.isin.lower())

        # Domains to skip
        skip_domains = {"google.com", "bing.com", "duckduckgo.com", "linkedin.com",
                        "twitter.com", "x.com", "facebook.com", "instagram.com",
                        "spotify.com", "apple.com"}

        for r in results:
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            if any(d in url.lower() for d in skip_domains):
                continue

            title = r.get("title", r.get("titulo", "")).lower()
            snippet = r.get("snippet", "").lower()
            combined = url.lower() + " " + title + " " + snippet

            # Must mention the fund
            if not any(term in combined for term in fund_terms):
                continue

            seen_urls.add(url)
            validated.append(r)

        return validated

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _classify_content(self, url: str, title: str, snippet: str, text_start: str) -> str:
        """Classify content type based on URL, title, snippet."""
        combined = (url + " " + title + " " + snippet + " " + text_start).lower()

        for ctype, keywords in CONTENT_TYPES.items():
            if any(kw in combined for kw in keywords):
                return ctype

        return "articulo"  # default

    def _identify_source(self, url: str) -> str:
        """Identify the source from the URL."""
        url_lower = url.lower()
        for domain, name in PRIORITY_SOURCES:
            if domain in url_lower:
                return name

        # Extract domain as source name
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc.lstrip("www.")

    def _extract_date(self, url: str, title: str, text_start: str) -> str:
        """Extract date from URL or content."""
        combined = url + " " + title + " " + text_start

        # URL date patterns
        m = re.search(r'/(20[12]\d)/(\d{2})/', combined)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

        # Spanish month names
        months = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                  "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                  "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"}
        for name, num in months.items():
            m = re.search(rf'{name}\s*(?:de\s+)?(20[12]\d)', combined, re.IGNORECASE)
            if m:
                return f"{m.group(1)}-{num}"

        # Just year
        m = re.search(r'(20[12]\d)', combined)
        return m.group(1) if m else ""

    def _save(self, result: dict):
        out = self.fund_dir / "readings_data.json"
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
    args = parser.parse_args()

    agent = ReadingsAgent(args.isin, fund_name=args.fund_name, gestora=args.gestora)
    result = asyncio.run(agent.run())
    print(f"\nAnálisis: {len(result.get('analisis', []))} | Lecturas: {len(result.get('lecturas', []))}")
