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
from tools.trusted_sources import get_priority_sources

console = Console(highlight=False, force_terminal=False)

# Priority sources for fund analysis. Cargada desde data/trusted_sources.json
# (loader en tools/trusted_sources.py). Para añadir/quitar fuentes, editar el JSON.
PRIORITY_SOURCES = get_priority_sources() or [
    # Fallback hardcoded — solo se usa si el JSON falta o falla.
    ("saludfinanciera.substack.com", "Substack Salud Financiera"),
    ("moclano.substack.com", "Moclano"),
    ("astralisfundsacademy.com", "Astralis Funds Academy"),
    ("rankia.com", "Rankia"),
    ("finect.com", "Finect"),
    ("morningstar.es", "Morningstar ES"),
    ("morningstar.com", "Morningstar US"),
    ("citywire.com", "Citywire Global"),
    ("ft.com", "Financial Times"),
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
            "_queries_log": [],
            "_discarded_cross_fund": [],
            "_pro_sources_attempted": [],
        }

        # ── Paso 1: Definir búsquedas ────────────────────────────────────────
        queries = self._build_queries()
        result["_queries_log"] = list(queries)
        result["_pro_sources_attempted"] = getattr(self, "_queries_log_pro_sources", [])
        self._log("INFO", f"Paso 1: {len(queries)} búsquedas definidas (incluye {len(result['_pro_sources_attempted'])} site:pro)")

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

        # ── Paso 4: Cargar fondos conocidos (cross-fund check) ───────────────
        known_funds = self._load_known_fund_names()

        # ── Paso 5: Descargar, validar full-text, clasificar ─────────────────
        from tools.trusted_sources import get_pro_source_domains
        pro_domains = set(get_pro_source_domains())

        for entry in validated[:30]:  # Max 30 artículos (Bug 2: ampliado para más cobertura)
            url = entry.get("url", "")
            title = entry.get("title", entry.get("titulo", ""))
            snippet = entry.get("snippet", "")

            text = await fetch_page_text(url, max_chars=0)  # Sin límite
            if not text or len(text) < 300:
                continue

            url_l = url.lower()
            is_pro = any(d in url_l for d in pro_domains)
            text_l = text.lower()
            isin_in_text = self.isin.lower() in text_l
            name_count = text_l.count((self.fund_short or "").lower()) if self.fund_short else 0

            # Validation log
            validation_log = []
            if is_pro:
                validation_log.append("pro_source")
            if isin_in_text:
                validation_log.append("isin_match")
            if name_count >= 2:
                validation_log.append(f"name_match_{name_count}x")

            # Filtro Bug 2 B.4: pro source requiere ISIN o nombre ≥2x; generalista ≥3x
            min_name = 2 if is_pro else 3
            if not isin_in_text and name_count < min_name:
                self._log("SKIP", f"insufficient match: {url[:60]}")
                continue

            # Filtro Bug 2 B.3: cross-fund contamination
            this_count, other_count, other_name = self._check_cross_fund_contamination(text, known_funds)
            if other_count > this_count and other_count >= 2:
                self._log("DROP_CROSS", f"reading habla más de '{other_name}' ({other_count}x) que de '{self.fund_short}' ({this_count}x): {url[:60]}")
                result["_discarded_cross_fund"].append({
                    "url": url,
                    "this_fund_mentions": this_count,
                    "other_fund": other_name,
                    "other_mentions": other_count,
                })
                continue

            content_type = self._classify_content(url, title, snippet, text[:500])
            quality = self._classify_quality(url, title, text, self._identify_source(url))
            source = self._identify_source(url)

            item = {
                "fuente": source,
                "tipo": content_type,
                "quality_classification": quality,
                "titulo": title,
                "url": url,
                "snippet": snippet,
                "texto_completo": text,
                "fecha": self._extract_date(url, title, text[:300]),
                "fecha_publicacion": self._extract_date(url, title, text[:300]),
                "_validation_log": validation_log,
                "_is_pro_source": is_pro,
                "_fund_name_mentions": name_count,
                "_isin_in_text": isin_in_text,
            }

            if content_type in ("analisis", "articulo", "comunidad"):
                result["analisis"].append(item)
            else:
                result["lecturas"].append(item)

            self._log("INFO", f"[{quality:10s}] {source:20s} {title[:40]}")

        result["fuentes_consultadas"] = list({r.get("url", "") for r in validated})[:50]

        n_analisis = len(result["analisis"])
        n_lecturas = len(result["lecturas"])
        n_discarded = len(result["_discarded_cross_fund"])
        self._log("OK", f"Análisis: {n_analisis} | Lecturas: {n_lecturas} | Cross-fund descartados: {n_discarded}")

        self._save(result)
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 1: DEFINIR BÚSQUEDAS
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_queries(self) -> list[str]:
        """Build targeted search queries for fund analysis content.

        Fase G Bug 2 (2026-04-28): cada pro source aplicable a la región del fondo
        recibe una query explícita `site:{domain} "{fund}"` para asegurar cobertura
        (Google enterraba blogs largos cuando no se filtra por sitio).
        """
        from tools.trusted_sources import get_pro_source_domains

        queries = []
        fund_q = self.fund_short or self.isin
        is_es = self.isin.startswith("ES")
        region = "ES" if is_es else "INT"

        # Fase 1: Site-specific queries por cada pro source de la región
        pro_domains = get_pro_source_domains(region=region)
        self._queries_log_pro_sources = list(pro_domains)  # auditoría
        for domain in pro_domains:
            queries.append(f'site:{domain} "{fund_q}"')
            # Para INT: también buscar por gestora dentro de cada pro source
            if not is_es and self.gestora:
                queries.append(f'site:{domain} "{self.gestora}"')

        # Fase 2: Queries genéricas (cobertura amplia para sources NO pro)
        if is_es:
            queries.extend([
                f'"{fund_q}" análisis fondo',
                f'"{fund_q}" opinión cartera',
                f'"{fund_q}" entrevista gestor',
                f'"{fund_q}" carta inversores',
                f'"{fund_q}" YouTube',
            ])
        else:
            queries.extend([
                f'"{fund_q}" review fund analysis',
                f'"{fund_q}" interview manager',
                f'"{fund_q}" investor letter',
                f'"{fund_q}" YouTube',
            ])

        # Gestora-specific
        if self.gestora:
            queries.append(f'"{self.gestora}" análisis fondos')

        # ISIN fallback
        queries.append(f'{self.isin}')

        return queries

    def _load_known_fund_names(self) -> dict[str, str]:
        """Carga nombres de TODOS los fondos del sistema → {nombre_lower: isin}.
        Usado por _check_cross_fund_contamination para detectar readings que
        hablan de OTRO fondo más que del actual.
        """
        funds_dir = Path(__file__).parent.parent / "data" / "funds"
        out = {}
        if not funds_dir.exists():
            return out
        for fd in funds_dir.iterdir():
            if not fd.is_dir():
                continue
            out_path = fd / "output.json"
            if not out_path.exists():
                continue
            try:
                d = json.loads(out_path.read_text(encoding="utf-8"))
                nombre = d.get("nombre", "").strip()
                if nombre and len(nombre) > 5:
                    # Tomar nombre corto (primer término antes de coma)
                    nombre_short = nombre.split(",")[0].strip().lower()
                    out[nombre_short] = fd.name
            except Exception:
                continue
        return out

    def _check_cross_fund_contamination(self, text: str, known_funds: dict) -> tuple[int, int, str]:
        """Cuenta menciones del fondo actual vs otros fondos conocidos.
        Returns: (this_count, other_max_count, other_name).
        """
        text_lower = text.lower()
        this_short = (self.fund_short or "").lower()
        if not this_short:
            return (0, 0, "")
        this_count = text_lower.count(this_short)
        # Contar otros fondos
        other_max = 0
        other_name = ""
        for other_short, other_isin in known_funds.items():
            if other_isin == self.isin or other_short == this_short:
                continue
            cnt = text_lower.count(other_short)
            if cnt > other_max:
                other_max = cnt
                other_name = other_short
        return (this_count, other_max, other_name)

    def _classify_quality(self, url: str, title: str, text: str, source: str) -> str:
        """Clasifica calidad de un reading:
        - analysis: ≥1500 chars + dominio pro O kw "análisis|review|opinion|tesis|carta"
        - news: ≥500 chars + kw "noticia|news|publica|anuncia"
        - marketing: dominio gestora directo + kw "rentabilidad histórica|premio"
        - data: factsheet/KIID/prospectus
        """
        from tools.trusted_sources import get_pro_source_domains
        url_l = url.lower()
        text_l = (title + " " + text[:500]).lower()
        is_pro = any(d in url_l for d in get_pro_source_domains())

        if any(kw in text_l for kw in ["factsheet", "kiid", "prospectus", "folleto"]):
            return "data"
        if is_pro and len(text) >= 1500:
            return "analysis"
        if any(kw in text_l for kw in ["análisis", "review", "opinion", "opinión", "tesis", "carta a inversores"]) and len(text) >= 1500:
            return "analysis"
        if any(kw in text_l for kw in ["noticia", "news", "publica", "anuncia", "lanza"]) and len(text) >= 500:
            return "news"
        if any(kw in text_l for kw in ["rentabilidad histórica", "premio", "galardón"]):
            return "marketing"
        return "news" if len(text) >= 500 else "marketing"

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
