"""
Readings Agent — Lecturas, análisis externos y perfiles de gestores

Busca en DuckDuckGo:
1. Entrevistas, vídeos y podcasts de los gestores del fondo
2. Análisis del fondo en webs especializadas (Salud Financiera, Astralis, Morningstar...)
3. Artículos y noticias generales sobre el fondo
4. Perfil de gestores en Citywire / Trustnet

Output:
  data/funds/{ISIN}/lecturas.json
  data/funds/{ISIN}/analisis_externos.json
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

SOURCES_ANALYSIS = [
    "saludfinanciera.substack.com",   # Substack newsletter de análisis de fondos ES
    "saludfinanciera.es",
    "astralisfundsacademy.com",       # Astralis — entrevistas y análisis detallados
    "astralis.es",
    "morningstar.es",
    "rankia.com",
    "finect.com",
    "investing.com",
    "elblogsalmon.com",
    "riverpatrimonio.com",
    "inversor-tranquilo.com",
    "valueschool.es",
    "masdividendos.com",
]

# Dominios donde la query con site: no funciona bien (subdominios, SPAs)
# → usar búsqueda amplia "{fund}" + nombre del sitio como keyword
BROAD_SEARCH_SOURCES = [
    "saludfinanciera.substack.com",
    "astralisfundsacademy.com",
]

# Para Substack usamos búsqueda amplia (no site:) porque los perfiles son subdominios
SUBSTACK_QUERY_PATTERN = '"{fund_short}" site:substack.com'

DDG_HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://duckduckgo.com/",
}


class ReadingsAgent:
    """
    Agente de lecturas y análisis externos.
    async def run() -> dict según convenio del proyecto.
    """

    def __init__(self, isin: str, fund_name: str = "", gestora: str = "", gestores: list[str] | None = None):
        self.isin       = isin.strip().upper()
        self.fund_name  = fund_name
        self.gestora    = gestora
        self.gestores   = gestores or []

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

        self._log_path = root / "progress.log"

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [READINGS] [{level}] {msg}"
        # Use print for Windows console safety (avoids cp1252 UnicodeEncodeError from rich)
        safe_line = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe_line, flush=True)
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    async def _ddg_search(self, query: str, max_results: int = 5) -> list[dict]:
        """
        Busca en DuckDuckGo HTML (sin API key).
        Devuelve [{titulo, url, snippet}]
        """
        enc_q = urllib.parse.quote_plus(query)
        ddg_url = f"https://html.duckduckgo.com/html/?q={enc_q}"
        try:
            html = await get_with_headers(ddg_url, DDG_HEADERS)
        except Exception as exc:
            self._log("WARN", f"DDG error para '{query}': {exc}")
            return []

        soup = BeautifulSoup(html, "lxml")
        results = []
        for a_tag in soup.select(".result__a"):
            titulo = a_tag.get_text(strip=True)
            href   = a_tag.get("href", "")
            # DDG envuelve las URLs en redirects; extraer URL real
            url = self._extract_ddg_url(href)
            if not url:
                continue
            # Snippet: hermano siguiente del result
            snippet_tag = a_tag.find_parent(".result")
            snippet = ""
            if snippet_tag:
                snip = snippet_tag.select_one(".result__snippet")
                if snip:
                    snippet = snip.get_text(strip=True)
            results.append({"titulo": titulo, "url": url, "snippet": snippet})
            if len(results) >= max_results:
                break

        self._log("INFO", f"DDG '{query}' -> {len(results)} resultados")
        return results

    def _extract_ddg_url(self, href: str) -> str:
        """Extrae URL real del redirect de DuckDuckGo."""
        if href.startswith("http") and "duckduckgo" not in href:
            return href
        # /l/?uddg=... o //duckduckgo.com/l/?uddg=...
        m = re.search(r"uddg=([^&]+)", href)
        if m:
            return urllib.parse.unquote(m.group(1))
        # Param "u=" en algunos formatos
        m2 = re.search(r"\bu=([^&]+)", href)
        if m2:
            return urllib.parse.unquote(m2.group(1))
        return ""

    def _is_fund_related(self, result: dict) -> bool:
        """Comprueba que el resultado menciona el fondo (primeros 10 chars)."""
        fund_kw = self.fund_name.lower()[:12] if self.fund_name else ""
        gestora_kw = self.gestora.lower()[:10] if self.gestora else ""
        text = (result.get("titulo", "") + " " + result.get("snippet", "")).lower()
        if fund_kw and fund_kw in text:
            return True
        if gestora_kw and gestora_kw in text:
            return True
        return False

    def _classify(self, results: list[dict], source_type: str = "fondo") -> list[dict]:
        """Añade tipo y fuente a los resultados."""
        out = []
        for r in results:
            url_lower = r.get("url", "").lower()
            titulo_lower = r.get("titulo", "").lower()
            snippet_lower = r.get("snippet", "").lower()
            combined = url_lower + " " + titulo_lower + " " + snippet_lower

            tipo = "articulo"
            if any(kw in combined for kw in ["youtube.com", "youtu.be", "vimeo.com"]):
                tipo = "video"
            elif any(kw in combined for kw in ["podcast", "ivoox", "spotify", "anchor"]):
                tipo = "podcast"
            elif any(kw in combined for kw in ["entrevista", "interview"]):
                tipo = "entrevista"

            fuente = self._extract_domain(r.get("url", ""))
            out.append({
                "tipo":       tipo if source_type != "gestor" else ("perfil_gestor" if "citywire" in url_lower or "trustnet" in url_lower else tipo),
                "titulo":     r.get("titulo", ""),
                "url":        r.get("url", ""),
                "fuente":     fuente,
                "fecha":      "",
                "descripcion": r.get("snippet", ""),
            })
        return out

    def _extract_domain(self, url: str) -> str:
        try:
            return urllib.parse.urlparse(url).netloc.lstrip("www.")
        except Exception:
            return ""

    async def _fetch_and_summarize(self, url: str) -> str:
        """Fetches a real article URL and generates a Claude summary of 10-15 lines."""
        search_domains = ("google.com", "duckduckgo.com", "bing.com", "yahoo.com",
                          "twitter.com", "x.com", "linkedin.com")
        if any(d in url for d in search_domains):
            return ""
        try:
            html = await get_with_headers(url, DDG_HEADERS)
            from bs4 import BeautifulSoup as _BS
            soup = _BS(html, "lxml")
            for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
                tag.decompose()
            paragraphs = [
                p.get_text(strip=True)
                for p in soup.find_all(["p", "h2", "h3"])
                if len(p.get_text(strip=True)) > 50
            ]
            text = " ".join(paragraphs)
            if len(text) < 300:
                return ""
            from tools.claude_extractor import extract_structured_data
            result = extract_structured_data(
                text[:5000],
                {"resumen": "resumen del análisis en 10-15 líneas, en español, sobre el fondo de inversión"},
                context=f"Análisis del fondo {self.fund_name} ({self.isin})",
            )
            return result.get("resumen", "")
        except Exception as exc:
            self._log("WARN", f"fetch_summarize error {url}: {exc}")
            return ""

    async def _get_citywire_profile(self, gestor: str) -> dict | None:
        """Intenta obtener perfil de Citywire del gestor."""
        slug = re.sub(r"[^a-z0-9]+", "-", gestor.lower().strip()).strip("-")
        url = f"https://citywire.com/selector/manager/profile/{slug}"
        try:
            html = await get_with_headers(url, DDG_HEADERS)
            soup = BeautifulSoup(html, "lxml")
            # Verificar que la página existe (no 404)
            title = soup.find("title")
            if title and ("404" in title.text or "not found" in title.text.lower()):
                return None
            # Extraer título de la página como descripción
            h1 = soup.find("h1")
            desc = h1.get_text(strip=True) if h1 else gestor
            return {
                "tipo": "perfil_gestor",
                "titulo": f"Citywire — {desc}",
                "url": url,
                "fuente": "citywire.com",
                "fecha": "",
                "descripcion": f"Perfil de {gestor} en Citywire Selector",
            }
        except Exception:
            return None

    def _short_fund_name(self) -> str:
        """Strip legal suffixes for better search results."""
        s = re.sub(r'\b(FI|SICAV|FP|SIL|FUND|FONDO)\b', '', self.fund_name, flags=re.IGNORECASE)
        return s.strip().strip(",").strip()

    async def run(self) -> dict:
        self._log("INFO", f"ReadingsAgent iniciando para {self.isin} — {self.fund_name}")
        lecturas: list[dict] = []
        analisis: list[dict] = []
        fund_short = self._short_fund_name() or self.isin
        seen_analysis_urls: set[str] = set()

        async def _process_analysis_result(r: dict, site_label: str):
            url = r.get("url", "")
            if not url or url in seen_analysis_urls:
                return
            if any(d in url for d in ("google.com", "duckduckgo.com", "bing.com")):
                return
            seen_analysis_urls.add(url)
            self._log("INFO", f"Fetching article: {url[:80]}")
            resumen_generado = await self._fetch_and_summarize(url)
            if not resumen_generado and not r.get("snippet"):
                return
            analisis.append({
                "fuente":            site_label,
                "titulo":            r.get("titulo", ""),
                "url":               url,
                "fecha":             "",
                "resumen":           r.get("snippet", ""),
                "resumen_generado":  resumen_generado,
                "palabras_estimadas": self._estimate_words(r.get("snippet", "")),
            })

        # ── PASO 1: Fuentes PRIORITARIAS — Substack, Astralis, Rankia ────────
        # Búsquedas simples y directas, como haría un usuario en Google.
        # Usar nombre del fondo (NO depender de gestores que pueden ser null).
        if fund_short:
            self._log("INFO", "Paso 1: Búsqueda en fuentes prioritarias (Substack, Astralis, Rankia)")
            priority_queries = [
                # Substack Salud Financiera — fuente principal de análisis de fondos ES
                (f'"{fund_short}" salud financiera', "saludfinanciera.substack.com"),
                (f'"{fund_short}" site:substack.com', "substack.com"),
                # Astralis — entrevistas y análisis detallados
                (f'"{fund_short}" astralis', "astralisfundsacademy.com"),
                # Rankia — foros y análisis comunitarios
                (f'"{fund_short}" rankia', "rankia.com"),
            ]
            for query, source_label in priority_queries:
                results = await self._ddg_search(query, max_results=5)
                for r in results:
                    await _process_analysis_result(r, source_label)
                await asyncio.sleep(1)

        # ── PASO 2: Otras webs especializadas ─────────────────────────────────
        if fund_short:
            self._log("INFO", "Paso 2: Búsqueda en webs especializadas")
            secondary_queries = [
                (f'"{fund_short}" morningstar', "morningstar.es"),
                (f'"{fund_short}" finect', "finect.com"),
                (f'"{fund_short}" masdividendos', "masdividendos.com"),
                (f'"{fund_short}" valueschool', "valueschool.es"),
            ]
            for query, source_label in secondary_queries:
                results = await self._ddg_search(query, max_results=3)
                for r in results:
                    await _process_analysis_result(r, source_label)
                await asyncio.sleep(1)

        # ── PASO 3: Búsqueda amplia (cualquier fuente) ───────────────────────
        if fund_short:
            self._log("INFO", "Paso 3: Búsqueda amplia de análisis")
            broad_queries = [
                f'"{fund_short}" análisis fondo inversión',
                f'"{fund_short}" opinión cartera',
            ]
            if self.gestora:
                broad_queries.append(f'"{fund_short}" "{self.gestora}"')
            for bq in broad_queries:
                results = await self._ddg_search(bq, max_results=5)
                for r in results:
                    if self._is_fund_related(r):
                        site_label = self._extract_domain(r.get("url", ""))
                        await _process_analysis_result(r, site_label)
                await asyncio.sleep(1)

        # ── PASO 4: Gestores — entrevistas + vídeos + perfiles ────────────────
        # Usar nombre del fondo como query principal, gestores como refuerzo
        if fund_short:
            self._log("INFO", "Paso 4: Buscando entrevistas y vídeos")
            media_queries = [
                (f'"{fund_short}" entrevista gestor', "entrevista"),
                (f'"{fund_short}" site:youtube.com', "youtube"),
                (f'"{fund_short}" podcast', "podcast"),
            ]
            for q, src in media_queries:
                results = await self._ddg_search(q, max_results=4)
                lecturas.extend(self._classify(results, source_type="fondo"))
                await asyncio.sleep(1)

        # Gestores individuales (solo si tenemos nombres)
        for gestor in self.gestores[:3]:
            self._log("INFO", f"Buscando perfil gestor: {gestor}")
            gestor_queries = [
                (f'"{gestor}" entrevista inversión', 3),
                (f'"{gestor}" citywire OR rankia OR finect', 3),
            ]
            for q, max_r in gestor_queries:
                res = await self._ddg_search(q, max_results=max_r)
                lecturas.extend(self._classify(res, source_type="gestor"))
                await asyncio.sleep(1)
            cw = await self._get_citywire_profile(gestor)
            if cw:
                lecturas.append(cw)

        # ── PASO 5: Fallback por ISIN si pocos resultados ────────────────────
        if len(analisis) < 3:
            self._log("INFO", f"Pocos análisis ({len(analisis)}), buscando por ISIN")
            isin_queries = [
                f'{self.isin} análisis fondo',
                f'{self.isin} morningstar',
                f'{self.isin} rankia',
            ]
            for iq in isin_queries:
                results = await self._ddg_search(iq, max_results=4)
                for r in results:
                    url = r.get("url", "")
                    if not url or any(d in url for d in ("google.com", "duckduckgo.com")):
                        continue
                    site_label = self._extract_domain(url)
                    await _process_analysis_result(r, site_label)
                await asyncio.sleep(1)

        # ── PASO 6: Iteración sobre URLs buenas ──────────────────────────────
        # Si encontramos URLs de análisis, intentar descubrir más del mismo sitio
        if analisis:
            good_domains = set()
            for a in analisis[:5]:
                domain = self._extract_domain(a.get("url", ""))
                if domain and domain not in good_domains:
                    good_domains.add(domain)
            for domain in list(good_domains)[:3]:
                self._log("INFO", f"Iterando sobre dominio exitoso: {domain}")
                iter_query = f'"{fund_short}" site:{domain}'
                results = await self._ddg_search(iter_query, max_results=5)
                for r in results:
                    await _process_analysis_result(r, domain)
                await asyncio.sleep(1)
                await asyncio.sleep(1.5)

        # ── PASO 7: Cruzar con letters_data — entrevistas/podcasts ────────────
        letters_path = self.fund_dir / "letters_data.json"
        if letters_path.exists():
            try:
                letters = json.loads(letters_path.read_text(encoding="utf-8"))
                for carta in letters.get("cartas", []):
                    url = carta.get("url_fuente", "")
                    if not url:
                        continue
                    combined = (url + " " + carta.get("titulo", "")).lower()
                    # If it looks like interview/podcast/video, add to lecturas
                    if any(kw in combined for kw in [
                        "entrevista", "interview", "podcast", "youtube",
                        "ivoox", "spotify", "video", "repasando", "value investing fm"
                    ]):
                        lecturas.append({
                            "tipo": "entrevista" if "entrevista" in combined or "interview" in combined
                                    else "podcast" if "podcast" in combined or "ivoox" in combined
                                    else "video" if "youtube" in combined or "video" in combined
                                    else "articulo",
                            "titulo": carta.get("titulo", ""),
                            "url": url,
                            "fuente": self._extract_domain(url),
                            "fecha": carta.get("periodo", ""),
                            "descripcion": (carta.get("tesis_inversion") or carta.get("resumen_mercado") or "")[:200],
                        })
            except Exception:
                pass

        # Dedup lecturas por URL
        seen_urls: set[str] = set()
        lecturas_dedup = []
        for item in lecturas:
            url = item.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                lecturas_dedup.append(item)

        # analisis ya está deduplicado por seen_analysis_urls
        analisis_dedup = analisis

        # Guardar
        lecturas_out = {"lecturas": lecturas_dedup, "generado": datetime.now().isoformat()}
        analisis_out = analisis_dedup  # guardado como lista directamente

        lecturas_path = self.fund_dir / "lecturas.json"
        analisis_path = self.fund_dir / "analisis_externos.json"

        lecturas_path.write_text(json.dumps(lecturas_out, ensure_ascii=False, indent=2), encoding="utf-8")
        analisis_path.write_text(json.dumps(analisis_out, ensure_ascii=False, indent=2), encoding="utf-8")

        self._log("INFO", f"OK Lecturas: {len(lecturas_dedup)} | Analisis externos: {len(analisis_dedup)}")
        return {"lecturas": lecturas_dedup, "analisis_externos": analisis_dedup}

    def _is_substantial(self, result: dict) -> bool:
        """Resultado con snippet suficiente que menciona el fondo."""
        snippet = result.get("snippet", "")
        return len(snippet) > 80

    def _estimate_words(self, snippet: str) -> int:
        """Estima palabras del artículo desde el snippet (muy aproximado)."""
        return len(snippet.split()) * 10  # ~10x la longitud del snippet


# ── CLI standalone ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--fund-name", default="")
    parser.add_argument("--gestora", default="")
    parser.add_argument("--gestores", default="", help="Nombres separados por ;")
    args = parser.parse_args()

    gestores_list = [g.strip() for g in args.gestores.split(";") if g.strip()] if args.gestores else []
    agent = ReadingsAgent(
        isin=args.isin,
        fund_name=args.fund_name,
        gestora=args.gestora,
        gestores=gestores_list,
    )
    result = asyncio.run(agent.run())
    print(f"Lecturas: {len(result['lecturas'])}")
    print(f"Análisis externos: {len(result['analisis_externos'])}")
