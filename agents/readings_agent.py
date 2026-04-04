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

console = Console()

SOURCES_ANALYSIS = [
    "saludfinanciera.es",
    "astralis.es",
    "morningstar.es",
    "rankia.com",
    "finect.com",
    "investing.com",
    "elblogsalmon.com",
]

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
        console.log(line)
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

        self._log("INFO", f"DDG '{query}' → {len(results)} resultados")
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

    async def run(self) -> dict:
        self._log("INFO", f"ReadingsAgent iniciando para {self.isin} — {self.fund_name}")
        lecturas: list[dict] = []
        analisis: list[dict] = []

        # 1. Gestores: entrevistas + vídeos + perfiles
        for gestor in self.gestores[:4]:  # máx 4 gestores
            self._log("INFO", f"Buscando entrevistas para gestor: {gestor}")
            results = await self._ddg_search(f'"{gestor}" entrevista inversión fondo', max_results=4)
            results += await self._ddg_search(f'"{gestor}" youtube podcast inversión', max_results=3)
            lecturas.extend(self._classify(results, source_type="gestor"))
            await asyncio.sleep(1.5)  # Respetar rate limit DDG

            # Perfil Citywire
            cw = await self._get_citywire_profile(gestor)
            if cw:
                lecturas.append(cw)
            await asyncio.sleep(1)

        # 2. Análisis en webs especializadas
        for site in SOURCES_ANALYSIS:
            self._log("INFO", f"Buscando en {site}")
            query = f'site:{site} "{self.fund_name}"' if self.fund_name else f'site:{site} {self.isin}'
            results = await self._ddg_search(query, max_results=5)
            for r in results:
                if self._is_substantial(r):
                    analisis.append({
                        "fuente":            site,
                        "titulo":            r.get("titulo", ""),
                        "url":               r.get("url", ""),
                        "fecha":             "",
                        "resumen":           r.get("snippet", ""),
                        "palabras_estimadas": self._estimate_words(r.get("snippet", "")),
                    })
            await asyncio.sleep(1.5)

        # 3. Artículos generales sobre el fondo
        if self.fund_name:
            self._log("INFO", "Buscando artículos generales del fondo")
            results = await self._ddg_search(f'"{self.fund_name}" análisis reseña')
            lecturas.extend(self._classify(results, source_type="fondo"))
            await asyncio.sleep(1)

            results2 = await self._ddg_search(f'"{self.fund_name}" fondo inversión opinión')
            lecturas.extend(self._classify(results2, source_type="fondo"))
            await asyncio.sleep(1)

        # Dedup por URL
        seen_urls: set[str] = set()
        lecturas_dedup = []
        for item in lecturas:
            url = item.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                lecturas_dedup.append(item)

        analisis_dedup = []
        for item in analisis:
            url = item.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                analisis_dedup.append(item)

        # Guardar
        lecturas_out = {"lecturas": lecturas_dedup, "generado": datetime.now().isoformat()}
        analisis_out = analisis_dedup  # guardado como lista directamente

        lecturas_path = self.fund_dir / "lecturas.json"
        analisis_path = self.fund_dir / "analisis_externos.json"

        lecturas_path.write_text(json.dumps(lecturas_out, ensure_ascii=False, indent=2), encoding="utf-8")
        analisis_path.write_text(json.dumps(analisis_out, ensure_ascii=False, indent=2), encoding="utf-8")

        self._log("INFO", f"✓ Lecturas: {len(lecturas_dedup)} | Análisis externos: {len(analisis_dedup)}")
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
