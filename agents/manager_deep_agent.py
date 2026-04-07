"""
Manager Deep Agent — Perfil profundo del gestor del fondo

Busca en fuentes web para construir un perfil completo del gestor principal:
1. Morningstar fund page -> seccion "Equipo"
2. Citywire -> manager profiles
3. Trustnet -> manager profiles
4. Web de la gestora -> pagina de equipo
5. DDG search -> entrevistas, articulos, background

Output:
  data/funds/{ISIN}/manager_profile.json
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

from tools.http_client import get_with_headers
from tools.claude_extractor import extract_structured_data

console = Console(highlight=False, force_terminal=False)

DDG_HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://duckduckgo.com/",
}

# Schema for Claude extraction of manager profile
MANAGER_EXTRACTION_SCHEMA = {
    "nombre": "Nombre completo del gestor",
    "cargo": "Cargo actual (ej. 'CEO y Director de Inversiones')",
    "empresa": "Empresa/gestora actual",
    "formacion": [
        {
            "titulo": "Titulo academico (ej. 'Lic. ADE', 'MBA', 'CFA')",
            "institucion": "Universidad o institucion",
            "anio": "Anio de graduacion (entero o null)",
        }
    ],
    "historial_empleos": [
        {
            "periodo": "Periodo (ej. '2004-2009')",
            "empresa": "Nombre de la empresa",
            "cargo": "Cargo desempenado",
            "descripcion": "Breve descripcion del rol",
        }
    ],
    "skin_in_the_game": {
        "descripcion": "Descripcion del compromiso del gestor con el fondo",
        "compromisos": ["Lista de compromisos concretos"],
        "documentos": ["URLs o referencias a documentos de compromiso"],
    },
    "filosofia_detallada": "Filosofia de inversion del gestor en detalle",
    "influencias": [
        "Lista de inversores, libros o escuelas que influyen en su estilo"
    ],
    "entrevistas_recientes": [
        {
            "fecha": "Fecha aproximada (ej. '2026-03')",
            "medio": "Nombre del medio (ej. 'Value Investing FM')",
            "url": "URL de la entrevista",
            "tema": "Tema principal tratado",
            "ideas_clave": ["Ideas principales extraidas"],
        }
    ],
}

# Domains to skip when fetching pages (search engines, social media walls)
SKIP_DOMAINS = (
    "google.com",
    "duckduckgo.com",
    "bing.com",
    "yahoo.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
)


def _empty_profile(isin: str, manager_name: str = "") -> dict:
    """Returns an empty manager profile skeleton."""
    return {
        "nombre": manager_name,
        "cargo": None,
        "empresa": None,
        "formacion": [],
        "historial_empleos": [],
        "skin_in_the_game": {
            "descripcion": None,
            "compromisos": [],
            "documentos": [],
        },
        "filosofia_detallada": None,
        "influencias": [],
        "entrevistas_recientes": [],
        "isin": isin,
        "generado": datetime.now().isoformat(),
        "fuentes_consultadas": [],
    }


class ManagerDeepAgent:
    """
    Agente de perfil profundo de gestores de fondos.
    async def run() -> dict segun convenio del proyecto.
    """

    def __init__(
        self,
        isin: str,
        fund_name: str = "",
        gestora: str = "",
        manager_names: list[str] | None = None,
    ):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name
        self.gestora = gestora
        self.manager_names = manager_names or []

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

        self._log_path = root / "progress.log"
        self._sources_consulted: list[str] = []
        self._collected_texts: list[dict] = []  # {"source": str, "url": str, "text": str}
        self._interview_urls: list[dict] = []   # {"url": str, "titulo": str, "medio": str}

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [MANAGER_DEEP] [{level}] {msg}"
        safe_line = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe_line, flush=True)
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    # ── DDG Search ───────────────────────────────────────────────────────────

    async def _ddg_search(self, query: str, max_results: int = 5) -> list[dict]:
        """
        Search DuckDuckGo HTML (no API key needed).
        Returns [{"titulo", "url", "snippet"}]
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
            href = a_tag.get("href", "")
            url = self._extract_ddg_url(href)
            if not url:
                continue
            snippet_tag = a_tag.find_parent(".result")
            snippet = ""
            if snippet_tag:
                snip = snippet_tag.select_one(".result__snippet")
                if snip:
                    snippet = snip.get_text(strip=True)
            results.append({"titulo": titulo, "url": url, "snippet": snippet})
            if len(results) >= max_results:
                break

        self._log("INFO", f"DDG '{query[:60]}' -> {len(results)} resultados")
        return results

    def _extract_ddg_url(self, href: str) -> str:
        """Extract real URL from DDG redirect."""
        if href.startswith("http") and "duckduckgo" not in href:
            return href
        m = re.search(r"uddg=([^&]+)", href)
        if m:
            return urllib.parse.unquote(m.group(1))
        m2 = re.search(r"\bu=([^&]+)", href)
        if m2:
            return urllib.parse.unquote(m2.group(1))
        return ""

    def _extract_domain(self, url: str) -> str:
        try:
            return urllib.parse.urlparse(url).netloc.lstrip("www.")
        except Exception:
            return ""

    # ── Page fetching ────────────────────────────────────────────────────────

    async def _fetch_page_text(self, url: str, max_chars: int = 3000) -> str:
        """
        Fetch a page and extract clean text content.
        Removes script, style, nav, footer tags. Returns truncated text.
        """
        domain = self._extract_domain(url)
        if any(d in domain for d in SKIP_DOMAINS):
            return ""
        try:
            html = await get_with_headers(url, DDG_HEADERS)
            soup = BeautifulSoup(html, "lxml")
            for tag in soup(["script", "style", "nav", "footer", "aside", "header", "noscript"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            return text[:max_chars]
        except Exception as exc:
            self._log("WARN", f"Fetch error {url[:80]}: {exc}")
            return ""

    async def _fetch_and_collect(self, url: str, source_label: str):
        """Fetch a page and add its text to the collected texts pool."""
        text = await self._fetch_page_text(url)
        if text and len(text) > 100:
            self._collected_texts.append({
                "source": source_label,
                "url": url,
                "text": text,
            })
            self._sources_consulted.append(url)
            self._log("INFO", f"Collected {len(text)} chars from {source_label}: {url[:70]}")

    # ── Manager name discovery ───────────────────────────────────────────────

    async def _discover_manager_names(self) -> list[str]:
        """
        Try to find manager names from existing data files or DDG search.
        Priority:
        1. output.json -> cualitativo.gestores
        2. cnmv_data.json -> cualitativo.gestores
        3. analisis_externos.json -> look for name mentions
        4. DDG search: "{fund_name} gestor equipo manager"
        """
        names = []

        # Try output.json
        output_path = self.fund_dir / "output.json"
        if output_path.exists():
            try:
                data = json.loads(output_path.read_text(encoding="utf-8"))
                gestores = data.get("cualitativo", {}).get("gestores", [])
                for g in gestores:
                    name = g.get("nombre", "")
                    if name:
                        names.append(name)
            except Exception:
                pass

        if names:
            self._log("INFO", f"Gestores from output.json: {names}")
            return names

        # Try cnmv_data.json
        cnmv_path = self.fund_dir / "cnmv_data.json"
        if cnmv_path.exists():
            try:
                data = json.loads(cnmv_path.read_text(encoding="utf-8"))
                gestores = data.get("cualitativo", {}).get("gestores", [])
                for g in gestores:
                    name = g.get("nombre", "")
                    if name:
                        names.append(name)
            except Exception:
                pass

        if names:
            self._log("INFO", f"Gestores from cnmv_data.json: {names}")
            return names

        # Try analisis_externos.json for name mentions via Claude
        analisis_path = self.fund_dir / "analisis_externos.json"
        if analisis_path.exists():
            try:
                analisis = json.loads(analisis_path.read_text(encoding="utf-8"))
                # Collect snippets that may mention manager names
                snippets = []
                for item in analisis[:5]:
                    r = item.get("resumen", "")
                    rg = item.get("resumen_generado", "")
                    if r:
                        snippets.append(r)
                    if rg:
                        snippets.append(rg[:500])
                if snippets:
                    combined = " ".join(snippets)[:2000]
                    try:
                        result = extract_structured_data(
                            combined,
                            {"gestores": ["nombre completo del gestor o gestores del fondo mencionados en el texto"]},
                            context=f"Identificar gestores del fondo {self.fund_name} ({self.isin})",
                        )
                        found = result.get("gestores", [])
                        if found and isinstance(found, list):
                            names = [n for n in found if isinstance(n, str) and len(n) > 3]
                    except Exception:
                        pass
            except Exception:
                pass

        if names:
            self._log("INFO", f"Gestores from analisis_externos.json: {names}")
            return names

        # DDG search as last resort
        search_terms = []
        if self.fund_name:
            search_terms.append(f'"{self.fund_name}" gestor equipo manager')
        if self.gestora:
            search_terms.append(f'"{self.gestora}" equipo gestor director inversiones')

        for query in search_terms:
            results = await self._ddg_search(query, max_results=5)
            snippets = " ".join(r.get("snippet", "") for r in results)
            if len(snippets) > 50:
                try:
                    result = extract_structured_data(
                        snippets[:2000],
                        {"gestores": ["nombre completo del gestor o gestores mencionados"]},
                        context=f"Identificar gestores del fondo {self.fund_name} ({self.isin})",
                    )
                    found = result.get("gestores", [])
                    if found and isinstance(found, list):
                        names = [n for n in found if isinstance(n, str) and len(n) > 3]
                        if names:
                            break
                except Exception:
                    pass
            await asyncio.sleep(1)

        if names:
            self._log("INFO", f"Gestores from DDG search: {names}")
        else:
            self._log("WARN", "No se encontraron nombres de gestores")

        return names

    # ── Source-specific searches ─────────────────────────────────────────────

    async def _search_morningstar(self, manager_name: str):
        """Search Morningstar fund page for 'Equipo' section."""
        queries = []
        if self.fund_name:
            queries.append(f'"{self.fund_name}" morningstar equipo')
            queries.append(f'"{self.fund_name}" site:morningstar.es equipo gestor')
        if not queries:
            queries.append(f'"{manager_name}" morningstar fund manager')

        for query in queries:
            results = await self._ddg_search(query, max_results=3)
            for r in results:
                url = r.get("url", "")
                if "morningstar" in url.lower():
                    await self._fetch_and_collect(url, "morningstar")
            await asyncio.sleep(1)

    async def _search_citywire(self, manager_name: str):
        """Search Citywire for manager profile."""
        query = f'"{manager_name}" citywire'
        results = await self._ddg_search(query, max_results=3)
        for r in results:
            url = r.get("url", "")
            if "citywire" in url.lower():
                await self._fetch_and_collect(url, "citywire")
        await asyncio.sleep(1)

        # Also try direct slug URL
        slug = re.sub(r"[^a-z0-9]+", "-", manager_name.lower().strip()).strip("-")
        direct_url = f"https://citywire.com/selector/manager/profile/{slug}"
        try:
            text = await self._fetch_page_text(direct_url)
            if text and len(text) > 200 and "404" not in text[:200].lower():
                self._collected_texts.append({
                    "source": "citywire_direct",
                    "url": direct_url,
                    "text": text,
                })
                self._sources_consulted.append(direct_url)
                self._log("INFO", f"Citywire direct profile found: {direct_url}")
        except Exception:
            pass

    async def _search_trustnet(self, manager_name: str):
        """Search Trustnet for manager profile."""
        query = f'"{manager_name}" trustnet'
        results = await self._ddg_search(query, max_results=3)
        for r in results:
            url = r.get("url", "")
            if "trustnet" in url.lower():
                await self._fetch_and_collect(url, "trustnet")
        await asyncio.sleep(1)

    async def _search_gestora_web(self, manager_name: str):
        """Search gestora website for team/equipo page."""
        queries = []
        if self.gestora:
            queries.append(f'"{manager_name}" "{self.gestora}" equipo')
            queries.append(f'"{self.gestora}" equipo directivo gestor')
        if self.fund_name:
            queries.append(f'"{manager_name}" "{self.fund_name}"')

        for query in queries:
            results = await self._ddg_search(query, max_results=3)
            for r in results:
                url = r.get("url", "")
                if any(d in url.lower() for d in SKIP_DOMAINS):
                    continue
                # Prefer gestora domain if we can identify it
                await self._fetch_and_collect(url, "gestora_web")
            await asyncio.sleep(1)

    async def _search_interviews(self, manager_name: str):
        """Search for interviews, podcasts, and articles about the manager."""
        current_year = datetime.now().year
        queries = [
            f'"{manager_name}" entrevista {current_year}',
            f'"{manager_name}" entrevista {current_year - 1}',
            f'"{manager_name}" interview fund manager',
            f'"{manager_name}" podcast inversión',
        ]
        if self.fund_name:
            queries.append(f'"{manager_name}" "{self.fund_name}" entrevista')

        seen_urls: set[str] = set()
        for query in queries:
            results = await self._ddg_search(query, max_results=4)
            for r in results:
                url = r.get("url", "")
                if not url or url in seen_urls:
                    continue
                if any(d in url for d in SKIP_DOMAINS):
                    continue
                seen_urls.add(url)

                # Track as interview candidate
                self._interview_urls.append({
                    "url": url,
                    "titulo": r.get("titulo", ""),
                    "medio": self._extract_domain(url),
                    "snippet": r.get("snippet", ""),
                })

                # Fetch content for profile enrichment
                await self._fetch_and_collect(url, "entrevista")
            await asyncio.sleep(1)

    async def _search_general(self, manager_name: str):
        """General DDG search for background information."""
        queries = [
            f'"{manager_name}" biografia trayectoria inversión',
            f'"{manager_name}" CFA MBA formacion',
            f'"{manager_name}" patrimonio personal fondo skin in the game',
        ]

        for query in queries:
            results = await self._ddg_search(query, max_results=3)
            for r in results:
                url = r.get("url", "")
                if not url or any(d in url for d in SKIP_DOMAINS):
                    continue
                await self._fetch_and_collect(url, "general_search")
            await asyncio.sleep(1)

    # ── Profile synthesis via Claude ─────────────────────────────────────────

    def _build_extraction_prompt_text(self, manager_name: str) -> str:
        """
        Combine all collected texts into a single prompt for Claude extraction.
        Limits total size to avoid excessive token usage.
        """
        max_total_chars = 12000
        combined_parts = []
        chars_used = 0

        # Sort by source priority: citywire > morningstar > gestora > interviews > general
        priority_map = {
            "morningstar": 1,
            "citywire": 2,
            "citywire_direct": 2,
            "trustnet": 3,
            "gestora_web": 4,
            "entrevista": 5,
            "general_search": 6,
        }
        sorted_texts = sorted(
            self._collected_texts,
            key=lambda x: priority_map.get(x["source"], 99),
        )

        for item in sorted_texts:
            text = item["text"]
            available = max_total_chars - chars_used
            if available <= 200:
                break
            truncated = text[:available]
            combined_parts.append(
                f"\n--- Fuente: {item['source']} ({item['url'][:80]}) ---\n{truncated}"
            )
            chars_used += len(truncated)

        return "\n".join(combined_parts)

    def _extract_profile_with_claude(self, manager_name: str) -> dict:
        """
        Send collected texts to Claude for structured extraction.
        Returns the extracted profile dict.
        """
        prompt_text = self._build_extraction_prompt_text(manager_name)
        if not prompt_text or len(prompt_text) < 50:
            self._log("WARN", "Insufficient text collected for Claude extraction")
            return {}

        context = (
            f"Perfil del gestor '{manager_name}' del fondo {self.fund_name} ({self.isin}). "
            f"Gestora: {self.gestora}. "
            f"Extrae toda la informacion disponible sobre este gestor. "
            f"Para entrevistas_recientes, incluye solo las que mencionan explicitamente al gestor. "
            f"Para skin_in_the_game, busca menciones de patrimonio personal invertido, "
            f"compromisos ante notario, co-inversion, etc."
        )

        try:
            result = extract_structured_data(
                prompt_text,
                MANAGER_EXTRACTION_SCHEMA,
                context=context,
            )
            self._log("INFO", f"Claude extraction complete: {len(result)} fields")
            return result if isinstance(result, dict) else {}
        except Exception as exc:
            self._log("ERROR", f"Claude extraction failed: {exc}")
            return {}

    # ── Interview enrichment ─────────────────────────────────────────────────

    def _merge_interviews(self, profile: dict) -> dict:
        """
        Merge interview URLs found during search into profile.entrevistas_recientes.
        Avoids duplicates by URL.
        """
        existing = profile.get("entrevistas_recientes", [])
        existing_urls = {e.get("url", "") for e in existing if e.get("url")}

        for iv in self._interview_urls:
            url = iv.get("url", "")
            if url and url not in existing_urls:
                existing.append({
                    "fecha": "",
                    "medio": iv.get("medio", ""),
                    "url": url,
                    "tema": iv.get("titulo", ""),
                    "ideas_clave": [],
                })
                existing_urls.add(url)

        profile["entrevistas_recientes"] = existing[:15]  # Cap at 15
        return profile

    # ── Main run ─────────────────────────────────────────────────────────────

    async def run(self) -> dict:
        self._log("INFO", f"ManagerDeepAgent iniciando para {self.isin} -- {self.fund_name}")

        # Step 1: Discover manager names if not provided
        if not self.manager_names:
            self._log("INFO", "No manager names provided, discovering...")
            self.manager_names = await self._discover_manager_names()

        if not self.manager_names:
            self._log("WARN", "No managers found. Returning minimal profile.")
            profile = _empty_profile(self.isin)
            self._save_profile(profile)
            return profile

        # Use the first (primary) manager
        primary_manager = self.manager_names[0]
        self._log("INFO", f"Primary manager: {primary_manager}")

        # Step 2: Search all sources for the primary manager
        self._log("INFO", "Paso 1/5: Morningstar equipo")
        await self._search_morningstar(primary_manager)

        self._log("INFO", "Paso 2/5: Citywire profile")
        await self._search_citywire(primary_manager)

        self._log("INFO", "Paso 3/5: Trustnet profile")
        await self._search_trustnet(primary_manager)

        self._log("INFO", "Paso 4/5: Gestora website")
        await self._search_gestora_web(primary_manager)

        self._log("INFO", "Paso 5/5: Entrevistas y articulos")
        await self._search_interviews(primary_manager)

        # Additional general search if we have few results
        if len(self._collected_texts) < 3:
            self._log("INFO", "Pocos resultados, busqueda general adicional")
            await self._search_general(primary_manager)

        self._log(
            "INFO",
            f"Recopilados {len(self._collected_texts)} textos de "
            f"{len(set(t['source'] for t in self._collected_texts))} fuentes",
        )

        # Step 3: Extract structured profile via Claude
        if self._collected_texts:
            self._log("INFO", "Extrayendo perfil estructurado con Claude...")
            extracted = self._extract_profile_with_claude(primary_manager)
        else:
            self._log("WARN", "No texts collected, using empty profile")
            extracted = {}

        # Step 4: Build final profile
        profile = _empty_profile(self.isin, primary_manager)

        # Merge extracted data over the empty skeleton
        for key in MANAGER_EXTRACTION_SCHEMA:
            if key in extracted and extracted[key] is not None:
                profile[key] = extracted[key]

        # Ensure name is set
        if not profile.get("nombre") or profile["nombre"] == "null":
            profile["nombre"] = primary_manager

        # Set empresa from gestora if not extracted
        if not profile.get("empresa"):
            profile["empresa"] = self.gestora or None

        # Merge interview URLs
        profile = self._merge_interviews(profile)

        # Add metadata
        profile["isin"] = self.isin
        profile["generado"] = datetime.now().isoformat()
        profile["fuentes_consultadas"] = list(set(self._sources_consulted))
        profile["otros_gestores"] = self.manager_names[1:] if len(self.manager_names) > 1 else []

        # Step 5: Save
        self._save_profile(profile)

        fields_filled = sum(
            1 for k, v in profile.items()
            if v is not None and v != [] and v != {} and v != ""
            and k not in ("isin", "generado", "fuentes_consultadas", "otros_gestores")
        )
        self._log(
            "INFO",
            f"OK Manager profile: {profile.get('nombre', '?')} | "
            f"{fields_filled} campos con datos | "
            f"{len(profile.get('entrevistas_recientes', []))} entrevistas | "
            f"{len(profile.get('fuentes_consultadas', []))} fuentes",
        )

        return profile

    def _save_profile(self, profile: dict):
        """Save profile to manager_profile.json."""
        out_path = self.fund_dir / "manager_profile.json"
        out_path.write_text(
            json.dumps(profile, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._log("INFO", f"Guardado en {out_path}")


# -- CLI standalone ------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manager Deep Agent - Perfil profundo del gestor")
    parser.add_argument("--isin", required=True, help="ISIN del fondo")
    parser.add_argument("--fund-name", default="", help="Nombre del fondo")
    parser.add_argument("--gestora", default="", help="Nombre de la gestora")
    parser.add_argument(
        "--managers",
        default="",
        help="Nombres de gestores separados por ; (si se omite, se descubren automaticamente)",
    )
    args = parser.parse_args()

    manager_list = (
        [m.strip() for m in args.managers.split(";") if m.strip()]
        if args.managers
        else []
    )
    agent = ManagerDeepAgent(
        isin=args.isin,
        fund_name=args.fund_name,
        gestora=args.gestora,
        manager_names=manager_list,
    )
    result = asyncio.run(agent.run())
    print(f"\nManager: {result.get('nombre', '?')}")
    print(f"Cargo: {result.get('cargo', '?')}")
    print(f"Formacion: {len(result.get('formacion', []))} items")
    print(f"Historial: {len(result.get('historial_empleos', []))} empleos")
    print(f"Entrevistas: {len(result.get('entrevistas_recientes', []))} encontradas")
    print(f"Fuentes consultadas: {len(result.get('fuentes_consultadas', []))}")
