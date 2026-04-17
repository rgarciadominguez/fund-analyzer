"""
Manager Deep Agent — Recopilador de información del equipo gestor

NO resume ni sintetiza — eso es tarea del analyst_agent.
Recopila MÁXIMA información relevante, ordenada cronológicamente, sin duplicados.
Valida que TODA la info se refiere al equipo gestor correcto.

Pipeline:
  1. Identificar equipo gestor (web gestora, snippets Google, CNMV, cartas)
  2. Búsquedas Google por cada gestor (Citywire, Trustnet, medios, podcasts...)
  3. Filtrar URLs buenas (descartar basura)
  4. Fetch contenido + extraer de cartas/CNMV
  5. Validar que todo se refiere al gestor correcto
  6. Gemini estructura (fallback a raw)
  7. Guardar manager_profile.json

Se ejecuta DESPUÉS de cnmv_agent y letters_agent.
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
from tools.google_search import SearchEngine, fetch_page_text

console = Console(highlight=False, force_terminal=False)


class ManagerDeepAgent:

    def __init__(self, isin: str, fund_name: str = "", gestora: str = "",
                 manager_names: list[str] | None = None):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name
        self.fund_short = fund_name.split(",")[0].strip() if fund_name else ""
        self.gestora = gestora
        self.manager_names = manager_names or []
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = root / "progress.log"
        self.search = SearchEngine(isin=self.isin)

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [MANAGER] [{level}] {msg}"
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _get_gemini_client(self):
        """Get or create Gemini client (new google-genai SDK)."""
        if not hasattr(self, '_gemini_client'):
            from google import genai
            self._gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))
        return self._gemini_client

    # ═══════════════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"Manager Deep Agent — {self.isin} ({self.fund_short})")

        # ── Paso 1: Identificar equipo gestor ────────────────────────────────
        if not self.manager_names:
            self.manager_names = await self._discover_team()
        if not self.manager_names:
            self._log("WARN", "No se encontró nombre de gestor")
            return self._save({"error": "gestor no encontrado", "isin": self.isin})

        self._log("OK", f"Equipo gestor identificado: {self.manager_names}")

        # ── Paso 1b: Si hay página de equipo, extraer TODOS los roles ────────
        team_detail = await self._extract_team_from_web()
        if team_detail:
            # Add any new names found in team page
            for person in team_detail:
                name = person.get("nombre", "")
                if name and name not in self.manager_names and "Equipo" not in name:
                    self.manager_names.append(name)
            self._log("INFO", f"Equipo ampliado desde web: {len(self.manager_names)} personas")

        # ── Paso 2-4: Discovery Agent (búsqueda + fetch unificado) ──────────
        # Reemplaza las búsquedas individuales + fetch_page_text silencioso
        # con el DiscoveryAgent que usa web_fetcher con escalada + caché.
        from agents.discovery_agent import DiscoveryAgent
        discovery = DiscoveryAgent(
            isin=self.isin,
            fund_name=self.fund_name,
            gestora=self.gestora,
            manager_names=self.manager_names,
        )
        discovery_sources = await discovery.find_manager_info()
        self._log("OK", f"Discovery: {len(discovery_sources)} fuentes con contenido")

        # Convert FetchedSource → format expected by downstream steps
        fetched_pages = []
        filtered = []
        for src in discovery_sources:
            fetched_pages.append({
                "url": src.url,
                "title": src.titulo,
                "snippet": src.metadata.get("snippet", ""),
                "text": src.text,
            })
            filtered.append({
                "title": src.titulo,
                "url": src.url,
                "snippet": src.metadata.get("snippet", ""),
            })
        self._log("INFO", f"Páginas descargadas: {len(fetched_pages)}")

        # Extraer de cartas y CNMV
        letters_info = self._extract_from_letters()
        cnmv_info = self._extract_from_cnmv()

        # ── Paso 5: Validar relevancia ───────────────────────────────────────
        validated_pages = self._validate_content(fetched_pages)
        self._log("INFO", f"Páginas validadas: {len(validated_pages)} (de {len(fetched_pages)})")

        # ── Paso 6: Extraer info de cada página con Gemini (1 llamada/pág) ──
        extracted_info = []
        names_str = ", ".join(n for n in self.manager_names if not n.startswith("Equipo"))
        for page in validated_pages:
            info = await self._extract_page_info(page, names_str)
            if info:
                extracted_info.append(info)
        self._log("INFO", f"Info extraída con Gemini: {len(extracted_info)} páginas")

        # ── Paso 7: Output — TODO lo recopilado ─────────────────────────────
        profile = {
            "equipo_gestor": self.manager_names,
            "equipo_detalle_web": team_detail or [],
            "info_extraida_por_fuente": extracted_info,
            "fuentes_web_raw": [
                {"url": p["url"], "title": p["title"], "text": p["text"]}
                for p in validated_pages
            ],
            "informacion_cartas": letters_info,
            "informacion_cnmv": cnmv_info,
        }
        profile["isin"] = self.isin
        profile["fondo"] = self.fund_name
        profile["generado"] = datetime.now().isoformat()
        profile["fuentes_consultadas"] = [p["url"] for p in validated_pages]
        profile["recursos_encontrados"] = [
            {"titulo": r["title"], "url": r["url"], "snippet": r.get("snippet", "")}
            for r in filtered[:25]
        ]

        return self._save(profile)

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 1: IDENTIFICAR EQUIPO GESTOR
    # ═══════════════════════════════════════════════════════════════════════════

    async def _discover_team(self) -> list[str]:
        """Discover manager names from multiple sources."""
        names: list[str] = []

        # Source 1: Google snippets — most reliable for name discovery
        discovery_queries = [
            f'"{self.fund_short}" gestor' if self.fund_short else f'"{self.isin}" gestor',
            f'"{self.fund_short}" morningstar gestor' if self.fund_short else f'"{self.isin}" morningstar',
            f'"{self.fund_short}" equipo director inversiones',
            f'{self.isin} gestor equipo',
        ]
        if self.gestora:
            discovery_queries.append(f'"{self.gestora}" equipo gestor')
            discovery_queries.append(f'"{self.gestora}" asset management equipo')

        all_snippets = []
        for query in discovery_queries:
            results = await self.search.search(query, num=5, agent="manager_discover")
            for r in results:
                combined = r.get("title", "") + " " + r.get("snippet", "")
                all_snippets.append(combined)
                found = self._extract_person_names(combined)
                for n in found:
                    if n not in names:
                        names.append(n)

        # Source 1b: If regex didn't find names, ask Gemini to extract from snippets
        if not names and all_snippets:
            gemini_names = await self._gemini_extract_names(all_snippets)
            for n in gemini_names:
                if n not in names:
                    names.append(n)

        # Source 2: cnmv_data.json
        cnmv_path = self.fund_dir / "cnmv_data.json"
        if cnmv_path.exists():
            try:
                data = json.loads(cnmv_path.read_text(encoding="utf-8"))
                for g in data.get("cualitativo", {}).get("gestores", []):
                    if isinstance(g, dict) and g.get("nombre") and g["nombre"] not in names:
                        names.append(g["nombre"])
            except Exception:
                pass

        # Source 3: letters_data.json URLs (slugs like juan-gomez-bada)
        letters_path = self.fund_dir / "letters_data.json"
        if letters_path.exists():
            try:
                data = json.loads(letters_path.read_text(encoding="utf-8"))
                for carta in data.get("cartas", []):
                    url = carta.get("url_fuente", "")
                    slug_names = self._names_from_url_slug(url)
                    for n in slug_names:
                        if n not in names:
                            names.append(n)
            except Exception:
                pass

        # Source 4: Web gestora — fetch and look for names
        if self.gestora and not names:
            web_results = await self.search.search(
                f'"{self.gestora}" equipo', num=3, agent="manager_discover"
            )
            for r in web_results[:2]:
                text = await fetch_page_text(r["url"], max_chars=3000)
                if text:
                    found = self._extract_person_names(text[:2000])
                    for n in found:
                        if n not in names:
                            names.append(n)

        # Source 5: If still no names (gestión colegiada) → search for gestora leadership
        if not names and self.gestora:
            self._log("INFO", "No se encontró gestor personal — buscando directivos de la gestora")
            leadership_queries = [
                f'"{self.gestora}" director inversiones CIO',
                f'"{self.gestora}" asset management equipo directivo',
            ]
            leadership_snippets = []
            for q in leadership_queries:
                results = await self.search.search(q, num=5, agent="manager_discover")
                for r in results:
                    leadership_snippets.append(r.get("title", "") + " " + r.get("snippet", ""))

            if leadership_snippets:
                # Use Gemini to extract leadership names from snippets
                gemini_names = await self._gemini_extract_names(leadership_snippets)
                for n in gemini_names:
                    if n not in names:
                        names.append(n)

            # If still nothing, use gestora name as team identifier
            if not names:
                names.append(f"Equipo {self.gestora}")
                self._log("INFO", f"Gestión colegiada: usando '{names[0]}' como equipo")

        return names

    def _extract_person_names(self, text: str) -> list[str]:
        """Extract person names from text — ONLY in explicit management context."""
        names = []

        # Only strict patterns: name must appear after a management role keyword
        strict_patterns = [
            r'[Gg]estor(?:a)?(?:\s+principal)?(?:\s*:|\s+D\.\s+|\s+d?e?\s*)([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            r'[Dd]irigid[oa]\s+por\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            r'[Dd]irector\s+de\s+[Ii]nversiones\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            r'[Aa]sesor(?:ado)?\s+por\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            r'[Rr]esponsable\s+(?:de\s+)?(?:gesti[oó]n|inversiones)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            r'[Cc]ogestor(?:a)?\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
            # "D. Nombre Apellido" after role context
            r'(?:[Gg]estor|[Dd]irector|[Aa]sesor|[Rr]esponsable)\s+D\.\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
        ]

        # Skip list: fund names, company names, generic terms, historical figures
        skip_words = {"fondo", "fund", "inversión", "capital", "gestora", "renta",
                      "morningstar", "clase", "patrimonio", "equity", "cartera",
                      "permanente", "flexible", "variable", "valor", "avantage",
                      "myinvestor", "pure", "plan", "pensiones", "sicav", "river",
                      "kronos", "obtiene", "convierte", "ajustes", "guia",
                      "harry", "browne", "graham", "buffett", "lynch", "bogle"}

        for pat in strict_patterns:
            for m in re.finditer(pat, text):
                name = m.group(1).strip()
                name_lower = name.lower()
                if any(w in name_lower for w in skip_words):
                    continue
                # Must be 2-4 words, each starting with uppercase
                parts = name.split()
                if 2 <= len(parts) <= 4 and all(p[0].isupper() for p in parts):
                    if name not in names:
                        names.append(name)

        # Email patterns: carlos.santiso@ → Carlos Santiso
        for first, last in re.findall(r'([a-z]{3,})\.([a-z]{3,})@', text.lower()):
            name = f"{first.capitalize()} {last.capitalize()}"
            if name not in names and not any(w in name.lower() for w in skip_words):
                names.append(name)

        return names

    def _names_from_url_slug(self, url: str) -> list[str]:
        """Extract person names from URL slugs — very conservative."""
        # Only extract if slug looks clearly like a person name (2-3 short words)
        # and the URL context suggests it's a person page
        names = []
        person_contexts = ["entrevista", "gestor", "manager", "equipo", "perfil", "biografia"]
        url_lower = url.lower()
        if not any(ctx in url_lower for ctx in person_contexts):
            return []  # URL doesn't look like a person page

        slugs = re.findall(r'/([a-z]+-[a-z]+-[a-z]+(?:-[a-z]+)?)', url_lower)
        skip_words = {"informe", "carta", "semestral", "trimestral", "mensual", "entrevista",
                       "estrategias", "inversion", "oportunidades", "credito", "repasando",
                       "cartera", "permanente", "myinvestor", "avantage", "fund", "fondo",
                       "river", "patrimonio", "kronos", "equity", "pure", "valor"}
        for slug in slugs:
            parts = slug.split("-")
            if 2 <= len(parts) <= 3 and not any(p in skip_words for p in parts):
                if all(2 < len(p) < 15 for p in parts):
                    name = " ".join(p.capitalize() for p in parts)
                    names.append(name)
        return names

    async def _extract_team_from_web(self) -> list[dict]:
        """Fetch gestora team page and extract all people with roles using Gemini.
        Strategy: Google search → navigate from gestora home → follow team/equipo links."""
        if not self.gestora:
            return []

        team_text = ""
        team_url = ""

        # Strategy 1: Google search for team page
        team_results = await self.search.search(
            f'"{self.gestora}" equipo', num=3, agent="manager_team"
        )
        for r in team_results[:3]:
            url = r.get("url", "")
            if any(kw in url.lower() for kw in ["equipo", "team", "about", "asset-management"]):
                text = await fetch_page_text(url, max_chars=8000)
                if text and len(text) > 500:
                    team_text = text
                    team_url = url
                    break

        # Strategy 2: Navigate from gestora home page — follow team/equipo links
        if not team_text:
            # Find gestora domain from search results or gestora name
            gestora_domains = set()
            for r in team_results:
                from urllib.parse import urlparse
                domain = urlparse(r.get("url", "")).netloc
                if domain and self.gestora.lower().split()[0] in domain.lower():
                    gestora_domains.add(domain)

            for domain in list(gestora_domains)[:2]:
                # Fetch home page
                home_url = f"https://{domain}"
                home_text_raw = ""
                try:
                    html = await get_with_headers(home_url, _HEADERS_WEB)
                    home_text_raw = html
                except Exception:
                    continue

                if not home_text_raw:
                    continue

                # Find internal links to team/equipo pages
                from bs4 import BeautifulSoup as BS
                soup = BS(home_text_raw, "html.parser")
                team_keywords = ["equipo", "team", "about", "quienes", "asset-management", "nosotros"]
                for a in soup.find_all("a", href=True):
                    href = a["href"].lower()
                    link_text = a.get_text(strip=True).lower()
                    if any(kw in href or kw in link_text for kw in team_keywords):
                        # Build full URL
                        full_url = a["href"]
                        if full_url.startswith("/"):
                            full_url = f"https://{domain}{full_url}"
                        elif not full_url.startswith("http"):
                            continue
                        text = await fetch_page_text(full_url, max_chars=8000)
                        if text and len(text) > 500:
                            team_text = text
                            team_url = full_url
                            self._log("INFO", f"Navegado hasta equipo: {full_url[:60]}")
                            break

                if team_text:
                    break

        if not team_text:
            return []

        self._log("INFO", f"Página equipo encontrada: {team_url[:60]} ({len(team_text)}c)")

        # Use Gemini to extract all people and their roles
        try:
            from google.genai import types
            client = self._get_gemini_client()

            prompt = (
                f"Lista TODAS las personas de esta pagina de equipo de {self.gestora}. "
                f"JSON array: [{{\"nombre\":\"\",\"cargo\":\"\",\"area\":\"inversiones|comercial|operaciones|direccion\"}}]\n\n"
                f"{team_text}"
            )

            resp = client.models.generate_content(
                model="gemini-2.5-flash", contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json", temperature=0.1, max_output_tokens=3000))
            raw = resp.text.strip() if resp.text else ""
            # Extract JSON array
            m = re.search(r'\[.*\]', raw, re.DOTALL)
            if m:
                result = json.loads(m.group())
                if isinstance(result, list):
                    self._log("OK", f"Equipo extraído: {len(result)} personas")
                    return result
        except Exception as exc:
            self._log("WARN", f"Error extrayendo equipo: {exc}")

        return []

    async def _gemini_extract_names(self, snippets: list[str]) -> list[str]:
        """Use Gemini to extract manager names from Google snippets when regex fails."""
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception:
            return []

        text = "\n".join(s[:200] for s in snippets[:15])
        prompt = (
            f"De los siguientes textos sobre el fondo {self.fund_short}, "
            f"extrae SOLO los nombres de PERSONAS que son gestores, directores de inversiones, "
            f"analistas o responsables de la gestion del fondo. "
            f"NO incluyas nombres de empresas, fondos, o personas historicas. "
            f"Responde SOLO con un JSON array de strings con los nombres.\n\n{text}"
        )

        for attempt in range(2):
            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash", contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json", temperature=0.1, max_output_tokens=300))
                raw = resp.text.strip() if resp.text else ""
                self._log("INFO", f"Gemini names raw: {raw[:200]}")
                # Try to extract JSON array from response
                m = re.search(r'\[.*\]', raw, re.DOTALL)
                if m:
                    result = json.loads(m.group())
                    if isinstance(result, list):
                        names = [n for n in result if isinstance(n, str) and len(n.split()) >= 2]
                        self._log("OK", f"Gemini extrajo nombres: {names}")
                        return names
            except Exception as exc:
                if "429" in str(exc) or "ResourceExhausted" in str(exc):
                    self._log("WARN", f"Gemini rate limit — espera 45s")
                    await asyncio.sleep(45)
                else:
                    self._log("WARN", f"Gemini name extraction error: {exc}")
                    break
        return []

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 2: BÚSQUEDAS POR GESTOR
    # ═══════════════════════════════════════════════════════════════════════════

    async def _search_for_manager(self, name: str) -> list[dict]:
        """Run targeted searches for a specific manager (ES + INT)."""
        is_int = not self.isin.startswith("ES")
        queries = [
            f'"{name}" citywire',
            f'"{name}" trustnet',
            f'"{name}" fundsociety',
            f'"{name}" "{self.fund_short}"' if self.fund_short else f'"{name}" {self.isin}',
        ]
        if is_int:
            # Queries internacionales multi-idioma
            queries.extend([
                f'"{name}" interview fund manager',
                f'"{name}" "fund manager" profile OR biography OR track record',
                f'"{name}" fundspeople',
                f'"{name}" institutionalinvestor.com',
                f'"{name}" youtube presentation OR conference OR webinar',
                f'"{name}" podcast investment',
                f'site:citywire.co.uk "{name}"',
                f'site:morningstar.co.uk "{name}"',
            ])
        else:
            # Queries ES
            queries.extend([
                f'"{name}" rankia',
                f'"{name}" entrevista',
                f'"{name}" podcast',
                f'"{name}" youtube conferencia',
                f'"{name}" El Confidencial OR Cinco Dias OR Expansion',
            ])
        return await self.search.search_multiple(queries, num_per_query=3, agent="manager_deep")

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 3: FILTRAR URLs
    # ═══════════════════════════════════════════════════════════════════════════

    def _filter_relevant(self, results: list[dict]) -> list[dict]:
        """Filter URLs: keep only those mentioning the manager or fund."""
        skip_domains = {"google.com", "bing.com", "duckduckgo.com", "linkedin.com",
                        "twitter.com", "x.com", "facebook.com", "instagram.com"}
        manager_terms = set()
        for name in self.manager_names:
            parts = name.lower().split()
            manager_terms.update(parts)
            if len(parts) >= 2:
                manager_terms.add(parts[-1])  # apellido

        fund_terms = {self.fund_short.lower(), self.isin.lower()}
        if self.gestora:
            fund_terms.add(self.gestora.lower().split()[0])  # first word of gestora

        filtered = []
        for r in results:
            url = r.get("url", "")
            # Skip blocked domains
            if any(d in url for d in skip_domains):
                continue
            # Check relevance: title or snippet must mention manager or fund
            combined = (r.get("title", "") + " " + r.get("snippet", "")).lower()
            has_manager = any(term in combined for term in manager_terms if len(term) > 2)
            has_fund = any(term in combined for term in fund_terms if len(term) > 2)
            if has_manager or has_fund:
                filtered.append(r)

        # Prioritize: citywire > trustnet > morningstar > fundspeople > rest
        priority_domains = ["citywire", "trustnet", "fundsociety", "morningstar",
                           "fundspeople", "institutionalinvestor", "ft.com",
                           "rankia", "finect", "substack"]

        def _priority(r):
            url = r.get("url", "").lower()
            for i, d in enumerate(priority_domains):
                if d in url:
                    return i
            return len(priority_domains)

        return sorted(filtered, key=_priority)

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 5: VALIDAR CONTENIDO
    # ═══════════════════════════════════════════════════════════════════════════

    def _validate_content(self, pages: list[dict]) -> list[dict]:
        """Validate that each page actually contains info about OUR manager/fund."""
        validated = []
        manager_terms = set()
        for name in self.manager_names:
            for part in name.lower().split():
                if len(part) > 2:
                    manager_terms.add(part)

        fund_terms = set()
        if self.fund_short:
            for part in self.fund_short.lower().split():
                if len(part) > 3:
                    fund_terms.add(part)

        for page in pages:
            text_lower = page["text"].lower()
            # Must mention at least one manager name component AND fund
            has_manager = any(term in text_lower for term in manager_terms)
            has_fund = any(term in text_lower for term in fund_terms) or self.isin.lower() in text_lower

            if has_manager or has_fund:
                validated.append(page)
            else:
                self._log("WARN", f"DESCARTADO (no relevante): {page['title'][:50]}")

        return validated

    # ═══════════════════════════════════════════════════════════════════════════
    # EXTRACT FROM LOCAL DATA
    # ═══════════════════════════════════════════════════════════════════════════

    def _extract_from_letters(self) -> list[dict]:
        """Extract manager-relevant info from letters_data.json."""
        path = self.fund_dir / "letters_data.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        info = []
        for carta in data.get("cartas", []):
            if not isinstance(carta, dict):
                continue
            entry = {"periodo": carta.get("periodo") or ""}
            for field in ["tesis_inversion", "resumen_mercado", "perspectivas",
                          "decisiones_cartera", "vision_macro", "resumen_ejecutivo"]:
                val = carta.get(field)
                if val:
                    entry[field] = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
            if len(entry) > 1:
                info.append(entry)
        return sorted(info, key=lambda x: x.get("periodo") or "")

    def _extract_from_cnmv(self) -> dict:
        """Extract manager-relevant info from cnmv_data.json."""
        path = self.fund_dir / "cnmv_data.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        cual = data.get("cualitativo", {})
        result = {}
        for field in ["seccion_1_politica_texto", "seccion_9_texto_completo",
                       "seccion_10_perspectivas_texto", "seccion_4_5_hechos_texto"]:
            if cual.get(field):
                result[field] = cual[field]
        if cual.get("hechos_relevantes"):
            result["hechos_relevantes"] = cual["hechos_relevantes"]
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PASO 6: EXTRAER INFO POR PÁGINA

    async def _extract_page_info(self, page: dict, manager_names: str) -> dict | None:
        """Use Gemini to extract relevant manager info from a single page."""
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception:
            # Fallback: return page as-is
            return {"fuente": page["url"], "titulo": page["title"], "texto_raw": page["text"][:3000]}

        prompt = (
            f"Extrae TODA la información relevante de este texto sobre el fondo {self.fund_short} "
            f"y su equipo gestor ({manager_names}). Incluye:\n"
            f"- Nombres y cargos de personas mencionadas\n"
            f"- Trayectoria profesional, formación\n"
            f"- Filosofía de inversión, estrategia, opiniones de mercado\n"
            f"- Decisiones de cartera, posicionamiento\n"
            f"- Datos del fondo: rentabilidad, AUM, premios\n"
            f"- Entrevistas, citas textuales\n"
            f"- Cualquier dato útil para un analista de fondos\n"
            f"Extrae todo lo que encuentres. Responde en JSON.\n\n"
            f"Fuente: {page['title']}\n"
            f"{page['text'][:4000]}"
        )

        try:
            resp = client.models.generate_content(
                model="gemini-2.5-flash", contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json", temperature=0.1, max_output_tokens=2000))
            raw = resp.text.strip() if resp.text else ""
            if not raw:
                return {"_fuente": page["url"], "_titulo": page["title"], "texto_raw": page["text"][:3000]}
            # Try to parse JSON — handle malformed responses
            # Remove markdown wrappers (may span multiple lines)
            clean = re.sub(r'^```(?:json)?\s*\n?', '', raw, flags=re.MULTILINE)
            clean = re.sub(r'\n?```\s*$', '', clean, flags=re.MULTILINE)
            clean = clean.strip()
            try:
                result = json.loads(clean)
            except json.JSONDecodeError:
                # Try to find first complete JSON object
                depth = 0
                start = clean.find('{')
                if start >= 0:
                    for i in range(start, len(clean)):
                        if clean[i] == '{': depth += 1
                        elif clean[i] == '}': depth -= 1
                        if depth == 0:
                            try:
                                result = json.loads(clean[start:i+1])
                                break
                            except json.JSONDecodeError:
                                result = None
                                break
                    else:
                        result = None
                else:
                    result = None
            if result and isinstance(result, dict) and len(result) > 0:
                result["_fuente"] = page["url"]
                result["_titulo"] = page["title"]
                return result
            # If JSON failed but we have text, save raw Gemini response
            return {"_fuente": page["url"], "_titulo": page["title"],
                    "gemini_text": raw[:2000], "texto_raw": page["text"][:2000]}
        except Exception as exc:
            self._log("WARN", f"Gemini extract error for {page['title'][:30]}: {exc}")
        return {"_fuente": page["url"], "_titulo": page["title"], "texto_raw": page["text"][:3000]}

    # ═══════════════════════════════════════════════════════════════════════════
    # GEMINI STRUCTURE (legacy — kept for compatibility)
    # ═══════════════════════════════════════════════════════════════════════════

    async def _structure_with_gemini(self, pages: list[dict],
                                      letters: list[dict], cnmv: dict) -> dict:
        """Use Gemini to structure collected info. Fallback to raw if fails."""
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception:
            self._log("WARN", "Gemini no disponible")
            return self._raw_profile(pages, letters, cnmv)

        # Build prompt from all sources
        parts = []
        for p in pages[:6]:
            parts.append(f"FUENTE: {p['title']}\nURL: {p['url']}\n{p['text'][:2500]}")
        if letters:
            lt = "\n".join(f"CARTA {l['periodo']}: " + " | ".join(
                f"{k}={str(v)[:150]}" for k, v in l.items() if k != "periodo"
            ) for l in letters[-4:])
            parts.append(f"CARTAS:\n{lt}")
        if cnmv.get("seccion_1_politica_texto"):
            parts.append(f"CNMV POLITICA:\n{cnmv['seccion_1_politica_texto'][:1000]}")

        context = "\n\n---\n\n".join(parts)
        names_str = ", ".join(self.manager_names)

        prompt = (
            f"Estructura en JSON el perfil del equipo gestor de {self.fund_short} ({self.isin}). "
            f"Gestores identificados: {names_str}. "
            f"CRITICO: Solo incluir informacion que se refiera a estos gestores y este fondo. "
            f"Si no estas seguro de un dato, ponlo como null. "
            f"Incluye TODA la info relevante de cada fuente.\n\n"
            f"{context[:9000]}"
        )

        for attempt in range(3):
            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash", contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json", temperature=0.1,
                        max_output_tokens=4000))
                result = json.loads(resp.text)
                self._log("OK", f"Gemini OK ({resp.usage_metadata.candidates_token_count} tokens)")
                return result
            except json.JSONDecodeError:
                self._log("WARN", f"Gemini JSON truncado (intento {attempt+1})")
                if hasattr(resp, "text"):
                    return self._raw_profile(pages, letters, cnmv, gemini_raw=resp.text)
            except Exception as exc:
                if "429" in str(exc) or "ResourceExhausted" in str(exc):
                    wait = 45 * (attempt + 1)
                    self._log("WARN", f"Rate limit — espera {wait}s")
                    await asyncio.sleep(wait)
                else:
                    self._log("WARN", f"Gemini error: {exc}")
                    break

        return self._raw_profile(pages, letters, cnmv)

    def _raw_profile(self, pages, letters, cnmv, gemini_raw="") -> dict:
        return {
            "nombre_completo": self.manager_names[0] if self.manager_names else "",
            "gemini_raw": gemini_raw[:3000],
            "fuentes_web": [{"url": p["url"], "title": p["title"],
                              "text": p["text"][:2000]} for p in pages],
            "informacion_cartas": letters,
            "informacion_cnmv": {k: v[:2000] if isinstance(v, str) else v
                                  for k, v in cnmv.items()},
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # SAVE
    # ═══════════════════════════════════════════════════════════════════════════

    def _save(self, profile: dict) -> dict:
        out = self.fund_dir / "manager_profile.json"
        out.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log("OK", f"Guardado: {out}")
        return profile


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="ES0112231008")
    parser.add_argument("--fund-name", default="Avantage Fund")
    parser.add_argument("--gestora", default="Avantage Capital")
    parser.add_argument("--manager", default="")
    args = parser.parse_args()

    agent = ManagerDeepAgent(
        args.isin, fund_name=args.fund_name, gestora=args.gestora,
        manager_names=[args.manager] if args.manager else None)
    result = asyncio.run(agent.run())
    print(f"\nEquipo: {result.get('equipo_gestor', [])}")
    print(f"Fuentes: {len(result.get('fuentes_consultadas', []))}")
    print(f"Recursos: {len(result.get('recursos_encontrados', []))}")
