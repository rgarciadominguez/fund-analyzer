"""
Manager Profiler — identifica y perfila los gestores del fondo.

Estrategia de búsqueda inteligente (patrón validado manualmente):
  1. Nombres de gestores: del extractor (intl_data.json) o del AR directamente
  2. Web gestora /team/{slug}: perfil completo, educación, carrera
  3. Trustnet manager factsheet: track record, años, FE rating
  4. Citywire fund page por ISIN/nombre: gestores confirmados, AUM, mix

NO usa queries genéricas de Google. Busca en SITES ESPECÍFICOS con
nombres de PERSONAS concretas.

Output: data/funds/{ISIN}/manager_profile.json
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console

console = Console()


class ManagerProfiler:
    def __init__(self, isin: str, fund_name: str = "", gestora: str = "",
                 manager_names: list[str] | None = None):
        self.isin = isin.upper().strip()
        self.fund_name = fund_name
        self.gestora = gestora
        self.manager_names = manager_names or []
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, msg: str):
        safe = msg.encode("cp1252", errors="replace").decode("cp1252")
        print(f"[MANAGER] [{level}] {safe}", flush=True)

    # ══════════════════════════════════════════════════════════════════════
    # PASO 1: Obtener nombres de gestores
    # ══════════════════════════════════════════════════════════════════════

    def _load_names_from_intl_data(self) -> list[str]:
        """Leer nombres del extractor v3 (intl_data.json)."""
        p = self.fund_dir / "intl_data.json"
        if not p.exists():
            return []
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            return [
                g["nombre"] for g in (d.get("cualitativo") or {}).get("gestores", [])
                if isinstance(g, dict) and g.get("nombre")
            ]
        except Exception:
            return []

    def _extract_names_from_ar(self) -> list[str]:
        """Fallback: leer primeras páginas de CUALQUIER PDF grande del fondo."""
        # Buscar todos los PDFs en el directorio de discovery (no solo el
        # registrado como AR — discovery a veces registra el AR equivocado)
        disc_dir = self.fund_dir / "raw" / "discovery"
        if not disc_dir.exists():
            return []

        # Buscar en TODOS los PDFs (factsheets suelen tener nombre gestor
        # en la última página; ARs a veces no lo mencionan explícitamente).
        # Priorizar: factsheets + KID (cortos, info condensada) → AR
        candidates = sorted(
            disc_dir.glob("*.pdf"),
            key=lambda p: (
                -int("fact-sheet" in p.name.lower() or "factsheet" in p.name.lower()),
                -int("kid" in p.name.lower() or "kiid" in p.name.lower()),
                -int("annual" in p.name.lower() and "ireland" in p.name.lower()),
                p.stat().st_size,  # más pequeños primero (factsheets)
            ),
        )
        if not candidates:
            return []

        try:
            import pdfplumber
            # Leer texto de los 5 primeros candidatos (factsheets primero)
            text = ""
            for pdf_path in candidates[:5]:
                try:
                    with pdfplumber.open(str(pdf_path)) as pdf:
                        for pg in pdf.pages[:5]:
                            text += (pg.extract_text() or "") + "\n"
                except Exception:
                    continue
            from tools.gemini_wrapper import extract_fast
            result = extract_fast(
                text=text[:30000],
                schema={"gestores": [{"nombre": "str - nombre y apellido de la PERSONA", "cargo": "str - su rol (CIO, Fund Manager, Co-Manager, etc.)"}]},
                context=(
                    f"Fondo {self.fund_name} ({self.isin}), gestionado por {self.gestora}. "
                    f"Extrae los nombres de las PERSONAS individuales que gestionan este sub-fondo. "
                    f"NO devuelvas el nombre de la empresa gestora — solo personas fisicas con nombre y apellido. "
                    f"Busca en: 'Investment Manager's Report', firmas, 'managed by', 'co-manager', 'lead manager', "
                    f"'fund manager', 'CIO'. Los nombres suelen aparecer en las primeras paginas o al final de la carta del gestor."
                ),
            )
            if isinstance(result, dict):
                return [g["nombre"] for g in result.get("gestores", [])
                        if isinstance(g, dict) and g.get("nombre")]
        except Exception as e:
            self._log("WARN", f"AR name extraction failed: {e}")
        return []

    # ══════════════════════════════════════════════════════════════════════
    # PASO 2: Buscar perfiles en web
    # ══════════════════════════════════════════════════════════════════════

    async def _find_managers_from_web(self) -> list[str]:
        """Buscar nombres de gestores en webs especializadas cuando los PDFs
        no los mencionan (comun en SICAVs umbrella grandes)."""
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)

        # Queries inteligentes: buscar la pagina del fondo en distribuidores
        # y plataformas que SIEMPRE listan los portfolio managers
        fund_short = self.fund_name.split(" - ")[-1] if " - " in self.fund_name else self.fund_name
        queries = [
            f'"{fund_short}" "portfolio manager" OR "fund manager" site:im.natixis.com OR site:morningstar.co.uk OR site:citywire.com',
            f'"{self.fund_name}" fund manager name',
            f'"{self.isin}" portfolio manager',
        ]
        results = await search.search_multiple(queries, num_per_query=3, agent="manager_profiler")

        for r in results[:6]:
            url = r.get("url", "")
            if not url:
                continue
            text = await self._fetch_and_extract(url, fund_short.split()[0])
            if not text:
                continue
            try:
                from tools.gemini_wrapper import extract_fast
                res = extract_fast(
                    text=text[:8000],
                    schema={"gestores": [{"nombre": "str - nombre y apellido de PERSONA, no empresa"}]},
                    context=f"Extrae los nombres de las PERSONAS que gestionan el fondo {self.fund_name}. Solo personas fisicas.",
                )
                if isinstance(res, dict):
                    names = [g["nombre"] for g in res.get("gestores", [])
                             if isinstance(g, dict) and g.get("nombre")
                             and len(g["nombre"].split()) >= 2]  # al menos nombre + apellido
                    if names:
                        self._log("OK", f"Gestores encontrados via web: {names}")
                        return names
            except Exception:
                continue
        return []

    async def _search_profiles(self, names: list[str]) -> list[dict]:
        """Busca perfiles usando queries ESPECÍFICAS por persona y site."""
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)

        profiles: list[dict] = []
        for name in names[:3]:  # máx 3 gestores
            self._log("INFO", f"Buscando perfil: {name}")
            profile = {"nombre": name, "fuentes": []}

            queries = [
                # Web gestora /team
                f'site:{self._gestora_domain()} "{name}"' if self._gestora_domain() else None,
                # Trustnet manager factsheet
                f'site:trustnet.com "{name}" manager factsheet',
                # Citywire
                f'site:citywire.com "{name}"',
                # General con cargo
                f'"{name}" fund manager profile biography',
            ]
            queries = [q for q in queries if q]

            results = await search.search_multiple(queries, num_per_query=3, agent="manager_profiler")

            # Fetch las mejores URLs
            for r in results[:8]:
                url = r.get("url", "")
                if not url or any(d in url for d in ["google.com", "bing.com", "duckduckgo.com"]):
                    continue
                text = await self._fetch_and_extract(url, name)
                if text:
                    profile["fuentes"].append({"url": url, "texto": text[:6000]})
                    self._log("INFO", f"  fetched {len(text)} chars from {url[:50]}")

            profiles.append(profile)

        return profiles

    def _gestora_domain(self) -> str:
        """Inferir dominio gestora del discovery."""
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                for doc in disc.get("documents", []):
                    url = doc.get("url", "")
                    if url and "manual://" not in url:
                        from urllib.parse import urlparse
                        host = urlparse(url).netloc.lower()
                        if host and "kneip" not in host and "universal-investment" not in host:
                            return host
            except Exception:
                pass
        return ""

    async def _fetch_and_extract(self, url: str, name: str) -> str:
        """Fetch URL y extraer info relevante del gestor."""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html",
            }
            async with httpx.AsyncClient(follow_redirects=True, timeout=15) as c:
                r = await c.get(url, headers=headers)
                if r.status_code != 200:
                    return ""
                ct = (r.headers.get("content-type") or "").lower()
                if "html" not in ct:
                    return ""
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(r.text, "html.parser")
                text = soup.get_text(" ", strip=True)[:8000]
                # Filtrar: solo si menciona el nombre del gestor
                if name.split()[-1].lower() not in text.lower():
                    return ""
                return text
        except Exception:
            return ""

    # ══════════════════════════════════════════════════════════════════════
    # PASO 3: Buscar en Citywire fund page (datos del fondo + gestores)
    # ══════════════════════════════════════════════════════════════════════

    async def _search_citywire_fund(self) -> dict | None:
        """Busca la página del fondo en Citywire por ISIN o nombre."""
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)
        queries = [
            f'site:citywire.com "{self.fund_name}"',
            f'site:citywire.com "{self.isin}"',
        ]
        results = await search.search_multiple(queries, num_per_query=3, agent="manager_profiler")
        for r in results:
            url = r.get("url", "")
            if "citywire.com" in url and "/fund/" in url:
                text = await self._fetch_and_extract(url, self.fund_name.split()[0])
                if text:
                    return {"url": url, "texto": text[:5000]}
        return None

    # ══════════════════════════════════════════════════════════════════════
    # PASO 4: Compilar perfil con LLM
    # ══════════════════════════════════════════════════════════════════════

    def _compile_profiles(self, raw_profiles: list[dict], citywire: dict | None) -> dict:
        """Usa Gemini Flash para compilar perfil estructurado."""
        all_text = ""
        for p in raw_profiles:
            all_text += f"\n=== PERSONA: {p['nombre']} ===\n"
            for f in p.get("fuentes", []):
                all_text += f"[Fuente: {f['url'][:80]}]\n{f['texto'][:5000]}\n\n"

        if citywire:
            all_text += f"\n=== CITYWIRE FUND PAGE ===\n{citywire['texto'][:5000]}\n"

        if not all_text.strip():
            return {"equipo": [], "fuentes_web": []}

        self._log("INFO", f"Compilando perfiles desde {len(all_text)} chars de texto web")

        try:
            from tools.gemini_wrapper import extract_fast
            result = extract_fast(
                text=all_text[:20000],
                schema={
                    "equipo": [{
                        "nombre": "str - nombre completo",
                        "cargo": "str - cargo en el fondo (ej. CIO, Co-Manager, Fund Manager)",
                        "biografia": "str - trayectoria completa en espanol (educacion, carrera, anos experiencia)",
                        "educacion": "str - titulos y certificaciones",
                        "anio_incorporacion": "int - ano que empezo a gestionar este fondo",
                        "otros_fondos": "str - otros fondos que gestiona",
                        "filosofia": "str - citas o resumen de su filosofia de inversion",
                        "reconocimientos": "str - premios, ratings (FE Alpha Manager, Citywire Elite, etc.)",
                    }],
                    "datos_fondo_citywire": {
                        "aum": "str", "fee": "str", "mix_activos": "str",
                    },
                },
                context=(
                    f"Compila perfiles de los gestores del fondo {self.fund_name} ({self.isin}). "
                    f"Gestora: {self.gestora}. "
                    f"Para CADA persona, extrae del texto web: nombre completo, cargo exacto, "
                    f"educacion (universidad, titulos, certificaciones como CFA/ASIP), "
                    f"trayectoria profesional completa (empresas anteriores con anos), "
                    f"ano que empezo en esta gestora, otros fondos que gestiona, "
                    f"citas o resumen de su filosofia de inversion, "
                    f"premios/ratings (FE Alpha Manager, Citywire Elite, etc). "
                    f"Todo en ESPANOL. Solo datos que aparezcan en el texto, no inventar. "
                    f"Si un campo no aparece en las fuentes, pon null."
                ),
            )
            if isinstance(result, dict):
                # Enriquecer con Opus si los perfiles son pobres
                equipo = result.get("equipo", [])
                needs_enrichment = any(
                    not g.get("biografia") or len(g.get("biografia", "") or "") < 50
                    for g in equipo if isinstance(g, dict)
                )
                if equipo and needs_enrichment:
                    result = self._enrich_with_opus(result)
                return result
        except Exception as e:
            self._log("WARN", f"Compile failed: {e}")

        return {"equipo": [], "fuentes_web": []}

    def _enrich_with_opus(self, compiled: dict) -> dict:
        """Enriquece perfiles pobres con Claude Opus (conocimiento financiero).
        Solo se llama si Gemini produjo perfiles con biografías <50 chars.
        Coste: ~$0.03 por call. 1 call por fondo, no por gestor."""
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

            nombres = [g["nombre"] for g in compiled.get("equipo", [])
                       if isinstance(g, dict) and g.get("nombre")]
            if not nombres:
                return compiled

            prompt = (
                f"Fondo: {self.fund_name} ({self.isin}), Gestora: {self.gestora}\n"
                f"Gestores: {', '.join(nombres)}\n\n"
                f"Para cada gestor, proporciona en español:\n"
                f"- Cargo exacto en el fondo\n"
                f"- Biografía profesional (educación, carrera, empresas anteriores)\n"
                f"- Año de incorporación a la gestora\n"
                f"- Otros fondos que gestiona\n"
                f"- Filosofía de inversión (si es conocida)\n"
                f"- Reconocimientos (FE Alpha Manager, Citywire, etc)\n\n"
                f"Solo datos que conozcas con certeza. Si no sabes algo, di null."
            )

            r = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}],
            )
            opus_text = r.content[0].text
            self._log("INFO", f"Opus enriquecimiento ({r.usage.input_tokens}+"
                      f"{r.usage.output_tokens} tok)")

            # Merge: para cada gestor, si Opus da más info, actualizar
            from tools.gemini_wrapper import extract_fast
            enriched = extract_fast(
                text=opus_text,
                schema={"equipo": [{
                    "nombre": "str", "cargo": "str",
                    "biografia": "str", "educacion": "str",
                    "anio_incorporacion": "int", "otros_fondos": "str",
                    "filosofia": "str", "reconocimientos": "str",
                }]},
                context="Estructura este texto sobre gestores de fondos en JSON.",
            )
            if isinstance(enriched, dict):
                opus_list = [g for g in enriched.get("equipo", [])
                             if isinstance(g, dict) and g.get("nombre")]

                for g in compiled.get("equipo", []):
                    name = (g.get("nombre") or "").lower()
                    # Match por nombre exacto, o por apellido (ultimo token)
                    apellido = name.split()[-1] if name else ""
                    opus_g = None
                    for og in opus_list:
                        og_name = og["nombre"].lower()
                        if og_name == name or apellido in og_name:
                            opus_g = og
                            break
                    if not opus_g:
                        continue
                    # Solo actualizar campos vacíos o pobres
                    for k in ("biografia", "educacion", "filosofia",
                              "reconocimientos", "otros_fondos"):
                        existing = g.get(k) or ""
                        opus_val = opus_g.get(k) or ""
                        if len(opus_val) > len(existing) + 20:
                            g[k] = opus_val
                    if not g.get("anio_incorporacion") and opus_g.get("anio_incorporacion"):
                        g["anio_incorporacion"] = opus_g["anio_incorporacion"]
                    if not g.get("cargo") and opus_g.get("cargo"):
                        g["cargo"] = opus_g["cargo"]

        except Exception as e:
            self._log("INFO", f"Opus enrichment skipped: {type(e).__name__}")

        return compiled

    # ══════════════════════════════════════════════════════════════════════
    # RUN
    # ══════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"ManagerProfiler {self.isin} — {self.fund_name}")

        # 1. Obtener nombres
        if not self.manager_names:
            self.manager_names = self._load_names_from_intl_data()
        if not self.manager_names:
            self._log("INFO", "Extrayendo nombres del AR directamente...")
            self.manager_names = self._extract_names_from_ar()
        if not self.manager_names:
            self._log("INFO", "Sin nombres en PDFs. Buscando en web (Citywire, Natixis, Morningstar)...")
            self.manager_names = await self._find_managers_from_web()

        if not self.manager_names:
            self._log("WARN", "Ultimo intento: Citywire fund page...")
            cw = await self._search_citywire_fund()
            if cw:
                try:
                    from tools.gemini_wrapper import extract_fast
                    r = extract_fast(
                        text=cw["texto"][:5000],
                        schema={"gestores": [{"nombre": "str"}]},
                        context="Extrae nombres de los gestores del fondo",
                    )
                    if isinstance(r, dict):
                        self.manager_names = [g["nombre"] for g in r.get("gestores", []) if g.get("nombre")]
                except Exception:
                    pass

        if not self.manager_names:
            self._log("ERROR", "No se encontraron gestores en ninguna fuente")
            return self._save({"error": "gestores no encontrados", "isin": self.isin})

        self._log("OK", f"Gestores: {self.manager_names}")

        # 2. Buscar perfiles web
        raw_profiles = await self._search_profiles(self.manager_names)

        # 3. Citywire fund page
        citywire = await self._search_citywire_fund()

        # 4. Compilar con LLM
        compiled = self._compile_profiles(raw_profiles, citywire)

        # 5. Guardar
        output = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "generated": datetime.now().isoformat(),
            **compiled,
            "fuentes_web": [
                f["url"] for p in raw_profiles
                for f in p.get("fuentes", [])
            ] + ([citywire["url"]] if citywire else []),
        }

        n_equipo = len(output.get("equipo", []))
        n_fuentes = len(output.get("fuentes_web", []))
        self._log("OK", f"Perfilados: {n_equipo} gestores de {n_fuentes} fuentes")

        return self._save(output)

    def _save(self, data: dict) -> dict:
        path = self.fund_dir / "manager_profile.json"
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return data
