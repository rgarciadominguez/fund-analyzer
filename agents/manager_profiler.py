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
import unicodedata
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console

console = Console()


# ════════════════════════════════════════════════════════════════════════════
# Bloque 2 Fase I (2026-04-28): helpers para deduplicación + cross-fund + lead/co
# ════════════════════════════════════════════════════════════════════════════

def _normalize_name_key(nombre: str) -> str:
    """Normaliza un nombre para deduplicación: quita acentos + lowercase + collapse spaces.
    Ejemplo: 'Iván Martín Aránguez' → 'ivan martin aranguez'.
    """
    if not nombre:
        return ""
    # Normalize NFKD descompone acentos como combinaciones (a + ´), encode ascii ignora.
    s = unicodedata.normalize("NFKD", nombre)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _dedup_names(names: list[str]) -> list[str]:
    """Deduplicar lista de nombres normalizando acentos y casing.
    De cada grupo equivalente, mantener el formato MÁS LARGO (con acentos correctos).
    """
    by_key: dict[str, str] = {}
    for n in names:
        if not n or not n.strip():
            continue
        k = _normalize_name_key(n)
        if not k:
            continue
        # Si ya hay uno con esta key, quedarse con el más largo (más completo)
        if k in by_key:
            if len(n) > len(by_key[k]):
                by_key[k] = n.strip()
        else:
            by_key[k] = n.strip()
    return list(by_key.values())


def _validate_name_in_fund_sources(name: str, fund_dir: Path) -> bool:
    """Verifica que un nombre aparece en al menos UNA fuente local del fondo.
    Filtro contra cross-fund contamination (gestores de otros fondos coladas).

    Bonus Fix Fase J (2026-04-28): relajado para nombres específicos.
    - Nombre ≥3 tokens (ej. "Iván Martín Aránguez") = persona específica con
      muy bajo riesgo de cross-fund. Aceptar SIN exigir presencia local.
      (Evita falso negativo cuando el nombre completo no aparece literal en
      cnmv_data pero sí es el gestor real, ej. solo se cita por apellido.)
    - Nombre con 2 tokens o menos (apellido genérico): exigir presencia local.

    Busca en:
    - cnmv_data.json cualitativo (sec 9, 10)
    - letters_data.json (cartas)
    - intl_data.json / annual_report (INT)
    - cssf_data.json / amf_data.json / etc. (regulator output)
    """
    if not name:
        return False
    name_norm = _normalize_name_key(name)
    if not name_norm:
        return False

    # Bonus: nombres ≥3 tokens son específicos (nombre + 2 apellidos típico ES)
    # Probabilidad de cross-fund con esos = muy baja. Pasarlos directos.
    tokens = name_norm.split()
    if len(tokens) >= 3:
        return True

    # Para nombres más cortos (1-2 tokens): exigir aparición en fuentes locales
    if not fund_dir.exists():
        return False
    apellido = tokens[-1] if tokens else ""
    if len(apellido) < 4:
        return False

    candidate_files = [
        "cnmv_data.json", "letters_data.json", "intl_data.json",
        "cssf_data.json", "amf_data.json", "cbi_data.json", "bundesanzeiger_data.json",
        "pdf_cache.json",
    ]
    for fname in candidate_files:
        fpath = fund_dir / fname
        if not fpath.exists():
            continue
        try:
            text_normalized = _normalize_name_key(fpath.read_text(encoding="utf-8"))
            if apellido in text_normalized:
                return True
        except Exception:
            continue
    return False


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
        # G1 (2026-05-18): helper unificado, antes [-1] devolvía "Action F" inútil
        from tools.fund_name_utils import extract_fund_short
        fund_short = extract_fund_short(self.fund_name)
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
        """Inferir dominio gestora — GENÉRICO (Fix 2 Fase J+ 2026-04-28).

        Cascada:
        1. intl_discovery_data.json (INT)
        2. cnmv_data.json fuentes/urls (ES)
        3. letters_data.json URLs (cualquier fondo)
        4. LLM lookup desde nombre gestora (Gemini Flash, $0.001)
        """
        from urllib.parse import urlparse

        # Skip-list de dominios infraestructura (no son la gestora real)
        SKIP_DOMAINS = (
            "kneip", "universal-investment", "morningstar", "citywire", "trustnet",
            "bloomberg", "reuters", "ft.com", "google", "youtube", "linkedin",
            "facebook", "twitter", "wikipedia", "investing.com", "finect.com",
            "rankia.com", "cnmv.es", "cssf.lu", "amf-france", "fundsquare",
            "kii.allfunds", "allfunds.com", "es.investing", "tradingeconomics",
        )

        def _is_gestora_host(host: str) -> bool:
            return host and not any(s in host for s in SKIP_DOMAINS)

        # 1) INT discovery
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                for doc in disc.get("documents", []):
                    url = doc.get("url", "")
                    if url and "manual://" not in url:
                        host = urlparse(url).netloc.lower().replace("www.", "")
                        if _is_gestora_host(host):
                            return host
            except Exception:
                pass

        # 2) cnmv_data fuentes (ES)
        cnmv_path = self.fund_dir / "cnmv_data.json"
        if cnmv_path.exists():
            try:
                cd = json.loads(cnmv_path.read_text(encoding="utf-8"))
                # Buscar URLs en fuentes / cualitativo / cartas
                candidates = []
                fuentes = cd.get("fuentes", {}) or {}
                for k in ("urls_consultadas", "cartas_gestores", "informes_descargados"):
                    candidates.extend(fuentes.get(k, []) or [])
                for url in candidates:
                    if isinstance(url, str) and url.startswith("http"):
                        host = urlparse(url).netloc.lower().replace("www.", "")
                        if _is_gestora_host(host):
                            return host
            except Exception:
                pass

        # 3) letters_data URLs
        letters_path = self.fund_dir / "letters_data.json"
        if letters_path.exists():
            try:
                ld = json.loads(letters_path.read_text(encoding="utf-8"))
                for c in ld.get("cartas", []):
                    url = c.get("url_fuente", "") or c.get("url", "")
                    if url:
                        host = urlparse(url).netloc.lower().replace("www.", "")
                        if _is_gestora_host(host):
                            return host
            except Exception:
                pass

        # 4) LLM lookup como última opción (Gemini Flash, ~$0.001)
        if self.gestora:
            try:
                from tools.gemini_wrapper import extract_fast
                r = extract_fast(
                    text=self.gestora,
                    schema={"website": "str - dominio web oficial de la gestora (ej. magallanesvalue.com), null si no conoces"},
                    context=(
                        f"¿Cuál es el dominio web OFICIAL de la gestora '{self.gestora}'? "
                        f"Devuelve solo el dominio (sin https://, sin www., sin path). "
                        f"Si no lo conoces con certeza, devuelve null. NO inventes."
                    ),
                )
                if isinstance(r, dict):
                    web = r.get("website", "") or ""
                    web = web.lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
                    if web and "." in web and _is_gestora_host(web):
                        self._log("INFO", f"Gestora domain via LLM: {web}")
                        return web
            except Exception:
                pass

        return ""

    async def _explore_gestora_team_pages(self) -> list[dict]:
        """Fix 3 Fase J+ (2026-04-28): explora SISTEMÁTICAMENTE la web de la
        gestora intentando URLs típicas de páginas de equipo/about/filosofía.

        Genérico — funciona para CUALQUIER gestora que tenga web propia.
        Devuelve lista de "fuentes" compatible con _compile_profiles.

        Patrones probados (ES + EN):
        - /equipo, /equipo-gestor, /quienes-somos, /sobre-nosotros, /nosotros
        - /team, /our-team, /about, /about-us, /who-we-are
        - /investment-philosophy, /filosofia, /filosofia-inversion, /vision
        - /founders, /partners, /people, /management
        """
        domain = self._gestora_domain()
        if not domain:
            self._log("INFO", "Sin dominio gestora — skip explore")
            return []

        TYPICAL_PATHS = [
            # Equipo (ES)
            "equipo", "equipo-gestor", "nuestro-equipo", "quienes-somos",
            "sobre-nosotros", "nosotros", "fundadores", "socios",
            # Equipo (EN)
            "team", "our-team", "about", "about-us", "who-we-are",
            "founders", "partners", "people", "management", "leadership",
            # Filosofía
            "investment-philosophy", "filosofia", "filosofia-inversion",
            "investment-approach", "approach", "metodologia",
            # Versions con /es/ /en/ prefix
            "es/equipo", "en/team", "es/quienes-somos", "en/about",
        ]

        sources: list[dict] = []
        seen_urls: set[str] = set()
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            for path in TYPICAL_PATHS:
                for scheme in ("https", "http"):
                    url = f"{scheme}://{domain}/{path}"
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    try:
                        r = await client.get(url)
                        if r.status_code != 200:
                            continue
                        ct = (r.headers.get("content-type") or "").lower()
                        if "html" not in ct:
                            continue
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(r.text, "html.parser")
                        # Eliminar boilerplate
                        for tag in soup(["script", "style", "nav", "footer", "header",
                                         "aside", "form", "iframe"]):
                            tag.decompose()
                        text = soup.get_text(" ", strip=True)
                        if len(text) < 300:
                            continue
                        # Solo conservar páginas con contenido relevante
                        text_lower = text.lower()
                        relevant_kws = ("equipo", "team", "gestor", "manager", "fundad",
                                       "founder", "filosof", "philosophy", "inversi", "invest",
                                       "ceo", "cio", "director", "partner")
                        if not any(kw in text_lower for kw in relevant_kws):
                            continue
                        sources.append({
                            "url": url,
                            "texto": text[:8000],
                            "_source_type": "gestora_web_explore",
                        })
                        self._log("INFO", f"  Gestora explore [{path}]: {len(text)} chars OK")
                        break  # solo necesitamos 1 scheme por path
                    except Exception:
                        continue
        if sources:
            self._log("OK", f"Gestora explore: {len(sources)} páginas relevantes en {domain}")
        return sources

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
                # Fase J Fix (2026-04-28): SIEMPRE invocar Opus para
                # (a) enriquecer perfiles pobres (lo que ya hacía)
                # (b) identificar lead/co/confidence con conocimiento del mundo.
                # Coste extra ~$0.02 por fondo, eliminando la heurística frágil
                # _rank_lead_first.
                # Refactor L2 (2026-05-05): in cowork mode, defer this Anthropic
                # call to the manager-deep-cowork skill (its step 2 absorbs
                # _enrich_with_opus). Emit a "identify_lead_co" task carrying
                # the candidate names + URLs so the skill has the same context.
                equipo = result.get("equipo", [])
                if equipo:
                    from tools.api_mode import is_cowork_mode
                    if is_cowork_mode():
                        try:
                            from pathlib import Path as _Path
                            from tools.pending_manifest import append_manager_deep_task as _emit_md
                            fund_dir = _Path("data/funds") / self.isin
                            fund_dir.mkdir(parents=True, exist_ok=True)
                            candidate_names = [
                                (g.get("nombre") if isinstance(g, dict) else str(g))
                                for g in equipo
                                if (g.get("nombre") if isinstance(g, dict) else g)
                            ]
                            candidate_urls = result.get("fuentes_web") or []
                            _emit_md(
                                fund_dir, self.isin,
                                task_type="identify_lead_co",
                                fund_name=self.fund_name or "",
                                gestora=getattr(self, "gestora", "") or "",
                                candidate_names=candidate_names,
                                candidate_urls=candidate_urls,
                                context=(
                                    f"Identify the canonical lead and co manager of "
                                    f"{self.fund_name or self.isin}. Use the candidate "
                                    f"names below + URL evidence. Apply dedup acentual + "
                                    f"cross-fund check. If ambiguous, mark all as peers "
                                    f"with confidence=low (do NOT invent lead arbitrarily)."
                                ),
                            )
                            self._log("INFO",
                                "cowork mode: identify_lead_co task emitted to "
                                "pending_manager_deep.json (skill: manager-deep-cowork)")
                        except Exception as exc:
                            self._log("WARN", f"could not emit identify_lead_co task: {exc}")
                    else:
                        result = self._enrich_with_opus(result)
                return result
        except Exception as e:
            self._log("WARN", f"Compile failed: {e}")

        return {"equipo": [], "fuentes_web": []}

    def _enrich_with_opus(self, compiled: dict) -> dict:
        """Enriquece perfiles + identifica lead/co con Claude Opus.

        Fase J (2026-04-28): además del enriquecimiento histórico, pide:
        - lead: nombre canónico del LEAD/principal manager
        - co: nombre canónico del co-manager (o null)
        - confidence: high|medium|low|desconocido

        Esto reemplaza la heurística frágil _rank_lead_first (Fase I).
        Coste: ~$0.02 por call. 1 call por fondo.
        """
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
                f"Candidatos detectados por nuestro pipeline: {', '.join(nombres)}\n\n"
                f"TAREA 1 — Para cada gestor de la lista de candidatos, proporciona en español:\n"
                f"- Cargo exacto en el fondo\n"
                f"- Biografía profesional (educación, carrera, empresas anteriores)\n"
                f"- Año de incorporación a la gestora\n"
                f"- Otros fondos que gestiona\n"
                f"- Filosofía de inversión (si es conocida)\n"
                f"- Reconocimientos (FE Alpha Manager, Citywire, etc)\n\n"
                f"TAREA 2 — Identifica el LEAD y el CO actual del fondo {self.fund_name}:\n"
                f"- 'lead': nombre completo CANÓNICO (acentos correctos) del LEAD/principal manager\n"
                f"  actual del fondo. Si NO ESTÁS SEGURO de quién es el lead → null.\n"
                f"- 'co': nombre canónico del co-manager o cofundador relevante (1 nombre o null).\n"
                f"- 'confidence': 'high' | 'medium' | 'low' | 'desconocido'\n"
                f"  - high: conoces el lead con certeza (gestor público documentado)\n"
                f"  - medium: tienes evidencia razonable pero no 100% certeza\n"
                f"  - low: inferencia con poca evidencia\n"
                f"  - desconocido: no tienes información fiable de este fondo\n"
                f"- Si los candidatos detectados NO incluyen el lead real (cross-fund\n"
                f"  contamination), puedes proponer el lead correcto aunque no esté en la lista.\n\n"
                f"REGLAS CRÍTICAS:\n"
                f"- Solo datos que conozcas con certeza. Si no sabes algo, di null.\n"
                f"- 'desconocido' es respuesta válida y preferida frente a inventar.\n"
                f"- Devuelve nombres en formato oficial con acentos."
            )

            # Fase M (2026-05-04): downgrade Opus → Haiku para clasificación
            # de lead/co + biografías cortas. Tareas estructuradas (no reasoning
            # profundo) que Haiku 4.5 resuelve igual a 1/5 del coste.
            from tools.llm_models import HAIKU_HINTS, SONNET_FALLBACK
            _model_used = HAIKU_HINTS
            r = client.messages.create(
                model=_model_used,
                max_tokens=1500,  # +500 para tarea 2 + jerarquía
                temperature=0,  # K5 Fase K: determinismo entre runs
                messages=[{"role": "user", "content": prompt}],
            )
            # Cost-Opt Fase 2 (2026-05-03): instrumentar coste
            try:
                from tools.llm_logger import log_llm_response
                log_llm_response(r, agent="manager_profiler",
                                  isin=self.isin, model=_model_used,
                                  provider="anthropic")
            except Exception:
                pass
            opus_text = r.content[0].text
            self._log("INFO", f"Haiku enriquecimiento+lead ({r.usage.input_tokens}+"
                      f"{r.usage.output_tokens} tok)")

            # Merge: para cada gestor, si Opus da más info, actualizar.
            # Schema extendido — ahora también pide lead/co/confidence.
            from tools.gemini_wrapper import extract_fast
            enriched = extract_fast(
                text=opus_text,
                schema={
                    "equipo": [{
                        "nombre": "str", "cargo": "str",
                        "biografia": "str", "educacion": "str",
                        "anio_incorporacion": "int", "otros_fondos": "str",
                        "filosofia": "str", "reconocimientos": "str",
                    }],
                    "lead": "str - nombre canónico del lead manager o null si desconocido",
                    "co": "str - nombre canónico del co-manager o null",
                    "confidence": "str - high|medium|low|desconocido",
                },
                context="Estructura este texto sobre gestores de fondos en JSON. Incluye lead/co/confidence si están en el texto.",
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

                # Fase J (2026-04-28): propagar lead/co/confidence de Opus al
                # compiled dict, para que run() los use en lugar de la heurística.
                lead_opus = enriched.get("lead")
                co_opus = enriched.get("co")
                confidence_opus = (enriched.get("confidence") or "").lower().strip()

                # Fase M (2026-05-04): si Haiku duda (low/desconocido) Y hay >1
                # candidato a lead, escalar a Sonnet (no Opus). Solo en este
                # caso ambiguo. Mantiene determinismo + coste contenido.
                n_candidates = len([g for g in compiled.get("equipo", []) if g.get("nombre")])
                if (confidence_opus in {"low", "desconocido"}
                        and n_candidates > 1
                        and _model_used == HAIKU_HINTS):
                    self._log("INFO", f"Haiku confidence={confidence_opus} con {n_candidates} candidatos — escalation a Sonnet")
                    try:
                        r2 = client.messages.create(
                            model=SONNET_FALLBACK,
                            max_tokens=1500,
                            temperature=0,
                            messages=[{"role": "user", "content": prompt}],
                        )
                        try:
                            from tools.llm_logger import log_llm_response as _log
                            _log(r2, agent="manager_profiler_escalation",
                                 isin=self.isin, model=SONNET_FALLBACK,
                                 provider="anthropic")
                        except Exception:
                            pass
                        sonnet_text = r2.content[0].text
                        sonnet_enriched = extract_fast(
                            text=sonnet_text,
                            schema={
                                "lead": "str", "co": "str",
                                "confidence": "str - high|medium|low|desconocido",
                            },
                            context="Estructura este texto sobre gestores de fondos en JSON.",
                        )
                        if isinstance(sonnet_enriched, dict):
                            new_conf = (sonnet_enriched.get("confidence") or "").lower().strip()
                            # Aceptar Sonnet solo si mejora confidence
                            if new_conf in {"high", "medium"}:
                                lead_opus = sonnet_enriched.get("lead") or lead_opus
                                co_opus = sonnet_enriched.get("co") or co_opus
                                confidence_opus = new_conf
                                self._log("INFO", f"Sonnet escalation aceptada: conf={new_conf}")
                    except Exception as _e:
                        self._log("WARN", f"Sonnet escalation falló: {type(_e).__name__}")

                if lead_opus and lead_opus.lower() not in ("null", "desconocido", ""):
                    compiled["_lead_opus"] = lead_opus
                if co_opus and co_opus.lower() not in ("null", "desconocido", ""):
                    compiled["_co_opus"] = co_opus
                if confidence_opus:
                    compiled["_confidence_opus"] = confidence_opus
                self._log("INFO", f"Opus lead/co: lead={lead_opus!r} co={co_opus!r} conf={confidence_opus!r}")

        except Exception as e:
            self._log("INFO", f"Opus enrichment skipped: {type(e).__name__}")

        return compiled

    # ══════════════════════════════════════════════════════════════════════
    # RUN
    # ══════════════════════════════════════════════════════════════════════

    def _empty_profile(self, error_msg: str = "") -> dict:
        """P2 (2026-05-19): perfil mínimo válido cuando todo falla.
        bundle_exporter exige que manager_profile.json exista — esto garantiza
        que SIEMPRE haya un archivo escribible, aunque manager_profiler no
        encontrara gestores ni pudiera completar su pipeline.
        """
        return {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "generated": datetime.now().isoformat(),
            "equipo": [],
            "equipo_gestor": [],
            "equipo_roles": {},
            "fuentes_web": [],
            **({"_error": error_msg} if error_msg else {}),
        }

    async def run(self) -> dict:
        """Public entrypoint. P2 (2026-05-19): wrap en try/except para
        garantizar que SIEMPRE se escribe manager_profile.json incluso si
        algo internal crashea. bundle_exporter depende de ese archivo."""
        try:
            result = await self._run_inner()
            # Defensa extra: si _run_inner retornó algo raro, escribir vacío
            if not isinstance(result, dict):
                self._log("WARN", f"_run_inner returned non-dict ({type(result)}), saving empty profile")
                return self._save(self._empty_profile(error_msg="non_dict_return"))
            return result
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self._log("ERROR", f"run() crashed: {e}")
            # Asegurar archivo on-disk para que bundle_exporter no aborte
            profile = self._empty_profile(error_msg=str(e)[:500])
            profile["_traceback"] = tb[:2000]
            return self._save(profile)

    async def _run_inner(self) -> dict:
        """Lógica original del pipeline. Wrapped por run() arriba."""
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

        # ═════════════════════════════════════════════════════════════════
        # Bloque 2 Fase I + Fase J (2026-04-28): dedup → cross-fund → search
        # → Opus identifica lead/co con conocimiento del mundo (no heurística).
        # ═════════════════════════════════════════════════════════════════

        # 1.5a. Deduplicar acentos ("Aránguez" vs "Aranguez")
        names_dedup = _dedup_names(self.manager_names)
        if len(names_dedup) < len(self.manager_names):
            self._log("INFO", f"Dedup acentos: {len(self.manager_names)} → {len(names_dedup)}")

        # 1.5b. K9 Fase K (2026-04-29): pre-filter cross-fund ELIMINADO.
        # Opus en _enrich_with_opus tiene conocimiento del mundo y filtra
        # cross-fund correctamente. El pre-filter local generaba false negatives
        # con nombres legítimos (ej. "Álvaro Guzmán" 2 tokens no presente literal
        # en cnmv_data se rechazaba aunque sea el lead real).
        # Mantenemos `_dedup_names` para limpieza acentual.
        names_validated = names_dedup
        rejected_cross_fund: list[str] = []  # mantenido por backward-compat output
        self.manager_names = names_validated
        self._log("INFO", f"Candidatos pre-Opus (post-dedup, sin pre-filter): {self.manager_names}")

        # 2. Buscar perfiles web (sobre todos los candidatos)
        raw_profiles = await self._search_profiles(self.manager_names)

        # 2b. Fix 3 Fase J+ (2026-04-28): explorar SISTEMÁTICAMENTE web gestora
        # — fetch directo a /equipo, /team, /about, /filosofía (genérico).
        # Las páginas relevantes se incorporan como una "persona" virtual con
        # texto de la web gestora — el _compile_profiles las procesará para
        # extraer gestores, filosofía, etc.
        gestora_pages = await self._explore_gestora_team_pages()
        if gestora_pages:
            raw_profiles.append({
                "nombre": f"_GESTORA_WEB_{self.gestora}",
                "fuentes": gestora_pages,
                "_source_type": "gestora_web_systematic",
            })

        # 3. Citywire fund page
        citywire = await self._search_citywire_fund()

        # 4. Compilar + IDENTIFICAR LEAD/CO con Opus (Fase J)
        compiled = self._compile_profiles(raw_profiles, citywire)

        # 5. Resolver lead/co usando Opus output
        # K4 Fase K (2026-04-29): si Opus desconoce, NO inventar fallback "orden
        # detección" arbitrario. Usar nombres dedup tal cual sin asignar lead/co.
        # Analyst tratará todos como peers (perfiles iguales en peso).
        lead_opus = compiled.get("_lead_opus")
        co_opus = compiled.get("_co_opus")
        confidence_opus = compiled.get("_confidence_opus", "")

        equipo_roles: dict = {}
        if lead_opus and confidence_opus in ("high", "medium"):
            # Opus dio respuesta fiable → usar directamente
            final_names = [lead_opus]
            if co_opus:
                final_names.append(co_opus)
            equipo_roles[lead_opus] = {"is_lead": True, "_source": f"opus_{confidence_opus}"}
            if co_opus:
                equipo_roles[co_opus] = {"is_co": True, "_source": f"opus_{confidence_opus}"}
            self._log("OK", f"Lead/co via Opus (conf={confidence_opus}): lead={lead_opus!r} co={co_opus!r}")
        else:
            # Sin asignación lead/co — todos los nombres dedup son peers.
            # Limitar a 2 igual (filosofía usuario: 1-2 perfiles, no 8).
            final_names = list(self.manager_names[:2])
            for n in final_names:
                equipo_roles[n] = {"_source": "no_opus_validation", "is_peer": True}
            self._log("INFO",
                f"Opus desconoce lead (conf={confidence_opus!r}) → tratando {final_names} como peers (sin lead/co)")

        self.manager_names = final_names
        self._log("OK", f"Gestores finales (max 2): {self.manager_names}")

        # 6. Filtrar compiled.equipo a SOLO los finales (lead/co)
        # Si Opus identificó nombres canónicos (con acentos correctos) que NO
        # coincidan literalmente con los del compiled, hacemos match por apellido.
        equipo_dicts_full = compiled.get("equipo", []) or []
        final_dicts = []
        for fname in self.manager_names:
            fname_norm = _normalize_name_key(fname)
            fname_apellido = fname_norm.split()[-1] if " " in fname_norm else fname_norm
            matched = None
            for g in equipo_dicts_full:
                if not isinstance(g, dict):
                    continue
                g_norm = _normalize_name_key(g.get("nombre", ""))
                g_apellido = g_norm.split()[-1] if " " in g_norm else g_norm
                if g_norm == fname_norm or (fname_apellido and fname_apellido == g_apellido):
                    matched = dict(g)  # copy
                    matched["nombre"] = fname  # usar nombre canónico (con acentos)
                    break
            if not matched:
                # Lead canónico de Opus que no estaba en candidatos detectados:
                # crear entry mínimo (Opus enrichment debería tener bio aunque
                # no esté en compiled; en peor caso, solo nombre)
                matched = {"nombre": fname, "_source": "opus_canonical_only"}
            final_dicts.append(matched)

        # 7. Guardar (Fix A Fase J 2026-04-28: schema unificado)
        # `equipo_gestor` = lista plana de strings (canónico para analyst y manager_deep_agent)
        # `equipo` = lista de dicts con biografia/educacion/etc. (detalle para perfiles)
        # Ambos coexisten — distintos consumidores leen el que necesitan.

        # Limpiar metadata interna de compiled (no debe ir al JSON final)
        compiled_clean = {k: v for k, v in compiled.items()
                         if k not in ("_lead_opus", "_co_opus", "_confidence_opus", "equipo")}

        output = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "generated": datetime.now().isoformat(),
            **compiled_clean,
            "equipo": final_dicts,  # lista de dicts con bio/cargo/etc. (max 2)
            "equipo_gestor": list(self.manager_names),  # canónico plano (max 2)
            "equipo_roles": equipo_roles,
            "fuentes_web": [
                f["url"] for p in raw_profiles
                for f in p.get("fuentes", [])
            ] + ([citywire["url"]] if citywire else []),
        }
        # K10 Fase K: persistir metadata interna SOLO si tiene valor
        if confidence_opus:
            output["_opus_lead_confidence"] = confidence_opus
        if rejected_cross_fund:
            output["_rejected_cross_fund"] = rejected_cross_fund

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
