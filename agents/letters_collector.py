"""
Letters Collector — extrae cartas/commentary de gestores desde PDFs en disco + web.

Principio: PRIMERO leer lo que discovery ya descargo. Web como segundo paso
inteligente para cubrir anos sin carta.

Pipeline:
  1. Leer intl_discovery_data.json -> docs con commentary (cartas, presentations,
     factsheets con commentary)
  2. Para cada PDF en disco: extraer texto con pdfplumber -> Gemini Flash
  3. Evaluar cobertura: objetivo = 1 carta/ano desde anio_creacion
  4. APRENDER del discovery: extraer nombres reales de documentos de la gestora
     (ej. "Investment Report No.87") y usarlos como plantilla para buscar antiguos
  5. Web search dirigido para anos sin carta: gestora + plataformas
  6. Guardar letters_data.json

Objetivo: 1 carta por ano como minimo. Si la gestora publica trimestrales, mejor.

Output: data/funds/{ISIN}/letters_data.json
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from rich.console import Console

console = Console()

# Schema para extraccion de cartas
LETTER_SCHEMA = {
    "cartas": [{
        "periodo": "str - formato OBLIGATORIO: YYYY (ej. 2024) o YYYY-QX (ej. 2024-Q4) o YYYY-SX (ej. 2024-S2). Usa el año del periodo cubierto, NO la fecha de publicacion",
        "contexto_mercado": "str - resumen del entorno macro/mercado en ese periodo",
        "tesis_gestora": "str - vision/tesis del gestor sobre posicionamiento",
        "decisiones_tomadas": "str - cambios en cartera: entradas, salidas, ajustes de peso",
        "resultado_real": "str - performance del fondo en el periodo, vs benchmark si se menciona",
        "outlook": "str - perspectivas y expectativas para el siguiente periodo",
        "citas_textuales": ["str - frases literales del gestor entre comillas"],
        "posiciones_mencionadas": ["str - nombres de activos/empresas mencionados"],
    }]
}


class LettersCollector:
    """Extrae cartas/commentary de gestores — PDF-first, web-search inteligente."""

    def __init__(self, isin: str, fund_name: str = "", gestora: str = "",
                 anio_creacion: int | None = None):
        self.isin = isin.upper().strip()
        self.fund_name = fund_name
        self.fund_short = fund_name.split(" - ")[-1] if " - " in fund_name else fund_name
        self.gestora = gestora
        self.anio_creacion = anio_creacion or 2018
        self.current_year = datetime.now().year
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, msg: str):
        safe = msg.encode("cp1252", errors="replace").decode("cp1252")
        print(f"[LETTERS] [{level}] {safe}", flush=True)

    @staticmethod
    def _retry_gemini(fn, *args, retries: int = 2, delay: float = 5.0, **kwargs):
        """Retry wrapper para llamadas a Gemini que fallan por conexión."""
        import time
        last_err = None
        for attempt in range(retries):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_err = e
                err_str = str(e).lower()
                # Solo reintentar errores de conexión, no errores de contenido
                if any(kw in err_str for kw in ["10053", "10054", "connect",
                                                  "timeout", "reset", "eof",
                                                  "504", "503", "429"]):
                    if attempt < retries - 1:
                        time.sleep(delay)
                        continue
                raise
        raise last_err  # type: ignore

    # ══════════════════════════════════════════════════════════════════════
    # PASO 1: Leer PDFs de discovery
    # ══════════════════════════════════════════════════════════════════════

    def _get_discovery_docs(self) -> list[dict]:
        """Leer documentos con commentary potencial del discovery."""
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if not disc_path.exists():
            return []
        try:
            disc = json.loads(disc_path.read_text(encoding="utf-8"))
        except Exception:
            return []

        relevant_types = {
            "quarterly_letter", "manager_presentation",
            "semi_annual_report", "annual_report", "factsheet",
        }
        docs = []
        for doc in disc.get("documents", []):
            dtype = doc.get("doc_type", "")
            if dtype not in relevant_types:
                continue
            # NO reprocesar PDFs que letters_collector ya descargó y procesó
            if doc.get("source") == "letters_collector_wayback":
                continue
            priority = {
                "quarterly_letter": 1,
                "manager_presentation": 2,
                "semi_annual_report": 3,
                "annual_report": 4,
                "factsheet": 5,
            }.get(dtype, 9)
            docs.append({**doc, "_priority": priority})

        docs.sort(key=lambda d: (d["_priority"], d.get("periodo", "") or "0000"))
        return docs

    def _extract_pdf_text(self, local_path: str, max_pages: int = 30) -> str:
        """Extraer texto de PDF con pdfplumber. Si es image-based, intenta Gemini."""
        p = Path(local_path)
        if not p.exists():
            p = self.fund_dir / "raw" / "discovery" / p.name
        if not p.exists():
            return ""
        try:
            import pdfplumber
            text = ""
            with pdfplumber.open(str(p)) as pdf:
                for page in pdf.pages[:max_pages]:
                    text += (page.extract_text() or "") + "\n"
            # Si pdfplumber extrajo muy poco texto, es un PDF de imagenes
            # Intentar con Gemini multimodal (puede leer PDFs como imagenes)
            if len(text.strip()) < 100 and p.stat().st_size > 50000:
                self._log("INFO", f"PDF imagen detectado: {p.name}, usando Gemini")
                text = self._extract_pdf_with_gemini(str(p), max_pages)
            return text
        except Exception as e:
            self._log("WARN", f"Error PDF {p.name}: {e}")
            return ""

    def _extract_pdf_with_gemini(self, pdf_path: str, max_pages: int = 15) -> str:
        """Extraer texto de PDF image-based usando Gemini multimodal."""
        def _do_ocr():
            import google.generativeai as genai
            import os
            genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))
            uploaded = genai.upload_file(pdf_path)
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(
                [uploaded, "Extract ALL text from this PDF document. "
                 "Return the full text content, preserving paragraphs. "
                 "If there are images with text, OCR them."],
                generation_config={"temperature": 0.0, "max_output_tokens": 30000},
            )
            return response.text or ""
        try:
            return self._retry_gemini(_do_ocr)
        except Exception as e:
            self._log("WARN", f"Gemini PDF extraction failed: {e}")
            return ""

    def _isolate_subfund_text(self, full_text: str) -> str:
        """Para ARs umbrella con multiples sub-fondos: aislar SOLO el texto
        que habla de ESTE sub-fondo.

        Generalizado: funciona con cualquier SICAV umbrella (DNCA, GAM, Allianz...)
        Dos estrategias:
        1. Buscar heading del sub-fondo y extraer hasta el siguiente heading
        2. Fallback: extraer ventana de ±5000 chars alrededor de cada mencion
        """
        if len(full_text) < 30000:
            return full_text  # No es umbrella

        fund_terms = [w.lower() for w in self.fund_short.split() if len(w) > 3]
        if not fund_terms:
            return full_text

        text_lower = full_text.lower()

        # Estrategia 1: buscar nombre del sub-fondo en el texto
        # Buscar todas las apariciones (TOC, Directors' Report, Financial Statements)
        # El fund_short es "ALPHA BONDS" -> buscar como substring
        fund_query = self.fund_short.lower()
        heading_matches = list(re.finditer(re.escape(fund_query), text_lower))

        if heading_matches:
            # Saltar las TOCs: el nombre del fondo aparece en:
            # 1. TOC general (p.1-3) ~0-12000 chars
            # 2. TOC financial statements (p.3-7) ~12000-28000 chars
            # 3. Directors' Report (p.11+) ~40000+ chars — ESTO queremos
            # Buscar la mencion que viene DESPUES de 30000 chars para saltar ambas TOCs
            best_match = heading_matches[-1]  # ultima mencion como fallback
            for m in heading_matches:
                if m.start() > 30000:  # pasadas ambas TOCs
                    best_match = m
                    break

            start = max(0, best_match.start() - 500)
            # Buscar fin: el siguiente heading de otro sub-fondo
            # o +15000 chars (lo que sea primero)
            end = min(len(full_text), start + 15000)

            # Intentar encontrar donde empieza el siguiente sub-fondo
            # Patron: lineas ALL CAPS que parecen titulos de otros fondos
            remaining = full_text[best_match.end():]
            # Buscar headings tipo "FONDO NOMBRE" (all caps, >15 chars) despues de >1000 chars
            next_fund = re.search(
                r'\n\s*[A-Z][A-Z\s\-]{15,}(?:FUND|BOND|EQUITY|INCOME|INVEST|GROWTH|VALUE)',
                remaining[1000:])
            if next_fund:
                end = best_match.end() + 1000 + next_fund.start()

            isolated = full_text[start:end]
            self._log("INFO", f"  Umbrella AR: heading encontrado, "
                      f"{len(isolated)} chars aislados para {self.fund_short}")
            return isolated

        # Estrategia 2 (fallback): ventana alrededor de menciones
        mentions = [m.start() for m in re.finditer(re.escape(fund_query), text_lower)]
        if not mentions:
            return full_text

        chunks = []
        for pos in mentions[:3]:  # max 3 menciones
            start = max(0, pos - 3000)
            end = min(len(full_text), pos + 5000)
            chunks.append(full_text[start:end])

        isolated = "\n\n---\n\n".join(chunks)
        self._log("INFO", f"  Umbrella AR: {len(mentions)} menciones, "
                  f"{len(isolated)} chars aislados para {self.fund_short}")
        return isolated

    # ══════════════════════════════════════════════════════════════════════
    # PASO 2: Extraer commentary estructurado con Gemini Flash
    # ══════════════════════════════════════════════════════════════════════

    def _extract_commentary(self, text: str, doc_type: str, periodo: str) -> list[dict]:
        """Usa Gemini Flash para extraer commentary estructurado."""
        if not text or len(text) < 200:
            return []

        # Para AR/SAR umbrella: aislar solo paginas del sub-fondo
        if doc_type in ("annual_report", "semi_annual_report"):
            text = self._isolate_subfund_text(text)

        from tools.gemini_wrapper import extract_fast

        if doc_type == "factsheet":
            context = (
                f"Este es un factsheet del fondo {self.fund_name} ({self.isin}), "
                f"gestora {self.gestora}, periodo {periodo}. "
                f"Extrae SOLO el commentary/comentario del gestor si existe. "
                f"En factsheets suele ser 1-2 parrafos cortos. "
                f"Si no hay commentary del gestor, devuelve cartas vacia. "
                f"Todo en ESPANOL."
            )
            max_chars = 15000
        elif doc_type in ("annual_report", "semi_annual_report"):
            context = (
                f"Este es un {doc_type} del fondo {self.fund_name} ({self.isin}), "
                f"gestora {self.gestora}. "
                f"Extrae UNICAMENTE el commentary del gestor sobre ESTE fondo especifico "
                f"({self.fund_short}). "
                f"Busca: Directors Report, Investment Manager Report, Manager Commentary, "
                f"Rapport de Gestion, Bericht des Fondsmanagers, o equivalente. "
                f"IGNORA secciones sobre OTROS sub-fondos de la misma gestora "
                f"(ej. si el doc habla de Trojan Income, Trojan Ethical, etc. — ignorar). "
                f"IGNORA datos contables/financieros puros. "
                f"Solo quiero la VISION y DECISIONES del gestor sobre {self.fund_short}. "
                f"Todo en ESPANOL."
            )
            max_chars = 60000
        else:
            context = (
                f"Esta es una carta/presentacion del gestor del fondo {self.fund_name} "
                f"({self.isin}), gestora {self.gestora}, periodo {periodo}. "
                f"Extrae: contexto de mercado, tesis del gestor, decisiones de cartera, "
                f"resultado real, perspectivas, y citas textuales relevantes. "
                f"Si cubre MULTIPLES periodos, separa cada uno. "
                f"Todo en ESPANOL."
            )
            max_chars = 40000

        try:
            result = self._retry_gemini(
                extract_fast,
                text=text[:max_chars],
                schema=LETTER_SCHEMA,
                context=context,
            )
            if isinstance(result, dict):
                cartas = result.get("cartas", [])
                return [c for c in cartas if isinstance(c, dict)
                        and (c.get("contexto_mercado") or c.get("tesis_gestora")
                             or c.get("decisiones_tomadas"))]
        except Exception as e:
            self._log("WARN", f"Extraction failed: {e}")
        return []

    # ══════════════════════════════════════════════════════════════════════
    # PASO 3: Aprender nombres reales de documentos de la gestora
    # ══════════════════════════════════════════════════════════════════════

    def _learn_doc_patterns(self, docs: list[dict]) -> dict:
        """Analiza los documentos de discovery para aprender:
        - Dominio de la gestora
        - Nombres reales de documentos (para buscar versiones antiguas)
        - Patron de URLs (para predecir URLs de otros periodos)
        """
        patterns = {
            "gestora_domain": "",
            "doc_names": [],       # nombres de archivos reales de cartas/letters
            "url_patterns": [],    # URLs de ejemplo para buscar mas
        }

        for doc in docs:
            url = doc.get("url", "")
            local = doc.get("local_path", "")
            dtype = doc.get("doc_type", "")

            # Dominio gestora
            if url and "manual://" not in url and not patterns["gestora_domain"]:
                host = urlparse(url).netloc.lower()
                if host and not any(x in host for x in ["kneip", "universal-investment",
                                                         "morningstar", "finect"]):
                    patterns["gestora_domain"] = host

            # Nombre real del documento (solo cartas/presentations)
            if dtype in ("quarterly_letter", "manager_presentation"):
                # Preferir filename original, pero si es generico (ej "quarterly_letter_latest")
                # intentar extraer nombre de la URL
                fname = Path(local).stem if local else ""
                is_generic = any(g in fname.lower() for g in
                                 ["latest", "quarterly_letter", "annual_report",
                                  "semi_annual", "factsheet", "kid"])

                if is_generic and url:
                    # Extraer de URL: /download/Investment-Report-No-87.pdf
                    url_fname = url.rstrip("/").split("/")[-1].split("?")[0]
                    url_fname = re.sub(r'\.\w+$', '', url_fname)  # quitar extension
                    if len(url_fname) > 5 and not any(g in url_fname.lower() for g in
                                                       ["download", "share", "latest"]):
                        fname = url_fname

                # Limpiar: quitar numeros, fechas, dejar el patron base
                clean = re.sub(r'[-_]', ' ', fname)
                clean = re.sub(r'\b(No\.?\s*\d+|Q[1-4]|20\d{2}|S[12]|latest)\b', '',
                               clean, flags=re.IGNORECASE)
                clean = re.sub(r'\s+', ' ', clean).strip()
                if clean and len(clean) > 5 and clean.lower() not in (
                        "quarterly letter", "annual report", "semi annual report"):
                    patterns["doc_names"].append(clean)

            # URL pattern
            if url and dtype in ("quarterly_letter", "manager_presentation"):
                patterns["url_patterns"].append(url)

        # Dedup doc_names
        patterns["doc_names"] = list(set(patterns["doc_names"]))
        return patterns

    # ══════════════════════════════════════════════════════════════════════
    # PASO 4: Buscar cartas GLOBALES del gestor/gestora (umbrella/multi-fondo)
    # ══════════════════════════════════════════════════════════════════════

    def _get_fund_context(self) -> dict:
        """Leer contexto del fondo: gestores, clase de activo, estrategia."""
        ctx = {"gestores": [], "asset_class": "", "strategy": ""}
        # De manager_profile.json
        mp = self.fund_dir / "manager_profile.json"
        if mp.exists():
            try:
                d = json.loads(mp.read_text(encoding="utf-8"))
                for g in d.get("equipo", []):
                    if isinstance(g, dict) and g.get("nombre"):
                        ctx["gestores"].append(g["nombre"])
            except Exception:
                pass
        # De intl_data.json
        intl = self.fund_dir / "intl_data.json"
        if intl.exists():
            try:
                d = json.loads(intl.read_text(encoding="utf-8"))
                cual = d.get("cualitativo") or {}
                ctx["asset_class"] = cual.get("tipo_activos", "") or ""
                ctx["strategy"] = cual.get("estrategia", "") or ""
                # Gestores de intl_data como fallback
                if not ctx["gestores"]:
                    for g in cual.get("gestores", []):
                        if isinstance(g, dict) and g.get("nombre"):
                            ctx["gestores"].append(g["nombre"])
            except Exception:
                pass
        return ctx

    async def _search_gestora_global_letters(self, missing_years: list[int],
                                              patterns: dict) -> list[dict]:
        """Buscar cartas GLOBALES de la gestora o del gestor sobre la clase de activo.

        Caso tipico: DNCA no publica cartas por sub-fondo, pero Francois Collet
        publica cartas sobre renta fija que aplican a Alpha Bonds.
        Troy publica investment reports globales que cubren todos sus fondos.
        """
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)

        ctx = self._get_fund_context()
        gestores = ctx["gestores"]
        asset_class = ctx["asset_class"]
        gestora_domain = patterns.get("gestora_domain", "") or self._infer_gestora_domain()

        if not gestores and not self.gestora:
            return []

        self._log("INFO", f"Buscando cartas globales: gestores={gestores[:2]}, "
                  f"asset_class={asset_class[:30]}")

        queries = []

        # ── NUEVO: Commentary a nivel GESTORA (universal) ──
        # Toda gestora publica algun tipo de vision anual/trimestral
        if self.gestora:
            queries.extend([
                f'"{self.gestora}" annual letter OR year review OR year in review',
                f'"{self.gestora}" market commentary OR market outlook',
                f'"{self.gestora}" investor update OR shareholder letter',
                # Alternativas si la gestora NO publica cartas clasicas
                f'"{self.gestora}" annual report chairman OR CEO letter',
                f'"{self.gestora}" year-end review OR mid-year outlook',
                f'"{self.gestora}" press release results annual semestral',
                f'"{self.gestora}" investor presentation conference',
                f'"{self.gestora}" webinar OR conference call transcript',
                f'"{self.gestora}" quarterly review OR semestral outlook',
            ])
            # Por año faltante (max 5 para no saturar)
            for year in missing_years[:5]:
                queries.append(
                    f'"{self.gestora}" {year} outlook OR review OR commentary')

        # ── Blog/news de la gestora ──
        if gestora_domain:
            queries.extend([
                f'site:{gestora_domain} outlook OR review OR commentary',
                f'site:{gestora_domain} market OR investment outlook',
                f'site:{gestora_domain} letter OR update investors',
            ])

        # ── Por nombre del gestor + clase de activo ──
        for gestor in gestores[:2]:
            queries.extend([
                f'"{gestor}" commentary OR letter OR outlook',
                f'"{gestor}" quarterly OR annual review',
            ])
            if gestora_domain:
                queries.append(f'site:{gestora_domain} "{gestor}"')

        # ── Por gestora + clase de activo ──
        ac_keywords = self._infer_asset_class_keywords(asset_class)
        if ac_keywords and self.gestora:
            for kw in ac_keywords[:2]:
                queries.extend([
                    f'"{self.gestora}" {kw} commentary OR letter OR outlook',
                    f'"{self.gestora}" {kw} quarterly OR annual review',
                ])
            if gestora_domain:
                for kw in ac_keywords[:1]:
                    queries.append(f'site:{gestora_domain} {kw} commentary OR letter')

        # ── Por gestora generico ──
        if self.gestora:
            queries.extend([
                f'"{self.gestora}" investor letter OR quarterly commentary',
                f'"{self.gestora}" market outlook OR investment outlook',
            ])

        queries = list(dict.fromkeys(queries))
        results = await search.search_multiple(queries[:15], num_per_query=3,
                                                agent="letters_global")
        self._log("INFO", f"Cartas globales: {len(results)} resultados de "
                  f"{min(len(queries),15)} queries")

        cartas = []
        seen_urls: set[str] = set()
        for r in results[:15]:
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            if any(d in url for d in ["google.com", "bing.com", "linkedin.com"]):
                continue
            seen_urls.add(url)

            if url.lower().endswith(".pdf"):
                text = await self._fetch_pdf_from_url(url)
            else:
                text = await self._fetch_web_text(url)

            if not text or len(text) < 500:
                continue

            # Para cartas globales: verificar que menciona al gestor O a la gestora
            # (no necesita mencionar el sub-fondo especifico)
            text_lower = text.lower()
            is_relevant = False
            for g in gestores:
                if g.split()[-1].lower() in text_lower:
                    is_relevant = True
                    break
            if not is_relevant and self.gestora:
                if self.gestora.lower().split()[0] in text_lower:
                    is_relevant = True
            if not is_relevant:
                continue

            self._log("INFO", f"  Global: {len(text)} chars from {url[:60]}")

            # Extraer con contexto especifico: filtrar lo que aplique al fondo
            extracted = self._extract_global_commentary(text, ctx)
            for carta in extracted:
                carta["fuente_tipo"] = "web_global"
                carta["url_fuente"] = url
                cartas.append(carta)

        return cartas

    def _opus_historical_hints(self, missing_years: list[int]) -> dict:
        """1 call Opus para identificar DONDE buscar docs historicos.

        Opus conoce los patrones de publicacion de cada gestora:
        - URL patterns de annual reports (ej. /wp-content/uploads/YYYY/MM/Report-No-NN.pdf)
        - Dominios de distribuidores que archivan PDFs del fondo
        - Nombres exactos de los documentos que publica la gestora

        Coste: ~$0.02. Se llama 1 vez por fondo, solo si faltan años.
        """
        if not missing_years:
            return {}
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            r = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": (
                    f"Fondo: {self.fund_name} ({self.isin})\n"
                    f"Gestora: {self.gestora}\n"
                    f"Necesito encontrar annual reports o cartas del gestor de "
                    f"los años {missing_years}.\n\n"
                    f"Responde SOLO con:\n"
                    f"DOMINIOS: dominios web donde esta gestora publica documentos "
                    f"(incluir distribuidores como fundinfo, Natixis IM, etc)\n"
                    f"PATRON_URL: si conoces el patron de URL de los documentos, "
                    f"dámelo (ej. /wp-content/uploads/YYYY/MM/Report-No-NN.pdf)\n"
                    f"NOMBRE_DOCS: como se llaman las cartas/reports de esta gestora "
                    f"(ej. 'Investment Report', 'Lettre trimestrielle')\n"
                    f"Sé conciso. Solo datos que conozcas con certeza."
                )}],
            )
            text = r.content[0].text
            self._log("INFO", f"Opus hints ({r.usage.input_tokens}+{r.usage.output_tokens} tok)")

            # Parsear respuesta libre de Opus
            hints: dict = {"domains": [], "url_patterns": [], "doc_names": []}

            for line in text.split("\n"):
                line_lower = line.lower().strip()
                # Extraer dominios
                if "dominio" in line_lower or "domain" in line_lower or "site" in line_lower:
                    for word in line.split():
                        w = word.strip(",.;:()[]").lower()
                        if "." in w and len(w) > 5 and "/" not in w:
                            hints["domains"].append(w.replace("www.", ""))
                # Extraer URLs
                if "http" in line or "/wp-content" in line or ".pdf" in line:
                    import re as _re
                    urls = _re.findall(r'https?://\S+|/[\w\-/]+\.pdf', line)
                    hints["url_patterns"].extend(urls)
                # Dominios sueltos en cualquier línea
                for word in line.split():
                    w = word.strip(",.;:()[]").lower()
                    if w.endswith(".com") or w.endswith(".co.uk") or w.endswith(".lu"):
                        if w not in hints["domains"] and len(w) > 5:
                            hints["domains"].append(w.replace("www.", ""))

            # Dedup
            hints["domains"] = list(dict.fromkeys(hints["domains"]))[:5]
            return hints

        except Exception as e:
            self._log("INFO", f"Opus hints skipped: {type(e).__name__}")
            return {}

    def _infer_asset_class_keywords(self, asset_class: str) -> list[str]:
        """Inferir keywords de busqueda desde la clase de activo."""
        ac = asset_class.lower()
        keywords = []
        if any(w in ac for w in ["bond", "renta fija", "fixed income", "obligat"]):
            keywords.extend(["fixed income", "bonds", "renta fija"])
        if any(w in ac for w in ["equit", "renta variable", "acciones", "stock"]):
            keywords.extend(["equity", "stocks", "renta variable"])
        if any(w in ac for w in ["multi", "mixto", "balanced", "allocation"]):
            keywords.extend(["multi-asset", "balanced", "allocation"])
        if any(w in ac for w in ["alternativ", "hedge", "absolute"]):
            keywords.extend(["alternatives", "absolute return"])
        return keywords

    def _extract_global_commentary(self, text: str, ctx: dict) -> list[dict]:
        """Extraer de una carta GLOBAL solo lo aplicable al fondo."""
        if not text or len(text) < 300:
            return []

        from tools.gemini_wrapper import extract_fast

        gestores_str = ", ".join(ctx["gestores"][:3]) if ctx["gestores"] else self.gestora
        asset_str = ctx["asset_class"] or "desconocida"
        strategy_str = (ctx["strategy"] or "")[:200]

        context = (
            f"Este texto es una carta/commentary GLOBAL de la gestora {self.gestora}. "
            f"Gestores del fondo: {gestores_str}. "
            f"Clase de activo del fondo: {asset_str}. "
            f"Estrategia: {strategy_str}. "
            f"\n\nExtrae SOLO lo que sea aplicable al fondo {self.fund_name} ({self.isin}). "
            f"Esto incluye: vision macro que afecte a su clase de activo, "
            f"decisiones del gestor sobre {asset_str}, "
            f"posicionamiento en mercados relevantes para el fondo, outlook. "
            f"IGNORA secciones sobre clases de activo completamente distintas "
            f"(ej. si el fondo es de renta fija, ignora secciones sobre renta variable pura). "
            f"Si no hay nada aplicable, devuelve cartas vacia. "
            f"Todo en ESPANOL."
        )

        try:
            result = self._retry_gemini(
                extract_fast,
                text=text[:40000],
                schema=LETTER_SCHEMA,
                context=context,
            )
            if isinstance(result, dict):
                cartas = result.get("cartas", [])
                return [c for c in cartas if isinstance(c, dict)
                        and (c.get("contexto_mercado") or c.get("tesis_gestora")
                             or c.get("decisiones_tomadas"))]
        except Exception as e:
            self._log("WARN", f"Global extraction failed: {e}")
        return []

    # ══════════════════════════════════════════════════════════════════════
    # PASO 5a: Buscar documentos historicos (Wayback + URL extrapolation)
    # ══════════════════════════════════════════════════════════════════════

    async def _search_historical_docs(self, missing_years: list[int],
                                       patterns: dict) -> list[dict]:
        """Buscar documentos historicos — approach UNIVERSAL que funciona con
        cualquier fondo/gestora.

        Estrategia (prioridad):
        1. Wayback CDX: buscar TODOS los PDFs del dominio gestora, filtrar por
           keywords de cartas/commentary. Funciona con cualquier dominio.
        2. URL extrapolation: si hay serie numerada (ej. Report No.87),
           generar URLs anteriores. Solo bonus, no universal.

        Limitaciones aplicadas:
        - Max 1 descarga por año faltante (parar cuando cubierto)
        - 2s delay entre descargas (evitar throttling Wayback)
        - Gemini OCR solo si pdfplumber falla (<100 chars)
        """
        import asyncio as _aio
        cartas: list[dict] = []
        gestora_domain = patterns.get("gestora_domain", "")
        known_urls = patterns.get("url_patterns", [])
        remaining = set(missing_years)

        # ── Tier -1: Opus identifica donde buscar docs historicos ──
        opus_hints = self._opus_historical_hints(list(remaining))
        if opus_hints.get("domains"):
            # Añadir dominios sugeridos por Opus al CDX search
            for domain in opus_hints["domains"]:
                if domain and domain != gestora_domain:
                    patterns.setdefault("extra_domains", []).append(domain)
        if opus_hints.get("url_patterns"):
            known_urls = known_urls + opus_hints["url_patterns"]

        # ── Estrategia 1 (UNIVERSAL): Wayback CDX por dominio ──
        # OPTIMIZACION: seleccionar 1 doc por año faltante ANTES de descargar.
        # Preferir: diciembre > Q4 > el mas cercano a fin de año.
        if gestora_domain:
            wayback_urls = await self._search_wayback_cdx(gestora_domain, patterns)
            if wayback_urls:
                # Seleccionar 1 doc por año faltante (el mejor candidato)
                selected = self._select_best_per_year(wayback_urls, list(remaining))
                self._log("INFO", f"Wayback CDX: {len(wayback_urls)} disponibles, "
                          f"seleccionados {len(selected)} (1/año faltante)")

                for year, wb in selected.items():
                    if year not in remaining:
                        continue
                    url = wb["wayback_url"]
                    text = await self._fetch_pdf_from_url(url, timeout=60)
                    if not text or len(text) < 300:
                        await _aio.sleep(1)
                        continue
                    fname = wb["original"].split("/")[-1][:50]
                    self._log("INFO", f"  Wayback {year}: {len(text)} chars - {fname}")
                    extracted = self._extract_commentary(text, "quarterly_letter", "")
                    for carta in extracted:
                        carta["fuente_tipo"] = "wayback"
                        carta["url_fuente"] = url
                    cartas.extend(extracted)
                    remaining.discard(year)
                    await _aio.sleep(2)  # rate limit

        # ── Estrategia 1b: Wayback CDX en DISTRIBUIDORES (para gestoras SPA) ──
        # Cuando la gestora tiene web SPA (URLs dinámicas), Wayback no archiva PDFs.
        # Pero los distribuidores (Natixis IM, fundinfo, AllFunds) tienen PDFs estáticos.
        if remaining:
            distributor_domains = [
                "im.natixis.com", "fundinfo.com", "allfunds.com",
                "funds.ft.com", "fundslibrary.co.uk",
            ]
            # Añadir dominios sugeridos por Opus
            for d in patterns.get("extra_domains", []):
                if d not in distributor_domains:
                    distributor_domains.append(d)
            for dist_domain in distributor_domains:
                if not remaining:
                    break
                dist_urls = await self._search_wayback_cdx(dist_domain, patterns)
                if not dist_urls:
                    continue
                # Filtrar: solo los que mencionan el fondo en la URL
                fund_kw = [w.lower() for w in self.fund_short.split() if len(w) > 3]
                fund_kw.append(self.isin.lower())
                relevant = [u for u in dist_urls
                            if any(kw in u["original"].lower() for kw in fund_kw)]
                if not relevant:
                    continue
                selected = self._select_best_per_year(relevant, list(remaining))
                if selected:
                    self._log("INFO", f"Wayback {dist_domain}: "
                              f"{len(relevant)} docs, {len(selected)} seleccionados")
                for year, wb in selected.items():
                    if year not in remaining:
                        continue
                    url = wb["wayback_url"]
                    text = await self._fetch_pdf_from_url(url, timeout=60)
                    if not text or len(text) < 300:
                        await _aio.sleep(1)
                        continue
                    fname = wb["original"].split("/")[-1][:50]
                    self._log("INFO", f"  Distribuidor {year}: {len(text)} chars - {fname}")
                    extracted = self._extract_commentary(text, "quarterly_letter", "")
                    for carta in extracted:
                        carta["fuente_tipo"] = "wayback_distribuidor"
                        carta["url_fuente"] = url
                    cartas.extend(extracted)
                    remaining.discard(year)
                    await _aio.sleep(2)

        # ── Estrategia 2 (BONUS): URL extrapolation numerada ──
        if remaining and known_urls:
            numbered_urls = self._extrapolate_numbered_urls(known_urls, list(remaining))
            if numbered_urls:
                by_number: dict[int, list[str]] = {}
                for url in numbered_urls:
                    m = re.search(r'(\d+)[^/]*\.pdf', url)
                    if m:
                        by_number.setdefault(int(m.group(1)), []).append(url)

                numbers = sorted(by_number.keys(), reverse=True)
                sampled = numbers[::4]
                self._log("INFO", f"Extrapolacion: probando {len(sampled)} numeros")

                for num in sampled:
                    if not remaining:
                        break
                    for url in by_number[num]:
                        text = await self._fetch_pdf_from_wayback_or_direct(url)
                        if text and len(text) > 300:
                            break
                    else:
                        await _aio.sleep(1)
                        continue
                    self._log("INFO", f"  No.{num}: {len(text)} chars")
                    extracted = self._extract_commentary(text, "quarterly_letter", "")
                    for carta in extracted:
                        carta["fuente_tipo"] = "historico_extrapolado"
                        carta["url_fuente"] = by_number[num][0]
                    cartas.extend(extracted)
                    remaining -= self._get_covered_years(cartas) & remaining
                    await _aio.sleep(2)

        return cartas

    def _select_best_per_year(self, wayback_urls: list[dict],
                              missing_years: list[int]) -> dict[int, dict]:
        """Seleccionar 1 documento por año faltante de los resultados CDX.

        Criterio: para cada año, elegir el documento mas cercano a diciembre.
        Usa el filename para inferir mes/año. Si no puede inferir, usa el
        timestamp de Wayback como aproximacion.

        Esto evita descargar 95 PDFs cuando solo necesitamos 7.
        """
        # Para cada doc, inferir año y mes
        candidates_by_year: dict[int, list[tuple[int, dict]]] = {}  # year -> [(month_score, doc)]

        for wb in wayback_urls:
            fname = wb["original"].split("/")[-1].split("?")[0].lower()
            ts = wb.get("timestamp", "")

            year = None
            month_score = 6  # default: mitad de año

            # Intentar extraer año/mes del filename
            # Patron: "december-2021", "oct-2020", "q4-2019"
            month_map = {
                "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
                "january": 1, "february": 2, "march": 3, "april": 4,
                "june": 6, "july": 7, "august": 8, "september": 9,
                "october": 10, "november": 11, "december": 12,
            }
            # Buscar "Month-YYYY" o "YYYY-Month" en filename
            for mname, mnum in month_map.items():
                if mname in fname:
                    month_score = mnum
                    # Buscar año cercano al mes
                    ym = re.search(r'(20[012]\d)', fname)
                    if ym:
                        year = int(ym.group(1))
                    break

            # Patron numerico: "No-70" -> estimar año desde serie conocida
            if not year:
                m = re.search(r'no[._\-]?(\d+)', fname, re.IGNORECASE)
                if m:
                    num = int(m.group(1))
                    # Estimar: si No.78=2023, ~4/año => year = 2023 - (78-num)/4
                    # Usar los conocidos del discovery para calibrar
                    year = self._estimate_year_from_number(num)
                    # El número más alto del grupo de 4 es ~Q4
                    month_score = 12 if num % 4 == 2 else (num % 4) * 3 + 3

            # Fallback: año del timestamp de Wayback (cuando fue archivado)
            if not year and len(ts) >= 4:
                # El timestamp es cuando Wayback lo grabó, no cuando se publicó
                # Menos fiable pero mejor que nada
                year = int(ts[:4]) - 1  # asumimos que lo archivó 1 año después

            if year and year in missing_years:
                candidates_by_year.setdefault(year, []).append((month_score, wb))

        # Para cada año, elegir el más cercano a diciembre (month_score=12)
        selected: dict[int, dict] = {}
        for year in missing_years:
            candidates = candidates_by_year.get(year, [])
            if not candidates:
                continue
            # Ordenar por cercanía a diciembre (12 es mejor, luego 11, 10...)
            candidates.sort(key=lambda x: -x[0])
            selected[year] = candidates[0][1]

        return selected

    def _estimate_year_from_number(self, num: int) -> int | None:
        """Estimar año de publicación desde número de serie.
        Calibrado con los docs de la serie PRINCIPAL del discovery
        (no mezclar Investment Report con Special Paper)."""
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                # Agrupar por serie (mismo base name)
                series: dict[str, list[tuple[int, int]]] = {}
                for doc in disc.get("documents", []):
                    url = doc.get("url", "")
                    periodo = doc.get("periodo", "")
                    m_num = re.search(r'([\w\-]+?)no[._\-]?(\d+)', url, re.IGNORECASE)
                    m_year = re.search(r'(20[012]\d)', str(periodo))
                    if m_num and m_year:
                        base = m_num.group(1).lower().rstrip("-_.")
                        series.setdefault(base, []).append(
                            (int(m_num.group(2)), int(m_year.group(1))))
                # Usar la serie mas larga (Investment Report, no Special Paper)
                if series:
                    main = max(series.values(), key=len)
                    main.sort()
                    if len(main) >= 2:
                        num_range = main[-1][0] - main[0][0]
                        year_range = main[-1][1] - main[0][1]
                        if year_range > 0 and num_range > 0:
                            reports_per_year = num_range / year_range
                            ref_num, ref_year = main[-1]
                            estimated = ref_year - (ref_num - num) / reports_per_year
                            return round(estimated)
            except Exception:
                pass
        # Default: asumir 4 reports/año, No.78=2023
        return round(2023 - (78 - num) / 4)

    def _extrapolate_numbered_urls(self, known_urls: list[str],
                                    missing_years: list[int]) -> list[str]:
        """Generar URLs candidatas extrapolando series numeradas.

        Si tenemos Investment-Report-No-87.pdf y No-78.pdf,
        genera No-77, No-76, ... No-60 (aprox 1 report/trimestre).
        Genera multiples variantes de formato (guion, punto, slash).
        """
        candidates = []
        # Agrupar URLs por tipo de documento (mismo prefijo base)
        # "Investment-Report-No" y "Special-Paper-No" son series distintas
        series: dict[str, list[tuple]] = {}  # base_name -> [(num, match)]
        for url in known_urls:
            m = re.search(r'(.*(?:No|no|Nº|num|number))([._\-\s]*)(\d+)(.*\.pdf)',
                          url, re.IGNORECASE)
            if m:
                # Extraer base: "Investment-Report-No" vs "Special-Paper-No"
                base = re.sub(r'https?://[^/]+', '', m.group(1)).strip()
                series.setdefault(base, []).append((int(m.group(3)), m))

        if not series:
            return candidates

        # Usar la serie con MAS numeros (la principal)
        main_series = max(series.values(), key=len)
        known_numbers = {n for n, _ in main_series}
        best_match = main_series[0][1]  # primer match

        prefix = best_match.group(1)
        separator = best_match.group(2)
        suffix = best_match.group(4)
        min_known = min(known_numbers)

        # Generar numeros ANTERIORES al minimo conocido
        # Estimando ~4 reports/año
        target_min = max(1, min_known - len(missing_years) * 4)
        separators = list(set([separator, "-", ".", "_"]))

        for n in range(min_known - 1, target_min - 1, -1):
            if n in known_numbers:
                continue
            for sep in separators:
                candidate = f"{prefix}{sep}{n}{suffix}"
                candidates.append(candidate)

        return candidates

    async def _search_wayback_cdx(self, domain: str, patterns: dict) -> list[dict]:
        """Buscar PDFs archivados en Wayback Machine CDX API.

        Approach universal: busca TODOS los PDFs del dominio y filtra por
        keywords en el filename. Funciona con cualquier gestora.
        Usa output=text (mas fiable que JSON para respuestas grandes).
        """
        results = []
        doc_names = patterns.get("doc_names", [])

        # Keywords que indican carta/commentary (multi-idioma)
        letter_kw = {
            "report", "letter", "commentary", "comment", "update",
            "lettre", "rapport", "bericht", "outlook", "review",
            "quarterly", "annual", "semestral", "semestriel",
            "investment", "investor", "market", "markt",
        }
        # Keywords que indican NO carta (evitar descargar factsheets/prospectus/AR)
        skip_kw = {
            "kid", "kiid", "prospectus", "dfi", "dic", "priip",
            "fact-sheet", "factsheet", "fact_sheet",
            "annual-report", "annual_report", "annualreport",
            "interim-report", "interim_report",
        }

        cdx_url = (
            f"https://web.archive.org/cdx/search/cdx?"
            f"url={domain}/*&output=text"
            f"&filter=mimetype:application/pdf"
            f"&limit=300&fl=timestamp,original"
        )

        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r = await c.get(cdx_url)
                if r.status_code != 200:
                    self._log("WARN", f"Wayback CDX status {r.status_code}")
                    return []
                for line in r.text.strip().split("\n"):
                    parts = line.split(" ", 1)
                    if len(parts) != 2:
                        continue
                    ts, original = parts[0].strip(), parts[1].strip()
                    fname = original.split("/")[-1].split("?")[0].lower()

                    # Descartar docs que no son cartas
                    if any(kw in fname for kw in skip_kw):
                        continue
                    # Aceptar si filename contiene keywords de carta
                    is_letter = any(kw in fname for kw in letter_kw)
                    # O si coincide con nombres aprendidos del discovery
                    if not is_letter and doc_names:
                        for name in doc_names:
                            if any(w.lower() in fname for w in name.split()
                                   if len(w) > 4):
                                is_letter = True
                                break
                    if is_letter:
                        wb_url = f"https://web.archive.org/web/{ts}/{original}"
                        results.append({
                            "wayback_url": wb_url,
                            "original": original,
                            "timestamp": ts,
                        })
        except Exception as e:
            self._log("WARN", f"Wayback CDX error: {type(e).__name__}: {e}")

        # Dedup por filename (no URL completa — misma carta puede estar en paths distintos)
        seen_fnames: set[str] = set()
        deduped = []
        for r in results:
            fname = r["original"].split("/")[-1].split("?")[0].lower()
            if fname not in seen_fnames:
                seen_fnames.add(fname)
                deduped.append(r)

        # Ordenar por timestamp descendente (mas recientes primero)
        deduped.sort(key=lambda x: x["timestamp"], reverse=True)
        self._log("INFO", f"Wayback CDX: {len(results)} raw -> {len(deduped)} unicos")
        return deduped

    _failed_domains: set[str] = set()  # domains que ya fallaron (Cloudflare, etc)

    async def _fetch_pdf_from_wayback_or_direct(self, url: str) -> str:
        """Intentar descargar PDF: directo si posible, sino via Wayback.
        Recuerda dominios que fallaron para no reintentar."""
        # Si ya es URL de Wayback, descargar directamente
        if "web.archive.org" in url:
            return await self._fetch_pdf_from_url(url)

        # Comprobar si el dominio ya falló antes
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        skip_direct = domain in self._failed_domains

        # Intentar directo solo si el dominio no ha fallado antes
        if not skip_direct:
            text = await self._fetch_pdf_from_url(url)
            if text and len(text) > 200:
                return text
            # Marcar dominio como fallido
            self._failed_domains.add(domain)

        # Ir directo a Wayback
        original = url
        if original.startswith("http"):
            variants = [original]
            if "/wp-content/" in original:
                variants.append(original.replace("/wp-content/", "/2/wp-content/"))
            for v in list(variants):
                if "://www." in v:
                    variants.append(v.replace("://www.", "://"))
                elif "://" in v and "://www." not in v:
                    variants.append(v.replace("://", "://www."))

            for variant in variants:
                wb_url = f"https://web.archive.org/web/{variant}"
                text = await self._fetch_pdf_from_url(wb_url, timeout=60)
                if text and len(text) > 200:
                    return text

        return ""

    # ══════════════════════════════════════════════════════════════════════
    # PASO 5b: Buscar cartas antiguas usando patrones aprendidos (Google)
    # ══════════════════════════════════════════════════════════════════════

    async def _search_missing_years(self, missing_years: list[int],
                                     patterns: dict) -> list[dict]:
        """Buscar cartas para anos sin cobertura usando patrones aprendidos."""
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)

        cartas_web: list[dict] = []
        fund_q = self.fund_short or self.fund_name
        gestora_domain = patterns.get("gestora_domain", "") or self._infer_gestora_domain()
        doc_names = patterns.get("doc_names", [])

        # ── Estrategia 1: Buscar en web de la gestora con nombres reales ──
        # Si sabemos que la gestora publica "Investment Report", buscamos eso
        queries = []
        if gestora_domain and doc_names:
            for name in doc_names[:2]:
                queries.append(f'site:{gestora_domain} "{name}"')
                # Buscar por anos especificos si faltan pocos
                if len(missing_years) <= 4:
                    for year in missing_years:
                        queries.append(f'site:{gestora_domain} "{name}" {year}')

        # ── Estrategia 2: Buscar en web de la gestora con terminos genericos ──
        # Multi-idioma: cada gestora usa su idioma
        if gestora_domain:
            queries.extend([
                f'site:{gestora_domain} "{fund_q}" commentary OR letter OR report OR review',
                f'site:{gestora_domain} "{fund_q}" lettre OR rapport OR Bericht OR relazione',
                f'site:{gestora_domain} "{fund_q}" annual review OR year in review OR balance',
            ])

        # ── Estrategia 3: Buscar en plataformas de distribucion ──
        # Muchas gestoras suben cartas a plataformas de terceros
        fund_variants = self._fund_name_variants()
        for variant in fund_variants[:2]:
            queries.extend([
                f'"{variant}" quarterly letter OR commentary filetype:pdf',
                f'"{variant}" annual review OR year in review',
                f'"{variant}" investor letter OR shareholder letter',
                # Multi-idioma
                f'"{variant}" lettre trimestrielle OR rapport semestriel',
                f'"{variant}" Quartalsbericht OR Jahresbericht',
                f'"{variant}" carta trimestral OR informe semestral',
            ])

        # ── Estrategia 4: PDFs en distribuidores (universal) ──
        # Google indexa PDFs de fundinfo, Natixis IM, AllFunds, etc.
        # filetype:pdf + nombre fondo + año = lo más directo
        for variant in fund_variants[:1]:
            queries.append(f'"{variant}" filetype:pdf commentary OR letter OR report')
            queries.append(f'"{variant}" filetype:pdf quarterly OR annual review')

        # ── Estrategia 5: Por anos especificos si faltan pocos ──
        if len(missing_years) <= 5:
            for year in missing_years:
                queries.append(f'"{fund_q}" {year} commentary OR letter OR review OR report')
                queries.append(f'"{fund_q}" {year} filetype:pdf')

        # Dedup queries
        queries = list(dict.fromkeys(queries))

        results = await search.search_multiple(queries[:20], num_per_query=3,
                                                agent="letters_collector")
        self._log("INFO", f"Web search: {len(results)} resultados de {min(len(queries),20)} queries")

        # Fetch y extraer
        seen_urls: set[str] = set()
        for r in results[:20]:
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            if any(d in url for d in ["google.com", "bing.com", "linkedin.com"]):
                continue
            seen_urls.add(url)

            # PDF directo
            if url.lower().endswith(".pdf"):
                text = await self._fetch_pdf_from_url(url)
            else:
                text = await self._fetch_web_text(url)

            if not text or len(text) < 300:
                continue

            # Verificar relevancia: debe mencionar el fondo
            fund_terms = [w.lower() for w in self.fund_short.split() if len(w) > 3]
            fund_terms.append(self.isin.lower())
            text_lower = text.lower()
            if not any(t in text_lower for t in fund_terms):
                continue

            self._log("INFO", f"  Web: {len(text)} chars from {url[:60]}")

            extracted = self._extract_commentary(text, "quarterly_letter", "")
            for carta in extracted:
                carta["fuente_tipo"] = "web"
                carta["url_fuente"] = url
                cartas_web.append(carta)

        return cartas_web

    def _fund_name_variants(self) -> list[str]:
        """Variantes del nombre para mejor matching."""
        fund_q = self.fund_short or self.fund_name
        variants = [fund_q]
        # Sin parentesis
        no_parens = re.sub(r'\s*\([^)]*\)', '', fund_q).strip()
        if no_parens and no_parens != fund_q:
            variants.insert(0, no_parens)
        # Sin "Fund"
        no_fund = re.sub(r'\s+Fund\b', '', no_parens, flags=re.IGNORECASE).strip()
        if no_fund and len(no_fund) > 3 and no_fund != no_parens:
            variants.append(no_fund)
        # Con gestora
        if self.gestora:
            variants.append(f"{self.gestora.split()[0]} {variants[0]}")
        return variants

    async def _fetch_pdf_from_url(self, url: str, timeout: int = 15) -> str:
        """Descarga PDF de URL y extrae texto."""
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as c:
                r = await c.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                })
                if r.status_code != 200 or not r.content[:5].startswith(b"%PDF"):
                    return ""
                # Guardar temporalmente y extraer
                fname = Path(url.split("?")[0]).stem[:50]
                tmp = self.fund_dir / "raw" / "discovery" / f"_web_{fname}.pdf"
                tmp.parent.mkdir(parents=True, exist_ok=True)
                tmp.write_bytes(r.content)
                return self._extract_pdf_text(str(tmp), max_pages=30)
        except Exception:
            return ""

    async def _fetch_web_text(self, url: str) -> str:
        """Fetch pagina web y extraer texto limpio."""
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
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()
                return soup.get_text(" ", strip=True)[:15000]
        except Exception:
            return ""

    def _infer_gestora_domain(self) -> str:
        """Inferir dominio de la gestora desde discovery o Google."""
        # Intentar desde discovery
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                for doc in disc.get("documents", []):
                    url = doc.get("url", "")
                    if url and "manual://" not in url and "local://" not in url:
                        host = urlparse(url).netloc.lower()
                        if host and not any(x in host for x in
                                            ["kneip", "universal-investment",
                                             "descarga", "local"]):
                            return host
            except Exception:
                pass
        # Fallback: buscar en knowledge base del regulador
        kb_path = Path(__file__).parent.parent / "data" / "regulators_knowledge.json"
        if kb_path.exists() and self.gestora:
            try:
                kb = json.loads(kb_path.read_text(encoding="utf-8"))
                for prefix, info in kb.items():
                    for gestora_name, urls in info.get("successful_gestora_urls", {}).items():
                        if self.gestora.lower() in gestora_name.lower():
                            for url in urls:
                                host = urlparse(url).netloc.lower()
                                if host:
                                    return host
            except Exception:
                pass
        return ""

    # ══════════════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _normalize_periodo(periodo: str | None) -> str:
        """Normalizar periodo a formato YYYY o YYYY-QX.
        'Octubre 2023' -> '2023-Q4', '2024-Anual' -> '2024',
        'Primer semestre 2023' -> '2023-S1', 'Q3 2020' -> '2020-Q3'."""
        if not periodo:
            return ""
        p = str(periodo).strip()

        # Extraer año
        years = re.findall(r'(20[012]\d)', p)
        if not years:
            return p  # no podemos normalizar sin año
        year = max(years)  # el más reciente

        p_lower = p.lower()

        # Detectar trimestre
        q_match = re.search(r'q([1-4])', p_lower)
        if q_match:
            return f"{year}-Q{q_match.group(1)}"

        # Detectar semestre
        if any(w in p_lower for w in ["s1", "h1", "primer semestre", "first half"]):
            return f"{year}-S1"
        if any(w in p_lower for w in ["s2", "h2", "segundo semestre", "second half"]):
            return f"{year}-S2"

        # Detectar mes -> mapear a trimestre
        month_to_q = {
            "enero": "Q1", "january": "Q1", "jan": "Q1", "febrero": "Q1",
            "february": "Q1", "feb": "Q1", "marzo": "Q1", "march": "Q1", "mar": "Q1",
            "abril": "Q2", "april": "Q2", "apr": "Q2", "mayo": "Q2", "may": "Q2",
            "junio": "Q2", "june": "Q2", "jun": "Q2",
            "julio": "Q3", "july": "Q3", "jul": "Q3", "agosto": "Q3",
            "august": "Q3", "aug": "Q3", "septiembre": "Q3", "september": "Q3", "sep": "Q3",
            "octubre": "Q4", "october": "Q4", "oct": "Q4", "noviembre": "Q4",
            "november": "Q4", "nov": "Q4", "diciembre": "Q4", "december": "Q4", "dec": "Q4",
        }
        for month_name, quarter in month_to_q.items():
            if month_name in p_lower:
                return f"{year}-{quarter}"

        # Solo año
        return year

    def _get_covered_years(self, cartas: list[dict]) -> set[int]:
        """Extraer anos cubiertos de las cartas."""
        years = set()
        for c in cartas:
            periodo = c.get("periodo", "")
            for m in re.finditer(r'(20[12]\d)', str(periodo)):
                years.add(int(m.group(1)))
        return years

    def _dedup_by_periodo(self, cartas: list[dict]) -> list[dict]:
        """Dedup: mantener 1 carta por AÑO (la mas rica, preferiblemente Q4/fin de año).

        Objetivo: 1 carta/año con la vision del gestor sobre ese periodo.
        Si hay varias del mismo año, elegir la de fin de año (Q4/Dec/Oct).
        """
        def richness(carta):
            score = 0
            for k in ("contexto_mercado", "tesis_gestora", "decisiones_tomadas",
                      "resultado_real", "outlook"):
                v = carta.get(k, "")
                if v and len(str(v)) > 20:
                    score += 1
            score += len(carta.get("citas_textuales", []) or [])
            score += len(carta.get("posiciones_mencionadas", []) or [])
            return score

        def end_of_year_bonus(carta):
            """Bonus si es carta de fin de año (Q4/S2) — usa periodo normalizado."""
            p = (carta.get("periodo") or "").upper()
            if "Q4" in p or "S2" in p:
                return 10
            if "Q3" in p:
                return 5
            return 0

        # Normalizar periodos ANTES de agrupar
        for c in cartas:
            c["periodo"] = self._normalize_periodo(c.get("periodo"))

        # Agrupar por año
        by_year: dict[int, list[dict]] = {}
        no_year: list[dict] = []
        for c in cartas:
            p = c.get("periodo") or ""
            years_found = re.findall(r'(20[012]\d)', str(p))
            if years_found:
                year = max(int(y) for y in years_found)
                by_year.setdefault(year, []).append(c)
            else:
                no_year.append(c)

        # Para cada año, elegir la mejor carta (mas rica + bonus fin de año)
        result = []
        for year in sorted(by_year.keys()):
            candidates = by_year[year]
            best = max(candidates,
                       key=lambda c: richness(c) + end_of_year_bonus(c))
            result.append(best)

        return result

    # ══════════════════════════════════════════════════════════════════════
    # RUN
    # ══════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"LettersCollector {self.isin} - {self.fund_name}")

        all_cartas: list[dict] = []
        fuentes: list[str] = []

        # ── PASO 1: PDFs de discovery ──
        docs = self._get_discovery_docs()
        self._log("INFO", f"Discovery: {len(docs)} docs con commentary potencial")

        primary_docs = [d for d in docs if d.get("doc_type") in
                        ("quarterly_letter", "manager_presentation")]
        secondary_docs = [d for d in docs if d.get("doc_type") in
                          ("semi_annual_report", "annual_report")]
        factsheet_docs = [d for d in docs if d.get("doc_type") == "factsheet"]

        # ── PASO 2: Extraer de PDFs primarios (cartas/presentations) ──
        for doc in primary_docs:
            local = doc.get("local_path", "")
            if not local:
                continue
            text = self._extract_pdf_text(local)
            if not text:
                continue

            periodo = doc.get("periodo", "")
            extracted = self._extract_commentary(text, doc["doc_type"], periodo)
            for carta in extracted:
                carta["fuente_tipo"] = "pdf_discovery"
                carta["doc_type"] = doc["doc_type"]
                if not carta.get("periodo"):
                    carta["periodo"] = periodo
            all_cartas.extend(extracted)
            fuentes.append(doc.get("url", local))
            self._log("INFO", f"  [{doc['doc_type']}] {periodo}: "
                      f"{len(extracted)} periodos extraidos")

        # ── PASO 3: SIEMPRE extraer de reports (balance anual del gestor) ──
        # Los annual/semi-annual reports contienen el balance del gestor
        # sobre el ano — esto es contenido de "carta" aunque no sea un doc separado
        for doc in secondary_docs[:4]:
            local = doc.get("local_path", "")
            if not local:
                continue
            text = self._extract_pdf_text(local, max_pages=60)
            if not text:
                continue

            periodo = doc.get("periodo", "")
            extracted = self._extract_commentary(text, doc["doc_type"], periodo)
            for carta in extracted:
                carta["fuente_tipo"] = "pdf_discovery"
                carta["doc_type"] = doc["doc_type"]
                if not carta.get("periodo"):
                    carta["periodo"] = periodo
            all_cartas.extend(extracted)
            if extracted:
                fuentes.append(doc.get("url", local))
            self._log("INFO", f"  [{doc['doc_type']}] {periodo}: "
                      f"{len(extracted)} periodos")

        # ── PASO 4: Factsheets con commentary ──
        # Muchas gestoras incluyen 1-2 parrafos del gestor en factsheets
        covered = self._get_covered_years(all_cartas)
        for doc in factsheet_docs[:6]:
            local = doc.get("local_path", "")
            if not local:
                continue
            # Solo si cubre un ano sin carta
            periodo = doc.get("periodo", "")
            doc_year = None
            ym = re.search(r'(20[12]\d)', str(periodo))
            if ym:
                doc_year = int(ym.group(1))
            if doc_year and doc_year in covered:
                continue  # ya tenemos carta para este ano

            text = self._extract_pdf_text(local, max_pages=5)
            if not text:
                continue

            extracted = self._extract_commentary(text, "factsheet", periodo)
            for carta in extracted:
                carta["fuente_tipo"] = "pdf_factsheet"
                carta["doc_type"] = "factsheet"
                if not carta.get("periodo"):
                    carta["periodo"] = periodo
            all_cartas.extend(extracted)
            if extracted:
                fuentes.append(doc.get("url", local))
                covered = self._get_covered_years(all_cartas)

        # ── PASO 5: Aprender patrones y buscar anos faltantes ──
        covered = self._get_covered_years(all_cartas)
        target_years = set(range(self.anio_creacion, self.current_year + 1))
        missing_years = sorted(target_years - covered)
        patterns = self._learn_doc_patterns(docs)

        if missing_years:
            self._log("INFO", f"Cobertura: {sorted(covered)} | "
                      f"Faltan: {missing_years}")

            if patterns["doc_names"]:
                self._log("INFO", f"Patron aprendido: {patterns['doc_names']}")
            if patterns["gestora_domain"]:
                self._log("INFO", f"Dominio gestora: {patterns['gestora_domain']}")

            web_cartas = await self._search_missing_years(missing_years, patterns)
            for carta in web_cartas:
                carta["fuente_tipo"] = "web"
            all_cartas.extend(web_cartas)
            self._log("INFO", f"Web search: {len(web_cartas)} cartas adicionales")

        # ── PASO 5b: Documentos historicos (Wayback + URL extrapolation) ──
        covered = self._get_covered_years(all_cartas)
        still_missing = sorted(target_years - covered)
        if still_missing:
            self._log("INFO", f"Buscando docs historicos para {len(still_missing)} anos...")
            hist_cartas = await self._search_historical_docs(still_missing, patterns)
            all_cartas.extend(hist_cartas)
            self._log("INFO", f"Docs historicos: {len(hist_cartas)} cartas")

        # ── PASO 6: Cartas globales del gestor/gestora si aun faltan anos ──
        covered = self._get_covered_years(all_cartas)
        still_missing = sorted(target_years - covered)
        if still_missing:
            self._log("INFO", f"Aun faltan {len(still_missing)} anos. "
                      f"Buscando cartas globales del gestor/gestora...")
            global_cartas = await self._search_gestora_global_letters(
                still_missing, patterns)
            all_cartas.extend(global_cartas)
            self._log("INFO", f"Cartas globales: {len(global_cartas)} adicionales")

        # ── Merge con existentes (NUNCA perder cartas entre runs) + dedup ──
        all_cartas = self._merge_with_existing_letters(all_cartas)
        cartas_dedup = self._dedup_by_periodo(all_cartas)

        # ── Report final ──
        final_covered = self._get_covered_years(cartas_dedup)
        final_missing = sorted(target_years - final_covered)

        output = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "generated": datetime.now().isoformat(),
            "num_cartas": len(cartas_dedup),
            "periodos_cubiertos": sorted(list({c.get("periodo", "") for c in cartas_dedup
                                               if c.get("periodo")})),
            "anos_cubiertos": sorted(final_covered),
            "anos_sin_carta": final_missing,
            "cartas": sorted(cartas_dedup,
                             key=lambda c: c.get("periodo") or "",
                             reverse=True),
            "fuentes": fuentes,
        }

        self._log("OK", f"Total: {len(cartas_dedup)} cartas, "
                  f"anos cubiertos: {sorted(final_covered)}, "
                  f"sin carta: {final_missing}")

        # Registrar PDFs descargados en discovery para que extractor los procese
        self._register_new_pdfs_in_discovery()

        return self._save(output)

    def _register_new_pdfs_in_discovery(self):
        """Registrar PDFs descargados por Wayback en intl_discovery_data.json
        para que el EXTRACTOR v3 los procese (datos cuantitativos: AUM, posiciones).

        IMPORTANTE: se registran como 'annual_report' (para el extractor),
        NO como 'quarterly_letter'. El letters_collector ya extrajo el commentary.
        Source='letters_collector_wayback' para que letters NO los reprocese.
        """
        disc_path = self.fund_dir / "intl_discovery_data.json"
        disc_dir = self.fund_dir / "raw" / "discovery"
        if not disc_path.exists() or not disc_dir.exists():
            return

        try:
            disc = json.loads(disc_path.read_text(encoding="utf-8"))
        except Exception:
            return

        # Comprobar que no estan ya registrados (por local_path O por filename)
        existing_paths = {d.get("local_path", "") for d in disc.get("documents", [])}
        existing_fnames = {Path(d.get("local_path", "")).name
                           for d in disc.get("documents", [])}
        new_pdfs = sorted(disc_dir.glob("_web_*.pdf"))
        added = 0

        for pdf in new_pdfs:
            if str(pdf) in existing_paths or pdf.name in existing_fnames:
                continue

            periodo = ""
            ym = re.search(r'(20[012]\d)', pdf.name.lower())
            if ym:
                periodo = ym.group(1)

            disc["documents"].append({
                "doc_type": "annual_report",  # para EXTRACTOR, no para letters
                "periodo": periodo,
                "url": f"wayback://letters_collector/{pdf.name}",
                "local_path": str(pdf),
                "source": "letters_collector_wayback",
                "content_type": "pdf",
                "size_bytes": pdf.stat().st_size,
                "validated": True,
            })
            added += 1

        if added:
            disc_path.write_text(
                json.dumps(disc, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._log("INFO", f"Registrados {added} PDFs historicos en discovery "
                      f"para extractor")

    def _merge_with_existing_letters(self, new_cartas: list[dict]) -> list[dict]:
        """Merge con cartas existentes — NUNCA perder cartas entre runs."""
        existing_path = self.fund_dir / "letters_data.json"
        if not existing_path.exists():
            return new_cartas
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
        except Exception:
            return new_cartas

        # Indexar existentes por periodo normalizado
        by_periodo: dict[str, dict] = {}
        for c in existing.get("cartas", []):
            p = self._normalize_periodo(c.get("periodo"))
            if p:
                by_periodo[p] = c

        # Añadir nuevas (o reemplazar si tienen más contenido)
        def _richness(carta):
            score = 0
            for k in ("contexto_mercado", "tesis_gestora", "decisiones_tomadas",
                      "resultado_real", "outlook"):
                v = carta.get(k) or ""
                if len(v) > 20:
                    score += len(v)
            return score

        for c in new_cartas:
            p = self._normalize_periodo(c.get("periodo"))
            if not p:
                continue
            existing_c = by_periodo.get(p)
            if existing_c:
                if _richness(c) > _richness(existing_c):
                    by_periodo[p] = c
            else:
                by_periodo[p] = c

        merged = list(by_periodo.values())
        if len(merged) > len(new_cartas):
            self._log("INFO", f"Merge cartas: {len(new_cartas)} nuevas + "
                      f"{len(merged) - len(new_cartas)} preservadas = "
                      f"{len(merged)} total")
        return merged

    def _save(self, data: dict) -> dict:
        path = self.fund_dir / "letters_data.json"
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        self._log("OK", f"Guardado: {path}")
        return data


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--fund-name", default="")
    parser.add_argument("--gestora", default="")
    parser.add_argument("--anio-creacion", type=int, default=None)
    args = parser.parse_args()

    agent = LettersCollector(args.isin, fund_name=args.fund_name,
                             gestora=args.gestora, anio_creacion=args.anio_creacion)
    result = asyncio.run(agent.run())
    print(f"\nCartas: {result['num_cartas']}")
    print(f"Anos cubiertos: {result['anos_cubiertos']}")
    print(f"Anos sin carta: {result['anos_sin_carta']}")
    for c in result["cartas"][:5]:
        p = c.get("periodo", "?")
        tesis = (c.get("tesis_gestora") or "")[:80]
        print(f"  [{p}] {tesis}")
