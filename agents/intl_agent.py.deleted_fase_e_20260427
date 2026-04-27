"""
International Agent — Fondos de gestoras extranjeras (LU, IE, FR, GB, DE…)

Flujo por fondo:
  1. Localizar annual report PDF en web de la gestora
     → Múltiples estrategias de scraping + fallback a ruta local
  2. Descargar PDF (o reutilizar si ya está en disco)
  3. Parsear TOC (pp. 2-8) → calcular offset PDF
  4. Extraer 3 secciones del fondo:
       A. Directors' Report  (cualitativo + tabla performance)
       B. Statistics         (NAV / nº acciones por clase, últimos 3 años)
       C. Financial Statements (AUM total, top holdings, breakdowns país/sector)
  5. Para histórico → repetir con annual reports de años anteriores

Patrón documentado: DNCA INVEST Annual Report Dec 2024, 522 págs, 25 subfondos
  - Directors' Report: pp. 9-59 del doc (~2 págs/fondo)
  - Statistics:        pp. 64-73
  - Fund Statements:   desde TOC + offset (+2 para DNCA)
  - Offset:            TOC dice p.134 → PDF real p.136 → offset = +2
"""
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_bytes
from tools.pdf_extractor import (
    parse_toc,
    calculate_pdf_offset,
    extract_page_range,
    extract_pages_by_keyword,
    get_pdf_metadata,
    find_fund_in_toc,
)
from tools.claude_extractor import (
    extract_structured_data,
    extract_performance_table,
    extract_top_holdings,
    extract_portfolio_breakdown,
)

console = Console()

# ── Rangos de búsqueda documentados para SICAVs luxemburguesas ───────────────
DIRECTORS_REPORT_RANGE = (8, 60)    # páginas 0-indexed donde buscar Directors' Report
STATISTICS_RANGE       = (63, 75)   # páginas donde buscar Statistics
MAX_STATEMENTS_PAGES   = 12         # máx páginas a extraer de Financial Statements por fondo

# Keywords para localizar secciones dentro del PDF
DIRECTORS_KEYWORDS  = ["directors' report", "investment manager's report",
                        "fund manager's report", "management report"]
STATISTICS_KEYWORDS = ["statistics", "net asset value per share",
                        "number of shares outstanding", "estadísticas"]
STATEMENTS_KEYWORDS = ["securities portfolio", "statement of net assets",
                        "top ten holdings", "portfolio breakdown",
                        "country breakdown", "sector breakdown"]

# ── Estrategias de búsqueda del annual report por prefijo ISIN ───────────────
# Cada entrada: función async que recibe (gestora_slug, year) → URL | None
# Se prueban en orden hasta encontrar un PDF válido.

_GESTORA_SEARCH_STRATEGIES: dict[str, list] = {}  # poblado con register_strategy()


def register_strategy(prefijo: str):
    """Decorador para registrar estrategias de búsqueda por prefijo ISIN."""
    def decorator(fn):
        _GESTORA_SEARCH_STRATEGIES.setdefault(prefijo, []).append(fn)
        return fn
    return decorator


# ── Estrategias DNCA (LU) ────────────────────────────────────────────────────

@register_strategy("LU")
async def _find_dnca_annual_report(gestora_slug: str, year: int) -> str | None:
    """
    Busca el annual report de gestoras LU en la web de DNCA.
    Intenta varias URLs conocidas del CDN de Natixis/DNCA.
    """
    if "dnca" not in gestora_slug.lower():
        return None

    # Patrón CDN de Natixis (dueña de DNCA) — observado en múltiples gestoras LU
    candidates = [
        f"https://www.dnca-investments.com/uploads/dnca-invest-annual-report-{year}.pdf",
        f"https://www.dnca-investments.com/uploads/dnca-invest-annual-accounts-{year}.pdf",
        f"https://www.dnca-investments.com/uploads/DNCA-INVEST-Annual-Report-{year}.pdf",
    ]
    import httpx
    for url in candidates:
        try:
            # HEAD con verificación de Content-Type para evitar falsos positivos
            # (CDNs de SPAs devuelven 200+HTML para todas las rutas)
            async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
                r = await client.head(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                })
                ct = r.headers.get("content-type", "")
                if r.status_code == 200 and "pdf" in ct.lower():
                    console.log(f"[green]PDF encontrado en CDN: {url}")
                    return url
        except Exception:
            continue
    return None


@register_strategy("LU")
async def _scrape_gestora_documents_page(gestora_slug: str, year: int) -> str | None:
    """
    Scraping genérico de la página de documentos de la gestora.
    Busca links con 'annual' y el año en href o texto del link.
    """
    # Construir URL base de la gestora desde el slug
    slug_clean = gestora_slug.lower().replace(" ", "-").replace("_", "-")
    # Intentar con el patrón más común para gestoras LU
    base_urls = [
        f"https://www.{slug_clean}.com/en/documents",
        f"https://www.{slug_clean}.com/documents",
        f"https://www.{slug_clean}.lu/documents",
    ]

    annual_kws = re.compile(
        rf'annual.{{0,30}}{year}|{year}.{{0,30}}annual|rapport.annuel.{year}',
        re.IGNORECASE
    )

    for base in base_urls:
        try:
            html = await get(base)
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                combined = href + " " + text
                if annual_kws.search(combined) and (
                    ".pdf" in href.lower() or "download" in href.lower()
                ):
                    full = href if href.startswith("http") else urljoin(base, href)
                    console.log(f"[green]PDF encontrado via scraping: {full}")
                    return full
        except Exception:
            continue
    return None


# ── Agent principal ──────────────────────────────────────────────────────────

class IntlAgent:
    """
    Agente para fondos internacionales (ISIN prefijo LU, IE, FR, GB, DE…).
    Clase con async def run() -> dict según convenio del proyecto.
    """

    def __init__(self, isin: str, config: dict):
        self.isin = isin.strip().upper()
        self.config = config
        self.current_year = datetime.now().year
        self.isin_prefix = self.isin[:2]

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.reports_dir = self.fund_dir / "raw" / "reports"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.kb_path = Path(__file__).parent.parent / "data" / "regulators_knowledge.json"

    # ── Knowledge base ────────────────────────────────────────────────────────

    def _load_kb(self) -> dict:
        """Carga el knowledge base de reguladores desde disco."""
        try:
            return json.loads(self.kb_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_successful_url(self, gestora_slug: str, url: str) -> None:
        """Guarda una URL que funcionó para esta gestora en el knowledge base."""
        try:
            kb = self._load_kb()
            prefix_data = kb.get(self.isin_prefix, {})
            successful = prefix_data.get("successful_gestora_urls", {})
            successful[gestora_slug] = url
            prefix_data["successful_gestora_urls"] = successful
            kb[self.isin_prefix] = prefix_data
            self.kb_path.write_text(json.dumps(kb, ensure_ascii=False, indent=2), encoding="utf-8")
            console.log(f"[dim]Knowledge base actualizado: {gestora_slug} -> {url}")
        except Exception as exc:
            console.log(f"[yellow]Error actualizando knowledge base: {exc}")

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> dict:
        """Orquesta el pipeline y devuelve intl_data completo."""
        console.print(Panel(
            f"[bold cyan]Intl Agent[/bold cyan]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        result: dict = {
            "isin": self.isin,
            "tipo": "INT",
            "ultima_actualizacion": datetime.now().isoformat(),
            "kpis": {},
            "cualitativo": {},
            "cuantitativo": {
                "serie_aum": [],
                "serie_participes": [],
                "serie_ter": [],
                "serie_rentabilidad": [],
                "mix_activos_historico": [],
                "mix_geografico_historico": [],
            },
            "posiciones": {"actuales": [], "historicas": []},
            "fuentes": {"informes_descargados": [], "urls_consultadas": []},
        }

        # Metadatos básicos del fondo
        nombre, gestora = await self._get_fund_metadata()
        result["nombre"] = nombre
        result["gestora"] = gestora

        # Años a procesar según horizonte
        years = self._years_to_process()
        console.log(f"[blue]Años a procesar: {years}")

        with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                      console=console, transient=True) as progress:
            task = progress.add_task("Procesando annual reports...", total=len(years))

            for year in years:
                progress.update(task, description=f"Annual report {year}...")
                await self._process_annual_report(year, gestora, result)
                progress.advance(task)

        self._save(result)
        return result

    # ── Metadata ─────────────────────────────────────────────────────────────

    async def _get_fund_metadata(self) -> tuple[str, str]:
        """
        Obtiene nombre del fondo y nombre de la gestora.
        Prioridad:
          1. intl_data.json existente (ya extraído en ejecución previa)
          2. Config manual (nombre/gestora en config)
          3. JSON de fondos de DNCA
        """
        # 1. Leer intl_data.json previo si existe
        prev_path = self.fund_dir / "intl_data.json"
        if prev_path.exists():
            try:
                prev = json.loads(prev_path.read_text(encoding="utf-8"))
                nombre = prev.get("nombre", "")
                gestora = prev.get("gestora", "")
                if nombre and gestora:
                    console.log(f"[green]Metadata desde intl_data.json previo: {nombre} / {gestora}")
                    return nombre, gestora
            except Exception:
                pass

        # 2. Config manual
        if self.config.get("nombre") and self.config.get("gestora"):
            return self.config["nombre"], self.config["gestora"]

        # 3. JSON de DNCA (recorre estructura anidada de categorías)
        try:
            json_url = "https://www.dnca-investments.com/assets/json/funds_INT_en.json"
            html = await get(json_url)
            data = json.loads(html)

            def _search(node):
                if isinstance(node, list):
                    for item in node:
                        r = _search(item)
                        if r:
                            return r
                elif isinstance(node, dict):
                    shares = node.get("shares", [])
                    if any(s.get("isin_code", "").upper() == self.isin for s in shares):
                        return node.get("fund_name", ""), "DNCA Investments"
                    for v in node.values():
                        r = _search(v)
                        if r:
                            return r
                return None

            found = _search(data)
            if found:
                nombre, gestora = found
                console.log(f"[green]Metadata DNCA JSON: {nombre} / {gestora}")
                return nombre, gestora
        except Exception:
            pass

        # 4. Último fallback — vacíos con advertencia
        nombre = self.config.get("nombre", "")
        gestora = self.config.get("gestora", "")
        if not nombre:
            console.log(f"[yellow]Nombre no encontrado para {self.isin}. "
                        f"Añade 'nombre' y 'gestora' en config.")
        return nombre, gestora

    # ── Años ─────────────────────────────────────────────────────────────────

    def _years_to_process(self) -> list[int]:
        """
        Devuelve lista de años a procesar según config horizonte_historico.

        El annual report se nombra por el año fiscal que cubre (cierre diciembre).
        Para un fondo con FY ending Dec 2024 el fichero se llama *-2024.pdf.
        Intentamos current_year-1 Y current_year-2 para el informe más reciente.
        """
        horizonte = self.config.get("horizonte_historico", "1")
        # El informe más reciente puede ser del año pasado o del antepenúltimo
        # según la fecha de publicación → incluir ambos como candidatos
        latest = self.current_year - 1   # probablemente aún no publicado si <mid-year
        latest2 = self.current_year - 2  # fiscal year cerrado, siempre disponible

        if horizonte == "4":
            return [latest, latest2]      # intentar los dos, el agente usa el que exista
        elif horizonte == "3":
            return [latest, latest2, latest2 - 1, latest2 - 2]
        elif horizonte == "2":
            return [latest, latest2, latest2 - 1, latest2 - 2, latest2 - 3, latest2 - 4]
        else:
            return [latest] + list(range(latest2, latest2 - 6, -1))

    # ── Procesar un annual report ─────────────────────────────────────────────

    async def _process_annual_report(self, year: int, gestora: str, result: dict) -> None:
        """Descarga y extrae datos de un annual report para el año dado."""
        pdf_path = await self._get_pdf(year, gestora)
        if not pdf_path:
            console.log(f"[yellow]No se pudo obtener el PDF del annual report {year}")
            return

        result["fuentes"]["informes_descargados"].append(pdf_path.name)
        meta = get_pdf_metadata(str(pdf_path))
        console.log(
            f"[blue]{pdf_path.name}: {meta['num_pages']} págs, "
            f"{meta['file_size_mb']} MB"
        )

        if meta["num_pages"] < 10:
            console.log(f"[yellow]PDF {year} demasiado corto ({meta['num_pages']} págs) — posible error")
            return

        # Paso 3: TOC + offset
        toc = parse_toc(str(pdf_path))
        offset = calculate_pdf_offset(str(pdf_path), toc) if toc else 0

        # Localizar el fondo en el TOC
        fund_name = result.get("nombre", "")
        toc_entry = find_fund_in_toc(toc, fund_name=fund_name, isin=self.isin)
        console.log(f"[blue]TOC entry: {toc_entry}")

        # Paso 4A: Directors' Report
        objetivo = self.config.get("objetivo", "1")
        if objetivo not in ("2", "4") and not result["cualitativo"]:
            cualitativo = self._extract_directors_report(str(pdf_path), toc_entry, offset)
            if cualitativo:
                result["cualitativo"] = cualitativo

        # Paso 4B: Statistics → series NAV/rentabilidad (3 años por report)
        if objetivo not in ("3",):
            self._extract_statistics(str(pdf_path), toc_entry, offset, year, result)

        # Paso 4C: Financial Statements → AUM, top holdings, breakdowns
        if objetivo not in ("3",):
            self._extract_financial_statements(str(pdf_path), toc_entry, offset, year, result)

    # ── Obtener PDF ───────────────────────────────────────────────────────────

    async def _get_pdf(self, year: int, gestora: str) -> Path | None:
        """
        Obtiene la ruta local al PDF del annual report para el año dado.

        Orden de preferencia:
        1. Ruta local ya descargada (cualquier PDF del año en reports_dir)
        2. pdf_url en config (el usuario la especificó)
        3. Estrategias de scraping registradas por prefijo ISIN
        """
        filename = f"{gestora.replace(' ', '_')}_{year}_annual_report.pdf"
        local = self.reports_dir / filename

        # 1. Ya existe en disco (nombre exacto o cualquier PDF del año)
        if local.exists() and local.stat().st_size > 50_000:
            console.log(f"[dim]PDF ya existe: {filename}")
            return local

        # Buscar cualquier PDF del año en el directorio
        for p in self.reports_dir.glob(f"*{year}*.pdf"):
            if p.stat().st_size > 50_000:
                console.log(f"[dim]PDF {year} encontrado en disco: {p.name}")
                return p

        # 1b. Probar URLs exitosas del knowledge base (aprendizaje acumulado)
        kb = self._load_kb()
        kb_urls = kb.get(self.isin_prefix, {}).get("successful_gestora_urls", {})
        gestora_slug_clean = gestora.lower().replace(" ", "-")
        if gestora_slug_clean in kb_urls:
            kb_url = kb_urls[gestora_slug_clean]
            # Adaptar la URL al año actual si tiene el año en el path
            if str(year) in kb_url or str(year - 1) in kb_url:
                console.log(f"[blue]Probando URL del knowledge base: {kb_url}")
                result_kb = await self._download_pdf(kb_url, local)
                if result_kb:
                    return result_kb

        # 2. URL en config
        if self.config.get("pdf_url"):
            url = self.config["pdf_url"]
            console.log(f"[blue]Descargando PDF desde config url: {url}")
            result = await self._download_pdf(url, local)
            if result:
                self._save_successful_url(gestora_slug_clean, url)
                return result

        # 3. Estrategias por prefijo
        gestora_slug = gestora.lower().replace(" ", "-")
        strategies = _GESTORA_SEARCH_STRATEGIES.get(self.isin_prefix, [])

        for strategy in strategies:
            try:
                url = await strategy(gestora_slug, year)
                if url:
                    result = await self._download_pdf(url, local)
                    if result:
                        self._save_successful_url(gestora_slug, url)
                        return result
            except Exception as exc:
                console.log(f"[yellow]Estrategia fallida: {exc}")

        console.log(
            f"[yellow]PDF {year} no encontrado. Descárgalo manualmente en:\n"
            f"  {local}"
        )
        return None

    async def _download_pdf(self, url: str, target: Path) -> Path | None:
        """Descarga un PDF y lo guarda en target. Verifica que sea PDF válido."""
        try:
            console.log(f"[blue]Descargando PDF: {url}")
            data = await get_bytes(url)
            if not (data[:4] == b"%PDF" or b"%PDF" in data[:20]):
                console.log(f"[yellow]Respuesta no es PDF válido (primeros bytes: {data[:20]})")
                return None
            target.write_bytes(data)
            size_mb = round(len(data) / 1_048_576, 2)
            console.log(f"[green]PDF descargado: {target.name} ({size_mb} MB)")
            return target
        except Exception as exc:
            console.log(f"[yellow]Error descargando {url}: {exc}")
            return None

    # ── Paso 4A: Directors' Report ────────────────────────────────────────────

    def _extract_directors_report(
        self, pdf_path: str, toc_entry: dict, offset: int
    ) -> dict:
        """
        Extrae datos cualitativos del Directors' Report.

        Estrategia:
        1. Si el fondo está en el TOC, buscar por keyword en un rango estrecho
           centrado en la página del fondo (±5 págs del doc_page + offset)
        2. Si no, buscar por keyword en el rango global DIRECTORS_REPORT_RANGE
        """
        fund_doc_page = toc_entry.get("doc_page")

        if fund_doc_page:
            center = fund_doc_page + offset
            search_range = (max(0, center - 2), center + 8)
        else:
            search_range = DIRECTORS_REPORT_RANGE

        text = extract_pages_by_keyword(
            pdf_path,
            keywords=DIRECTORS_KEYWORDS,
            context_pages=1,
            search_range=search_range,
        )

        if not text.strip():
            # Ampliar búsqueda
            text = extract_pages_by_keyword(
                pdf_path, keywords=DIRECTORS_KEYWORDS,
                context_pages=2, search_range=DIRECTORS_REPORT_RANGE,
            )

        if not text.strip():
            console.log("[yellow]Directors' Report: texto no encontrado")
            return {}

        # Filtrar al fondo específico si hay varios en el texto
        text = self._filter_to_fund(text)

        schema = {
            "estrategia": "descripción de la estrategia de inversión del fondo",
            "filosofia_inversion": "filosofía y proceso de inversión",
            "tipo_activos": "tipos de activos (renta fija, variable, mixto...)",
            "objetivos_reales": "objetivos de rentabilidad o de gestión declarados",
            "proceso_seleccion": "proceso de selección de activos",
            "historia_fondo": "contexto o historia relevante si se menciona",
            "benchmark": "benchmark del fondo si se menciona",
            "clasificacion_sfdr": "clasificación SFDR (artículo 6, 8 o 9) si se menciona",
            "outlook": "perspectivas o outlook para el próximo periodo",
            "gestores": [
                {
                    "nombre": "nombre del gestor",
                    "cargo": "cargo o rol",
                    "background": "trayectoria mencionada (vacío si no hay)",
                    "anio_incorporacion": None,
                }
            ],
        }

        try:
            result = extract_structured_data(
                text, schema,
                context=(
                    f"Directors' Report del fondo {self.isin} en annual report SICAV luxemburguesa. "
                    "Extrae datos cualitativos del fondo específico."
                ),
            )
            return result if isinstance(result, dict) else {}
        except Exception as exc:
            console.log(f"[yellow]Error Claude Directors' Report: {exc}")
            return {}

    # ── Paso 4B: Statistics ───────────────────────────────────────────────────

    def _extract_statistics(
        self, pdf_path: str, toc_entry: dict, offset: int, year: int, result: dict
    ) -> None:
        """
        Extrae NAV y número de acciones por clase de los últimos 3 años.
        La sección Statistics suele estar en pp. 64-73 del doc (global al SICAV).
        """
        # Buscar la sección Statistics con keyword
        fund_name = result.get("nombre", "")
        text = extract_pages_by_keyword(
            pdf_path,
            keywords=STATISTICS_KEYWORDS + [fund_name] if fund_name else STATISTICS_KEYWORDS,
            context_pages=1,
            search_range=STATISTICS_RANGE,
        )

        if not text.strip():
            text = extract_pages_by_keyword(
                pdf_path, keywords=STATISTICS_KEYWORDS,
                context_pages=1,
            )

        if not text.strip():
            console.log("[yellow]Statistics: texto no encontrado")
            return

        text = self._filter_to_fund(text)

        clase_filtro = self.config.get("clase_accion", "").strip()

        schema = {
            "series": [
                {
                    "anio": "año (entero)",
                    "clase": "nombre de la clase de acción (ej. 'A EUR', 'I EUR')",
                    "nav_por_accion": "NAV por acción (número)",
                    "num_acciones": "número de acciones en circulación (número)",
                    "nav_total_eur": "NAV total de esa clase en EUR (número, puede ser null)",
                }
            ]
        }

        try:
            extracted = extract_structured_data(
                text, schema,
                context=(
                    f"Sección Statistics del annual report SICAV, fondo {self.isin}. "
                    f"Clase de acción de interés: {clase_filtro or 'todas'}."
                ),
            )
        except Exception as exc:
            console.log(f"[yellow]Error Claude Statistics: {exc}")
            return

        series_raw = extracted.get("series", []) if isinstance(extracted, dict) else []

        # Filtrar por clase si se especificó
        if clase_filtro and clase_filtro.lower() != "todas":
            series_raw = [
                s for s in series_raw
                if clase_filtro.lower() in (s.get("clase") or "").lower()
            ]

        for entry in series_raw:
            anio = entry.get("anio")
            periodo = str(anio) if anio else str(year)
            nav = entry.get("nav_total_eur")
            nav_por = entry.get("nav_por_accion")

            if nav and nav > 0:
                # Convertir a M€
                result["cuantitativo"]["serie_aum"].append({
                    "periodo": periodo,
                    "clase": entry.get("clase", ""),
                    "valor_meur": round(nav / 1_000_000, 4),
                })

            if nav_por:
                result["cuantitativo"]["serie_rentabilidad"].append({
                    "periodo": periodo,
                    "clase": entry.get("clase", ""),
                    "nav_por_accion": nav_por,
                })

        # Intentar también tabla de performance (rentabilidad % anual vs benchmark)
        perf_text = extract_pages_by_keyword(
            pdf_path,
            keywords=["performance", "return", "benchmark", "rentabilidad"],
            context_pages=0,
            search_range=(max(0, (toc_entry.get("doc_page", 30) or 30) + offset - 2),
                          (toc_entry.get("doc_page", 30) or 30) + offset + 6),
        ) if toc_entry.get("doc_page") else ""

        if perf_text.strip():
            try:
                perfs = extract_performance_table(self._filter_to_fund(perf_text))
                for p in perfs:
                    result["cuantitativo"]["serie_rentabilidad"].append({
                        "periodo": str(p.get("anio", year)),
                        "clase": p.get("clase", ""),
                        "rentabilidad_pct": p.get("rentabilidad_pct"),
                        "benchmark_pct": p.get("benchmark_pct"),
                    })
            except Exception as exc:
                console.log(f"[yellow]Error extrayendo tabla performance: {exc}")

    # ── Paso 4C: Financial Statements ────────────────────────────────────────

    def _extract_financial_statements(
        self, pdf_path: str, toc_entry: dict, offset: int, year: int, result: dict
    ) -> None:
        """
        Extrae de Financial Statements:
        - AUM total del fondo
        - Top 10 holdings con pesos
        - Breakdown por país y sector
        """
        if toc_entry.get("doc_page"):
            # Usar TOC + offset para ir directo a las páginas del fondo
            start_pdf = toc_entry["doc_page"] + offset
            end_pdf = start_pdf + MAX_STATEMENTS_PAGES
        else:
            # Búsqueda por keyword en todo el doc
            start_pdf = 0
            end_pdf = 0  # señal de búsqueda por keyword

        if start_pdf > 0:
            text = extract_page_range(pdf_path, start_pdf, end_pdf)
        else:
            text = extract_pages_by_keyword(
                pdf_path, keywords=STATEMENTS_KEYWORDS, context_pages=1
            )

        if not text.strip():
            console.log("[yellow]Financial Statements: texto no encontrado")
            return

        text = self._filter_to_fund(text)

        # AUM total (extraer con regex primero — más barato)
        aum_eur = self._parse_aum_from_text(text)
        if aum_eur:
            result["cuantitativo"]["serie_aum"].append({
                "periodo": str(year),
                "clase": "total",
                "valor_meur": round(aum_eur / 1_000_000, 4),
            })
            result["kpis"]["aum_actual_meur"] = round(aum_eur / 1_000_000, 4)

        # Top holdings con Claude
        try:
            holdings = extract_top_holdings(text)
            if holdings:
                result["posiciones"]["actuales"] = holdings
                result["posiciones"]["historicas"].append({
                    "periodo": str(year),
                    "top10": holdings[:10],
                })
        except Exception as exc:
            console.log(f"[yellow]Error extrayendo holdings: {exc}")

        # Portfolio breakdown con Claude
        try:
            breakdown = extract_portfolio_breakdown(text)
            if breakdown.get("por_pais"):
                result["cuantitativo"]["mix_geografico_historico"].append({
                    "periodo": str(year),
                    "zonas": {z["pais"]: z["pct"] for z in breakdown["por_pais"]},
                })
            if breakdown.get("por_sector"):
                result["cuantitativo"]["mix_activos_historico"].append({
                    "periodo": str(year),
                    "sectores": {s["sector"]: s["pct"] for s in breakdown["por_sector"]},
                })
        except Exception as exc:
            console.log(f"[yellow]Error extrayendo breakdown: {exc}")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _filter_to_fund(self, text: str) -> str:
        """
        Intenta aislar la sección del fondo específico dentro de un texto
        que puede contener múltiples fondos del SICAV.

        Si el ISIN o el nombre del fondo aparece en el texto, recorta
        desde la primera mención hasta la siguiente mención de otro fondo
        o hasta el final si no hay más fondos.
        """
        if not text.strip():
            return text

        # Markers a buscar (ISIN y/o nombre en mayúsculas)
        markers = [self.isin]

        # Si el ISIN no aparece, devolver texto íntegro
        start_pos = text.upper().find(self.isin.upper())
        if start_pos == -1:
            return text

        # Buscar el siguiente ISIN (de otro fondo LU/IE/FR) para truncar
        next_isin = re.search(
            r'\b(LU|IE|FR|GB|DE)\d{10}\b',
            text[start_pos + len(self.isin):],
            re.IGNORECASE
        )

        if next_isin:
            end_pos = start_pos + len(self.isin) + next_isin.start()
        else:
            end_pos = len(text)

        # Extender un poco hacia atrás para capturar el header del fondo
        extended_start = max(0, start_pos - 500)
        return text[extended_start:end_pos]

    def _parse_aum_from_text(self, text: str) -> float | None:
        """
        Extrae el AUM total del fondo con regex.
        Busca patrones como "Total net assets  14,678,372,897.63"
        o "Net Assets  14 678 372 897,63 EUR".
        """
        patterns = [
            # "Total net assets" seguido de número grande
            r'(?:total\s+net\s+assets|total\s+activos\s+netos)[^\d]{0,30}'
            r'([\d]{1,3}(?:[,\.\s]\d{3})+(?:[,\.]\d{2})?)',
            # Número con 10+ dígitos (típico AUM en EUR)
            r'([\d]{1,3}(?:,\d{3}){3,}(?:\.\d{1,2})?)',
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").replace(" ", "").replace(".", "")
                # Si el número parece demasiado grande (>1 billón) o pequeño, ignorar
                try:
                    val = float(raw)
                    if 1_000_000 < val < 500_000_000_000:
                        return val
                except ValueError:
                    continue
        return None

    def _save(self, result: dict) -> None:
        """Guarda el resultado en intl_data.json."""
        output_path = self.fund_dir / "intl_data.json"
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path}")


# ── Ejecución standalone ─────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    config = {
        "objetivo": "1",
        "horizonte_historico": "4",          # Solo último año para la demo
        "fuentes": "1",
        "clase_accion": "I EUR",
        "contexto_adicional": "",
        # Nombre y gestora conocidos del CLAUDE.md para evitar búsqueda
        "nombre": "DNCA INVEST - ALPHA BONDS",
        "gestora": "DNCA Investments",
        # URL directa del annual report (si se conoce, evita scraping)
        # "pdf_url": "https://...",
    }

    ISIN = "LU1694789451"
    agent = IntlAgent(ISIN, config)
    result = asyncio.run(agent.run())

    console.print(Panel(
        f"[bold green]Análisis completado[/bold green]\n"
        f"Nombre: {result.get('nombre', '-')}\n"
        f"Gestora: {result.get('gestora', '-')}\n"
        f"PDFs descargados: {len(result.get('fuentes', {}).get('informes_descargados', []))}\n"
        f"Puntos AUM: {len(result.get('cuantitativo', {}).get('serie_aum', []))}\n"
        f"Posiciones actuales: {len(result.get('posiciones', {}).get('actuales', []))}",
        title=ISIN,
        expand=False,
    ))
