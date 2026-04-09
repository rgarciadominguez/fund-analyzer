"""
CNMV Agent — Fondos Españoles (prefijo ES)

Flujo:
  A. ISIN → NIF        GET /portal/Consultas/IIC/Fondo.aspx?isin={ISIN}
                       El NIF aparece en links de navegación: nif=XXXXXXXX
  B. Cuantitativos     XML bulk data CNMV (catálogo: /portal/publicaciones/descarga-informacion-individual)
                       Descargar ZIPs por año/mes desde año creación → extraer XMLs → build_historical_series
  C. Cualitativos      PDFs semestrales: /Portal/consultas/iic/fondo?nif={NIF}&vista=1
                       Tabla 3 cols: Ejercicio | Periodo | Documentos
                       Descargar H2 de cada año → regex para cuantitativos + Claude solo sección 9
"""
import asyncio
import io
import json
import re
import zipfile
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_bytes, post_form
from tools.xml_parser import build_historical_series
from tools.pdf_extractor import extract_pages_by_keyword, extract_page_range, get_pdf_metadata
from tools.claude_extractor import extract_structured_data

console = Console()

CNMV_BASE = "https://www.cnmv.es"
CNMV_ISIN_URL = f"{CNMV_BASE}/portal/Consultas/IIC/Fondo.aspx"
CNMV_REPORTS_URL = f"{CNMV_BASE}/Portal/consultas/iic/fondo"
CNMV_CATALOG_URL = f"{CNMV_BASE}/portal/publicaciones/descarga-informacion-individual"

# Keywords para extraer secciones relevantes de PDFs (ahorro de tokens)
QUALITATIVE_KEYWORDS = [
    "comentario del gestor",
    "comentarios del gestor",
    "política de inversión",
    "objetivo de inversión",
    "estrategia de inversión",
    "equipo gestor",
]
POSITIONS_KEYWORDS = [
    "cartera de valores",
    "composición de la cartera",
    "principales posiciones",
    "distribución de la cartera",
    "detalle de inversiones",
]


class CNMVAgent:
    """
    Agente para fondos españoles (ISIN prefijo ES).
    Clase con async def run() -> dict según convenio del proyecto.
    """

    def __init__(self, isin: str, config: dict):
        self.isin = isin.strip().upper()
        self.config = config
        self.current_year = datetime.now().year

        # Paths — relativos a la raíz del proyecto
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.xml_dir = self.fund_dir / "raw" / "xml"
        self.reports_dir = self.fund_dir / "raw" / "reports"

        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.xml_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_period(periodo) -> str:
        """Normaliza cualquier formato de periodo a YYYY.
        '202506' → '2025', '2025-S2' → '2025', '2025-H2' → '2025', '2025' → '2025'."""
        p = str(periodo)
        if len(p) == 6 and p.isdigit():
            return p[:4]
        return p.split("-")[0]

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> dict:
        """Orquesta los 3 pasos y devuelve cnmv_data completo."""
        console.print(Panel(
            f"[bold cyan]CNMV Agent[/bold cyan]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        result: dict = {
            "isin": self.isin,
            "tipo": "ES",
            "ultima_actualizacion": datetime.now().isoformat(),
            "kpis": {},
            "cualitativo": {},
            "cuantitativo": {},
            "posiciones": {"actuales": [], "historicas": []},
            "fuentes": {
                "xmls_cnmv": [],
                "informes_descargados": [],
                "urls_consultadas": [],
            },
        }

        # ── Paso A: ISIN → NIF ───────────────────────────────────────────────
        console.log("[bold]Paso A:[/bold] ISIN -> NIF")
        try:
            nif, gestora, anio_creacion = await self._get_nif()
            result["nif"] = nif
            result["gestora"] = gestora
            result["kpis"]["anio_creacion"] = anio_creacion
            result["fuentes"]["urls_consultadas"].append(
                f"{CNMV_ISIN_URL}?isin={self.isin}"
            )
        except Exception as exc:
            console.log(f"[red]Error Paso A: {exc}")
            self._save(result)
            return result

        start_year = anio_creacion or (self.current_year - 5)

        # ── Paso B: Datos cuantitativos via XMLs ─────────────────────────────
        objetivo = self.config.get("objetivo", "1")
        if objetivo not in ("3",):  # omitir si solo cualitativo
            console.log("[bold]Paso B:[/bold] Descargando XMLs bulk data CNMV")
            try:
                series = await self._download_xml_series(start_year)
                result["cuantitativo"] = series
                result["fuentes"]["xmls_cnmv"] = [
                    p.name for p in sorted(self.xml_dir.glob("*.xml"))
                ]
                # Nombre del fondo desde FONDREGISTRO (más fiable que el HTML)
                if not result.get("nombre"):
                    result["nombre"] = self._get_nombre_from_xml()
                # KPIs actuales del último dato disponible
                self._fill_kpis_from_series(result)
            except Exception as exc:
                console.log(f"[yellow]Paso B parcial: {exc}")

        # ── Paso C: PDFs semestrales ─────────────────────────────────────────
        fuentes = self.config.get("fuentes", "1")
        if fuentes not in ("3",):  # omitir si solo cartas
            console.log("[bold]Paso C:[/bold] Procesando PDFs semestrales CNMV")
            try:
                pdf_data = await self._process_pdfs(nif, start_year)
                self._merge_pdf_data(result, pdf_data)
                result["fuentes"]["informes_descargados"] = [
                    p.name for p in sorted(self.reports_dir.glob("*.pdf"))
                ]
                result["fuentes"]["urls_consultadas"].append(
                    f"{CNMV_REPORTS_URL}?nif={nif}&vista=1"
                )
            except Exception as exc:
                console.log(f"[yellow]Paso C parcial: {exc}")

        # ── Comisión de éxito: aggregate from serie_comisiones_por_clase ──
        cuant_final = result.get("cuantitativo", {})
        serie_com_final = cuant_final.get("serie_comisiones_por_clase", [])
        has_perf_fee = False
        perf_fee_serie = []
        for entry in serie_com_final:
            exito = entry.get("exito", {})
            if exito:
                perf_fee_serie.append({"periodo": entry.get("periodo"), "exito": exito})
                if any(v > 0 for v in exito.values()):
                    has_perf_fee = True
        result["comision_exito"] = {
            "existe": has_perf_fee,
            "serie_historica": perf_fee_serie,
        }

        self._save(result)
        self._print_summary_table(result)
        return result

    def _print_summary_table(self, result: dict) -> None:
        """Print a Rich table summarizing all quantitative data extracted per year."""
        from rich.table import Table
        cuant = result.get("cuantitativo", {})

        # Build year→data lookup from all series
        years_data: dict[str, dict] = {}
        for e in cuant.get("serie_aum", []):
            yr = self._normalize_period(e.get("periodo", ""))
            years_data.setdefault(yr, {})["aum"] = e.get("valor_meur")
            years_data[yr]["vl"] = e.get("vl")
        for e in cuant.get("serie_vl_base100", []):
            yr = self._normalize_period(e.get("periodo", ""))
            years_data.setdefault(yr, {})["base100"] = e.get("base100")
        for e in cuant.get("serie_participes", []):
            yr = self._normalize_period(e.get("periodo", ""))
            years_data.setdefault(yr, {})["part"] = e.get("valor")
        for e in cuant.get("serie_ter_por_clase", []):
            yr = self._normalize_period(e.get("periodo", ""))
            clases = e.get("clases", {})
            years_data.setdefault(yr, {})["ter_a"] = clases.get("A")
            years_data[yr]["ter_b"] = clases.get("B")
        for e in cuant.get("serie_rotacion", []):
            yr = self._normalize_period(e.get("periodo", ""))
            years_data.setdefault(yr, {})["rot"] = e.get("rotacion_pct")
        for e in cuant.get("serie_comisiones_por_clase", []):
            yr = self._normalize_period(e.get("periodo", ""))
            clases = e.get("clases", {})
            years_data.setdefault(yr, {})["gest_a"] = clases.get("A")
            years_data[yr]["gest_b"] = clases.get("B")
        # Posiciones count — use num_posiciones if available, else len(top10)
        pos_hist = result.get("posiciones", {}).get("historicas", [])
        for h in pos_hist:
            yr = self._normalize_period(h.get("periodo", ""))
            years_data.setdefault(yr, {})["n_pos"] = h.get("num_posiciones") or len(h.get("top10", []))
        pos_act = result.get("posiciones", {}).get("actuales", [])
        if pos_act:
            # Latest year
            latest_yr = max(years_data.keys()) if years_data else ""
            if latest_yr:
                years_data[latest_yr]["n_pos"] = len(pos_act)

        if not years_data:
            return

        table = Table(title=f"CNMV Agent — Resumen cuantitativo {self.isin}", show_lines=True)
        table.add_column("Año", style="bold cyan", width=6)
        table.add_column("AUM M€", justify="right", width=9)
        table.add_column("VL", justify="right", width=7)
        table.add_column("Base100", justify="right", width=8)
        table.add_column("Part.", justify="right", width=6)
        table.add_column("TER_A%", justify="right", width=7)
        table.add_column("TER_B%", justify="right", width=7)
        table.add_column("GestA%", justify="right", width=7)
        table.add_column("GestB%", justify="right", width=7)
        table.add_column("Rot%", justify="right", width=6)
        table.add_column("Pos", justify="right", width=4)

        def _fmt(v, dec=2):
            if v is None:
                return "—"
            return f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")

        for yr in sorted(years_data.keys()):
            d = years_data[yr]
            table.add_row(
                yr,
                _fmt(d.get("aum"), 1),
                _fmt(d.get("vl"), 1),
                _fmt(d.get("base100"), 1),
                str(int(d["part"])) if d.get("part") else "—",
                _fmt(d.get("ter_a"), 2),
                _fmt(d.get("ter_b"), 2),
                _fmt(d.get("gest_a"), 2),
                _fmt(d.get("gest_b"), 2),
                _fmt(d.get("rot"), 1),
                str(d.get("n_pos", "")) or "—",
            )
        console.print(table)

    # ── Paso A ───────────────────────────────────────────────────────────────

    async def _get_nif(self) -> tuple[str, str, int | None]:
        """
        Obtiene NIF del fondo, nombre de la gestora y año de creación.

        - NIF del fondo: primer link con patrón nif= que NO sea sgiic/depositaria
        - Gestora:       texto del link sgiic.aspx?nif=... (fiable en todos los fondos)
        - Nombre fondo:  se completa en run() desde el XML FONDREGISTRO
        - Año creación:  página de datos generales (vista=0)
        """
        url = f"{CNMV_ISIN_URL}?isin={self.isin}"
        html = await get(url)
        soup = BeautifulSoup(html, "lxml")

        # ── NIF del fondo: primer link fondo.aspx?nif=... ────────────────────
        # Los tabs de navegación usan fondo.aspx?nif=XXXXX — ese es el NIF del fondo
        # Los links sgiic.aspx y depositaria.aspx son gestora/depositaria (ignorar)
        nif = ""
        nif_pattern = re.compile(r'nif=([A-Z0-9]{8,9})', re.IGNORECASE)

        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Sólo coger links que apunten a fondo.aspx (tabs de nav del propio fondo)
            if "fondo.aspx" in href.lower() or "fondo?" in href.lower():
                m = nif_pattern.search(href)
                if m:
                    nif = m.group(1).upper()
                    break

        # Fallback: primer nif= en cualquier link que no sea gestora/depositaria
        if not nif:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if any(skip in href.lower() for skip in ("sgiic", "depositaria", "hr/")):
                    continue
                m = nif_pattern.search(href)
                if m:
                    nif = m.group(1).upper()
                    break

        if not nif:
            raise ValueError(
                f"No se encontró NIF para ISIN {self.isin}. "
                "Verifica que el ISIN sea válido en CNMV."
            )

        # ── Gestora: texto del link sgiic.aspx?nif=... ───────────────────────
        gestora = ""
        for a in soup.find_all("a", href=True):
            if "sgiic.aspx" in a["href"].lower():
                gestora = a.get_text(strip=True)
                break

        # ── Año de creación: página de datos generales (vista=0) ─────────────
        anio_creacion = await self._get_creation_year(nif)

        console.log(
            f"[green]NIF: {nif} | Gestora: {gestora or '(sin detectar)'} "
            f"| Creacion: {anio_creacion}"
        )
        return nif, gestora, anio_creacion

    async def _get_creation_year(self, nif: str) -> int | None:
        """Extrae el año de creación/inscripción del fondo desde la página vista=0."""
        try:
            url = f"{CNMV_REPORTS_URL}?nif={nif}&vista=0"
            html = await get(url)
        except Exception:
            return None

        # Patrón: fecha de inscripción/registro dd/mm/yyyy
        m = re.search(
            r'(?:inscripci[oó]n|registro|alta|constituci[oó]n)[^\d]{0,40}'
            r'(\d{1,2})[/\-](\d{1,2})[/\-]((?:19|20)\d{2})',
            html, re.IGNORECASE,
        )
        if m:
            return int(m.group(3))

        # Fallback: año más antiguo en rango sensato
        years = [int(y) for y in re.findall(r'\b(20(?:0[5-9]|1\d|2[0-5]))\b', html)]
        return min(years) if years else None

    # ── Paso B ───────────────────────────────────────────────────────────────

    async def _download_xml_series(self, start_year: int) -> dict:
        """
        Descarga ZIPs del catálogo CNMV (desde start_year hasta hoy),
        extrae XMLs y construye series históricas.

        El catálogo CNMV tiene una página por año (?ejercicio=YYYY).
        La página principal muestra solo el año en curso; los históricos
        se acceden via: /descarga-informacion-individual.aspx?ejercicio=YYYY
        """
        all_zip_links: list[dict] = []

        # Años a descargar: desde start_year hasta el año actual (inclusive)
        years_to_fetch = list(range(start_year, self.current_year + 1))

        for year in years_to_fetch:
            if year == self.current_year:
                url = CNMV_CATALOG_URL
            else:
                url = f"{CNMV_BASE}/portal/publicaciones/descarga-informacion-individual.aspx?ejercicio={year}"
            try:
                html = await get(url)
                soup = BeautifulSoup(html, "lxml")
                links = self._parse_catalog_links(soup, start_year)
                all_zip_links.extend(links)
            except Exception as exc:
                console.log(f"[yellow]Error catálogo {year}: {exc}")

        # Dedup por filename
        seen_fn: set[str] = set()
        zip_links = []
        for lnk in all_zip_links:
            if lnk["filename"] not in seen_fn:
                seen_fn.add(lnk["filename"])
                zip_links.append(lnk)

        console.log(f"[blue]Links de descarga encontrados: {len(zip_links)}")

        downloaded_new = 0
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("Descargando XMLs...", total=len(zip_links))

            for entry in zip_links:
                progress.update(task, description=f"Descargando {entry['label']}...")
                await self._download_and_extract_zip(entry)
                downloaded_new += 1
                progress.advance(task)

        console.log(f"[green]ZIPs procesados: {downloaded_new}")

        xml_count = len(list(self.xml_dir.glob("*.xml")))
        if xml_count == 0:
            console.log("[yellow]No hay XMLs en el directorio. Series vacías.")
            return {}

        console.log(f"[blue]Construyendo series desde {xml_count} XMLs...")
        return build_historical_series(str(self.xml_dir), self.isin)

    def _parse_catalog_links(self, soup: BeautifulSoup, start_year: int) -> list[dict]:
        """
        Extrae links de descarga de ZIPs del catálogo CNMV.

        Estructura de la página: tabla 2 cols (Periodo | Icono-ZIP)
        Links: /webservices/verdocumento/ver?e=<cifrado>
        """
        links = []
        verdoc_pattern = re.compile(r'/webservices/verdocumento/ver', re.IGNORECASE)

        # Todos los links de descarga de la página
        for a in soup.find_all("a", href=verdoc_pattern):
            href = a["href"]
            full_url = href if href.startswith("http") else f"{CNMV_BASE}{href}"

            # Contexto: fila de tabla que contiene este link
            row = a.find_parent("tr")
            period_text = ""
            if row:
                cells = row.find_all("td")
                period_text = cells[0].get_text(strip=True) if cells else ""

            # Extraer año del texto del periodo o del contexto circundante
            year_m = re.search(r'\b(20\d{2})\b', period_text)
            year = int(year_m.group(1)) if year_m else None

            if year and year < start_year:
                continue  # antes del año de creación del fondo

            # Nombre de fichero seguro para guardar en disco
            safe_label = re.sub(r'[^\w\-]', '_', period_text)[:40] or f"cnmv_{len(links)}"
            filename = f"{safe_label}.zip"

            # Evitar duplicados
            if any(e["filename"] == filename for e in links):
                filename = f"{safe_label}_{len(links)}.zip"

            links.append({
                "url": full_url,
                "label": period_text or f"fichero_{len(links)}",
                "year": year,
                "filename": filename,
            })

        return links

    async def _download_and_extract_zip(self, entry: dict) -> None:
        """Descarga un ZIP del catálogo CNMV y extrae los XMLs que contiene."""
        zip_target = self.xml_dir / entry["filename"]

        # Si ya se extrajo antes, skip
        base_name = zip_target.stem
        existing = list(self.xml_dir.glob(f"{base_name}*.xml"))
        if existing:
            console.log(f"[dim]Ya procesado: {entry['label']}")
            return

        try:
            data = await get_bytes(entry["url"])
        except Exception as exc:
            console.log(f"[yellow]Error descargando {entry['label']}: {exc}")
            return

        # Intentar extraer como ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".xml"):
                        xml_bytes = zf.read(name)
                        xml_name = f"{base_name}_{Path(name).name}"
                        (self.xml_dir / xml_name).write_bytes(xml_bytes)
            console.log(f"[green]ZIP extraído: {entry['label']}")
        except zipfile.BadZipFile:
            # Puede que sea directamente un XML
            if data[:5].startswith(b"<?xml") or b"<IIC" in data[:200]:
                xml_path = self.xml_dir / f"{base_name}.xml"
                xml_path.write_bytes(data)
                console.log(f"[green]XML guardado directamente: {entry['label']}")
            else:
                console.log(f"[yellow]Formato desconocido para: {entry['label']}")

    # ── Paso C ───────────────────────────────────────────────────────────────

    async def _process_pdfs(self, nif: str, start_year: int) -> dict:
        """
        Descarga PDFs H2, parsea cada uno con _parse_pdf_structured y
        agrega los resultados en un único dict con todos los campos PDF.
        """
        reports = await self._get_report_links(nif, start_year)
        if not reports:
            console.log("[yellow]No se encontraron informes semestrales")
            return {}

        h2_reports = self._select_h2_reports(reports)
        console.log(f"[blue]Informes H2 seleccionados: {len(h2_reports)}")

        # Cuántos años descargar según config
        horizonte = self.config.get("horizonte_historico", "1")
        if horizonte == "4":
            to_download = h2_reports[:1]
        elif horizonte == "3":
            to_download = h2_reports[:3]
        elif horizonte == "2":
            to_download = h2_reports[:5]
        else:
            to_download = h2_reports  # desde inicio

        downloaded: list[tuple[dict, Path]] = []
        for report in to_download:
            pdf_path = await self._download_pdf(report)
            if pdf_path:
                downloaded.append((report, pdf_path))

        if not downloaded:
            return {}

        objetivo = self.config.get("objetivo", "1")

        # Aggregate across all downloaded PDFs
        merged: dict = {
            "posiciones_actuales": [],
            "posiciones_historicas": [],
            "mix_activos_historico": [],
            "analisis_periodos": [],
        }

        for i, (report, pdf_path) in enumerate(downloaded):
            year = report.get("year") or self.current_year
            console.log(f"[blue]Parseando PDF {year}: {pdf_path.name}")

            try:
                parsed = await self._parse_pdf_structured(pdf_path, year)
            except Exception as exc:
                console.log(f"[yellow]Error parseando {pdf_path.name}: {exc}")
                continue

            # Most-recent PDF sets the "current" scalar fields
            if i == 0:
                for campo in [
                    "num_participes", "num_participes_anterior",
                    "coste_gestion_pct", "coste_deposito_pct",
                    "ter_pct", "volatilidad_pct", "clasificacion",
                    "perfil_riesgo", "divisa", "depositario",
                    "fecha_registro", "gestora_pdf",
                    "estrategia", "tipo_activos",
                    "rotacion_cartera_pct", "rotacion_cartera_anterior_pct",
                    "comisiones_gestion_por_clase",
                    "benchmark_mencionado",
                    "seccion_9_texto_completo", "seccion_10_perspectivas_texto",
                    "_periodo_pdf",
                ]:
                    if parsed.get(campo) is not None:
                        merged[campo] = parsed[campo]

            # Detect gestora name change between reports
            gestora_this = parsed.get("gestora_pdf", "")
            if gestora_this:
                prev_gestora = merged.get("_prev_gestora", "")
                if prev_gestora and prev_gestora != gestora_this:
                    merged.setdefault("hechos_relevantes", []).append({
                        "periodo": str(year),
                        "epigrafe": "Cambio de gestora/asesor",
                        "detalle": f"Cambio de {prev_gestora} a {gestora_this}",
                    })
                merged["_prev_gestora"] = gestora_this

            # Accumulate comisiones de gestión por clase from ALL PDFs
            # If per-class data exists, use it. Otherwise, use coste_gestion_pct as "clase A" (pre-split)
            comis_clase = parsed.get("comisiones_gestion_por_clase")
            if not comis_clase and parsed.get("coste_gestion_pct"):
                # Pre-class era: single class → assign to A (the class that continues post-split)
                comis_clase = {"A": parsed["coste_gestion_pct"]}
            if comis_clase:
                existing_cls = merged.setdefault("serie_comisiones_por_clase", [])
                yr_key = str(year)
                if not any(e.get("periodo") == yr_key for e in existing_cls):
                    entry = {
                        "periodo": yr_key,
                        "clases": comis_clase,
                    }
                    # Add performance fee (comisión de éxito) if available
                    exito = parsed.get("comisiones_exito_por_clase")
                    if exito:
                        entry["exito"] = exito
                    existing_cls.append(entry)

            # Accumulate serie_aum_pdf from ALL PDFs (dedup by periodo, VL enrichment)
            for entry in parsed.get("serie_aum_pdf", []):
                periodo = entry.get("periodo", "")
                existing = merged.setdefault("serie_aum_pdf", [])
                found = next((e for e in existing if e.get("periodo") == periodo), None)
                if not found:
                    existing.append(entry)
                elif entry.get("vl") and not found.get("vl"):
                    # Enrich existing entry with VL from this PDF's own-year data
                    found["vl"] = entry["vl"]

            # Accumulate serie_ter_pdf from ALL PDFs (dedup by periodo)
            for entry in parsed.get("serie_ter_pdf", []):
                periodo = entry.get("periodo", "")
                existing = merged.setdefault("serie_ter_pdf", [])
                if not any(e.get("periodo") == periodo for e in existing):
                    existing.append(entry)
            # Also add current year's TER from the scalar ter_pct (accumulated annual)
            if parsed.get("ter_pct") is not None:
                yr_key = str(year)
                existing_ter = merged.setdefault("serie_ter_pdf", [])
                found_ter = next((e for e in existing_ter if e.get("periodo") == yr_key), None)
                if not found_ter:
                    existing_ter.append({"periodo": yr_key, "ter_pct": parsed["ter_pct"]})

            # Accumulate TER per class from ALL PDFs
            if parsed.get("ter_por_clase"):
                existing_ter_cls = merged.setdefault("serie_ter_por_clase", [])
                yr_key = str(year)
                if not any(e.get("periodo") == yr_key for e in existing_ter_cls):
                    existing_ter_cls.append({"periodo": yr_key, "clases": parsed["ter_por_clase"]})
                # For pre-multi-class years: if only 1 class, assign to A
            elif parsed.get("ter_pct") is not None:
                existing_ter_cls = merged.setdefault("serie_ter_por_clase", [])
                yr_key = str(year)
                if not any(e.get("periodo") == yr_key for e in existing_ter_cls):
                    existing_ter_cls.append({"periodo": yr_key, "clases": {"A": parsed["ter_pct"]}})

            # Accumulate serie_participes_pdf — solo valor del año del report (no año-1)
            if parsed.get("num_participes") is not None:
                periodo_key = str(year)
                existing_part = merged.setdefault("serie_participes_pdf", [])
                if not any(e.get("periodo") == periodo_key for e in existing_part):
                    existing_part.append({"periodo": periodo_key, "valor": parsed["num_participes"]})

            # Accumulate serie_rotacion_pdf — solo valor del año del report (no año-1)
            if parsed.get("rotacion_cartera_pct") is not None:
                periodo_key = str(year)
                existing_rot = merged.setdefault("serie_rotacion_pdf", [])
                if not any(e.get("periodo") == periodo_key for e in existing_rot):
                    existing_rot.append({"periodo": periodo_key, "rotacion_pct": parsed["rotacion_cartera_pct"]})

            # Accumulate clases_info from ALL PDFs
            if parsed.get("clases_info"):
                existing_ci = merged.setdefault("serie_clases_info", [])
                yr_key = str(year)
                if not any(e.get("periodo") == yr_key for e in existing_ci):
                    existing_ci.append({"periodo": yr_key, **parsed["clases_info"]})

            # Accumulate hechos_relevantes from all PDFs
            if parsed.get("hechos_relevantes"):
                merged.setdefault("hechos_relevantes", []).extend(parsed["hechos_relevantes"])

            # Qualitative from section 9/10 (all PDFs, most-recent wins for each key)
            if objetivo not in ("2", "4"):
                cual = parsed.get("cualitativo_gestor", {})
                if isinstance(cual, dict):
                    for k, v in cual.items():
                        if i == 0 or k not in merged:
                            merged[k] = v

            # Positions: current from most-recent only
            posiciones = parsed.get("posiciones", [])
            if i == 0 and posiciones:
                merged["posiciones_actuales"] = posiciones

            # Historical positions: ALL positions per period (sorted by weight)
            if posiciones:
                sorted_pos = sorted(posiciones, key=lambda p: p.get("peso_pct", 0), reverse=True)
                merged["posiciones_historicas"].append({
                    "periodo": str(year),
                    "num_posiciones": len(posiciones),
                    "top10": sorted_pos[:10],
                    "todas": sorted_pos,
                })

            # Mix activos per period
            mix = parsed.get("mix_activos", {})
            if mix:
                merged["mix_activos_historico"].append({
                    "periodo": str(year),
                    **mix,
                })

            # Análisis de consistencia — raw text per year for analyst_agent
            has_text = bool(parsed.get("seccion_9_texto_completo") or parsed.get("seccion_10_perspectivas_texto"))
            if has_text:
                periodo_entry = {
                    "periodo": str(year),
                    "seccion_1_texto": parsed.get("seccion_1_politica_texto") or "",
                    "seccion_9_texto": parsed.get("seccion_9_texto_completo") or "",
                    "seccion_10_texto": parsed.get("seccion_10_perspectivas_texto") or "",
                    "seccion_4_5_texto": parsed.get("seccion_4_5_hechos_texto") or "",
                }
                merged["analisis_periodos"].append(periodo_entry)

        # Clean internal keys
        merged.pop("_prev_gestora", None)
        return merged

    async def _get_report_links(self, nif: str, start_year: int) -> list[dict]:
        """
        Obtiene TODOS los informes semestrales del portal CNMV desde start_year.

        El portal usa ASP.NET WebForms: la fecha del buscador se envía por POST
        con __VIEWSTATE. Cada consulta devuelve los ~4 informes más recientes
        hasta la fecha indicada (txtFecha = YYYY-MM-DD).

        Estrategia: GET inicial (informes más recientes) + POST con
        {year}-12-31 para cada año desde start_year hasta current_year-1,
        capturando así el S2 de cada año (publicado en Q1 del año siguiente).
        """
        base_url = f"{CNMV_REPORTS_URL}?nif={nif}&vista=1"
        seen: set[str] = set()  # deduplica por "YYYY-Semestre N"
        all_reports: list[dict] = []

        def _parse_and_add(soup: BeautifulSoup) -> None:
            for table in soup.find_all("table"):
                for row in table.find_all("tr")[1:]:
                    cols = row.find_all("td")
                    if len(cols) < 3:
                        continue
                    year_text = cols[0].get_text(strip=True)
                    period_text = cols[1].get_text(strip=True)
                    year_m = re.search(r'\b(20\d{2}|19\d{2})\b', year_text)
                    if not year_m:
                        continue
                    year = int(year_m.group(1))
                    label_key = f"{year}|{period_text}"
                    if label_key in seen:
                        continue
                    seen.add(label_key)
                    # H2 detection: "Semestre 2" (CNMV actual format)
                    semester = (
                        "H2"
                        if re.search(
                            r'semestre\s*2|2[oº°]\s*semestre|segundo|2s\b|h2',
                            period_text,
                            re.IGNORECASE,
                        )
                        else "H1"
                    )
                    # PDF link
                    verdoc_pat = re.compile(r'webservices/verdocumento|\.pdf', re.IGNORECASE)
                    pdf_url = None
                    for a in cols[2].find_all("a", href=verdoc_pat):
                        href = a["href"]
                        pdf_url = href if href.startswith("http") else f"{CNMV_BASE}{href}"
                        break
                    if not pdf_url:
                        continue
                    all_reports.append({
                        "url": pdf_url,
                        "year": year,
                        "semester": semester,
                        "label": f"{year}-{semester}",
                    })

        # ── GET inicial: informes más recientes (hoy) ────────────────────────
        try:
            html = await get(base_url)
        except Exception as exc:
            console.log(f"[red]Error GET inicial informes: {exc}")
            return all_reports

        soup = BeautifulSoup(html, "lxml")
        _parse_and_add(soup)

        # ── Extraer ViewState para POSTs históricos ──────────────────────────
        vs_tag = soup.find("input", {"name": "__VIEWSTATE"})
        vsg_tag = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        ev_tag = soup.find("input", {"name": "__EVENTVALIDATION"})

        if not (vs_tag and vsg_tag and ev_tag):
            console.log("[yellow]No ViewState encontrado; usando solo página inicial")
            console.log(f"[green]Informes encontrados: {len(all_reports)}")
            return all_reports

        viewstate = vs_tag["value"]
        vsg = vsg_tag["value"]
        ev = ev_tag["value"]

        # ── POST por año: {year}-12-31 captura S2 del año anterior ──────────
        # S2 de un año se publica en Q1 del año siguiente, así que el S2 de
        # {year} aparece en la consulta con fecha {year+1}-12-31 o {year+1}-06-30.
        # Iteramos cada año desde start_year hasta current_year para no perder ninguno.
        for year in range(start_year, self.current_year):
            query_date = f"{year}-12-31"
            try:
                html_post = await post_form(
                    base_url,
                    data={
                        "__EVENTTARGET": "",
                        "__EVENTARGUMENT": "",
                        "__VIEWSTATE": viewstate,
                        "__VIEWSTATEGENERATOR": vsg,
                        "__EVENTVALIDATION": ev,
                        "ctl00$ContentPrincipal$wFecha$txtFecha": query_date,
                        "ctl00$ContentPrincipal$wFecha$btnSeleccionarFecha": "Buscar",
                    },
                )
                soup_post = BeautifulSoup(html_post, "lxml")
                _parse_and_add(soup_post)
            except Exception as exc:
                console.log(f"[yellow]Error POST fecha {query_date}: {exc}")

        console.log(f"[green]Informes encontrados: {len(all_reports)}")
        return all_reports

    def _parse_reports_table(self, soup: BeautifulSoup, out: list[dict]) -> int:
        """
        Extrae filas de informes de la tabla CNMV.
        Cols: Ejercicio (año) | Periodo (semestre) | Documentos (link PDF)
        Devuelve número de filas añadidas.
        """
        added = 0
        verdoc_pat = re.compile(r'webservices/verdocumento|\.pdf', re.IGNORECASE)

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[1:]:  # saltar cabecera
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                year_text = cols[0].get_text(strip=True)
                period_text = cols[1].get_text(strip=True)  # "1º semestre" / "2º semestre"

                # Año
                year_m = re.search(r'\b(20\d{2}|19\d{2})\b', year_text)
                year = int(year_m.group(1)) if year_m else None

                # Semestre
                semester = "H2" if re.search(r'2[oº°]|segundo|2s|h2', period_text, re.IGNORECASE) else "H1"

                # Link PDF (primer link de descarga en la columna Documentos)
                pdf_url = None
                for a in cols[2].find_all("a", href=verdoc_pat):
                    href = a["href"]
                    pdf_url = href if href.startswith("http") else f"{CNMV_BASE}{href}"
                    break

                if not pdf_url:
                    continue

                out.append({
                    "url": pdf_url,
                    "year": year,
                    "semester": semester,
                    "label": f"{year}-{semester}",
                })
                added += 1

        return added

    def _select_h2_reports(self, reports: list[dict]) -> list[dict]:
        """
        Filtra informes H2, un por año, ordenados de más reciente a más antiguo.
        Si no hay H2 detectados, usa todos los informes disponibles.
        """
        h2 = [r for r in reports if r.get("semester") == "H2"]
        if not h2:
            console.log("[yellow]No se detectaron informes H2 explícitos; usando todos")
            h2 = reports

        # Un informe por año, el más reciente primero
        h2.sort(key=lambda r: r.get("year") or 0, reverse=True)
        seen: set = set()
        unique = []
        for r in h2:
            key = r.get("year")
            if key not in seen:
                unique.append(r)
                seen.add(key)
        return unique

    async def _download_pdf(self, report: dict) -> Path | None:
        """Descarga un PDF semestral del portal CNMV."""
        year = report.get("year", "unknown")
        semester = report.get("semester", "H2")
        filename = f"CNMV_{self.isin}_{year}_{semester}.pdf"
        target = self.reports_dir / filename

        if target.exists() and target.stat().st_size > 1000:
            console.log(f"[dim]PDF ya existe: {filename}")
            return target

        try:
            console.log(f"[blue]Descargando PDF {year} {semester}…")
            data = await get_bytes(report["url"])
            target.write_bytes(data)
            size_kb = len(data) // 1024
            console.log(f"[green]PDF descargado: {filename} ({size_kb} KB)")
            return target
        except Exception as exc:
            console.log(f"[yellow]Error descargando PDF {filename}: {exc}")
            return None

    # ── PDF cache ─────────────────────────────────────────────────────────────

    def _load_pdf_cache(self) -> dict:
        """Carga pdf_cache.json o devuelve dict vacío."""
        cache_path = self.fund_dir / "pdf_cache.json"
        if cache_path.exists():
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_pdf_cache(self, cache: dict) -> None:
        """Guarda pdf_cache.json."""
        cache_path = self.fund_dir / "pdf_cache.json"
        cache_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ── Main PDF parser ───────────────────────────────────────────────────────

    async def _parse_pdf_structured(self, pdf_path: Path, year: int) -> dict:
        """
        Parsea un PDF semestral CNMV completo usando regex + Claude solo para sección 9.
        Cachea resultados por (filename, file_size).
        """
        cache = self._load_pdf_cache()
        key = pdf_path.name
        fsize = pdf_path.stat().st_size
        if key in cache and cache[key].get("file_size") == fsize:
            console.log(f"[dim]Cache hit: {key}")
            return cache[key]["data"]

        # Extract full text
        meta = get_pdf_metadata(str(pdf_path))
        full_text = extract_page_range(str(pdf_path), 0, meta["num_pages"])
        # Clean (cid:X) ligature artifacts from pdfplumber
        full_text = re.sub(r'\(cid:\d+\)', ' ', full_text)

        result: dict = {"_periodo_pdf": str(year)}
        result.update(self._parse_seccion_politica(full_text))
        result.update(self._parse_seccion_datos_generales(full_text, year))
        result.update(self._parse_seccion_comportamiento(full_text, year))
        result.update(self._parse_seccion_hechos_relevantes(full_text, year))

        pos, mix = self._parse_seccion_posiciones(full_text)
        result["posiciones"] = pos
        result["mix_activos"] = mix

        # ── Raw text extraction for analyst_agent (NO API calls) ─────────────
        # Section 9: full text (visión gestora, decisiones, inversiones concretas)
        result["seccion_9_texto_completo"] = self._extract_seccion_9_full(full_text)

        # Section 10: perspectivas de mercado
        sec10_persp = self._extract_seccion_perspectivas(full_text)
        if sec10_persp:
            result["seccion_10_perspectivas_texto"] = sec10_persp

        # Section 1: política de inversión (raw text for analyst)
        sec1 = self._extract_seccion_1(full_text)
        if sec1.strip():
            result["seccion_1_politica_texto"] = sec1

        # Save to cache
        cache[key] = {
            "parsed_at": datetime.now().isoformat(),
            "file_size": fsize,
            "data": result,
        }
        self._save_pdf_cache(cache)
        return result

    # ── Section extractors (return raw text slices) ───────────────────────────

    def _extract_seccion_1(self, text: str) -> str:
        """Extract section 1 text (política inversión)."""
        m = re.search(r'1\.\s*Pol[íi]tica\s+de\s+inversi[oó]n', text, re.IGNORECASE)
        if not m:
            return ""
        start = m.start()
        end_m = re.search(r'\n\s*2\.\s*Datos\s+econ[oó]micos', text[start:], re.IGNORECASE)
        end = start + end_m.start() if end_m else start + 4000
        return text[start:end]

    def _extract_seccion_9(self, text: str) -> str:
        """
        Extract section 9 text (visión gestora, decisiones).

        Structure in all CNMV semiannual PDFs:
          9. Anexo explicativo del informe periódico
            1. SITUACIÓN DE LOS MERCADOS Y EVOLUCIÓN DEL FONDO
              a. Visión de la gestora sobre la situación de los mercados
              b. Decisiones generales de inversión adoptadas
              c. Índice de referencia
              d. Evolución del patrimonio, partícipes, rentabilidad y gastos
              e. Rendimientos del fondo en comparación con el resto
            2. INFORMACIÓN SOBRE LAS INVERSIONES
              a. Inversiones concretas realizadas durante el periodo
              ...

        We extract targeted subsections for Claude instead of the full 20K+ chars.
        """
        m = re.search(r'9\.\s*Anexo\s+explicativo\s+del\s+informe', text, re.IGNORECASE)
        if not m:
            m = re.search(r'Visi[oó]n\s+de\s+la\s+gestora', text, re.IGNORECASE)
        if not m:
            return ""
        start = m.start()
        end_m = re.search(r'10\.\s+(?:PERSPECTIVAS|Detalle)', text[start:], re.IGNORECASE)
        full_sec9 = text[start: start + end_m.start() if end_m else start + 20000]

        # Extract key subsections for a focused, token-efficient Claude prompt
        parts: list[str] = []

        # 1a. Visión de la gestora (contexto_mercado)
        vis_m = re.search(r'a\.\s*Visi[oó]n\s+de\s+la\s+gestora', full_sec9, re.IGNORECASE)
        vis_end = re.search(r'b\.\s*(?:Decisiones|Evoluci)', full_sec9[vis_m.start():], re.IGNORECASE) if vis_m else None
        if vis_m:
            vis_text = full_sec9[vis_m.start(): vis_m.start() + (vis_end.start() if vis_end else 3000)]
            parts.append(f"=== VISIÓN DE LA GESTORA ===\n{vis_text[:3000]}")

        # 1b. Decisiones generales de inversión (decisiones_tomadas)
        dec_m = re.search(r'b\.\s*Decisiones\s+generales', full_sec9, re.IGNORECASE)
        dec_end = re.search(r'c\.\s*[ÍI]ndice\s+de\s+referencia', full_sec9[dec_m.start():], re.IGNORECASE) if dec_m else None
        if dec_m:
            dec_text = full_sec9[dec_m.start(): dec_m.start() + (dec_end.start() if dec_end else 2000)]
            parts.append(f"=== DECISIONES GENERALES ===\n{dec_text[:2000]}")

        # 2a. Inversiones concretas realizadas (specific trades/positions)
        inv_m = re.search(r'a\.\s*Inversiones\s+concretas\s+realizadas', full_sec9, re.IGNORECASE)
        inv_end = re.search(r'b\.\s*Operativa\s+(?:del|en)', full_sec9[inv_m.start():], re.IGNORECASE) if inv_m else None
        if inv_m:
            inv_text = full_sec9[inv_m.start(): inv_m.start() + (inv_end.start() if inv_end else 2000)]
            parts.append(f"=== INVERSIONES CONCRETAS ===\n{inv_text[:2000]}")

        if parts:
            return "\n\n".join(parts)

        # Fallback: return first 6000 chars if subsection detection fails
        return full_sec9[:6000]

    def _extract_seccion_perspectivas(self, text: str) -> str:
        """Extract section 10 perspectivas text."""
        m = re.search(r'10\.\s+PERSPECTIVAS\s+DE\s+MERCADO', text, re.IGNORECASE)
        if not m:
            return ""
        start = m.start()
        # Stop before "10. Detalle de inversiones"
        end_m = re.search(r'10\.\s+Detalle\s+de\s+inversiones', text[start:], re.IGNORECASE)
        end = start + end_m.start() if end_m else start + 3000
        return text[start:end]

    def _extract_seccion_9_full(self, text: str) -> str:
        """Extract section 9 COMPLETE text (up to 20K chars) for analyst_agent."""
        m = re.search(r'9\.\s*Anexo\s+explicativo\s+del\s+informe', text, re.IGNORECASE)
        if not m:
            m = re.search(r'Visi[oó]n\s+de\s+la\s+gestora', text, re.IGNORECASE)
        if not m:
            return ""
        start = m.start()
        end_m = re.search(r'10\.\s+(?:PERSPECTIVAS|Detalle)', text[start:], re.IGNORECASE)
        return text[start: start + end_m.start() if end_m else start + 20000]

    # ── Section parsers (return structured dicts) ─────────────────────────────

    def _parse_seccion_politica(self, text: str) -> dict:
        """Parse section 1: vocación inversora, perfil riesgo, divisa, fecha registro."""
        result: dict = {}

        m = re.search(r'Vocaci[oó]n\s+inversora:\s*(.+?)(?:\n|$)', text)
        if m:
            result["clasificacion"] = m.group(1).strip()

        m = re.search(r'Perfil\s+de\s+Riesgo:\s*(\d)', text)
        if m:
            result["perfil_riesgo"] = int(m.group(1))

        m = re.search(r'Divisa\s+de\s+denominaci[oó]n\s+([A-Z]{3})', text)
        if m:
            result["divisa"] = m.group(1)

        # Inception date from cover
        m = re.search(r'Fecha\s+de\s+registro:\s*(\d{2}/\d{2}/\d{4})', text)
        if m:
            result["fecha_registro"] = m.group(1)

        return result

    def _parse_seccion_datos_generales(self, text: str, year: int) -> dict:
        """Parse section 2.1: partícipes, AUM table, comisiones, gestora, depositario."""
        result: dict = {}

        # ── Partícipes ────────────────────────────────────────────────────────
        # Table 2.1: rows with CLASE X ... numbers ... EUR
        # Partícipes = last 2 integers (no decimal comma, <100000) before EUR
        # This handles multi-line pdfplumber output where participaciones (decimals) break across lines
        participes_section = ""
        datos_m = re.search(r'2\.1\.?\s*a?\)?\s*Datos\s+generales|N.*de\s+part[ií]cipes', text, re.IGNORECASE)
        patrim_m = re.search(r'Patrimonio\s*\(en\s*miles\)', text, re.IGNORECASE)
        if datos_m and patrim_m and patrim_m.start() > datos_m.start():
            participes_section = text[datos_m.start(): patrim_m.start()]
        elif datos_m:
            participes_section = text[datos_m.start(): datos_m.start() + 2000]

        total_part_act = 0
        total_part_ant = 0
        found_any = False

        for line in participes_section.split("\n"):
            m_cls = re.match(r'\s*CLASE\s+\w+\s+(.+?)\s+(?:EUR|USD|GBP)', line, re.IGNORECASE)
            if not m_cls:
                continue
            nums_str = m_cls.group(1)
            tokens = re.findall(r'[\d.,]+', nums_str)
            # Find last 2 integer tokens (no comma = not decimal) before EUR
            ints_found: list[int] = []
            for t in reversed(tokens):
                if "," not in t:  # integer (may have dots as thousands separator)
                    val = int(t.replace(".", ""))
                    if val < 100000:
                        ints_found.insert(0, val)
                if len(ints_found) == 2:
                    break
            if len(ints_found) == 2:
                total_part_act += ints_found[0]
                total_part_ant += ints_found[1]
                found_any = True

        if found_any:
            result["num_participes"] = total_part_act
            result["num_participes_anterior"] = total_part_ant

        if "num_participes" not in result:
            # Fallback: aggregate row "Nº de Partícipes  5.176  4.902"
            m = re.search(r'N[oº°]\s*\.?\s*de\s+Part[íi]cipes\s+([\d.]+)\s+([\d.]+)', text)
            if m:
                result["num_participes"] = int(m.group(1).replace(".", ""))
                result["num_participes_anterior"] = int(m.group(2).replace(".", ""))

        # ── Clases info (inversión mínima, dividendos) ───────────────────────
        # Table rows: "CLASE A  455  460  EUR  100.000  0,00  0,00  0,80  NO"
        # or:         "CLASE A  455  460  EUR  0,00  0,00  0,40  0,80  NO"
        clases_info: dict = {}
        # Extended regex: CLASE X  part_actual  part_anterior  DIVISA  [nums...]  SI|NO
        clase_rows = re.findall(
            r'CLASE\s+(\w+)\s+[\d.]+\s+[\d.]+\s+(EUR|USD|GBP)\s+([\d.,]+)(?:\s+[\d.,]+)*\s+(SI|NO)\b',
            text, re.IGNORECASE,
        )
        for cls_name, divisa, first_num, dividendos in clase_rows:
            inv_min_raw = first_num.replace(".", "").replace(",", ".")
            try:
                inv_min = float(inv_min_raw)
            except ValueError:
                inv_min = 0
            clases_info[cls_name] = {
                "divisa": divisa,
                "inversion_minima": inv_min if inv_min >= 1 else 0,
                "dividendos": dividendos.upper() == "SI",
            }
        if clases_info:
            result["clases_info"] = clases_info

        # ── AUM (Patrimonio) ──────────────────────────────────────────────────
        # Two table formats exist in CNMV PDFs:
        #
        # FORMAT A (pre-2020): "CLASE A  119.070  96.488  22.106  7.831"
        #   → numbers immediately after class name
        #
        # FORMAT B (2021+):    "CLASE A EUR 31.619  28.590  24.660  22.355"
        #   → currency code between class name and numbers
        #
        # Header row in Format B: "CLASE Divisa Al final del periodo Diciembre 2024 ..."
        # We extract year labels from header to assign correct periods.
        serie_aum: list[dict] = []

        pat_section_m = re.search(
            r'Patrimonio\s*\(en\s*miles\)',
            text, re.IGNORECASE,
        )
        if pat_section_m:
            pat_block = text[pat_section_m.start(): pat_section_m.start() + 1500]

            # Extract year labels from header: "Al final del periodo Diciembre 2024 Diciembre 2023 ..."
            header_years = [str(year)]  # col 0 = current period (normalized to YYYY)
            for hm in re.finditer(r'(?:Diciembre|Junio)\s+(20\d{2})', pat_block[:300]):
                header_years.append(hm.group(1))

            # FORMAT B: CLASE X EUR val1 [val2] [val3] [val4] — all optional except val1
            class_rows_b = re.findall(
                r'CLASE\s+\w+\s+(?:EUR|USD|GBP|CHF)\s+([\d.]+)(?:\s+([\d.]+))?(?:\s+([\d.]+))?(?:\s+([\d.]+))?',
                pat_block, re.IGNORECASE,
            )
            # FORMAT A: CLASE X val1 [val2] [val3] [val4] (no currency)
            class_rows_a = re.findall(
                r'CLASE\s+\w+\s+([\d.]+)(?:\s+([\d.]+))?(?:\s+([\d.]+))?(?:\s+([\d.]+))?',
                pat_block, re.IGNORECASE,
            ) if not class_rows_b else []

            class_rows = class_rows_b or class_rows_a

            if class_rows:
                def _col_sum(rows, col):
                    total = 0.0
                    for row in rows:
                        v = row[col] if col < len(row) and row[col] else "0"
                        total += float(v.replace(".", "").replace(",", "."))
                    return total

                for col_idx in range(4):
                    aum_val = _col_sum(class_rows, col_idx)
                    if aum_val <= 0:
                        continue
                    periodo = header_years[col_idx] if col_idx < len(header_years) else str(year - col_idx)
                    if not any(e["periodo"] == periodo for e in serie_aum):
                        serie_aum.append({
                            "periodo": periodo,
                            "valor_meur": round(aum_val / 1000, 3),
                        })

        # Fallback: "Periodo del informe  93.272  111,4533"
        if not serie_aum:
            m = re.search(
                r'Periodo\s+del\s+informe\s+([\d.]+)\s+([\d,]+)',
                text, re.IGNORECASE,
            )
            if m:
                serie_aum.append({
                    "periodo": str(year),
                    "valor_meur": round(
                        float(m.group(1).replace(".", "").replace(",", ".")) / 1000, 3
                    ),
                    "vl": float(m.group(2).replace(",", ".")),
                })

        # ── VL (Valor liquidativo) — extraer de tabla separada ────────────────
        # Format A: "CLASE A  30,0272  26,5477  22,036  19,312"
        # Format B: "CLASE A EUR 30,0272  26,5477  22,036  19,312"
        vl_section_m = re.search(r'Valor\s+liquidativo\s+de\s+la\s+participaci', text, re.IGNORECASE)
        if vl_section_m:
            vl_block = text[vl_section_m.start(): vl_section_m.start() + 800]
            # Try Format B first (with currency)
            vl_rows = re.findall(
                r'CLASE\s+\w+\s+(?:EUR|USD|GBP|CHF)\s+([\d,]+)\s+([\d,]+)(?:\s+([\d,]+))?(?:\s+([\d,]+))?',
                vl_block, re.IGNORECASE,
            )
            if not vl_rows:
                # Format A (no currency)
                vl_rows = re.findall(
                    r'CLASE\s+\w+\s+([\d,]+)\s+([\d,]+)(?:\s+([\d,]+))?(?:\s+([\d,]+))?',
                    vl_block, re.IGNORECASE,
                )
            if vl_rows:
                vl_current = float(vl_rows[0][0].replace(",", "."))
                for e in serie_aum:
                    if e["periodo"] == str(year):
                        e["vl"] = round(vl_current, 1)
                        break

        if serie_aum:
            result["serie_aum_pdf"] = sorted(
                serie_aum, key=lambda x: x["periodo"], reverse=True
            )

        # ── Comisión de gestión — por clase ──────────────────────────────────
        # Table format (6 numbers per row, may include negatives):
        # CLASE I  0,33  -0,14  0,19  0,70  0,18  0,88  patrimonio  0,03  0,08
        #          ^per_spat ^per_sres ^per_total ^acum_spat ^acum_sres ^acum_total
        # We want acum_spat (position 4) = annual management fee on assets
        comis_section = ""
        comis_m = re.search(r'Comisiones\s+aplicadas\s+en\s+el\s+per', text, re.IGNORECASE)
        if comis_m:
            comis_section = text[comis_m.start(): comis_m.start() + 2000]
        else:
            comis_section = text

        # Flexible regex: captures clase name + 6 numbers (allowing negatives)
        class_comisiones = re.findall(
            r'CLASE\s+(\w+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)',
            comis_section, re.IGNORECASE,
        )
        if class_comisiones:
            por_clase = {}
            exito_clase = {}
            for m_row in class_comisiones:
                cls_name = m_row[0]
                # m_row: [0]=clase, [1]=per_spat, [2]=per_sres, [3]=per_total,
                #         [4]=acum_spat, [5]=acum_sres, [6]=acum_total
                acum_spat = float(m_row[4].replace(",", "."))  # Acumulada s/patrimonio
                acum_sres = float(m_row[5].replace(",", "."))  # Acumulada s/resultados (comisión éxito)
                if acum_spat >= 0:
                    por_clase[cls_name] = acum_spat
                exito_clase[cls_name] = acum_sres
            if por_clase:
                result["comisiones_gestion_por_clase"] = por_clase
                result["coste_gestion_pct"] = min(por_clase.values())
            # Comisión de éxito (sobre resultados)
            result["comisiones_exito_por_clase"] = exito_clase
            result["cobra_comision_exito"] = any(v > 0 for v in exito_clase.values())
        else:
            # Fallback: aggregate row (single-class funds)
            # Try 6-number pattern first (with s/resultados)
            m = re.search(
                r'Comisi[oó]n\s+de\s+gesti[oó]n\s+'
                r'(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)',
                text,
            )
            if m:
                acum_spat = float(m.group(4).replace(",", "."))
                acum_sres = float(m.group(5).replace(",", "."))
                result["coste_gestion_pct"] = acum_spat
                result["comisiones_exito_por_clase"] = {"UNICA": acum_sres}
                result["cobra_comision_exito"] = acum_sres > 0
                # Check base de cálculo for "mixta" → indicates performance fee structure exists
                base_m = re.search(r'(?:mixta|resultados)', text[m.start():m.start()+200], re.IGNORECASE)
                if base_m:
                    result["base_comision"] = "mixta"
            else:
                # Last resort: 4-number pattern
                m = re.search(
                    r'Comisi[oó]n\s+de\s+gesti[oó]n\s+'
                    r'([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)',
                    text,
                )
                if m:
                    result["coste_gestion_pct"] = float(m.group(4).replace(",", "."))

        # Comisión depositario: "Comisión de depositario  0,04  0,07 patrimonio"
        m = re.search(r'Comisi[oó]n\s+de\s+depositario\s+([\d,]+)\s+([\d,]+)', text)
        if m:
            result["coste_deposito_pct"] = float(m.group(2).replace(",", "."))

        # Gestora name from cover
        m = re.search(r'Gestora:\s*(.+?)(?:\s+Depositario:|$)', text)
        if m:
            result["gestora_pdf"] = m.group(1).strip()

        # Depositario from cover
        m = re.search(r'Depositario:\s*(.+?)(?:\s+Auditor:|$)', text)
        if m:
            result["depositario"] = m.group(1).strip()

        return result

    def _parse_seccion_comportamiento(self, text: str, year: int = 0) -> dict:
        """Parse section 2.2: TER (multi-year per class), volatility, índice rotación cartera."""
        result: dict = {}

        # ── TER (Ratio total de gastos) — per class extraction ───────────────
        # PDFs with 1 class: 1 table "Ratio total de gastos"
        # PDFs with 2+ classes: 1 table under "A) Individual Clase A", another under "Clase B"
        # Format: acum_actual trim1 trim2 trim3 trim4 año-1 año-2 año-3 [año-5]
        # nums[0] = TER of THIS year. nums[5+] = historical annual TER.
        ter_matches = list(re.finditer(r'Ratio\s+total\s+de\s+gastos', text, re.IGNORECASE))

        serie_ter: list[dict] = []
        ter_por_clase: dict = {}

        def _extract_ter_nums(start_pos: int) -> list[float]:
            """Extract TER-like numbers (0.01-5.0) from text after a match position."""
            block = text[start_pos: start_pos + 400]
            nums = []
            for n in re.findall(r'[\d,]+', block):
                try:
                    v = float(n.replace(",", "."))
                    if 0.01 <= v <= 5.0:
                        nums.append(v)
                except ValueError:
                    pass
            return nums

        for idx, ter_m in enumerate(ter_matches):
            ter_nums = _extract_ter_nums(ter_m.start())
            if not ter_nums:
                continue

            # Determine class: first table = clase A (or única), second = clase B
            clase = "A" if idx == 0 else chr(ord("A") + idx)
            ter_actual = ter_nums[0]
            ter_por_clase[clase] = ter_actual

            # Build historical serie from first table only (clase A / única)
            if idx == 0:
                result["ter_pct"] = ter_actual
                serie_ter.append({"periodo": str(year), "ter_pct": ter_actual})
                # Historical: positions 5+ (after 4 quarterly values)
                annual_start = 5 if len(ter_nums) >= 6 else 1
                annual_vals = ter_nums[annual_start:annual_start + 4]
                for i, v in enumerate(annual_vals):
                    yr = year - (i + 1) if year else 0
                    if yr > 2010 and v > 0:
                        serie_ter.append({"periodo": str(yr), "ter_pct": round(v, 4)})

        if serie_ter:
            result["serie_ter_pdf"] = sorted(serie_ter, key=lambda x: x["periodo"], reverse=True)
        if ter_por_clase:
            result["ter_por_clase"] = ter_por_clase

        # ── Índice de rotación de cartera ─────────────────────────────────────
        # Row: "Índice de rotación de la cartera  0,09  0,34  0,39  0,27"
        # Format: sem_actual sem_anterior año_actual año_anterior
        # We want año_actual (3rd number) as the annual rotation index
        m = re.search(
            r'[ÍI]ndice\s+de\s+rotaci[oó]n\s+(?:de\s+)?(?:la\s+)?cartera\s+([\d,]+)(?:\s+[\d,]+)?\s+([\d,]+)\s+([\d,]+)',
            text, re.IGNORECASE,
        )
        if not m:
            # Fallback: simpler pattern with just 2 numbers
            m = re.search(
                r'[ÍI]ndice\s+de\s+rotaci[oó]n\s+(?:de\s+)?(?:la\s+)?cartera\s+([\d,]+)\s+([\d,]+)',
                text, re.IGNORECASE,
            )
            if m:
                result["rotacion_cartera_pct"] = round(float(m.group(1).replace(",", ".")) * 100, 1)
                result["rotacion_cartera_anterior_pct"] = round(float(m.group(2).replace(",", ".")) * 100, 1)
        else:
            # 4-number format: take positions 3 and 4 (annual figures)
            result["rotacion_cartera_pct"] = round(float(m.group(2).replace(",", ".")) * 100, 1)
            result["rotacion_cartera_anterior_pct"] = round(float(m.group(3).replace(",", ".")) * 100, 1)

        # ── Volatilidad ───────────────────────────────────────────────────────
        m = re.search(r'Valor\s+liquidativo\s+([\d,]+)', text, re.IGNORECASE)
        if m:
            result["volatilidad_pct"] = float(m.group(1).replace(",", "."))

        return result

    def _parse_seccion_hechos_relevantes(self, text: str, year: int = 0) -> dict:
        """
        Parse sections 4 + 5: hechos relevantes.
        - Section 4: table of SI/NO flags → detect which epígrafes are SI
        - Section 5: annexe text → detailed explanation
        Returns {"hechos_relevantes": [{"periodo", "epigrafe", "detalle"}]}
        Only adds entry if at least one SI is found.
        """
        result: dict = {}
        epigrafe_si: list[str] = []
        detalle: str = ""

        # ── Sección 4: tabla de hechos relevantes ────────────────────────────
        sec4_m = re.search(r'4\.\s*Hechos\s+relevantes', text, re.IGNORECASE)
        if sec4_m:
            sec5_start = re.search(r'5\.\s*Anexo', text[sec4_m.start():])
            sec4_end = sec4_m.start() + (sec5_start.start() if sec5_start else 3000)
            sec4 = text[sec4_m.start(): sec4_end]

            # Rows can be:
            # "a. Suspensión temporal de suscripciones/reembolsos X"  (inline X=NO, X/SI)
            # "h. Cambio de control de la sociedad gestora X" followed by separate SI line
            # Also multi-line rows where epígrafe is on one line and SI/NO on next
            lines_sec4 = sec4.splitlines()
            pending_epigrafe = ""
            for line in lines_sec4:
                line_s = line.strip()
                # Detect epígrafe line: starts with letter + dot
                ep_m = re.match(r'^([a-z]\.\s+.{5,})', line_s, re.IGNORECASE)
                if ep_m:
                    # Check if SI is on same line
                    if re.search(r'\bSI\b', line_s, re.IGNORECASE) and not re.search(r'\bNO\b', line_s, re.IGNORECASE):
                        epigrafe_si.append(ep_m.group(1).strip())
                        pending_epigrafe = ""
                    elif re.search(r'\bNO\b', line_s, re.IGNORECASE):
                        pending_epigrafe = ""
                    else:
                        pending_epigrafe = ep_m.group(1).strip()
                elif pending_epigrafe and re.match(r'^\s*SI\s*$', line_s, re.IGNORECASE):
                    epigrafe_si.append(pending_epigrafe)
                    pending_epigrafe = ""
                elif pending_epigrafe and re.match(r'^\s*NO\s*$', line_s, re.IGNORECASE):
                    pending_epigrafe = ""

        # ── Sección 5: Anexo explicativo ─────────────────────────────────────
        m5 = re.search(r'5\.\s*Anexo\s+explicativo\s+de\s+hechos\s+relevantes', text, re.IGNORECASE)
        if not m5:
            m5 = re.search(r'Anexo\s+explicativo\s+de\s+hechos\s+relevantes', text, re.IGNORECASE)
        if m5:
            start = m5.start()
            end_m = re.search(r'\n\s*6\.', text[start:], re.IGNORECASE)
            end = start + end_m.start() if end_m else start + 3000
            bloque = text[start:end]
            lines = []
            for line in bloque.splitlines():
                line = line.strip()
                if not line:
                    continue
                if re.match(r'^(?:SI|NO|X|I{1,3}|J|[A-Z]|[-–_]{2,}|\d+)$', line):
                    continue
                if re.search(r'Anexo\s+explicativo\s+de\s+hechos', line, re.IGNORECASE):
                    continue
                if len(line) > 10:
                    lines.append(line)
            detalle = " ".join(lines).strip()

        # Always add hechos if any SI found or any detail text
        if epigrafe_si or (detalle and len(detalle) > 30):
            periodo_str = str(year) if year else ""
            result["hechos_relevantes"] = [{
                "periodo": periodo_str,
                "epigrafe": "; ".join(epigrafe_si) if epigrafe_si else "",
                "detalle": detalle,  # sin límite de caracteres
            }]

        # Also save raw text of sections 4+5 for analyst_agent
        sec4_raw = ""
        sec4_m2 = re.search(r'4\.\s*Hechos\s+relevantes', text, re.IGNORECASE)
        if sec4_m2:
            sec6_m = re.search(r'\n\s*6\.', text[sec4_m2.start():], re.IGNORECASE)
            sec4_raw = text[sec4_m2.start(): sec4_m2.start() + (sec6_m.start() if sec6_m else 5000)]
        if sec4_raw:
            result["seccion_4_5_hechos_texto"] = sec4_raw

        return result

    def _parse_seccion_posiciones(self, text: str) -> tuple[list, dict]:
        """
        Parse section 10 'Detalle de inversiones financieras':
        - Individual position lines (ISIN-TYPE|name currency value pct)
        - Aggregate mix activos (TOTAL RENTA FIJA, TOTAL RENTA VARIABLE, etc.)
        - Liquidez from section 2.3
        """
        posiciones: list = []
        mix: dict = {}

        # Locate "10. Detalle de inversiones financieras" — use LAST match
        # (first match may be section 3.1 summary; the detailed table is always near the end)
        all_detalle = list(re.finditer(
            r'10\.?\s*Detalle\s+de\s+inves?iones\s+financieras',
            text, re.IGNORECASE,
        ))
        m_sec = all_detalle[-1] if all_detalle else None
        if not m_sec:
            # Fallback: section 3.1 (last match)
            all_31 = list(re.finditer(r'3\.1\s*Inversiones\s+financieras', text, re.IGNORECASE))
            m_sec = all_31[-1] if all_31 else None
        if not m_sec:
            return posiciones, mix

        sec10 = text[m_sec.start():]
        # Limit to: section 11, OR the grand-total row "TOTAL INVERSIONES FINANCIERAS"
        # (without INTERIOR/EXTERIOR qualifier) — this row ends the summary table
        # and prevents picking up the duplicate table that appears later in the PDF.
        end_m = re.search(r'\n\s*11\.\s', sec10, re.IGNORECASE)
        if end_m:
            sec10 = sec10[:end_m.start()]
        else:
            # Find grand total row — must NOT contain INTERIOR or EXTERIOR
            grand_m = re.search(
                r'TOTAL\s+INVERSIONES\s+FINANCIERAS\s+(?!INTERIOR|EXTERIOR)[\d.,]+\s+[\d,]+[^\n]*\n',
                sec10, re.IGNORECASE,
            )
            if grand_m:
                # Keep through end of that line then stop (max 200 extra chars for liquidez row)
                sec10 = sec10[:grand_m.end() + 200]

        # ── Asset type determination ─────────────────────────────────────────
        # For pattern A (2019+): tipo comes directly from the line (BONO, ACCIONES, IIC, etc.)
        # For pattern B (pre-2019): tipo inferred from nearest TOTAL marker (RF→BONO, RV→ACCIONES)
        # Geographic origin (España/Internacional) is inferred from ISIN prefix (ES=España)

        def _get_tipo_from_position(char_pos: int) -> str:
            """For pattern B: infer asset type from the CLOSEST NEXT TOTAL marker."""
            search_text = sec10[char_pos:]
            candidates = []
            for pattern, tipo in [
                (r'TOTAL\s+(?:RENTA\s+FIJA|RF\b|ADQUISI)', "BONO"),
                (r'TOTAL\s+(?:RENTA\s+VARIABLE|RV\b)', "ACCIONES"),
                (r'TOTAL\s+IIC\b', "IIC"),
                (r'TOTAL\s+DEP[OÓ]SITOS', "DEPOSITO"),
            ]:
                m_t = re.search(pattern, search_text, re.IGNORECASE)
                if m_t:
                    candidates.append((m_t.start(), tipo))
            if candidates:
                _, closest_tipo = min(candidates, key=lambda x: x[0])
                return closest_tipo
            return "ACCIONES"

        # Pattern A (2019+): ISIN - TYPE|name CURRENCY val pct
        pat_a = re.compile(
            r'\b([A-Z]{2}[A-Z0-9]{10})\s*-\s*([A-Z][A-Z ]*?)\|(.+?)\s+'
            r'(EUR|USD|GBP|CHF|SEK|CAD|AUD|NOK|DKK|JPY)\s+'
            r'([\d.]+)\s+([\d,]+)',
            re.MULTILINE,
        )
        # Pattern B (pre-2019): ISIN - NAME CURRENCY val pct [val_ant pct_ant]
        pat_b = re.compile(
            r'\b([A-Z]{2}[A-Z0-9]{10})\s*-\s*(.+?)\s+'
            r'(EUR|USD|GBP|CHF|SEK|CAD|AUD|NOK|DKK|JPY)\s+'
            r'([\d.]+)\s+([\d,]+)',
            re.MULTILINE,
        )

        matches_a = list(pat_a.finditer(sec10))
        if matches_a:
            # Use pattern A (has TYPE|name split)
            for m in matches_a:
                isin_pos = m.group(1)
                tipo_raw = m.group(2).strip()  # BONO, ACCIONES, etc.
                rest = m.group(3).strip()
                divisa = m.group(4)
                valor_miles_str = m.group(5).replace(".", "")
                try:
                    valor_miles = int(valor_miles_str)
                except ValueError:
                    continue
                if valor_miles == 0:
                    continue

                peso = float(m.group(6).replace(",", "."))

                # Parse name, coupon, maturity from rest
                parts = [p.strip() for p in rest.split("|")]
                nombre = parts[0]
                cupon = None
                vencimiento = None
                for part in parts[1:]:
                    if re.match(r'\d{4}-\d{2}-\d{2}', part):
                        vencimiento = part
                    elif re.match(r'^[\d,]+$', part):
                        try:
                            cupon = float(part.replace(",", "."))
                        except ValueError:
                            pass

                entry: dict = {
                    "nombre": nombre,
                    "ticker": isin_pos,
                    "tipo": tipo_raw,
                    "pais": "España" if isin_pos.startswith("ES") else "Internacional",
                    "divisa": divisa,
                    "valor_mercado_miles": valor_miles,
                    "peso_pct": peso,
                }
                if cupon is not None:
                    entry["cupon"] = cupon
                if vencimiento:
                    entry["vencimiento"] = vencimiento

                posiciones.append(entry)
        else:
            # Pattern B fallback (pre-2019): ISIN - NAME CURRENCY val pct
            for m in pat_b.finditer(sec10):
                isin_pos = m.group(1)
                nombre = m.group(2).strip()
                divisa = m.group(3)
                valor_miles_str = m.group(4).replace(".", "")
                try:
                    valor_miles = int(valor_miles_str)
                except ValueError:
                    continue
                if valor_miles == 0:
                    continue
                peso = float(m.group(5).replace(",", "."))
                tipo = _get_tipo_from_position(m.start())
                posiciones.append({
                    "nombre": nombre,
                    "ticker": isin_pos,
                    "tipo": tipo,
                    "pais": "España" if isin_pos.startswith("ES") else "Internacional",
                    "divisa": divisa,
                    "valor_mercado_miles": valor_miles,
                    "peso_pct": peso,
                })

        # Mix activos — detect whether report has one block or two (INTERIOR + EXTERIOR).
        # Two-block reports: sum both blocks (INTERIOR + EXTERIOR = grand total).
        # Single-block reports (with subtotals): take LAST match (grand total row).
        has_two_blocks = bool(re.search(
            r'TOTAL\s+INVERSIONES\s+FINANCIERAS\s+INTERIOR', sec10, re.IGNORECASE
        ) and re.search(
            r'TOTAL\s+INVERSIONES\s+FINANCIERAS\s+EXTERIOR', sec10, re.IGNORECASE
        ))

        def _extract_pct(pattern: str) -> float:
            """Extract % for a category — sum if two blocks, take last if single block."""
            vals = re.findall(pattern, sec10, re.IGNORECASE)
            if not vals:
                return 0.0
            floats = [float(v.replace(",", ".")) for v in vals]
            if has_two_blocks:
                return round(sum(floats), 2)
            else:
                return round(floats[-1], 2)

        rf_val = _extract_pct(r'TOTAL\s+RENTA\s+FIJA\s+[\d.]+\s+([\d,]+)')
        if rf_val:
            mix["renta_fija_pct"] = rf_val

        rv_val = _extract_pct(r'TOTAL\s+RENTA\s+VARIABLE\s+[\d.]+\s+([\d,]+)')
        if rv_val:
            mix["rv_pct"] = rv_val

        iic_val = _extract_pct(r'TOTAL\s+IIC\s+[\d.]+\s+([\d,]+)')
        if iic_val:
            mix["iic_pct"] = iic_val

        dep_val = _extract_pct(r'TOTAL\s+DEP[ÓO]SITOS?\s+[\d.]+\s+([\d,]+)')
        if dep_val:
            mix["depositos_pct"] = dep_val

        # Liquidez from section 2.3
        m2 = re.search(
            r'LIQUIDEZ\s+\(TESORER[ÍI]A\)\s+[\d.]+\s+([\d,]+)',
            text, re.IGNORECASE,
        )
        if m2:
            mix["liquidez_pct"] = float(m2.group(1).replace(",", "."))

        console.log(f"[green]Posiciones: {len(posiciones)} | Mix: {list(mix.keys())}")
        return posiciones, mix

    async def _parse_seccion_cualitativo(self, text: str, year: int = 0) -> dict:
        """
        Use Claude to extract qualitative data from section 9 subsections.
        The text now comes pre-segmented into VISIÓN, DECISIONES, INVERSIONES blocks.
        """
        schema = {
            "contexto_mercado": (
                "Resumen en 150-250 palabras del contexto de mercado durante el periodo. "
                "Extraer de la sección 'VISIÓN DE LA GESTORA': entorno macro, movimientos "
                "de tipos/divisas/renta variable, eventos clave. Sintetizar fielmente."
            ),
            "decisiones_tomadas": (
                "Resumen de las decisiones de inversión del periodo en 100-200 palabras. "
                "Extraer de 'DECISIONES GENERALES' e 'INVERSIONES CONCRETAS': "
                "qué posiciones se compraron, vendieron o aumentaron/redujeron peso. "
                "Incluir nombres de activos específicos, sectores y geografías. "
                "Si se mencionan contribuidores/detractores al rendimiento, incluirlos. "
                "NUNCA devolver null si hay texto en estas secciones — siempre hay decisiones."
            ),
        }
        try:
            result = extract_structured_data(
                text[:7000],
                schema,
                context=(
                    f"Informe semestral CNMV {year}, fondo {self.isin}. "
                    "El texto contiene subsecciones etiquetadas del Anexo explicativo (sección 9). "
                    "Extraer visión del mercado y decisiones de cartera concretas. "
                    "Es CRÍTICO extraer decisiones_tomadas — buscar en todas las subsecciones."
                ),
            )
            return result if isinstance(result, dict) else {}
        except Exception as exc:
            console.log(f"[yellow]Error Claude sección 9: {exc}")
            return {}

    # ── PDF data merger ───────────────────────────────────────────────────────

    def _merge_pdf_data(self, result: dict, pdf_data: dict) -> None:
        """
        Distributes PDF-extracted data into the right fields of the main result dict.
        PDF data has priority over XML data for overlapping fields.
        """
        # KPIs
        kpis_campos = [
            "num_participes", "num_participes_anterior",
            "coste_gestion_pct", "coste_deposito_pct",
            "ter_pct", "volatilidad_pct", "clasificacion",
            "perfil_riesgo", "benchmark", "divisa",
            "depositario", "fecha_registro",
            "rotacion_cartera_pct", "rotacion_cartera_anterior_pct",
        ]
        for campo in kpis_campos:
            val = pdf_data.get(campo)
            if val is not None:
                result["kpis"][campo] = val

        # Comisiones por clase → cuantitativo (use accumulated series from _process_pdfs)
        serie_comis = pdf_data.get("serie_comisiones_por_clase", [])
        if serie_comis:
            cuant = result.setdefault("cuantitativo", {})
            existing_cls = {e["periodo"]: e for e in cuant.get("serie_comisiones_por_clase", [])}
            for entry in serie_comis:
                p = self._normalize_period(entry.get("periodo", ""))
                entry_copy = dict(entry)
                entry_copy["periodo"] = p
                existing_cls[p] = entry_copy  # PDF overwrites
            cuant["serie_comisiones_por_clase"] = sorted(existing_cls.values(), key=lambda x: x["periodo"])

        # Índice rotación → cuantitativo serie_rotacion (full series from all PDFs)
        serie_rotacion_pdf = pdf_data.get("serie_rotacion_pdf", [])
        if serie_rotacion_pdf:
            cuant = result.setdefault("cuantitativo", {})
            existing_rot = {}
            for e in cuant.get("serie_rotacion", []):
                p = self._normalize_period(e.get("periodo", ""))
                e_copy = dict(e); e_copy["periodo"] = p
                existing_rot[p] = e_copy
            for entry in serie_rotacion_pdf:
                p = self._normalize_period(entry["periodo"])
                entry_copy = dict(entry); entry_copy["periodo"] = p
                existing_rot[p] = entry_copy  # PDF overwrites
            cuant["serie_rotacion"] = sorted(existing_rot.values(), key=lambda x: x["periodo"])
        elif pdf_data.get("rotacion_cartera_pct") is not None:
            # Fallback: single-point from most-recent PDF scalar
            cuant = result.setdefault("cuantitativo", {})
            existing_rot = cuant.get("serie_rotacion", [])
            period_key = pdf_data.get("_periodo_pdf", "")
            year_key = period_key[:4] if period_key else ""
            if year_key and not any(e.get("periodo") == year_key for e in existing_rot):
                existing_rot.append({
                    "periodo": year_key,
                    "rotacion_pct": pdf_data["rotacion_cartera_pct"],
                })
            cuant["serie_rotacion"] = sorted(existing_rot, key=lambda x: x["periodo"])

        # Partícipes → cuantitativo serie_participes (full series from all PDFs)
        serie_participes_pdf = pdf_data.get("serie_participes_pdf", [])
        if serie_participes_pdf:
            cuant = result.setdefault("cuantitativo", {})
            existing_part = {}
            for e in cuant.get("serie_participes", []):
                p = self._normalize_period(e.get("periodo", ""))
                e_copy = dict(e); e_copy["periodo"] = p
                existing_part[p] = e_copy
            for entry in serie_participes_pdf:
                p = self._normalize_period(entry["periodo"])
                entry_copy = dict(entry); entry_copy["periodo"] = p
                existing_part[p] = entry_copy  # PDF overwrites XML
            cuant["serie_participes"] = sorted(existing_part.values(), key=lambda x: x["periodo"])

        # AUM series from PDF — merge with XML series, PDF OVERWRITES XML for same year
        serie_aum_pdf = pdf_data.get("serie_aum_pdf", [])
        if serie_aum_pdf:
            raw_xml = result.get("cuantitativo", {}).get("serie_aum", [])
            existing: dict = {}
            for e in raw_xml:
                p = self._normalize_period(e.get("periodo", ""))
                e_copy = dict(e)
                e_copy["periodo"] = p
                e_copy["_source"] = "xml"
                existing[p] = e_copy
            for entry in serie_aum_pdf:
                p = self._normalize_period(entry["periodo"])
                entry_copy = dict(entry)
                entry_copy["periodo"] = p
                entry_copy["_source"] = "pdf"
                # PDF always overwrites XML (semiannual report is more reliable)
                existing[p] = entry_copy
            # Remove internal _source tag before saving
            for e in existing.values():
                e.pop("_source", None)
            result.setdefault("cuantitativo", {})["serie_aum"] = sorted(
                existing.values(), key=lambda x: x["periodo"]
            )
            # Only update aum_actual_meur if PDF's most recent entry is newer than XML's
            sorted_pdf = sorted(serie_aum_pdf, key=lambda x: x["periodo"])
            latest_pdf_periodo = sorted_pdf[-1]["periodo"]
            all_series = list(result.get("cuantitativo", {}).get("serie_aum", {}).values()) \
                if isinstance(result.get("cuantitativo", {}).get("serie_aum"), dict) \
                else result.get("cuantitativo", {}).get("serie_aum", [])
            latest_xml_periodo = max(
                (e["periodo"] for e in all_series), default=""
            )
            if latest_pdf_periodo >= latest_xml_periodo or result["kpis"].get("aum_actual_meur") is None:
                result["kpis"]["aum_actual_meur"] = sorted_pdf[-1]["valor_meur"]

        # VL base 100 — calcular crecimiento desde primer VL conocido
        aum_with_vl = [e for e in result.get("cuantitativo", {}).get("serie_aum", []) if e.get("vl")]
        if len(aum_with_vl) >= 2:
            vl_sorted = sorted(aum_with_vl, key=lambda x: x["periodo"])
            base_vl = vl_sorted[0]["vl"]
            if base_vl and base_vl > 0:
                result.setdefault("cuantitativo", {})["serie_vl_base100"] = [
                    {"periodo": e["periodo"], "vl": e["vl"], "base100": round(e["vl"] / base_vl * 100, 1)}
                    for e in vl_sorted
                ]

        # TER por clase → cuantitativo
        serie_ter_cls = pdf_data.get("serie_ter_por_clase", [])
        if serie_ter_cls:
            cuant = result.setdefault("cuantitativo", {})
            existing_tcls = {e["periodo"]: e for e in cuant.get("serie_ter_por_clase", [])}
            for entry in serie_ter_cls:
                p = self._normalize_period(entry.get("periodo", ""))
                entry_copy = dict(entry); entry_copy["periodo"] = p
                existing_tcls[p] = entry_copy
            cuant["serie_ter_por_clase"] = sorted(existing_tcls.values(), key=lambda x: x["periodo"])

        # Clases info → cuantitativo
        if pdf_data.get("serie_clases_info"):
            result.setdefault("cuantitativo", {})["serie_clases_info"] = pdf_data["serie_clases_info"]

        # Mix activos historico
        if pdf_data.get("mix_activos_historico"):
            result.setdefault("cuantitativo", {})["mix_activos_historico"] = (
                pdf_data["mix_activos_historico"]
            )

        # TER histórico desde PDF (multi-año por informe) — normalize + PDF overwrites XML
        serie_ter_pdf = pdf_data.get("serie_ter_pdf", [])
        if serie_ter_pdf:
            cuant = result.setdefault("cuantitativo", {})
            existing_ter: dict = {}
            for e in cuant.get("serie_ter", []):
                p = self._normalize_period(e.get("periodo", ""))
                e_copy = dict(e); e_copy["periodo"] = p
                existing_ter[p] = e_copy
            for entry in serie_ter_pdf:
                p = self._normalize_period(entry["periodo"])
                entry_copy = dict(entry); entry_copy["periodo"] = p
                # PDF data overwrites XML for same year
                existing_ter[p] = entry_copy
            cuant["serie_ter"] = sorted(existing_ter.values(), key=lambda x: x["periodo"])

        # Cualitativo
        cual = result.setdefault("cualitativo", {})
        for campo in ["estrategia", "tipo_activos", "filosofia_inversion", "objetivos_reales"]:
            if pdf_data.get(campo):
                cual[campo] = pdf_data[campo]
        # gestores extraction delegated to manager_deep_agent
        if pdf_data.get("historia_fondo"):
            cual["historia_fondo"] = pdf_data["historia_fondo"]

        # Hechos relevantes — acumular de todos los PDFs
        if pdf_data.get("hechos_relevantes"):
            existing_hr = cual.get("hechos_relevantes", [])
            for hr in pdf_data["hechos_relevantes"]:
                if not any(e.get("periodo") == hr.get("periodo") for e in existing_hr):
                    existing_hr.append(hr)
            cual["hechos_relevantes"] = sorted(existing_hr, key=lambda x: x.get("periodo", ""))
        # Note: cualitativo synthesis (contexto_mercado, decisiones_tomadas, tesis_gestora)
        # is now done by analyst_agent from raw text — cnmv_agent only provides raw text

        # Full text of all qualitative sections for analyst_agent (NO char limits)
        for text_field in ["seccion_9_texto_completo", "seccion_10_perspectivas_texto",
                           "seccion_1_politica_texto", "seccion_4_5_hechos_texto"]:
            if pdf_data.get(text_field):
                cual[text_field] = pdf_data[text_field]
        if pdf_data.get("benchmark_mencionado"):
            result.setdefault("kpis", {})["benchmark_mencionado"] = pdf_data["benchmark_mencionado"]

        if pdf_data.get("gestora_pdf") and not result.get("gestora"):
            result["gestora"] = pdf_data["gestora_pdf"]

        # Posiciones
        if pdf_data.get("posiciones_actuales"):
            result["posiciones"]["actuales"] = pdf_data["posiciones_actuales"]
        if pdf_data.get("posiciones_historicas"):
            result["posiciones"]["historicas"] = pdf_data["posiciones_historicas"]

        # Análisis de consistencia
        periodos = pdf_data.get("analisis_periodos", [])
        if periodos:
            result.setdefault("analisis_consistencia", {})["periodos"] = periodos

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _get_nombre_from_xml(self) -> str:
        """
        Extrae la denominación del fondo desde los XMLs FONDREGISTRO.

        Estructura real: FondRegistro > Entidad > [Denominacion] > ... > Clase > [ISIN]
        Buscamos el ISIN en el árbol y subimos a buscar Denominacion del Entidad padre.
        """
        from xml.etree import ElementTree as ET
        for xml_path in sorted(self.xml_dir.glob("*FONDREGISTRO*")):
            try:
                tree = ET.parse(str(xml_path))
                root = tree.getroot()
                # Buscar Entidad que contenga nuestro ISIN
                for entidad in root.iter("Entidad"):
                    for el in entidad.iter("ISIN"):
                        if el.text and el.text.strip().upper() == self.isin.upper():
                            # Encontrado: devolver Denominacion del Entidad
                            denom = entidad.find("Denominacion")
                            if denom is not None and denom.text and denom.text.strip():
                                return denom.text.strip()
            except Exception:
                continue
        return ""

    def _fill_kpis_from_series(self, result: dict) -> None:
        """Rellena KPIs actuales con el último dato disponible de las series."""
        series = result.get("cuantitativo", {})

        aum_series = series.get("serie_aum", [])
        if aum_series:
            result["kpis"]["aum_actual_meur"] = aum_series[-1].get("valor_meur")

        part_series = series.get("serie_participes", [])
        if part_series:
            result["kpis"]["num_participes"] = part_series[-1].get("valor")

        ter_series = series.get("serie_ter", [])
        if ter_series:
            last = ter_series[-1]
            result["kpis"]["ter_pct"] = last.get("ter_pct")
            result["kpis"]["coste_gestion_pct"] = last.get("coste_gestion_pct")

        # Comisión de éxito: check if any year had performance fees > 0
        # Look in the RESULT cuantitativo (already merged at this point)
        cuant_result = result.get("cuantitativo", {})
        serie_com = cuant_result.get("serie_comisiones_por_clase", [])
        has_exito = False
        exito_serie = []
        base_com = "patrimonio"
        for entry in serie_com:
            exito = entry.get("exito", {})
            if exito:
                exito_serie.append({"periodo": entry.get("periodo"), "exito": exito})
                if any(v > 0 for v in exito.values()):
                    has_exito = True
        # Check pdf_data for base_comision (mixta = has performance fee structure)
        if isinstance(pdf_data, dict):
            base_com = pdf_data.get("base_comision", base_com)
            if pdf_data.get("cobra_comision_exito"):
                has_exito = True
        result["comision_exito"] = {
            "existe": has_exito or base_com == "mixta",
            "serie_historica": exito_serie,
            "base_comision": base_com,
        }

    def _save(self, result: dict) -> None:
        """Guarda el resultado parcial o final en cnmv_data.json."""
        output_path = self.fund_dir / "cnmv_data.json"
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path}")


# ── Ejecución standalone ─────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")

    # Config por defecto: análisis completo, desde inicio, todas las fuentes
    config = {
        "objetivo": "1",
        "horizonte_historico": "1",
        "fuentes": "1",
        "clase_accion": "N/A",
        "contexto_adicional": "",
    }

    ISIN = "ES0112231008"
    agent = CNMVAgent(ISIN, config)
    result = asyncio.run(agent.run())

    console.print(Panel(
        f"[bold green]Análisis completado[/bold green]\n"
        f"Nombre: {result.get('nombre', '-')}\n"
        f"NIF: {result.get('nif', '-')}\n"
        f"Gestora: {result.get('gestora', '-')}\n"
        f"XMLs procesados: {len(result.get('fuentes', {}).get('xmls_cnmv', []))}\n"
        f"PDFs descargados: {len(result.get('fuentes', {}).get('informes_descargados', []))}\n"
        f"Posiciones actuales: {len(result.get('posiciones', {}).get('actuales', []))}",
        title=ISIN,
        expand=False,
    ))
