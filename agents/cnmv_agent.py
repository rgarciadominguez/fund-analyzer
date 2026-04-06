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

        self._save(result)
        return result

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
                    "estrategia", "tipo_activos", "gestores",
                    "historia_fondo",
                    "rotacion_cartera_pct", "rotacion_cartera_anterior_pct",
                    "comisiones_gestion_por_clase",
                    "_periodo_pdf",
                ]:
                    if parsed.get(campo) is not None:
                        merged[campo] = parsed[campo]

            # Accumulate serie_aum_pdf from ALL PDFs (dedup by periodo)
            for entry in parsed.get("serie_aum_pdf", []):
                periodo = entry.get("periodo", "")
                existing = merged.setdefault("serie_aum_pdf", [])
                if not any(e.get("periodo") == periodo for e in existing):
                    existing.append(entry)

            # Accumulate serie_ter_pdf from ALL PDFs (dedup by periodo)
            for entry in parsed.get("serie_ter_pdf", []):
                periodo = entry.get("periodo", "")
                existing = merged.setdefault("serie_ter_pdf", [])
                if not any(e.get("periodo") == periodo for e in existing):
                    existing.append(entry)

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

            # Historical positions: top10 per period
            if posiciones:
                merged["posiciones_historicas"].append({
                    "periodo": f"{year}-H2",
                    "top10": posiciones[:10],
                })

            # Mix activos per period
            mix = parsed.get("mix_activos", {})
            if mix:
                merged["mix_activos_historico"].append({
                    "periodo": str(year),
                    **mix,
                })

            # Análisis de consistencia
            cual_gestor = parsed.get("cualitativo_gestor", {})
            if cual_gestor:
                periodo_entry = {
                    "periodo": f"{year}-H2",
                    "contexto_mercado": cual_gestor.get("contexto_mercado", ""),
                    "tesis_gestora": cual_gestor.get("tesis_gestora", ""),
                    "decisiones_tomadas": cual_gestor.get("decisiones_tomadas", ""),
                    "resultado_real": "",
                    "consistencia_score": None,
                    "notas": "",
                }
                merged["analisis_periodos"].append(periodo_entry)

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

        result: dict = {"_periodo_pdf": f"{year}-S2"}
        result.update(self._parse_seccion_politica(full_text))
        result.update(self._parse_seccion_datos_generales(full_text, year))
        result.update(self._parse_seccion_comportamiento(full_text, year))
        result.update(self._parse_seccion_hechos_relevantes(full_text, year))

        pos, mix = self._parse_seccion_posiciones(full_text)
        result["posiciones"] = pos
        result["mix_activos"] = mix

        # Section 9: Claude for qualitative market vision + decisions
        sec9 = self._extract_seccion_9(full_text)
        if sec9.strip():
            result["cualitativo_gestor"] = await self._parse_seccion_cualitativo(sec9)

        # Section 10 perspectivas: Claude for tesis_gestora (visión actual del gestor)
        sec10 = self._extract_seccion_perspectivas(full_text)
        if sec10.strip():
            gestor = result.get("cualitativo_gestor", {})
            if not gestor.get("tesis_gestora"):
                schema_persp = {
                    "tesis_gestora": (
                        "Resumen en 100-150 palabras de la visión actual del gestor sobre el mercado "
                        "y el posicionamiento del fondo. Extraído EXCLUSIVAMENTE de la sección 10 "
                        "'PERSPECTIVAS DE MERCADO Y ACTUACIÓN PREVISIBLE DEL FONDO'. "
                        "Incluir: visión macro del gestor, sectores/activos favorecidos, "
                        "riesgos identificados y cómo se posiciona el fondo. "
                        "No inventar, resumir fielmente el texto de la sección."
                    )
                }
                try:
                    persp = extract_structured_data(
                        sec10[:3000], schema_persp,
                        context=(
                            f"Sección 10 informe semestral CNMV {year}. Fondo {self.isin}. "
                            f"Perspectivas de mercado y actuación previsible."
                        ),
                    )
                    if isinstance(persp, dict):
                        result.setdefault("cualitativo_gestor", {})["tesis_gestora"] = persp.get("tesis_gestora", "")
                except Exception:
                    pass

        # Section 1 estrategia: Claude for narrative investment policy
        sec1 = self._extract_seccion_1(full_text)
        if sec1.strip():
            try:
                schema_base = {
                    "estrategia": "política de inversión del fondo en 2-3 frases",
                    "tipo_activos": "tipos de activos en los que invierte",
                    "gestores": [
                        {"nombre": "nombre del gestor", "cargo": "cargo", "background": ""}
                    ],
                }
                base = extract_structured_data(
                    sec1, schema_base,
                    context=f"Política de inversión {self.isin}",
                )
                if isinstance(base, dict):
                    result["estrategia"] = base.get("estrategia", "")
                    result["tipo_activos"] = base.get("tipo_activos", "")
                    result["gestores"] = base.get("gestores", [])
            except Exception as exc:
                console.log(f"[yellow]Error Claude sección 1: {exc}")

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
        """Extract section 9 text (visión gestora, decisiones). Stops before perspectivas (section 10)."""
        m = re.search(r'9\.\s*Anexo\s+explicativo\s+del\s+informe', text, re.IGNORECASE)
        if not m:
            m = re.search(r'Visi[oó]n\s+de\s+la\s+gestora', text, re.IGNORECASE)
        if not m:
            return ""
        start = m.start()
        # Stop before "10. PERSPECTIVAS" or "10. Detalle"
        end_m = re.search(r'10\.\s+(?:PERSPECTIVAS|Detalle)', text[start:], re.IGNORECASE)
        end = start + end_m.start() if end_m else start + 8000
        return text[start:end]

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
        # Per-class table: "CLASE A  455  460  EUR  0,00  0,00  ...  NO"
        # Columns after class name: nº_actual  nº_anterior  [divisa ...]
        # Numbers are small integers (participants count, not big floats)
        class_parts = re.findall(
            r'CLASE\s+\w+\s+([\d.]+)\s+([\d.]+)\s+(?:EUR|USD|GBP)',
            text, re.IGNORECASE,
        )
        if class_parts:
            try:
                result["num_participes"] = sum(int(p[0].replace(".", "")) for p in class_parts)
                result["num_participes_anterior"] = sum(int(p[1].replace(".", "")) for p in class_parts)
            except ValueError:
                pass

        if "num_participes" not in result:
            # Fallback: aggregate row "Nº de Partícipes  5.176  4.902"
            m = re.search(r'N[oº°]\s*\.?\s*de\s+Part[íi]cipes\s+([\d.]+)\s+([\d.]+)', text)
            if m:
                result["num_participes"] = int(m.group(1).replace(".", ""))
                result["num_participes_anterior"] = int(m.group(2).replace(".", ""))

        # ── AUM (Patrimonio) ──────────────────────────────────────────────────
        # Strategy: find the patrimonio table and sum all classes for each column.
        # Table header: "Patrimonio (en miles)" then rows: "CLASE A  119.070  96.488  22.106  7.831"
        # Columns: actual, dic_anterior, dic_ante_anterior, dic_ante_ante_anterior
        serie_aum: list[dict] = []

        pat_section_m = re.search(
            r'Patrimonio\s*\(en\s*miles\)',
            text, re.IGNORECASE,
        )
        if pat_section_m:
            pat_block = text[pat_section_m.start(): pat_section_m.start() + 1500]
            # Find all CLASE rows: CLASE X  val1  val2  val3  val4
            class_rows = re.findall(
                r'CLASE\s+\w+\s+([\d.]+)\s+([\d.]+)(?:\s+([\d.]+))?(?:\s+([\d.]+))?',
                pat_block, re.IGNORECASE,
            )
            if class_rows:
                # Sum column 0 = current period, col 1 = previous year, etc.
                def _col_sum(rows, col):
                    total = 0.0
                    for row in rows:
                        v = row[col] if col < len(row) and row[col] else "0"
                        total += float(v.replace(".", "").replace(",", "."))
                    return total

                aum_current = _col_sum(class_rows, 0)
                if aum_current > 0:
                    serie_aum.append({
                        "periodo": f"{year}-S2",
                        "valor_meur": round(aum_current / 1000, 3),
                    })
                # Previous years from subsequent columns
                for col_offset, yr_delta in [(1, 1), (2, 2), (3, 3)]:
                    aum_prev = _col_sum(class_rows, col_offset)
                    if aum_prev > 100:  # sanity: > 100 miles EUR
                        prev_yr = year - yr_delta
                        if not any(e["periodo"] == str(prev_yr) for e in serie_aum):
                            serie_aum.append({
                                "periodo": str(prev_yr),
                                "valor_meur": round(aum_prev / 1000, 3),
                            })

        # Fallback: "Periodo del informe  93.272  111,4533"
        if not serie_aum:
            m = re.search(
                r'Periodo\s+del\s+informe\s+([\d.]+)\s+([\d,]+)',
                text, re.IGNORECASE,
            )
            if m:
                serie_aum.append({
                    "periodo": f"{year}-S2",
                    "valor_meur": round(
                        float(m.group(1).replace(".", "").replace(",", ".")) / 1000, 3
                    ),
                    "vl": float(m.group(2).replace(",", ".")),
                })
            # Historical years (no VL — previous regex was buggy extracting year as VL)
            for m in re.finditer(r'\b(20\d{2})\b\s+([\d.]+)\s*\n', text):
                yr = int(m.group(1))
                if 2000 <= yr < year:
                    val = float(m.group(2).replace(".", "").replace(",", "."))
                    if 100 <= val <= 500_000 and not any(e["periodo"] == str(yr) for e in serie_aum):
                        serie_aum.append({"periodo": str(yr), "valor_meur": round(val / 1000, 3)})

        # ── VL (Valor liquidativo) — extraer de tabla separada ────────────────
        # Table: "Valor liquidativo de la participación (%)"
        # Rows: "CLASE A  30,0272  26,5477  22,036  19,312"
        vl_section_m = re.search(r'Valor\s+liquidativo\s+de\s+la\s+participaci', text, re.IGNORECASE)
        if vl_section_m:
            vl_block = text[vl_section_m.start(): vl_section_m.start() + 800]
            vl_rows = re.findall(
                r'CLASE\s+\w+\s+([\d,]+)\s+([\d,]+)(?:\s+([\d,]+))?(?:\s+([\d,]+))?',
                vl_block, re.IGNORECASE,
            )
            if vl_rows:
                # Use first class as representative VL (or average)
                vl_current = float(vl_rows[0][0].replace(",", "."))
                # Attach VL to current period entry
                for e in serie_aum:
                    if e["periodo"] == f"{year}-S2":
                        e["vl"] = vl_current
                        break

        if serie_aum:
            result["serie_aum_pdf"] = sorted(
                serie_aum, key=lambda x: x["periodo"], reverse=True
            )

        # ── Comisión de gestión — por clase ──────────────────────────────────
        # The table has a complex header; rows look like:
        # "CLASE A  0,40  0,00  0,40  0,80  0,04  0,08  0,04  0,08"
        # We need the 4th number (acumulada gestión s/patrimonio)
        # The table appears AFTER "Comisiones aplicadas en el período" header
        comis_section = ""
        comis_m = re.search(r'Comisiones\s+aplicadas\s+en\s+el\s+per', text, re.IGNORECASE)
        if comis_m:
            comis_section = text[comis_m.start(): comis_m.start() + 1000]
        else:
            comis_section = text  # fallback to full text

        class_comisiones = re.findall(
            r'CLASE\s+(\w+)\s+([\d,]+)\s+[\d,]+\s+[\d,]+\s+([\d,]+)',
            comis_section, re.IGNORECASE,
        )
        # Reformat: group(1)=clase, group(2)=periodo%, group(3)=acumulada%
        class_comisiones = [(m[0], m[2]) for m in class_comisiones]  # (clase, acumulada)
        if class_comisiones:
            por_clase = {cls: float(val.replace(",", ".")) for cls, val in class_comisiones}
            result["comisiones_gestion_por_clase"] = por_clase
            # coste_gestion_pct = mínima (clase más barata, generalmente A)
            result["coste_gestion_pct"] = min(por_clase.values())
        else:
            # Fallback: aggregate row
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
        """Parse section 2.2: TER (multi-year), volatility, índice rotación cartera."""
        result: dict = {}

        # ── TER (Ratio total de gastos) — extraer serie histórica ────────────
        # The table row looks like:
        # "Ratio total de gastos  0,90  0,23  0,23  0,22  0,22  0,91  0,92  0,92  0,94"
        # Columns: acum_actual, trim1, trim2, trim3, trim4, año_anterior, ante_anterior...
        # Annual columns (years) start at position 5 onwards depending on layout.
        # We also look for explicit year labels: "2024  0,91"
        # TER row: "Ratio total de gastos|0,90 0,23 0,23 0,22 0,22 0,91 0,92 0,92 0,94"
        # Format: acum_actual trim1 trim2 trim3 trim4 año_t-1 año_t-2 año_t-3 año_t-5
        # (9 numbers total for semi-annual; fewer for older reports)
        ter_m = re.search(r'Ratio\s+total\s+de\s+gastos', text, re.IGNORECASE)
        serie_ter: list[dict] = []
        if ter_m:
            ter_block = text[ter_m.start(): ter_m.start() + 400]
            nums = re.findall(r'[\d,]+', ter_block)
            # Filter: only numbers that look like TER percentages (0.01 – 5.0)
            ter_nums = []
            for n in nums:
                try:
                    v = float(n.replace(",", "."))
                    if 0.01 <= v <= 5.0:
                        ter_nums.append(v)
                except ValueError:
                    pass
            if ter_nums:
                result["ter_pct"] = ter_nums[0]  # acumulado actual
                # Annual values: positions 5,6,7,8 in the full sequence
                # (positions 1-4 are quarterly breakdowns)
                annual_vals = ter_nums[5:9] if len(ter_nums) >= 6 else ter_nums[1:]
                for i, v in enumerate(annual_vals):
                    yr = year - i if year else 0
                    if yr > 2010:
                        serie_ter.append({"periodo": str(yr), "ter_pct": round(v, 4)})

        if serie_ter:
            result["serie_ter_pdf"] = sorted(serie_ter, key=lambda x: x["periodo"], reverse=True)

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
                result["rotacion_cartera_pct"] = float(m.group(1).replace(",", "."))
                result["rotacion_cartera_anterior_pct"] = float(m.group(2).replace(",", "."))
        else:
            # 4-number format: take positions 3 and 4 (annual figures)
            result["rotacion_cartera_pct"] = float(m.group(2).replace(",", "."))
            result["rotacion_cartera_anterior_pct"] = float(m.group(3).replace(",", "."))

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

        # Only add if meaningful content found
        if epigrafe_si or (detalle and len(detalle) > 30):
            periodo_str = f"{year}-S2" if year else ""
            result["hechos_relevantes"] = [{
                "periodo": periodo_str,
                "epigrafe": "; ".join(epigrafe_si) if epigrafe_si else "",
                "detalle": detalle[:1500],
            }]

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

        # Locate "10. Detalle de inversiones financieras"
        m_sec = re.search(
            r'10\.\s*Detalle\s+de\s+inversiones\s+financieras',
            text, re.IGNORECASE,
        )
        if not m_sec:
            # Fallback: section 3.1
            m_sec = re.search(r'3\.1\s*Inversiones\s+financieras', text, re.IGNORECASE)
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
            # Find grand total row (not the INTERIOR / EXTERIOR subtotals)
            grand_m = re.search(
                r'TOTAL\s+INVERSIONES\s+FINANCIERAS\s+[\d.,]+\s+[\d,]+[^\n]*\n',
                sec10, re.IGNORECASE,
            )
            if grand_m:
                # Keep through end of that line then stop (max 200 extra chars for liquidez row)
                sec10 = sec10[:grand_m.end() + 200]

        # Position pattern:
        # ISIN - TYPE|name_fields  CURRENCY  value_miles  pct%
        pat = re.compile(
            r'\b([A-Z]{2}[A-Z0-9]{10})\s*-\s*([A-Z][A-Z ]*?)\|(.+?)\s+'
            r'(EUR|USD|GBP|CHF|SEK|CAD|AUD|NOK|DKK|JPY|CHF)\s+'
            r'([\d.]+)\s+([\d,]+)',
            re.MULTILINE,
        )

        for m in pat.finditer(sec10):
            isin_pos = m.group(1)
            tipo = m.group(2).strip()
            rest = m.group(3).strip()
            divisa = m.group(4)

            # Skip zero-value current positions
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
                "tipo": tipo,
                "divisa": divisa,
                "valor_mercado_miles": valor_miles,
                "peso_pct": peso,
            }
            if cupon is not None:
                entry["cupon"] = cupon
            if vencimiento:
                entry["vencimiento"] = vencimiento

            posiciones.append(entry)

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

    async def _parse_seccion_cualitativo(self, text: str) -> dict:
        """
        Use Claude to extract qualitative data from section 9
        (market vision + specific investment decisions).
        """
        schema = {
            "contexto_mercado": (
                "Resumen en 150-250 palabras del contexto de mercado durante el periodo, "
                "tal como lo describe la gestora en su informe semestral: entorno macro, "
                "movimientos relevantes de tipos/divisas/renta variable, eventos clave. "
                "Sintetizar fielmente, no copiar literalmente."
            ),
            "decisiones_tomadas": (
                "Inversiones concretas realizadas durante el periodo: posiciones que más aportaron "
                "o detractaron, cambios de cartera relevantes (entradas, salidas, aumentos de peso), "
                "con nombres de activos específicos si se mencionan. 100-200 palabras."
            ),
        }
        try:
            result = extract_structured_data(
                text[:6000],
                schema,
                context=(
                    f"Sección 9 informe semestral CNMV año {self.isin}. "
                    "Anexo explicativo de la visión gestora y decisiones de inversión. "
                    "Sintetizar los puntos clave sin copiar literalmente el texto."
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

        # Comisiones por clase → cuantitativo
        if pdf_data.get("comisiones_gestion_por_clase"):
            cuant = result.setdefault("cuantitativo", {})
            existing_cls = cuant.get("serie_comisiones_por_clase", [])
            # Add entry for this period if not already present
            period_key = pdf_data.get("_periodo_pdf", "")
            year_key = period_key[:4] if period_key else ""
            if year_key and not any(e.get("periodo") == year_key for e in existing_cls):
                existing_cls.append({
                    "periodo": year_key,
                    "clases": pdf_data["comisiones_gestion_por_clase"],
                })
            cuant["serie_comisiones_por_clase"] = existing_cls

        # Índice rotación → cuantitativo serie_rotacion
        if pdf_data.get("rotacion_cartera_pct") is not None:
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

        # AUM series from PDF — merge with XML series without duplicating
        serie_aum_pdf = pdf_data.get("serie_aum_pdf", [])
        if serie_aum_pdf:
            existing = {
                e["periodo"]: e
                for e in result.get("cuantitativo", {}).get("serie_aum", [])
            }
            for entry in serie_aum_pdf:
                p = entry["periodo"]
                if p not in existing:
                    existing[p] = entry
                elif "vl" not in existing.get(p, {}):
                    existing[p]["vl"] = entry.get("vl")
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

        # Mix activos historico
        if pdf_data.get("mix_activos_historico"):
            result.setdefault("cuantitativo", {})["mix_activos_historico"] = (
                pdf_data["mix_activos_historico"]
            )

        # TER histórico desde PDF (multi-año por informe)
        serie_ter_pdf = pdf_data.get("serie_ter_pdf", [])
        if serie_ter_pdf:
            cuant = result.setdefault("cuantitativo", {})
            existing_ter = {e["periodo"]: e for e in cuant.get("serie_ter", [])}
            for entry in serie_ter_pdf:
                p = entry["periodo"]
                if p not in existing_ter:
                    existing_ter[p] = entry
                elif entry.get("ter_pct") and not existing_ter[p].get("ter_pct"):
                    existing_ter[p]["ter_pct"] = entry["ter_pct"]
            cuant["serie_ter"] = sorted(existing_ter.values(), key=lambda x: x["periodo"])

        # Cualitativo
        cual = result.setdefault("cualitativo", {})
        for campo in ["estrategia", "tipo_activos", "filosofia_inversion", "objetivos_reales"]:
            if pdf_data.get(campo):
                cual[campo] = pdf_data[campo]
        if pdf_data.get("gestores"):
            cual["gestores"] = pdf_data["gestores"]
        if pdf_data.get("historia_fondo"):
            cual["historia_fondo"] = pdf_data["historia_fondo"]

        # Hechos relevantes — acumular de todos los PDFs
        if pdf_data.get("hechos_relevantes"):
            existing_hr = cual.get("hechos_relevantes", [])
            for hr in pdf_data["hechos_relevantes"]:
                if not any(e.get("periodo") == hr.get("periodo") for e in existing_hr):
                    existing_hr.append(hr)
            cual["hechos_relevantes"] = sorted(existing_hr, key=lambda x: x.get("periodo", ""))
        # Qualitative fields from section 9/10
        for campo in ["contexto_mercado", "decisiones_tomadas", "tesis_gestora"]:
            if pdf_data.get(campo):
                cual[campo] = pdf_data[campo]

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
