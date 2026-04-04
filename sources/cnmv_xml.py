"""
cnmv_xml.py
Descarga XMLs trimestrales de la CNMV y extrae la serie histórica cuantitativa
de un fondo dado su ISIN.

Los ficheros CNMV contienen:
  - Patrimonio (AUM)
  - Número de partícipes
  - TER
  - Exposición RV / RF / liquidez
  - Rentabilidades

URL patrón trimestral: se descubre desde la página de descarga de la CNMV.
Publicados desde 2009. Periodicidad: mensual (datos básicos) y trimestral (datos completos).
"""

import io
import re
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FundAnalyzer/1.0)"}

# Patrones de URL a intentar para cada periodo YYYYMM
# La CNMV no documenta la URL exacta públicamente, pero estos son los patrones conocidos
URL_PATTERNS = [
    "https://www.cnmv.es/Portal/Publicaciones/IIC/Fondos/IICFI_{mm}{yyyy}.zip",
    "https://www.cnmv.es/Portal/Publicaciones/IIC/IICFI_{mm}{yyyy}.zip",
    "https://www.cnmv.es/DocPortal/PublicacionesIIC/IICFI_{mm}{yyyy}.zip",
    "https://www.cnmv.es/Portal/Publicaciones/IIC/Fondos/IICFIm_{mm}{yyyy}.zip",
]

# Meses de informes semestrales: junio (1H) y diciembre (2H)
SEMIANNUAL_MONTHS = [6, 12]


def discover_zip_urls(year: int) -> list[str]:
    """
    Intenta descubrir las URLs de ZIPs para un año dado visitando
    la página de descarga de la CNMV.
    """
    page_url = f"https://www.cnmv.es/Portal/publicaciones/descarga-informacion-individual?ejercicio={year}&lang=es"
    found = []
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".zip" in href.lower() and "IIC" in href:
                full = href if href.startswith("http") else f"https://www.cnmv.es{href}"
                found.append(full)
    except Exception as e:
        print(f"[cnmv_xml] No se pudo acceder a página de descarga {year}: {e}")
    return found


def try_download_zip(yyyy: int, mm: int) -> bytes | None:
    """
    Intenta descargar el ZIP trimestral para un periodo dado.
    Prueba múltiples patrones de URL.
    """
    mm_str = f"{mm:02d}"
    yyyy_str = str(yyyy)
    
    for pattern in URL_PATTERNS:
        url = pattern.format(mm=mm_str, yyyy=yyyy_str)
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                print(f"[cnmv_xml] ✓ Descargado: {url}")
                return r.content
        except Exception:
            continue
    
    print(f"[cnmv_xml] ✗ No encontrado: {yyyy}-{mm_str}")
    return None


def parse_xml_for_isin(zip_content: bytes, isin: str, yyyy: int, mm: int) -> dict | None:
    """
    Extrae datos del fondo dado su ISIN desde un ZIP con XMLs de la CNMV.
    Devuelve dict con los campos del periodo o None si no se encuentra.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # Buscar el XML principal (puede haber varios)
            xml_files = [f for f in zf.namelist() if f.lower().endswith(".xml")]
            
            for xml_name in xml_files:
                with zf.open(xml_name) as xf:
                    content = xf.read()
                
                # Búsqueda rápida de texto antes de parsear completo
                if isin.encode() not in content:
                    continue
                
                root = ET.fromstring(content)
                
                # Buscar el nodo del fondo por ISIN
                # Los XMLs CNMV usan estructura <IIC><ISIN>...</ISIN>...</IIC>
                # pero el namespace puede variar
                isin_nodes = root.iter()
                for node in isin_nodes:
                    if node.text and node.text.strip() == isin:
                        # Encontrado — subir al padre para leer todos los campos
                        parent = _find_parent(root, node)
                        if parent is not None:
                            return _extract_fields(parent, yyyy, mm)
    
    except zipfile.BadZipFile:
        print(f"[cnmv_xml] ZIP corrupto para {yyyy}-{mm:02d}")
    except Exception as e:
        print(f"[cnmv_xml] Error parseando XML {yyyy}-{mm:02d}: {e}")
    
    return None


def _find_parent(root: ET.Element, target: ET.Element) -> ET.Element | None:
    """Encuentra el padre de un nodo en el árbol XML."""
    for parent in root.iter():
        for child in parent:
            if child is target:
                return parent
            # Subir un nivel más para capturar el registro completo
            for grandchild in child:
                if grandchild is target:
                    return child
    return None


def _extract_fields(node: ET.Element, yyyy: int, mm: int) -> dict:
    """
    Extrae campos relevantes de un nodo XML de fondo CNMV.
    Los nombres de campo son los usados en los XMLs reales de la CNMV.
    """
    def get(tag_variants: list[str]) -> str | None:
        for tag in tag_variants:
            el = node.find(f".//{tag}")
            if el is not None and el.text:
                return el.text.strip()
        return None

    # Patrimonio en miles de euros → convertir a millones
    patrimonio_raw = get(["PATRIMONIO", "Patrimonio", "patrimonio", "TOTAL_PATRIMONIO"])
    aum_meur = None
    if patrimonio_raw:
        try:
            # CNMV publica en miles de euros
            aum_meur = round(float(patrimonio_raw.replace(",", ".")) / 1000, 2)
        except ValueError:
            pass

    # Partícipes
    participes_raw = get(["PARTICIPES", "Participes", "NUM_PARTICIPES", "NPartícipes"])
    participes = None
    if participes_raw:
        try:
            participes = int(float(participes_raw.replace(",", ".")))
        except ValueError:
            pass

    # TER (Ratio de Gastos Totales)
    ter_raw = get(["TER", "GASTOS_CORRIENTES", "GastosCorrientes", "RATIO_GASTOS"])
    ter_pct = None
    if ter_raw:
        try:
            ter_pct = round(float(ter_raw.replace(",", ".")), 4)
        except ValueError:
            pass

    # Exposición RV
    rv_raw = get(["PERC_RV", "PorcentajeRV", "RENTA_VARIABLE_PCT", "PCT_RV"])
    rv_pct = None
    if rv_raw:
        try:
            rv_pct = round(float(rv_raw.replace(",", ".")), 2)
        except ValueError:
            pass

    # Exposición RF
    rf_raw = get(["PERC_RF", "PorcentajeRF", "RENTA_FIJA_PCT", "PCT_RF"])
    rf_pct = None
    if rf_raw:
        try:
            rf_pct = round(float(rf_raw.replace(",", ".")), 2)
        except ValueError:
            pass

    # Liquidez
    liq_raw = get(["PERC_LIQUIDEZ", "PorcentajeLiquidez", "LIQUIDEZ_PCT", "PCT_LIQUIDEZ"])
    liquidez_pct = None
    if liq_raw:
        try:
            liquidez_pct = round(float(liq_raw.replace(",", ".")), 2)
        except ValueError:
            pass

    # Rentabilidad del periodo
    rent_raw = get(["RENTABILIDAD", "Rentabilidad", "RENT_PERIODO"])
    rent_pct = None
    if rent_raw:
        try:
            rent_pct = round(float(rent_raw.replace(",", ".")), 4)
        except ValueError:
            pass

    semestre = "1H" if mm <= 6 else "2H"
    periodo = f"{semestre}{yyyy}"

    return {
        "periodo":      periodo,
        "año":          yyyy,
        "mes":          mm,
        "aum_meur":     aum_meur,
        "participes":   participes,
        "ter_pct":      ter_pct,
        "rv_pct":       rv_pct,
        "rf_pct":       rf_pct,
        "liquidez_pct": liquidez_pct,
        "rent_pct":     rent_pct,
        "fuente":       f"XML CNMV trimestral {yyyy}-{mm:02d}"
    }


def extract_serie_historica(isin: str, año_inicio: int = 2014) -> list[dict]:
    """
    Extrae la serie histórica completa de un fondo dado su ISIN.
    Solo descarga los periodos semestrales (junio y diciembre).
    
    Returns: lista de dicts con datos por semestre, ordenados cronológicamente.
    """
    print(f"[cnmv_xml] Extrayendo serie histórica para {isin} desde {año_inicio}...")
    
    año_actual = datetime.now().year
    serie = []

    for yyyy in range(año_inicio, año_actual + 1):
        for mm in SEMIANNUAL_MONTHS:
            # No pedir datos futuros
            if yyyy == año_actual and mm > datetime.now().month:
                continue
            
            print(f"[cnmv_xml] Descargando periodo {yyyy}-{mm:02d}...")
            zip_content = try_download_zip(yyyy, mm)
            
            if zip_content is None:
                # Marcar como pendiente
                semestre = "1H" if mm <= 6 else "2H"
                serie.append({
                    "periodo":      f"{semestre}{yyyy}",
                    "año":          yyyy,
                    "mes":          mm,
                    "aum_meur":     None,
                    "participes":   None,
                    "ter_pct":      None,
                    "rv_pct":       None,
                    "rf_pct":       None,
                    "liquidez_pct": None,
                    "rent_pct":     None,
                    "fuente":       "pendiente_cnmv"
                })
                continue
            
            datos = parse_xml_for_isin(zip_content, isin, yyyy, mm)
            if datos:
                serie.append(datos)
                print(f"[cnmv_xml] ✓ {datos['periodo']}: AUM={datos['aum_meur']}M€, Partícipes={datos['participes']}")
            else:
                print(f"[cnmv_xml] ✗ ISIN {isin} no encontrado en {yyyy}-{mm:02d}")
    
    print(f"[cnmv_xml] Serie histórica: {len(serie)} periodos extraídos")
    return serie
