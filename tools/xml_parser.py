"""
Parsers para XMLs del bulk data CNMV (IIC - Instituciones de Inversión Colectiva).

Catálogo: https://www.cnmv.es/portal/publicaciones/descarga-informacion-individual

Estructura REAL observada en los ZIPs del catálogo (2025):

  FondTrim (trimestral)       FondMens (mensual)          FondRegistro
  ──────────────────          ──────────────────          ────────────
  <FechaDatos>202506          <FechaDatos>202511          <FechaDatos>202511
  <Entidad>                   <Entidad>                   <Clase>
    <Tipo>FI                    <Tipo>FI                    <ISIN>...
    <NumeroRegistro>            <Compartimento>             <DenominacionClase>
    <Clase>                       <Clase>
      <ISIN>...                     <ISIN>...
      <Patrimonio>  ← EUR           <PatrimonioDiario> ← EUR
      <NumeroParticipes>            <ParticipesDiario>
      <ComisionGestion>             <VLDiario>
      <RatioTotalGastos>
      <Rentabilidad>

Notas de unidades:
  - FondTrim.Patrimonio      → EUR  → dividir /1e6 para obtener M€
  - FondMens.PatrimonioDiario → EUR → dividir /1e6 para obtener M€
  - FechaDatos siempre en root, no en el elemento Clase
"""
from pathlib import Path
from xml.etree import ElementTree as ET

from rich.console import Console

console = Console()

# Tags de elemento-fondo en XMLs CNMV (estructura real observada)
_FUND_ELEMENT_TAGS = {"CLASE", "FONDO", "IIC_DATO", "FONDO_IIC", "DATO", "REGISTRO"}

# Mapa tag XML (uppercase) → clave normalizada en el dict de salida
_FIELD_MAP = {
    # Identificación
    "ISIN": "isin",
    "DENOMINACION": "nombre",
    "DENOMINACIONCLASE": "nombre",
    "NIF": "nif",
    "GESTORA": "gestora",
    "NUMEROCLASE": "numero_clase",
    "NUMERORREGISTRO": "numero_registro",
    # Patrimonio / AUM  (siempre en EUR en XMLs reales → conv. a M€ en código)
    "PATRIMONIO": "patrimonio_eur",
    "PATRIMONIODIARIO": "patrimonio_eur",
    # Partícipes / participaciones
    "NUMEROPARTICIPES": "num_participes",
    "PARTICIPESDIARIO": "num_participes",
    "NUMEROPARTICIPACIONES": "num_participaciones",
    "NUM_PARTICIPES": "num_participes",
    # Valor liquidativo
    "VALORLIQUIDATIVO": "valor_liquidativo",
    "VLDIARIO": "valor_liquidativo",
    # Comisiones
    "COMISIONGESTION": "coste_gestion_pct",
    "COMISION_GESTION": "coste_gestion_pct",
    "COMISIONDEPOSITARIO": "coste_deposito_pct",
    "RATIOTOTALGSTOS": "ter_pct",
    "RATIOTOTALGASTOS": "ter_pct",
    "RATIO_GASTOS": "ter_pct",
    # Rentabilidad
    "RENTABILIDAD": "rentabilidad_anual_pct",
    "RENTABILIDAD_ANUAL": "rentabilidad_anual_pct",
    "RENTABILIDAD_TRIMESTRAL": "rentabilidad_trimestral_pct",
    "RENTABILIDAD_ACUMULADA": "rentabilidad_acumulada_pct",
    # Volatilidad
    "VOLATILIDAD_VL": "volatilidad_pct",
}

# Tags cuyo valor debe permanecer como string
_STRING_FIELDS = {"ISIN", "NIF", "DENOMINACION", "DENOMINACIONCLASE", "GESTORA"}


def _normalize_value(value: str | None) -> str | float | int | None:
    """Convierte strings numéricos a float/int; preserva strings no numéricos."""
    if value is None:
        return None
    value = value.strip().replace(",", ".")
    if not value:
        return None
    try:
        f = float(value)
        return int(f) if f == int(f) and abs(f) < 1e12 else f
    except ValueError:
        return value


def _parse_clase_element(element: ET.Element) -> dict:
    """Extrae todos los campos de un elemento <Clase> u homólogo."""
    record: dict = {}
    for child in element:
        tag = child.tag.strip()
        tag_upper = tag.upper()
        key = _FIELD_MAP.get(tag_upper, tag_upper.lower())
        if tag_upper in _STRING_FIELDS:
            record[key] = child.text.strip() if child.text else None
        else:
            record[key] = _normalize_value(child.text)

    # Convertir Patrimonio de EUR a M€
    if record.get("patrimonio_eur") is not None:
        try:
            record["patrimonio_meur"] = round(float(record["patrimonio_eur"]) / 1_000_000, 4)
        except (ValueError, TypeError):
            pass

    return record


def _find_clase_elements(root: ET.Element) -> list[ET.Element]:
    """
    Busca recursivamente todos los elementos tipo <Clase> (o equivalente).
    En los XMLs CNMV reales el fondo está en <Clase>, no en <FONDO>.
    """
    elements: list[ET.Element] = []

    def _walk(node: ET.Element) -> None:
        if node.tag.upper() in _FUND_ELEMENT_TAGS:
            elements.append(node)
        else:
            for child in node:
                _walk(child)

    _walk(root)

    # Fallback si no se encontraron tags conocidos
    if not elements and len(root) > 0:
        elements = list(root)

    return elements


def parse_cnmv_iic_xml(xml_path: str, isin: str) -> dict:
    """
    Extrae el registro del fondo identificado por ISIN de un XML CNMV IIC.

    Inyecta el periodo desde <FechaDatos> (root-level) en el registro.

    Returns:
        Dict con los campos del fondo para ese periodo, o {} si no se encuentra.
    """
    isin_upper = isin.strip().upper()

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as exc:
        console.log(f"[red]Error parseando {Path(xml_path).name}: {exc}")
        return {}

    root = tree.getroot()

    # Periodo siempre en <FechaDatos> del root (YYYYMM o YYYYQN)
    fd_el = root.find(".//FechaDatos")
    periodo = fd_el.text.strip() if (fd_el is not None and fd_el.text) else ""

    clase_elements = _find_clase_elements(root)

    for elem in clase_elements:
        # Buscar ISIN en sub-tags (case-insensitive)
        isin_el = None
        for child in elem.iter():
            if child.tag.upper() == "ISIN" and child.text:
                isin_el = child
                break

        if isin_el is not None and isin_el.text.strip().upper() == isin_upper:
            record = _parse_clase_element(elem)
            record["periodo"] = periodo  # inyectar periodo del root

            # Extraer nombre desde Entidad padre si no está en el Clase
            if not record.get("nombre"):
                parent = _find_parent(root, elem)
                if parent is not None:
                    denom = parent.find(".//DenominacionClase")
                    if denom is not None and denom.text:
                        record["nombre"] = denom.text.strip()

            console.log(
                f"[green]{Path(xml_path).name}: ISIN {isin} encontrado "
                f"(periodo={periodo}, AUM={record.get('patrimonio_meur', '?')} M€)"
            )
            return record

    console.log(f"[dim]{Path(xml_path).name}: ISIN {isin} no encontrado")
    return {}


def _find_parent(root: ET.Element, target: ET.Element) -> ET.Element | None:
    """Busca el padre directo de `target` en el árbol."""
    for parent in root.iter():
        for child in parent:
            if child is target:
                return parent
    return None


def build_historical_series(xml_dir: str, isin: str) -> dict:
    """
    Procesa todos los XMLs del directorio y construye series históricas para el ISIN.

    Detecta el tipo de XML por el nombre del fichero:
      - FONDTRIM  → serie_aum, serie_participes, serie_ter, serie_rentabilidad
      - FONDMENS  → serie_aum, serie_participes (datos diarios del último día)
      - FONDREGISTRO → solo nombre/denominación (sin datos cuantitativos)

    Returns:
        {
          "serie_aum":          [{"periodo": "YYYYMM", "valor_meur": float}],
          "serie_participes":   [{"periodo": "YYYYMM", "valor": int}],
          "serie_ter":          [{"periodo": "YYYYMM", "ter_pct": float|None, "coste_gestion_pct": float}],
          "serie_rentabilidad": [{"periodo": "YYYYMM", "rentabilidad_pct": float}],
        }
    """
    xml_files = sorted(Path(xml_dir).glob("*.xml"))
    if not xml_files:
        console.log(f"[yellow]No hay XMLs en {xml_dir}")
        return {}

    series: dict = {
        "serie_aum": [],
        "serie_participes": [],
        "serie_ter": [],
        "serie_rentabilidad": [],
    }

    console.log(f"[blue]Procesando {len(xml_files)} XMLs para {isin}...")

    for xml_path in xml_files:
        stem_upper = xml_path.stem.upper()
        is_trim = "FONDTRIM" in stem_upper
        is_mens = "FONDMENS" in stem_upper
        is_registro = "FONDREGISTRO" in stem_upper

        # FONDREGISTRO no tiene datos cuantitativos útiles
        if is_registro:
            continue

        record = parse_cnmv_iic_xml(str(xml_path), isin)
        if not record:
            continue

        periodo = record.get("periodo") or ""

        # AUM (disponible en FONDTRIM siempre; en FONDMENS sólo si no está vacío)
        aum = record.get("patrimonio_meur")
        if aum is not None and aum > 0:
            series["serie_aum"].append({"periodo": periodo, "valor_meur": aum})

        # Partícipes
        part = record.get("num_participes")
        if part is not None and part > 0:
            series["serie_participes"].append({"periodo": periodo, "valor": int(part)})

        # TER / comisiones (sólo FONDTRIM tiene estos campos)
        if is_trim:
            coste = record.get("coste_gestion_pct")
            ter = record.get("ter_pct") or None  # suele ser vacío → None
            if coste is not None:
                series["serie_ter"].append({
                    "periodo": periodo,
                    "ter_pct": ter,
                    "coste_gestion_pct": coste,
                })

            # Rentabilidad (suele estar vacía en FONDTRIM; incluir si hay valor)
            rent = record.get("rentabilidad_anual_pct")
            if rent is not None and rent != 0:
                series["serie_rentabilidad"].append({
                    "periodo": periodo,
                    "rentabilidad_pct": rent,
                })

    # Deduplicar por periodo (quedarse con el último por si hay dos XMLs del mismo mes)
    for key in series:
        seen: dict = {}
        for entry in series[key]:
            p = str(entry.get("periodo") or "")
            seen[p] = entry  # sobrescribe con el más reciente (por orden de fichero)
        series[key] = sorted(seen.values(), key=lambda x: str(x.get("periodo") or ""))

    total = sum(len(v) for v in series.values())
    console.log(f"[green]Series construidas: {total} puntos para {isin}")
    return series
