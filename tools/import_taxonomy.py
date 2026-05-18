"""
import_taxonomy.py — Importa el universo curado de fondos del usuario
desde Excel v2 (hoja "Listado fondos") y produce las estructuras del
schema Supabase v2-cowork.

FUENTE: data/fund_taxonomy_source.xlsx (Excel v2 del usuario, gitignored)
HOJA:   "Listado fondos" (única — las otras hojas son catálogos de brokers
        que van a fund_candidates por separado, no aquí)

OUTPUT (2 estructuras):
  - fund_groups_data: list[dict] (1 por nombre_base + gestora)
  - funds_data:       list[dict] (1 por ISIN, con fund_group_id apuntando)

Compatibilidad legacy: sigue generando `data/fund_taxonomy.json` para
consumidores existentes (build_catalog, web_server, etc.).

CORRECCIONES MANUALES: lee `data/excel_corrections.json` y aplica antes
del parsing — corrige typos del Excel sin modificar el original.

Uso:
    python -m tools.import_taxonomy                    # full build, sin subir
    python -m tools.import_taxonomy --dry-run          # resumen sin escribir
    python -m tools.import_taxonomy --upload-supabase  # upsert real a Supabase
    python -m tools.import_taxonomy --pretty
    python -m tools.import_taxonomy --source path/to/other.xlsx

Heurísticas:
  - normalize_nombre_base(): quita sufijos Clase/divisa/hedge/dist/wrappers
  - extract_gestora(): primera palabra del nombre (placeholder — agents downstream sustituyen)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Forzar UTF-8 en stdout/stderr para Windows PowerShell (cp1252 default).
# Sin esto, prints con caracteres no-ASCII (✓, ñ, é) crashean con UnicodeEncodeError.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, Exception):
    pass

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DEFAULT_SOURCE = DATA_DIR / "fund_taxonomy_source.xlsx"
OUTPUT_FILE = DATA_DIR / "fund_taxonomy.json"
CORRECTIONS_FILE = DATA_DIR / "excel_corrections.json"

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")
FUND_GROUPS_NAMESPACE = uuid.UUID("e4f29c6e-1f3a-4d8e-9ab2-7c0d2b5a9e10")

# ---------------------------------------------------------------------------
# Mapeo de columnas de la hoja "Listado fondos" (Excel v2)
# Validado contra el Excel real — header en filas 0-2, datos desde fila 4
# ---------------------------------------------------------------------------
SHEET_NAME = "Listado fondos"
HEADER_ROWS = 3              # filas 0,1,2 son cabeceras multi-nivel
DATA_START_ROW = 3           # primera fila de datos (4ª línea, 0-indexed)

COL_CATEGORIA = 3            # D
COL_TIPO_ACTIVO = 4          # E
COL_GEOGRAFIA = 5            # F
COL_ISSUER = 6               # G
COL_ESTILO = 7               # H (Tipo: Value/Growth/SmallCaps/Floating rate...)
COL_CLASE_COMERCIAL = 8      # I (Retail/Limpia/Institucional)
COL_DIVISA = 9               # J (Euro/Dolares/EuroHedge)
COL_ISIN = 16                # Q
COL_NOMBRE = 17              # R
COL_HORIZONTE_CORTO = 20     # U
COL_HORIZONTE_MEDIO = 21     # V
COL_HORIZONTE_LARGO = 22     # W
COL_AÑOS_ANTIGUEDAD = 23     # X (Nº años del fondo)
COL_AUM_MEUR = 24            # Y
COL_BROKERS_START = 25       # Z..AI (10 cols)
COL_BROKERS_END = 35         # exclusivo (range(25, 35) = idx 25-34)
COL_TER = 35                 # AJ (decimal 0.0086 = 0.86%)
COL_COMISION_EXITO = 36      # AK
COL_EVAL_START = 54          # BC
COL_EVAL_END = 60            # exclusivo (idx 54-59)
COL_CLASIFICACION = 60       # BI
COL_ENCAJE = 61              # BJ
COL_OPINION = 62             # BK

BROKER_NAMES = [
    "MyInvestor", "Renta4", "Mapfre", "BBVA", "Caixa",
    "ABANCA", "Santander", "CJRS", "Ironia", "EBN",
]

EVAL_KEYS = [
    "rent_riesgo", "comisiones", "accesibilidad",
    "equipo_gestor", "riesgo_ucits", "correlacion",
]


# ===========================================================================
# Utilidades de normalización
# ===========================================================================

def is_valid_isin(s: str) -> bool:
    if not isinstance(s, str):
        return False
    return bool(ISIN_REGEX.match(s.strip().upper()))


def normalize_isin(v) -> str | None:
    if v is None:
        return None
    try:
        s = str(v).strip().replace("\xa0", "").upper()
    except Exception:
        return None
    return s if ISIN_REGEX.match(s) else None


def normalize_str(v) -> str:
    if v is None:
        return ""
    try:
        s = str(v).replace("\xa0", " ").strip()
    except Exception:
        return ""
    if s.lower() in ("nan", "none", "", "0", "0.0"):
        return ""
    return s


def normalize_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


def normalize_int(v) -> int | None:
    f = normalize_float(v)
    if f is None:
        return None
    return int(round(f))


def normalize_flag(v) -> bool:
    """Convierte 'X', 1, 'x', True a True. Resto False."""
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("x", "1", "1.0", "true", "yes", "si", "sí")


# ===========================================================================
# Normalización de nombre_base + extracción gestora
# ===========================================================================

_CURRENCY_TOKENS = {
    "EUR", "USD", "CHF", "GBP", "JPY", "CAD", "AUD", "SEK", "NOK", "DKK",
    "HKD", "SGD", "CNH", "CNY",
}
_DIST_TOKENS = {"ACC", "DIST", "ACUMULACION", "ACUMULACIÓN", "DISTRIBUCION", "DISTRIBUCIÓN", "INC"}
_HEDGE_TOKENS = {"HEDGED", "HEDGE", "UNHEDGED"}
_WRAPPER_TOKENS = {"FI", "FIL", "FCP", "SICAV", "PLC", "LTD", "SA"}
_RETAIL_TOKENS = {"RETAIL", "INSTITUTIONAL", "INST", "CORPORATE"}
_CLASS_WORD_TOKENS = {"CLASE", "CLASS", "CL"}
_NOISE_TOKENS = (
    _CURRENCY_TOKENS | _DIST_TOKENS | _HEDGE_TOKENS
    | _WRAPPER_TOKENS | _RETAIL_TOKENS | _CLASS_WORD_TOKENS
)

_CLASS_LETTER_RE = re.compile(r"^[A-Z]{1,3}\d?$")
_CLASS_PREFIX_RE = re.compile(r"^(CLASE|CLASS|CL|SHARE\s*CLASS)\b", re.IGNORECASE)
_HEDGE_INLINE_RE = re.compile(
    r"\b(EUR|USD|CHF|GBP|JPY|CAD)\s+(HEDGED|HEDGE)\b", re.IGNORECASE,
)
_HEDGE_PREFIX_RE = re.compile(r"\bH[\-\s]+(EUR|USD|CHF|GBP|JPY|CAD)\b", re.IGNORECASE)
_PAREN_NOISE_RE = re.compile(
    r"\((acc|dist|inc|usd|eur|chf|gbp|jpy|hedged|hedge)\)", re.IGNORECASE,
)


def normalize_nombre_base(nombre: str) -> str:
    """Quita sufijos de clase/divisa/hedge/dist del nombre para agrupar clases."""
    if not nombre:
        return ""
    s = str(nombre).strip()
    s = _PAREN_NOISE_RE.sub(" ", s)
    s = _HEDGE_INLINE_RE.sub(" ", s)
    s = _HEDGE_PREFIX_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = s.split(" ")

    if len(tokens) >= 2 and _CLASS_PREFIX_RE.match(tokens[-2]):
        tokens = tokens[:-2]
    elif len(tokens) >= 1 and _CLASS_PREFIX_RE.match(tokens[-1]):
        tokens = tokens[:-1]

    changed = True
    while changed and tokens:
        changed = False
        last = tokens[-1].upper().rstrip(",.;:")
        if last in _NOISE_TOKENS:
            tokens.pop()
            changed = True
            continue
        if _CLASS_LETTER_RE.match(last) and len(tokens) > 1:
            tokens.pop()
            changed = True
            continue

    cleaned = " ".join(tokens).strip()
    return re.sub(r"\s+", " ", cleaned)


def extract_gestora(nombre: str) -> str:
    """Heurística best-effort. Sobreescribible por agents CNMV/CSSF luego."""
    if not nombre:
        return ""
    base = normalize_nombre_base(nombre) or nombre
    tokens = base.split()
    if not tokens:
        return ""
    if len(tokens) >= 2 and tokens[1].isdigit():
        return f"{tokens[0]} {tokens[1]}"
    return tokens[0]


def _group_key(nombre_base: str, gestora: str) -> str:
    return f"{gestora.strip().lower()}::{nombre_base.strip().lower()}"


def _deterministic_uuid(nombre_base: str, gestora: str) -> str:
    return str(uuid.uuid5(FUND_GROUPS_NAMESPACE, _group_key(nombre_base, gestora)))


# ===========================================================================
# Correcciones manuales (excel_corrections.json)
# ===========================================================================

def load_corrections(path: Path = CORRECTIONS_FILE) -> dict:
    """Carga el fichero de correcciones manuales del Excel.

    Schema: ver data/excel_corrections.json
    """
    if not path.exists():
        return {"corrections": [], "excluded_from_import": []}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] No se pudo cargar {path}: {e}", file=sys.stderr)
        return {"corrections": [], "excluded_from_import": []}


def build_correction_indexes(corrections_doc: dict) -> tuple[list, set, set]:
    """Indexa las correcciones y excluidos.

    Soporta dos schemas:
      - Schema 1.0 (legacy): por fila_excel
      - Schema 2.0 (robusto): por match={"isin": "...", "nombre_starts": "..."}

    Returns:
        (corrections_list, excluded_codes_set, excluded_rows_set)
        Cada corrección lleva sus criterios de matching y los valores a setear.
    """
    corrections_list = []
    for c in corrections_doc.get("corrections", []):
        # Schema 2.0: match + set
        match = c.get("match")
        set_values = c.get("set")
        if match and set_values:
            corrections_list.append({
                "match_isin": match.get("isin", "").strip().upper() if match.get("isin") else None,
                "match_nombre_starts": match.get("nombre_starts") or None,
                "set_isin": set_values.get("isin"),
                "set_nombre": set_values.get("nombre"),
                "razon": c.get("razon", ""),
                "_used": False,
            })
            continue
        # Schema 1.0 legacy: por fila + campo isin/nombre
        fila = c.get("fila_excel")
        if fila is not None:
            field = c.get("field")
            entry = {
                "fila_excel": int(fila),
                "match_isin": (c.get("isin") or c.get("isin_actual", "")).strip().upper() if c.get("isin") or c.get("isin_actual") else None,
                "match_nombre_starts": None,
                "set_isin": c.get("isin_correcto"),
                "set_nombre": c.get("value_correcto") if field == "nombre" else c.get("nombre_correcto"),
                "razon": c.get("razon", ""),
                "_used": False,
            }
            corrections_list.append(entry)

    excluded_codes = set()
    excluded_rows = set()
    for e in corrections_doc.get("excluded_from_import", []):
        code = e.get("code")
        fila = e.get("fila_excel")
        if code:
            excluded_codes.add(str(code).strip().upper())
        if fila is not None:
            excluded_rows.add(int(fila))

    return corrections_list, excluded_codes, excluded_rows


def find_matching_correction(corrections_list: list, isin: str, nombre: str, fila_excel: int) -> dict | None:
    """Busca corrección aplicable a una fila. Prioridad: match por ISIN+nombre > ISIN > fila."""
    isin_upper = isin.strip().upper() if isin else ""
    nombre_str = str(nombre or "").strip()

    # Pass 1: matching estricto por ISIN+nombre_starts
    for c in corrections_list:
        if c.get("_used"):
            continue
        if c.get("match_isin") and c.get("match_nombre_starts"):
            if c["match_isin"] == isin_upper and nombre_str.upper().startswith(c["match_nombre_starts"].upper()):
                return c

    # Pass 2: matching por solo ISIN (sin nombre_starts) - solo si ISIN aparece una vez en correcciones sin nombre
    isin_only_matches = [c for c in corrections_list
                         if not c.get("_used")
                         and c.get("match_isin") == isin_upper
                         and not c.get("match_nombre_starts")
                         and not c.get("fila_excel")]
    if len(isin_only_matches) == 1:
        return isin_only_matches[0]

    # Pass 3: legacy matching por fila
    for c in corrections_list:
        if c.get("_used"):
            continue
        if c.get("fila_excel") == fila_excel:
            return c

    return None


# ===========================================================================
# Parser principal — hoja "Listado fondos" del Excel v2
# ===========================================================================

def parse_listado_fondos(df_raw, corrections_doc: dict, verbose: bool = True) -> dict:
    """Procesa la hoja 'Listado fondos' del Excel v2 con header multi-nivel.

    Args:
        df_raw: pd.DataFrame leído con header=None (raw, 0-indexed)
        corrections_doc: dict cargado de excel_corrections.json

    Returns:
        dict {isin: legacy_record}  (formato intermedio para agrupar_por_fondo)
    """
    def log(msg):
        if verbose:
            print(msg)

    corrections_list, excluded_codes, excluded_rows = build_correction_indexes(corrections_doc)
    log(f"[PARSE] Correcciones: {len(corrections_list)} reglas, {len(excluded_codes)} codigos excluidos, {len(excluded_rows)} filas excluidas")

    funds: dict[str, dict] = {}

    skipped_invalid_isin = 0
    skipped_excluded = 0
    rows_corrected = 0

    for idx in range(DATA_START_ROW, df_raw.shape[0]):
        fila_excel = idx + 1  # Excel es 1-indexed

        # Skip filas marcadas como excluidas explícitamente
        if fila_excel in excluded_rows:
            skipped_excluded += 1
            continue

        # Leer cell crudas
        raw_isin = df_raw.iloc[idx, COL_ISIN] if COL_ISIN < df_raw.shape[1] else None
        raw_nombre = df_raw.iloc[idx, COL_NOMBRE] if COL_NOMBRE < df_raw.shape[1] else None

        # Aplicar corrección si hay match
        corr = find_matching_correction(corrections_list, str(raw_isin or ""), str(raw_nombre or ""), fila_excel)
        applied_correction = False
        if corr is not None:
            # Defensive: nombre solo se sobreescribe si match_nombre_starts está definido
            # o si el nombre actual está vacío (defensa para el caso v3 donde puede haber sido corregido ya)
            if corr.get("set_isin"):
                raw_isin = corr["set_isin"]
                applied_correction = True
            if corr.get("set_nombre"):
                nombre_actual = str(raw_nombre or "").strip()
                # Si nombre_starts está especificado, siempre sobreescribir (ya hizo match exacto)
                # Si NO está especificado (corrección "por defecto"), solo sobreescribir si nombre actual vacío
                if corr.get("match_nombre_starts") or not nombre_actual:
                    raw_nombre = corr["set_nombre"]
                    applied_correction = True
            corr["_used"] = True

        # Skip si código en lista de exclusión (planes pensiones, carteras modelo)
        raw_isin_str = str(raw_isin).strip().upper() if raw_isin is not None else ""
        if raw_isin_str in excluded_codes:
            skipped_excluded += 1
            continue

        # Validar ISIN
        isin = normalize_isin(raw_isin)
        if not isin:
            if normalize_str(raw_nombre):  # solo log si parecía haber data
                skipped_invalid_isin += 1
            continue

        if applied_correction:
            rows_corrected += 1

        nombre = normalize_str(raw_nombre)

        # Brokers (10 columnas Z-AI con flag X)
        brokers = []
        for offset, broker_name in enumerate(BROKER_NAMES):
            col_idx = COL_BROKERS_START + offset
            if col_idx < df_raw.shape[1] and normalize_flag(df_raw.iloc[idx, col_idx]):
                brokers.append(broker_name)

        # Horizonte temporal (3 flags Corto/Medio/Largo)
        horizonte = []
        if COL_HORIZONTE_CORTO < df_raw.shape[1] and normalize_flag(df_raw.iloc[idx, COL_HORIZONTE_CORTO]):
            horizonte.append("Corto")
        if COL_HORIZONTE_MEDIO < df_raw.shape[1] and normalize_flag(df_raw.iloc[idx, COL_HORIZONTE_MEDIO]):
            horizonte.append("Medio")
        if COL_HORIZONTE_LARGO < df_raw.shape[1] and normalize_flag(df_raw.iloc[idx, COL_HORIZONTE_LARGO]):
            horizonte.append("Largo")

        # Eval_* (6 sub-criterios → JSONB)
        evaluacion = {}
        for offset, key in enumerate(EVAL_KEYS):
            col_idx = COL_EVAL_START + offset
            if col_idx < df_raw.shape[1]:
                v = df_raw.iloc[idx, col_idx]
                s = normalize_str(v)
                if s:
                    # Para riesgo_ucits convertir a int si es numérico
                    if key == "riesgo_ucits":
                        v_int = normalize_int(v)
                        if v_int is not None:
                            evaluacion[key] = v_int
                            continue
                    evaluacion[key] = s

        # Clasificación: normalizar Top/Bueno/Clase similar/Clase sucia
        clasif_raw = normalize_str(df_raw.iloc[idx, COL_CLASIFICACION]) if COL_CLASIFICACION < df_raw.shape[1] else ""
        clasif_map = {
            "top": "Top",
            "bueno": "Bueno",
            "medio": "Medio",
            "clase similar": "Clase_similar",
            "clase sucia": "Clase_sucia",
        }
        clasificacion = clasif_map.get(clasif_raw.lower(), "")

        # TER: en Excel viene como decimal (0.0086 = 0.86%). Lo dejamos como % (0.86)
        ter_raw = normalize_float(df_raw.iloc[idx, COL_TER]) if COL_TER < df_raw.shape[1] else None
        if ter_raw is not None and ter_raw < 1.0:
            # Asumimos decimal < 1 = porcentaje (0.0086 = 0.86%)
            ter_pct = ter_raw * 100
        else:
            # Ya está en %
            ter_pct = ter_raw

        # Divisa + flag hedge
        divisa_raw = normalize_str(df_raw.iloc[idx, COL_DIVISA]) if COL_DIVISA < df_raw.shape[1] else ""
        divisa_hedge_bool = "hedge" in divisa_raw.lower()
        # Normalizar valor divisa: "EuroHedge" → "EUR", "Euro" → "EUR", "Dolares" → "USD"
        divisa_lower = divisa_raw.lower()
        if "euro" in divisa_lower:
            divisa = "EUR"
        elif "dolar" in divisa_lower or "usd" in divisa_lower:
            divisa = "USD"
        elif "gbp" in divisa_lower or "libra" in divisa_lower:
            divisa = "GBP"
        elif "chf" in divisa_lower or "franco" in divisa_lower:
            divisa = "CHF"
        else:
            divisa = divisa_raw if divisa_raw else None

        # Clase comercial: normalizar a enum del schema
        clase_raw = normalize_str(df_raw.iloc[idx, COL_CLASE_COMERCIAL]) if COL_CLASE_COMERCIAL < df_raw.shape[1] else ""
        clase_lower = clase_raw.lower()
        if "limpia" in clase_lower or "sin retroces" in clase_lower:
            clase_comercial = "Limpia"
        elif "retail" in clase_lower:
            clase_comercial = "Retail"
        elif "institu" in clase_lower or "inst" in clase_lower:
            clase_comercial = "Institucional"
        elif "seed" in clase_lower:
            clase_comercial = "Seed"
        else:
            clase_comercial = None

        # Tipo activo: normalizar a enum del schema
        tipo_activo_raw = normalize_str(df_raw.iloc[idx, COL_TIPO_ACTIVO]) if COL_TIPO_ACTIVO < df_raw.shape[1] else ""
        tipo_lower = tipo_activo_raw.lower()
        if tipo_lower.startswith("rv"):
            tipo_activo = "RV"
        elif tipo_lower.startswith("rf"):
            tipo_activo = "RF"
        elif "mixto" in tipo_lower:
            tipo_activo = "Mixtos"
        elif "alternativ" in tipo_lower:
            tipo_activo = "Alternativos"
        elif "monetar" in tipo_lower:
            tipo_activo = "Monetario"
        elif "materias" in tipo_lower or "comm" in tipo_lower:
            tipo_activo = "Materias_primas"
        else:
            tipo_activo = None

        # Categoria: normalizar a enum
        categoria_raw = normalize_str(df_raw.iloc[idx, COL_CATEGORIA]) if COL_CATEGORIA < df_raw.shape[1] else ""
        cat_lower = categoria_raw.lower()
        if "indexado" in cat_lower or "index" in cat_lower:
            categoria = "Indexado"
        elif "monetar" in cat_lower:
            categoria = "Monetario"
        elif "sectorial" in cat_lower:
            categoria = "Sectoriales"
        elif "etf" in cat_lower:
            categoria = "ETF"
        elif "ultra" in cat_lower:
            categoria = "Ultra_Short"
        elif "gestion" in cat_lower:
            categoria = "Gestionado"
        else:
            categoria = None

        # Construir registro intermedio
        record = {
            "isin": isin,
            "nombre": nombre,
            # Categorización
            "categoria": categoria,
            "tipo_activo": tipo_activo,
            "geografia": normalize_str(df_raw.iloc[idx, COL_GEOGRAFIA]) if COL_GEOGRAFIA < df_raw.shape[1] else None,
            "issuer": normalize_str(df_raw.iloc[idx, COL_ISSUER]) if COL_ISSUER < df_raw.shape[1] else None,
            "estilo": normalize_str(df_raw.iloc[idx, COL_ESTILO]) if COL_ESTILO < df_raw.shape[1] else None,
            "clase_comercial": clase_comercial,
            "divisa": divisa,
            "divisa_hedge_bool": divisa_hedge_bool,
            # Cuantitativos del fondo
            "años_antiguedad": normalize_int(df_raw.iloc[idx, COL_AÑOS_ANTIGUEDAD]) if COL_AÑOS_ANTIGUEDAD < df_raw.shape[1] else None,
            "aum_meur": normalize_float(df_raw.iloc[idx, COL_AUM_MEUR]) if COL_AUM_MEUR < df_raw.shape[1] else None,
            # KPIs de clase
            "ter_pct": ter_pct,
            "comision_exito_raw": normalize_str(df_raw.iloc[idx, COL_COMISION_EXITO]) if COL_COMISION_EXITO < df_raw.shape[1] else None,
            # Multi-broker
            "brokers": brokers,
            # Horizonte
            "horizonte_temporal": horizonte,
            # Evaluación cualitativa
            "evaluacion": evaluacion if evaluacion else None,
            # Clasificación + textos
            "clasificacion_user": clasificacion,
            "encaje": normalize_str(df_raw.iloc[idx, COL_ENCAJE]) if COL_ENCAJE < df_raw.shape[1] else None,
            "opinion": normalize_str(df_raw.iloc[idx, COL_OPINION]) if COL_OPINION < df_raw.shape[1] else None,
            # Meta
            "_source_row": fila_excel,
            "_corrected": applied_correction,
        }

        # Sanitizar None → "" para campos string
        for k in ("geografia", "issuer", "estilo", "encaje", "opinion", "comision_exito_raw"):
            if record[k] is None:
                record[k] = ""

        # Si ya existe ISIN (duplicado), conservar el primero pero marcar
        if isin in funds:
            funds[isin]["_duplicated_at_row"] = fila_excel
            continue

        funds[isin] = record

    log(f"[PARSE] OK: {len(funds)} ISINs únicos válidos")
    log(f"  Skipped (invalid ISIN o no es fondo): {skipped_invalid_isin}")
    log(f"  Skipped (excluded explícitamente): {skipped_excluded}")
    log(f"  Filas corregidas: {rows_corrected}")
    return funds


# ===========================================================================
# Agrupación: fund_groups + funds (schema Supabase v2-cowork)
# ===========================================================================

def agrupar_por_fondo(funds_dict: dict) -> tuple[list[dict], list[dict]]:
    """Convierte el dict {isin: legacy_record} en (fund_groups_data, funds_data).

    Agrupa por (normalize_nombre_base(nombre), extract_gestora(nombre)).
    Mantiene compatibilidad con schema v2-cowork + 8 campos nuevos del ALTER.
    """
    groups: dict[str, dict] = {}
    classes: dict[str, list[str]] = {}
    funds_data: list[dict] = []

    now_iso = datetime.now(timezone.utc).isoformat()

    for isin, f in funds_dict.items():
        nombre = f.get("nombre") or ""
        nombre_base = normalize_nombre_base(nombre)
        gestora = extract_gestora(nombre)
        gkey = _group_key(nombre_base, gestora)
        group_id = _deterministic_uuid(nombre_base, gestora)

        if gkey not in groups:
            groups[gkey] = {
                "fund_group_id": group_id,
                "nombre_base": nombre_base,
                "gestora": gestora,
                # Categorización (heredada por clases)
                "tipo_activo": f.get("tipo_activo") or None,
                "categoria": f.get("categoria") or None,
                "geografia": f.get("geografia") or None,
                "estilo": f.get("estilo") or None,
                "tema_sector": None,
                "issuer": f.get("issuer") or None,
                "categoria_morningstar": None,  # se rellenará desde análisis
                # KPIs del FONDO
                "aum_meur": f.get("aum_meur"),
                "num_participes": None,
                "años_antiguedad": f.get("años_antiguedad"),
                "fecha_creacion_fondo": None,
                # Cualitativo (se rellena tras análisis bat)
                "gestores_nombres": [],
                "gestores_perfiles_json": None,
                "top_holdings_json": None,
                "filosofia": None,
                "estrategia": None,
                "historia": None,
                # Default user (heredado por clases)
                "clasificacion_user_default": f.get("clasificacion_user") or None,
                "opinion_user_default": f.get("opinion") or None,
                # Híbrido C: ISINs conocidos del fondo (se rellena al final)
                "class_isins_known": [],
                # JSONB rellenados por el bat (rendimiento de fund-dashboard, etc.)
                "rendimiento_jsonb": None,
                "portfolio_metrics_jsonb": None,
                "equipo_metrics_jsonb": None,
                # Audit
                "revisado_cuantitativo_bool": False,
                "revisado_cuantitativo_at": None,
                "fecha_alta": now_iso,
                "fecha_ultimo_analisis": None,
                "cost_run_eur": None,
                "_verified_fields": None,
            }
            classes[gkey] = []
        else:
            # Rellenar huecos del grupo con datos de esta clase
            g = groups[gkey]
            for src, dst in [
                ("tipo_activo", "tipo_activo"),
                ("categoria", "categoria"),
                ("geografia", "geografia"),
                ("estilo", "estilo"),
                ("issuer", "issuer"),
                ("opinion", "opinion_user_default"),
                ("clasificacion_user", "clasificacion_user_default"),
            ]:
                if not g.get(dst) and f.get(src):
                    g[dst] = f.get(src)
            if g.get("aum_meur") is None and f.get("aum_meur") is not None:
                g["aum_meur"] = f.get("aum_meur")
            if g.get("años_antiguedad") is None and f.get("años_antiguedad") is not None:
                g["años_antiguedad"] = f.get("años_antiguedad")

        classes[gkey].append(isin)

        # Construir registro de la CLASE
        funds_data.append({
            "isin": isin,
            "fund_group_id": groups[gkey]["fund_group_id"],
            "nombre_clase": nombre,
            "clase_comercial": f.get("clase_comercial") or None,
            "divisa": f.get("divisa") or None,
            "divisa_hedge_bool": f.get("divisa_hedge_bool", False),
            # KPIs específicos de clase
            "ter_pct": f.get("ter_pct"),
            "comision_gestion_pct": None,  # se rellena con análisis CNMV
            "comision_exito_pct": None,    # comision_exito_raw es string
            "importe_minimo_eur": None,    # Cantidad min era string sucio (descartado)
            "fecha_creacion_clase": None,
            # Clasificación user (null = hereda del grupo)
            "clasificacion_user": f.get("clasificacion_user") or None,
            "opinion_user": f.get("opinion") or None,
            "notas_internas": None,
            "broker_disponible": f.get("brokers") or [],
            # Nuevos campos (ALTER TABLE)
            "encaje_texto": f.get("encaje") or None,
            "evaluacion_jsonb": f.get("evaluacion"),
            "horizonte_temporal": f.get("horizonte_temporal") or [],
            # Storage paths (se rellenan tras bat)
            "has_qualitative_analysis": False,
            "dashboard_storage_path": None,
            "output_json_storage_path": None,
            "cnmv_data_storage_path": None,
            "letters_data_storage_path": None,
            "manager_profile_storage_path": None,
            "horfin_id": None,
            "fecha_alta": now_iso,
        })

    # Rellenar class_isins_known en cada grupo (híbrido C)
    for gkey, isins in classes.items():
        groups[gkey]["class_isins_known"] = sorted(isins)

    return list(groups.values()), funds_data


# ===========================================================================
# Build pipeline + Upload Supabase
# ===========================================================================

def build_taxonomy(source_path: Path, verbose: bool = True) -> dict:
    """Construye la taxonomía completa desde el Excel v2."""

    def log(msg):
        if verbose:
            print(msg)

    try:
        import pandas as pd
    except ImportError:
        log("[ERROR] pandas no instalado. pip install pandas openpyxl")
        sys.exit(1)

    log(f"[IMPORT_TAXONOMY] Leyendo {source_path}")
    if not source_path.exists():
        log(f"[ERROR] No existe: {source_path}")
        sys.exit(1)

    # Cargar correcciones manuales
    corrections_doc = load_corrections()

    # Leer hoja "Listado fondos" en raw (header=None) — header es multi-nivel
    xl = pd.ExcelFile(source_path)
    if SHEET_NAME not in xl.sheet_names:
        log(f"[ERROR] Hoja '{SHEET_NAME}' no encontrada. Hojas disponibles: {xl.sheet_names}")
        sys.exit(1)

    df_raw = pd.read_excel(xl, sheet_name=SHEET_NAME, header=None)
    log(f"[IMPORT_TAXONOMY] Hoja '{SHEET_NAME}': shape {df_raw.shape}")

    funds = parse_listado_fondos(df_raw, corrections_doc, verbose=verbose)

    fund_groups_data, funds_data = agrupar_por_fondo(funds)

    # Stats agregados
    n_clasif = {}
    for f in funds.values():
        c = f.get("clasificacion_user", "") or "Sin clasificar"
        n_clasif[c] = n_clasif.get(c, 0) + 1

    n_with_opinion = sum(1 for f in funds.values() if f.get("opinion"))
    n_with_encaje = sum(1 for f in funds.values() if f.get("encaje"))
    n_with_evaluacion = sum(1 for f in funds.values() if f.get("evaluacion"))
    n_with_brokers = sum(1 for f in funds.values() if f.get("brokers"))

    log(f"[IMPORT_TAXONOMY] [OK] Total ISINs unicos: {len(funds)}")
    log(f"[IMPORT_TAXONOMY] [OK] Total fund_groups: {len(fund_groups_data)}")
    log("[IMPORT_TAXONOMY] Distribución clasificación:")
    for c, n in sorted(n_clasif.items(), key=lambda x: -x[1]):
        log(f"    {c:25s} {n}")
    log(f"  Con opinión escrita:    {n_with_opinion}")
    log(f"  Con encaje (texto):     {n_with_encaje}")
    log(f"  Con evaluación (Eval_*):{n_with_evaluacion}")
    log(f"  Con brokers asignados:  {n_with_brokers}")

    taxonomy = {
        "version": "2.0",
        "source_file": str(source_path.name),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "n_funds": len(funds),
        "n_fund_groups": len(fund_groups_data),
        "stats": {
            "clasificacion": n_clasif,
            "n_with_opinion": n_with_opinion,
            "n_with_encaje": n_with_encaje,
            "n_with_evaluacion": n_with_evaluacion,
            "n_with_brokers": n_with_brokers,
        },
        "funds": funds,
        "fund_groups_data": fund_groups_data,
        "funds_data": funds_data,
    }
    return taxonomy


def save_taxonomy(taxonomy: dict, pretty: bool = False, verbose: bool = True) -> Path:
    """Guarda data/fund_taxonomy.json (legacy compat)."""
    DATA_DIR.mkdir(exist_ok=True)
    legacy = {k: v for k, v in taxonomy.items()
              if k not in ("fund_groups_data", "funds_data", "n_fund_groups")}
    legacy["version"] = "1.0"
    indent = 2 if pretty else None
    separators = None if pretty else (",", ":")
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(legacy, f, ensure_ascii=False, indent=indent, separators=separators)
    if verbose:
        size_kb = OUTPUT_FILE.stat().st_size / 1024
        print(f"[IMPORT_TAXONOMY] Guardado legacy: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    return OUTPUT_FILE


def upload_to_supabase(fund_groups_data: list[dict], funds_data: list[dict], verbose: bool = True) -> dict:
    """Upsert real contra Supabase. PRESERVA campos del análisis cualitativo.

    Los campos del backfill (has_qualitative_analysis, *_storage_path, gestores_perfiles_json,
    top_holdings_json, filosofia, estrategia, historia, fecha_ultimo_analisis, etc.) NO se
    sobreescriben porque solo vienen del análisis del bat, no del Excel.

    Estrategia: en lugar de upsert simple, hacemos:
    1. SELECT de los ISINs/fund_group_ids existentes con campos del backfill
    2. Para cada record nuevo, EXCLUIR los campos del backfill antes del upsert
    3. Esto deja los campos del backfill intactos en filas que ya los tenían

    Requiere supabase_client configurado.
    """
    from tools.supabase_client import get_client

    def log(msg):
        if verbose:
            print(msg)

    client = get_client()
    if client is None:
        raise RuntimeError("Supabase client no disponible. Verifica .env (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY)")

    # ──────────────────────────────────────────────────────────────────
    # Campos que NO deben sobreescribirse en el upsert (vienen del bat)
    # ──────────────────────────────────────────────────────────────────
    FUND_GROUPS_BACKFILL_FIELDS = {
        "gestores_nombres", "gestores_perfiles_json", "top_holdings_json",
        "filosofia", "estrategia", "historia",
        "fecha_ultimo_analisis", "cost_run_eur",
        "rendimiento_jsonb", "portfolio_metrics_jsonb", "equipo_metrics_jsonb",
        "revisado_cuantitativo_bool", "revisado_cuantitativo_at",
        "_verified_fields",
    }
    FUNDS_BACKFILL_FIELDS = {
        "has_qualitative_analysis",
        "dashboard_storage_path", "output_json_storage_path",
        "cnmv_data_storage_path", "letters_data_storage_path", "manager_profile_storage_path",
        "horfin_id",
    }

    def clean_record(r: dict, exclude: set) -> dict:
        """Limpia record + excluye campos del backfill."""
        cleaned = {}
        for k, v in r.items():
            if k in exclude:
                continue  # NO sobreescribir campos del backfill
            if isinstance(v, list) and not v:
                cleaned[k] = []
            elif v is None:
                cleaned[k] = None
            else:
                cleaned[k] = v
        return cleaned

    fg_clean = [clean_record(r, FUND_GROUPS_BACKFILL_FIELDS) for r in fund_groups_data]
    f_clean = [clean_record(r, FUNDS_BACKFILL_FIELDS) for r in funds_data]

    log(f"[IMPORT_TAXONOMY] Upserting {len(fg_clean)} fund_groups (preservando campos del backfill)...")
    r1 = client.table("fund_groups").upsert(
        fg_clean, on_conflict="fund_group_id"
    ).execute()

    log(f"[IMPORT_TAXONOMY] Upserting {len(f_clean)} funds (preservando campos del backfill)...")
    r2 = client.table("funds").upsert(
        f_clean, on_conflict="isin"
    ).execute()

    return {
        "fund_groups_upserted": len(getattr(r1, "data", []) or []),
        "funds_upserted": len(getattr(r2, "data", []) or []),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Importa taxonomía Excel v2 → Supabase schema v2-cowork"
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help=f"Excel a procesar (default: {DEFAULT_SOURCE.name})")
    parser.add_argument("--pretty", action="store_true",
                        help="JSON con indentación (más grande pero legible)")
    parser.add_argument("--quiet", action="store_true",
                        help="Silenciar logs")
    parser.add_argument("--dry-run", action="store_true",
                        help="Imprime resumen sin escribir json ni subir a Supabase")
    parser.add_argument("--upload-supabase", action="store_true",
                        help="Tras el build, hace upsert real a Supabase")
    args = parser.parse_args()

    verbose = not args.quiet
    taxonomy = build_taxonomy(args.source, verbose=verbose)

    if args.dry_run:
        if verbose:
            print()
            print("[DRY-RUN] Top 5 fund_groups (con más clases):")
            sorted_groups = sorted(
                taxonomy["fund_groups_data"],
                key=lambda g: -len(g.get("class_isins_known", []))
            )
            for fg in sorted_groups[:5]:
                n_classes = len(fg.get("class_isins_known", []))
                print(
                    f"  {n_classes}x  {fg['nombre_base'][:40]:40s} | "
                    f"gestora={fg['gestora']:18s} | clasif={fg.get('clasificacion_user_default') or '-'}"
                )
            print()
            print("[DRY-RUN] Sample 3 funds:")
            for fd in taxonomy["funds_data"][:3]:
                nombre = (fd.get("nombre_clase") or "")[:50]
                print(f"  {fd.get('isin'):12s} | {nombre:50s} | TER={fd.get('ter_pct')} | clasif={fd.get('clasificacion_user') or '-'}")
            print()
            print(f"[DRY-RUN] [OK] {len(taxonomy['fund_groups_data'])} fund_groups + "
                  f"{len(taxonomy['funds_data'])} funds - sin escribir nada.")
        return 0

    save_taxonomy(taxonomy, pretty=args.pretty, verbose=verbose)

    if args.upload_supabase:
        try:
            stats = upload_to_supabase(
                taxonomy["fund_groups_data"], taxonomy["funds_data"], verbose=verbose
            )
            if verbose:
                print(f"[IMPORT_TAXONOMY] [OK] Supabase upsert OK: {stats}")
        except Exception as e:
            print(f"[IMPORT_TAXONOMY] [ERROR] Supabase: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
