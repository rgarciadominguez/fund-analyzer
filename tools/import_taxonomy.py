"""
import_taxonomy.py — Importa el universo de fondos del usuario desde Excel
y genera DOS estructuras para el schema Supabase v2-cowork:

  - fund_groups_data: list[dict] (1 por nombre_base + gestora)
  - funds_data:       list[dict] (1 por ISIN, con fund_group_id apuntando)

Sigue leyendo las mismas 4 hojas del Excel multi-hoja:
  - Datapack_dashboard (master con Clasificación)
  - Listado fondos (detalle cualitativo: Filosofía, Análisis, Equipo, Brokers)
  - Activos y Bancos (TER, AUM, brokers)
  - Fondos CJRS (Categoría Morningstar)

Genera además `data/fund_taxonomy.json` (formato legacy) por compatibilidad.

Uso:
    python -m tools.import_taxonomy                       # full build, sin subir
    python -m tools.import_taxonomy --dry-run             # resumen sin escribir nada
    python -m tools.import_taxonomy --upload-supabase     # upsert real a Supabase
    python -m tools.import_taxonomy --pretty
    python -m tools.import_taxonomy --source path/to/other.xlsx

Heurísticas:
  - normalize_nombre_base(): quita sufijos Clase/Class A-Z, divisa, hedged, dist/acc, FI/SICAV...
  - extract_gestora(): primera palabra del nombre (best-effort; downstream agents la sustituyen).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DEFAULT_SOURCE = DATA_DIR / "fund_taxonomy_source.xlsx"
OUTPUT_FILE = DATA_DIR / "fund_taxonomy.json"

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")

FUND_GROUPS_NAMESPACE = uuid.UUID("e4f29c6e-1f3a-4d8e-9ab2-7c0d2b5a9e10")


def is_valid_isin(s: str) -> bool:
    if not isinstance(s, str):
        return False
    return bool(ISIN_REGEX.match(s.strip().upper()))


def normalize_isin(v) -> str | None:
    if v is None:
        return None
    try:
        s = str(v).strip().upper()
    except Exception:
        return None
    return s if ISIN_REGEX.match(s) else None


def normalize_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "", "0", "0.0"):
        return ""
    return s


def normalize_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        if f != f:
            return None
        return f
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Normalización de nombre_base (agrupación de clases) y extracción de gestora
# ---------------------------------------------------------------------------

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
    """Quita sufijos de clase/divisa/hedge/dist del nombre para agrupar clases.

    Ejemplos:
        "Cobas Internacional Clase A"          -> "Cobas Internacional"
        "DNCA Alpha Bonds I EUR Hedged"        -> "DNCA Alpha Bonds"
        "Magallanes European Equity Class I EUR" -> "Magallanes European Equity"
        "Templeton Latin America Fund A(acc)USD" -> "Templeton Latin America Fund"
        "Renta 4 Bolsa FI"                     -> "Renta 4 Bolsa"
    """
    if not nombre:
        return ""

    s = str(nombre).strip()
    s = _PAREN_NOISE_RE.sub(" ", s)
    s = _HEDGE_INLINE_RE.sub(" ", s)
    s = _HEDGE_PREFIX_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = s.split(" ")

    # Strip "Clase X" / "Class X" suffix (consumes 1-2 trailing tokens)
    if len(tokens) >= 2 and _CLASS_PREFIX_RE.match(tokens[-2]):
        tokens = tokens[:-2]
    elif len(tokens) >= 1 and _CLASS_PREFIX_RE.match(tokens[-1]):
        tokens = tokens[:-1]

    # Iteratively strip trailing noise: currencies, dist tokens, single-letter class tokens, wrappers
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
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def extract_gestora(nombre: str) -> str:
    """Heurística best-effort: primera palabra del nombre.

    No es perfecta (multi-palabra como "Renta 4" caería como "Renta") pero
    sirve como placeholder hasta que CNMV/CSSF agents pueblen el dato real.
    Para nombres como "Renta 4 ..." devuelve "Renta 4" (caso especial).
    """
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


# ---------------------------------------------------------------------------
# Lectura del Excel (4 hojas) — schema legacy intermedio: dict por ISIN
# ---------------------------------------------------------------------------

def parse_datapack(df) -> dict:
    funds = {}
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        clasificacion = normalize_str(row.get("Clasificación"))
        funds[isin] = {
            "isin": isin,
            "nombre": normalize_str(row.get("Fondo")),
            "clase_comercial": normalize_str(row.get("Clase")),
            "categoria": normalize_str(row.get("Categoría")),
            "tipo_activo": normalize_str(row.get("Tipo activo")),
            "geografia": normalize_str(row.get("Geografía")),
            "divisa": normalize_str(row.get("Divisa")),
            "issuer": normalize_str(row.get("Issuer")),
            "clasificacion_user": clasificacion,
            "es_top": clasificacion.lower() == "top",
            "es_bueno": clasificacion.lower() == "bueno",
            "opinion": normalize_str(row.get("Opinión")),
            "_source_sheets": ["Datapack_dashboard"],
        }
    return funds


def enrich_listado_fondos(funds: dict, df) -> int:
    n = 0
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        if isin not in funds:
            funds[isin] = {"isin": isin, "nombre": normalize_str(row.get("Fondo")), "_source_sheets": []}
        f = funds[isin]
        for src_col, dst_key in [
            ("Filosofía", "filosofia"),
            ("Objetivo", "objetivo"),
            ("Horizonte Temporal", "horizonte_temporal"),
            ("Historia y Activos", "historia_y_activos"),
            ("Tipo", "estilo"),
            ("Comisiones", "comisiones_resumen"),
            ("Rentabilidad-Riesgo", "rentabilidad_riesgo"),
            ("Portfolio", "portfolio_resumen"),
            ("Equipo Gestor", "equipo_gestor_resumen"),
            ("Análisis", "analisis_resumen"),
            ("Cantidad min", "cantidad_minima"),
            ("Brokers", "brokers_resumen"),
        ]:
            v = normalize_str(row.get(src_col))
            if v and not f.get(dst_key):
                f[dst_key] = v
        mapfre_top = row.get("MAPFRE TOP")
        if mapfre_top is not None:
            try:
                f["is_mapfre_top"] = bool(float(mapfre_top))
            except (ValueError, TypeError):
                pass
        if "Listado fondos" not in f.get("_source_sheets", []):
            f.setdefault("_source_sheets", []).append("Listado fondos")
        n += 1
    return n


def enrich_activos_bancos(funds: dict, df) -> int:
    n = 0
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        if isin not in funds:
            funds[isin] = {"isin": isin, "nombre": normalize_str(row.get("Activo")), "_source_sheets": []}
        f = funds[isin]
        ter = normalize_float(row.get("TER"))
        if ter is not None and "ter" not in f:
            f["ter"] = ter
        aum = normalize_float(row.get("AUM"))
        if aum is not None and "aum_meur" not in f:
            f["aum_meur"] = aum
        brokers = []
        for col in ["Broker 1", "Broker 2", "Broker 3", "Brokers"]:
            v = normalize_str(row.get(col))
            if v and v not in brokers:
                brokers.extend([b.strip() for b in v.split(",") if b.strip()])
        if brokers and "brokers" not in f:
            f["brokers"] = brokers
        estrategia = normalize_str(row.get("Estrategia"))
        if estrategia and not f.get("estilo"):
            f["estilo"] = estrategia
        activo = normalize_str(row.get("Activo"))
        if activo and not f.get("activo_tipo"):
            f["activo_tipo"] = activo
        riesgo = normalize_float(row.get("Riesgo UCITS"))
        if riesgo is not None and "riesgo_ucits" not in f:
            f["riesgo_ucits"] = int(riesgo)
        imin = normalize_float(row.get("Importe mínimo"))
        if imin is not None and "importe_minimo" not in f:
            f["importe_minimo"] = imin
        if "Activos y Bancos" not in f.get("_source_sheets", []):
            f.setdefault("_source_sheets", []).append("Activos y Bancos")
        n += 1
    return n


def enrich_morningstar(funds: dict, df) -> int:
    n = 0
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        cat_ms = normalize_str(row.get("CATEGORIA MORNINGSTAR"))
        if not cat_ms:
            continue
        if isin not in funds:
            funds[isin] = {
                "isin": isin,
                "nombre": normalize_str(row.get("Fondo")) or normalize_str(row.get("Nombre")),
                "_source_sheets": [],
            }
        f = funds[isin]
        if not f.get("categoria_morningstar"):
            f["categoria_morningstar"] = cat_ms
        if "Fondos CJRS" not in f.get("_source_sheets", []):
            f.setdefault("_source_sheets", []).append("Fondos CJRS")
        n += 1
    return n


def compute_tags(f: dict) -> list[str]:
    tags = []
    if f.get("es_top"):
        tags.append("top")
    if f.get("es_bueno"):
        tags.append("bueno")
    if f.get("is_mapfre_top"):
        tags.append("mapfre-top")
    tipo_activo = f.get("tipo_activo", "").lower()
    if tipo_activo:
        if "rv" in tipo_activo:
            tags.append("rv")
        if "rf" in tipo_activo:
            tags.append("rf")
        if "mixto" in tipo_activo:
            tags.append("mixto")
        if "alternativ" in tipo_activo:
            tags.append("alternativos")
        if "monetar" in tipo_activo:
            tags.append("monetario")
    geo = f.get("geografia", "").lower()
    geo_map = {
        "global": "global", "europa": "europa", "usa": "usa",
        "developed": "developed", "emerging": "emerging",
        "españa": "espana", "espana": "espana", "pacifico": "pacifico",
        "nordics": "nordics",
    }
    for k, v in geo_map.items():
        if k in geo:
            tags.append(v)
    divisa = f.get("divisa", "").lower()
    if "eurohedge" in divisa:
        tags.append("eurohedged")
    elif "euro" in divisa:
        tags.append("eur")
    elif "dolar" in divisa or "usd" in divisa:
        tags.append("usd")
    estilo = f.get("estilo", "").lower()
    for k in ("value", "growth", "blend", "smallcaps", "esg", "indexado", "passive"):
        if k in estilo:
            tags.append(k.replace("smallcaps", "small-caps"))
    if f.get("clase_comercial", "").lower() == "limpia":
        tags.append("clase-limpia")
    return sorted(set(tags))


# ---------------------------------------------------------------------------
# Agrupación nueva (schema v2-cowork): fund_groups + funds
# ---------------------------------------------------------------------------

def agrupar_por_fondo(funds_dict: dict) -> tuple[list[dict], list[dict]]:
    """Convierte el dict {isin: legacy_record} en (fund_groups_data, funds_data).

    Agrupa por (normalize_nombre_base(nombre), extract_gestora(nombre)).
    """
    groups: dict[str, dict] = {}
    classes: dict[str, list[str]] = {}  # group_key -> [isins]
    funds_data: list[dict] = []

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
                "tipo_activo": f.get("tipo_activo") or None,
                "categoria": f.get("categoria") or None,
                "geografia": f.get("geografia") or None,
                "estilo": f.get("estilo") or None,
                "tema_sector": None,
                "issuer": f.get("issuer") or None,
                "categoria_morningstar": f.get("categoria_morningstar") or None,
                "aum_meur": f.get("aum_meur"),
                "num_participes": None,
                "fecha_creacion_fondo": None,
                "gestores_nombres": [],
                "gestores_perfiles_json": None,
                "top_holdings_json": None,
                "filosofia": f.get("filosofia") or None,
                "estrategia": f.get("analisis_resumen") or None,
                "historia": f.get("historia_y_activos") or None,
                "clasificacion_user_default": f.get("clasificacion_user") or None,
                "opinion_user_default": f.get("opinion") or None,
                "class_isins_known": [],
                "revisado_cuantitativo_bool": False,
                "revisado_cuantitativo_at": None,
                "fecha_alta": datetime.now(timezone.utc).isoformat(),
                "fecha_ultimo_analisis": None,
                "cost_run_eur": None,
                "_verified_fields": None,
            }
            classes[gkey] = []
        else:
            # Si el grupo ya existe, rellena huecos con datos de esta clase
            g = groups[gkey]
            for src, dst in [
                ("tipo_activo", "tipo_activo"),
                ("categoria", "categoria"),
                ("geografia", "geografia"),
                ("estilo", "estilo"),
                ("issuer", "issuer"),
                ("categoria_morningstar", "categoria_morningstar"),
                ("filosofia", "filosofia"),
                ("analisis_resumen", "estrategia"),
                ("historia_y_activos", "historia"),
                ("clasificacion_user", "clasificacion_user_default"),
                ("opinion", "opinion_user_default"),
            ]:
                if not g.get(dst) and f.get(src):
                    g[dst] = f.get(src)
            if g.get("aum_meur") is None and f.get("aum_meur") is not None:
                g["aum_meur"] = f.get("aum_meur")

        classes[gkey].append(isin)

        divisa = f.get("divisa") or ""
        divisa_hedge = "hedge" in divisa.lower()
        funds_data.append({
            "isin": isin,
            "fund_group_id": groups[gkey]["fund_group_id"],
            "nombre_clase": nombre,
            "clase_comercial": f.get("clase_comercial") or None,
            "divisa": divisa or None,
            "divisa_hedge_bool": divisa_hedge,
            "ter_pct": f.get("ter"),
            "comision_gestion_pct": None,
            "comision_exito_pct": None,
            "importe_minimo_eur": f.get("importe_minimo"),
            "fecha_creacion_clase": None,
            "clasificacion_user": f.get("clasificacion_user") or None,
            "opinion_user": f.get("opinion") or None,
            "notas_internas": None,
            "broker_disponible": f.get("brokers") or [],
            "has_qualitative_analysis": False,
            "dashboard_storage_path": None,
            "output_json_storage_path": None,
            "cnmv_data_storage_path": None,
            "letters_data_storage_path": None,
            "manager_profile_storage_path": None,
            "horfin_id": None,
            "fecha_alta": datetime.now(timezone.utc).isoformat(),
        })

    for gkey, isins in classes.items():
        groups[gkey]["class_isins_known"] = sorted(isins)

    return list(groups.values()), funds_data


# ---------------------------------------------------------------------------
# Build pipeline + (opcional) upload
# ---------------------------------------------------------------------------

def build_taxonomy(source_path: Path, verbose: bool = True) -> dict:
    """Construye la taxonomía legacy + las dos estructuras del schema nuevo."""

    def log(msg):
        if verbose:
            print(msg)

    try:
        import pandas as pd
    except ImportError:
        log("[ERROR] pandas no instalado. Instala con: pip install pandas openpyxl")
        sys.exit(1)

    log(f"[IMPORT_TAXONOMY] Leyendo {source_path}")
    if not source_path.exists():
        log(f"[ERROR] No existe el archivo: {source_path}")
        sys.exit(1)

    xl = pd.ExcelFile(source_path)
    log(f"[IMPORT_TAXONOMY] Hojas detectadas: {len(xl.sheet_names)}")

    funds: dict[str, dict] = {}

    if "Datapack_dashboard" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Datapack_dashboard", header=1)
        funds = parse_datapack(df)
        log(f"[IMPORT_TAXONOMY] Datapack_dashboard: {len(funds)} fondos master")
    else:
        log("[WARN] Datapack_dashboard no encontrada — sin scoring Top/Bueno")

    if "Listado fondos" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Listado fondos", header=1)
        n_pre = len(funds)
        n_enriched = enrich_listado_fondos(funds, df)
        log(
            f"[IMPORT_TAXONOMY] Listado fondos: {n_enriched} filas procesadas "
            f"(+{len(funds) - n_pre} fondos nuevos)"
        )

    if "Activos y Bancos" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Activos y Bancos")
        n_pre = len(funds)
        n_enriched = enrich_activos_bancos(funds, df)
        log(
            f"[IMPORT_TAXONOMY] Activos y Bancos: {n_enriched} filas "
            f"(+{len(funds) - n_pre} fondos nuevos)"
        )

    if "Fondos CJRS" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Fondos CJRS")
        n_enriched = enrich_morningstar(funds, df)
        log(f"[IMPORT_TAXONOMY] Fondos CJRS: {n_enriched} fondos con cat. Morningstar")

    for f in funds.values():
        f["tags"] = compute_tags(f)
        f.setdefault("clasificacion_user", "")
        f.setdefault("es_top", False)
        f.setdefault("es_bueno", False)
        f.setdefault("is_mapfre_top", False)

    fund_groups_data, funds_data = agrupar_por_fondo(funds)

    n_top = sum(1 for f in funds.values() if f.get("es_top"))
    n_bueno = sum(1 for f in funds.values() if f.get("es_bueno"))
    n_mapfre = sum(1 for f in funds.values() if f.get("is_mapfre_top"))
    n_morningstar = sum(1 for f in funds.values() if f.get("categoria_morningstar"))
    n_opinion = sum(1 for f in funds.values() if f.get("opinion"))

    log(f"[IMPORT_TAXONOMY] Total ISINs (funds): {len(funds_data)}")
    log(f"[IMPORT_TAXONOMY] Total fund_groups (agrupados): {len(fund_groups_data)}")
    log(f"  Clasificados Top: {n_top} | Bueno: {n_bueno} | Mapfre TOP: {n_mapfre}")
    log(f"  Con categoría Morningstar: {n_morningstar} | con opinión: {n_opinion}")

    taxonomy = {
        "version": "2.0",
        "source_file": str(source_path.name),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "n_funds": len(funds),
        "n_fund_groups": len(fund_groups_data),
        "stats": {
            "n_top": n_top,
            "n_bueno": n_bueno,
            "n_mapfre_top": n_mapfre,
            "n_morningstar": n_morningstar,
            "n_with_opinion": n_opinion,
        },
        "funds": funds,
        "fund_groups_data": fund_groups_data,
        "funds_data": funds_data,
    }
    return taxonomy


def save_taxonomy(taxonomy: dict, pretty: bool = False, verbose: bool = True) -> Path:
    """Guarda data/fund_taxonomy.json en el formato LEGACY (sin fund_groups_data/funds_data)
    por compatibilidad con consumidores existentes (build_catalog, web_server, etc.).
    """
    DATA_DIR.mkdir(exist_ok=True)
    legacy = {k: v for k, v in taxonomy.items() if k not in ("fund_groups_data", "funds_data", "n_fund_groups")}
    legacy["version"] = "1.0"  # los consumidores legacy esperan 1.0
    indent = 2 if pretty else None
    separators = None if pretty else (",", ":")
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(legacy, f, ensure_ascii=False, indent=indent, separators=separators)
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    if verbose:
        print(f"[IMPORT_TAXONOMY] Guardado legacy: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    return OUTPUT_FILE


def upload_to_supabase(fund_groups_data: list[dict], funds_data: list[dict], verbose: bool = True) -> dict:
    """Upsert real contra Supabase. Lanza si supabase_client no está configurado."""
    from tools.supabase_client import get_client

    client = get_client()

    def log(msg):
        if verbose:
            print(msg)

    log(f"[IMPORT_TAXONOMY] Upserting {len(fund_groups_data)} fund_groups...")
    r1 = client.table("fund_groups").upsert(
        fund_groups_data, on_conflict="fund_group_id"
    ).execute()

    log(f"[IMPORT_TAXONOMY] Upserting {len(funds_data)} funds...")
    r2 = client.table("funds").upsert(
        funds_data, on_conflict="isin"
    ).execute()

    return {
        "fund_groups_upserted": len(getattr(r1, "data", []) or []),
        "funds_upserted": len(getattr(r2, "data", []) or []),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Importa taxonomía de fondos desde Excel del usuario (schema v2-cowork)"
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Imprime resumen sin escribir fund_taxonomy.json ni subir a Supabase",
    )
    parser.add_argument(
        "--upload-supabase",
        action="store_true",
        help="Tras el build, hace upsert real a Supabase",
    )
    args = parser.parse_args()

    verbose = not args.quiet
    taxonomy = build_taxonomy(args.source, verbose=verbose)

    if args.dry_run:
        if verbose:
            print("\n[DRY-RUN] Sample fund_group_data (primeros 3):")
            for fg in taxonomy["fund_groups_data"][:3]:
                print(
                    f"  - {fg['nombre_base'][:40]:40s} | gestora={fg['gestora']:15s} "
                    f"| classes={len(fg['class_isins_known'])}"
                )
            print(f"\n[DRY-RUN] Sample funds_data (primeros 3):")
            for fd in taxonomy["funds_data"][:3]:
                nombre = (fd.get("nombre_clase") or "")[:50]
                divisa = fd.get("divisa") or ""
                print(f"  - {fd.get('isin','???')} | {nombre:50s} | divisa={divisa}")
            print(
                f"\n[DRY-RUN] {len(taxonomy['fund_groups_data'])} fund_groups, "
                f"{len(taxonomy['funds_data'])} funds — sin escribir nada."
            )
        return 0

    save_taxonomy(taxonomy, pretty=args.pretty, verbose=verbose)

    if args.upload_supabase:
        try:
            stats = upload_to_supabase(
                taxonomy["fund_groups_data"], taxonomy["funds_data"], verbose=verbose
            )
            if verbose:
                print(f"[IMPORT_TAXONOMY] Supabase upsert: {stats}")
        except Exception as e:
            print(f"[IMPORT_TAXONOMY] ERROR Supabase: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
