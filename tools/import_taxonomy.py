"""
import_taxonomy.py — Importa el universo de fondos del usuario desde Excel
y genera data/fund_taxonomy.json para enriquecer el catálogo.

Lee `data/fund_taxonomy_source.xlsx` (listado del usuario con scoring Top/Bueno).
Joinea las 4 hojas relevantes:
  - Datapack_dashboard (master con Clasificación)
  - Listado fondos (detalle cualitativo: Filosofía, Análisis, Equipo, Brokers)
  - Activos y Bancos (TER, AUM, brokers)
  - Fondos CJRS (Categoría Morningstar)

Genera `data/fund_taxonomy.json` con schema unificado por ISIN.

Uso:
    python -m tools.import_taxonomy
    python -m tools.import_taxonomy --pretty
    python -m tools.import_taxonomy --source path/to/other.xlsx
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DEFAULT_SOURCE = DATA_DIR / "fund_taxonomy_source.xlsx"
OUTPUT_FILE = DATA_DIR / "fund_taxonomy.json"

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


def is_valid_isin(s: str) -> bool:
    """ISIN format check (no checksum validation, just shape)."""
    if not isinstance(s, str):
        return False
    s = s.strip().upper()
    return bool(ISIN_REGEX.match(s))


def normalize_isin(v) -> str | None:
    """Normaliza ISIN a uppercase. Devuelve None si invalido o vacio."""
    if v is None:
        return None
    try:
        s = str(v).strip().upper()
    except Exception:
        return None
    return s if ISIN_REGEX.match(s) else None


def normalize_str(v) -> str:
    """Normaliza a string limpio. Devuelve '' para NaN/None/0/'0'."""
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
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


def parse_datapack(df) -> dict:
    """Parsea Datapack_dashboard como master. Devuelve dict por ISIN."""
    funds = {}
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        clasificacion = normalize_str(row.get("Clasificación"))
        funds[isin] = {
            "isin": isin,
            "nombre": normalize_str(row.get("Fondo")),
            "clase_comercial": normalize_str(row.get("Clase")),  # Limpia / Retail
            "categoria": normalize_str(row.get("Categoría")),  # Gestionado / Indexado / etc
            "tipo_activo": normalize_str(row.get("Tipo activo")),  # RV / RF MP / etc
            "geografia": normalize_str(row.get("Geografía")),
            "divisa": normalize_str(row.get("Divisa")),
            "issuer": normalize_str(row.get("Issuer")),
            "clasificacion_user": clasificacion,  # Top / Bueno / Medio / etc
            "es_top": clasificacion.lower() == "top",
            "es_bueno": clasificacion.lower() == "bueno",
            "opinion": normalize_str(row.get("Opinión")),
            "_source_sheets": ["Datapack_dashboard"],
        }
    return funds


def enrich_listado_fondos(funds: dict, df) -> int:
    """Enriquece con detalle cualitativo de Listado fondos. Devuelve nº enriquecidos."""
    n = 0
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        if isin not in funds:
            # Fondos en Listado pero no en Datapack — añadir con info parcial
            funds[isin] = {
                "isin": isin,
                "nombre": normalize_str(row.get("Fondo")),
                "_source_sheets": [],
            }
        f = funds[isin]
        # Campos exclusivos de Listado fondos
        for src_col, dst_key in [
            ("Filosofía", "filosofia"),
            ("Objetivo", "objetivo"),
            ("Horizonte Temporal", "horizonte_temporal"),
            ("Historia y Activos", "historia_y_activos"),
            ("Tipo", "estilo"),  # Value/Growth/SmallCaps/etc
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
        # Flag MAPFRE TOP
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
    """Enriquece con TER/AUM/brokers de Activos y Bancos."""
    n = 0
    for _, row in df.iterrows():
        isin = normalize_isin(row.get("ISIN"))
        if not isin:
            continue
        if isin not in funds:
            funds[isin] = {
                "isin": isin,
                "nombre": normalize_str(row.get("Activo")),
                "_source_sheets": [],
            }
        f = funds[isin]
        # TER y AUM
        ter = normalize_float(row.get("TER"))
        if ter is not None and "ter" not in f:
            f["ter"] = ter
        aum = normalize_float(row.get("AUM"))
        if aum is not None and "aum_meur" not in f:
            f["aum_meur"] = aum
        # Brokers (puede haber 1-3 cols)
        brokers = []
        for col in ["Broker 1", "Broker 2", "Broker 3", "Brokers"]:
            v = normalize_str(row.get(col))
            if v and v not in brokers:
                brokers.extend([b.strip() for b in v.split(",") if b.strip()])
        if brokers and "brokers" not in f:
            f["brokers"] = brokers
        # Estrategia (si no está ya como estilo)
        estrategia = normalize_str(row.get("Estrategia"))
        if estrategia and not f.get("estilo"):
            f["estilo"] = estrategia
        # Activo type (Fondo Monetario, RV, etc)
        activo = normalize_str(row.get("Activo"))
        if activo and not f.get("activo_tipo"):
            f["activo_tipo"] = activo
        # Riesgo UCITS
        riesgo = normalize_float(row.get("Riesgo UCITS"))
        if riesgo is not None and "riesgo_ucits" not in f:
            f["riesgo_ucits"] = int(riesgo)
        # Importe mínimo
        imin = normalize_float(row.get("Importe mínimo"))
        if imin is not None and "importe_minimo" not in f:
            f["importe_minimo"] = imin
        if "Activos y Bancos" not in f.get("_source_sheets", []):
            f.setdefault("_source_sheets", []).append("Activos y Bancos")
        n += 1
    return n


def enrich_morningstar(funds: dict, df) -> int:
    """Enriquece con Categoría Morningstar de Fondos CJRS."""
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
    """Genera tags útiles para filtrado."""
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


def build_taxonomy(source_path: Path, verbose: bool = True) -> dict:
    """Construye la taxonomía completa desde el Excel."""

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

    # 1. Master: Datapack_dashboard (header en fila 1, no fila 0)
    if "Datapack_dashboard" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Datapack_dashboard", header=1)
        funds = parse_datapack(df)
        log(f"[IMPORT_TAXONOMY] Datapack_dashboard: {len(funds)} fondos master")
    else:
        log("[WARN] Datapack_dashboard no encontrada — sin scoring Top/Bueno")

    # 2. Listado fondos (detalle cualitativo, header en fila 1)
    # NOTA: la hoja tiene 86 cols con doble header. La columna 'ISIN' real
    # está en col idx 16. Filas 2-3 pueden ser duplicado de header o separadores
    # — el filtro is_valid_isin descarta automáticamente esas filas inválidas.
    if "Listado fondos" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Listado fondos", header=1)
        n_pre = len(funds)
        n_enriched = enrich_listado_fondos(funds, df)
        log(
            f"[IMPORT_TAXONOMY] Listado fondos: {n_enriched} filas procesadas "
            f"(+{len(funds) - n_pre} fondos nuevos)"
        )

    # 3. Activos y Bancos (TER, AUM, brokers)
    if "Activos y Bancos" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Activos y Bancos")
        n_pre = len(funds)
        n_enriched = enrich_activos_bancos(funds, df)
        log(
            f"[IMPORT_TAXONOMY] Activos y Bancos: {n_enriched} filas "
            f"(+{len(funds) - n_pre} fondos nuevos)"
        )

    # 4. Fondos CJRS (Categoría Morningstar)
    if "Fondos CJRS" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Fondos CJRS")
        n_enriched = enrich_morningstar(funds, df)
        log(f"[IMPORT_TAXONOMY] Fondos CJRS: {n_enriched} fondos con cat. Morningstar")

    # 5. Generar tags y normalizar
    for f in funds.values():
        f["tags"] = compute_tags(f)
        # Defaults para campos siempre presentes
        f.setdefault("clasificacion_user", "")
        f.setdefault("es_top", False)
        f.setdefault("es_bueno", False)
        f.setdefault("is_mapfre_top", False)

    # Stats
    n_top = sum(1 for f in funds.values() if f.get("es_top"))
    n_bueno = sum(1 for f in funds.values() if f.get("es_bueno"))
    n_mapfre = sum(1 for f in funds.values() if f.get("is_mapfre_top"))
    n_morningstar = sum(1 for f in funds.values() if f.get("categoria_morningstar"))
    n_opinion = sum(1 for f in funds.values() if f.get("opinion"))

    # Distribución
    tipos_activo: dict[str, int] = {}
    geografias: dict[str, int] = {}
    for f in funds.values():
        ta = f.get("tipo_activo") or "(sin tipo)"
        tipos_activo[ta] = tipos_activo.get(ta, 0) + 1
        g = f.get("geografia") or "(sin geo)"
        geografias[g] = geografias.get(g, 0) + 1

    log(f"[IMPORT_TAXONOMY] Total fondos en taxonomía: {len(funds)}")
    log(f"  Clasificados Top: {n_top} | Bueno: {n_bueno} | Mapfre TOP: {n_mapfre}")
    log(f"  Con categoría Morningstar: {n_morningstar}")
    log(f"  Con opinión: {n_opinion}")
    log("  Distribución por tipo_activo:")
    for k, v in sorted(tipos_activo.items(), key=lambda x: -x[1]):
        log(f"    {k:25s} {v}")

    taxonomy = {
        "version": "1.0",
        "source_file": str(source_path.name),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "n_funds": len(funds),
        "stats": {
            "n_top": n_top,
            "n_bueno": n_bueno,
            "n_mapfre_top": n_mapfre,
            "n_morningstar": n_morningstar,
            "n_with_opinion": n_opinion,
            "tipos_activo": tipos_activo,
            "geografias": geografias,
        },
        "funds": funds,
    }
    return taxonomy


def save_taxonomy(taxonomy: dict, pretty: bool = False, verbose: bool = True) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    indent = 2 if pretty else None
    separators = None if pretty else (",", ":")
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(taxonomy, f, ensure_ascii=False, indent=indent, separators=separators)
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    if verbose:
        print(f"[IMPORT_TAXONOMY] Guardado: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    return OUTPUT_FILE


def main():
    parser = argparse.ArgumentParser(
        description="Importa taxonomía de fondos desde Excel del usuario"
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    verbose = not args.quiet
    taxonomy = build_taxonomy(args.source, verbose=verbose)
    save_taxonomy(taxonomy, pretty=args.pretty, verbose=verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
