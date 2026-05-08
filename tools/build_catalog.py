"""
build_catalog.py — Generador de funds_catalog_analyzer.json

Construye el catálogo unificado del visor combinando dos fuentes:

1. **Taxonomía del usuario** (data/fund_taxonomy.json): universo de fondos
   con clasificación Top/Bueno/Medio + opinión + tipo_activo + geografía + etc.
   (Se genera con `python -m tools.import_taxonomy` desde el Excel del usuario.)

2. **Análisis cualitativos** (data/funds/{ISIN}/output.json): los fondos que
   ya pasaron por el pipeline `analizar_fondo.bat` y tienen dashboard HTML.

El catálogo final lista TODOS los ISINs de la taxonomía + cualquier análisis
huérfano. Cada entrada lleva flag `has_qualitative_analysis` y enlaces al
dashboard cuando exista.

Salida: dashboard/funds_catalog_analyzer.json

Uso:
    python -m tools.build_catalog
    python -m tools.build_catalog --pretty
    python -m tools.build_catalog --analyses-only   # solo data/funds/, sin taxonomy
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
FUNDS_DIR = DATA_DIR / "funds"
DASHBOARD_DIR = ROOT / "dashboard"
TAXONOMY_FILE = DATA_DIR / "fund_taxonomy.json"
OUTPUT_FILE = DASHBOARD_DIR / "funds_catalog_analyzer.json"

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


def _safe_read_json(path: Path) -> dict | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _normalize_fecha(raw) -> str:
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw[:10] if len(raw) >= 10 else raw
    return str(raw)


def _compute_completitud(data: dict) -> float:
    """Completitud heurística sobre 8 secciones críticas (0-100)."""
    if not data:
        return 0.0
    score = 0.0
    weight = 100.0 / 8
    aum = data.get("kpis", {}).get("aum_actual_meur")
    if aum and isinstance(aum, (int, float)) and aum > 0:
        score += weight
    pos = data.get("posiciones", {}).get("actuales", [])
    if isinstance(pos, list) and len(pos) > 0:
        score += weight
    perfiles = (
        data.get("analyst_synthesis", {})
        .get("gestores", {})
        .get("perfiles", [])
    )
    if isinstance(perfiles, list) and len(perfiles) > 0:
        score += weight
    resumen = data.get("analyst_synthesis", {}).get("resumen", {}).get("texto", "")
    if isinstance(resumen, str) and len(resumen) > 200:
        score += weight
    estrategia = (
        data.get("analyst_synthesis", {}).get("estrategia", {}).get("texto", "")
    )
    if isinstance(estrategia, str) and len(estrategia) > 200:
        score += weight
    fuentes = data.get("analyst_synthesis", {}).get("fuentes_externas", {})
    if isinstance(fuentes, dict):
        opiniones = fuentes.get("opiniones_clave", [])
        if isinstance(opiniones, list) and len(opiniones) > 0:
            score += weight
    cartera = data.get("analyst_synthesis", {}).get("cartera", {}).get("texto", "")
    if isinstance(cartera, str) and len(cartera) > 200:
        score += weight
    hechos = data.get("hechos_relevantes", [])
    if isinstance(hechos, list) and len(hechos) > 0:
        score += weight
    return round(score, 1)


def load_taxonomy(log) -> dict:
    """Carga data/fund_taxonomy.json. Devuelve {isin: tax_entry} o {}."""
    if not TAXONOMY_FILE.exists():
        log(
            "[BUILD_CATALOG] WARN: No existe fund_taxonomy.json. "
            "Genera con: python -m tools.import_taxonomy"
        )
        return {}
    data = _safe_read_json(TAXONOMY_FILE)
    if not data:
        log("[BUILD_CATALOG] WARN: fund_taxonomy.json no parseable")
        return {}
    funds = data.get("funds", {})
    log(f"[BUILD_CATALOG] Taxonomía cargada: {len(funds)} fondos")
    return funds


def extract_analysis(isin: str, fund_dir: Path) -> dict:
    """Extrae datos del output.json + companions. Devuelve dict (vacío si no existe)."""
    out_path = fund_dir / "output.json"
    if not out_path.exists():
        return {}
    data = _safe_read_json(out_path)
    if not data:
        return {}

    result = {
        "has_qualitative_analysis": True,
        "analysis_nombre": (data.get("nombre") or "").strip(),
        "analysis_gestora": (data.get("gestora") or "").strip(),
        "analysis_tipo": data.get("tipo", "ES"),
        "ultima_actualizacion": _normalize_fecha(data.get("ultima_actualizacion")),
        "completitud_pct": _compute_completitud(data),
    }

    kpis = data.get("kpis", {}) or {}
    result["aum_meur"] = kpis.get("aum_actual_meur")
    result["ter_pct"] = kpis.get("ter_pct")
    result["num_participes"] = kpis.get("num_participes")
    result["divisa_kpi"] = kpis.get("divisa")
    result["clasificacion_cnmv"] = kpis.get("clasificacion") or ""
    result["anio_creacion"] = kpis.get("anio_creacion")

    pos = data.get("posiciones", {}).get("actuales", []) or []
    result["num_posiciones"] = len(pos) if isinstance(pos, list) else 0

    perfiles = (
        data.get("analyst_synthesis", {})
        .get("gestores", {})
        .get("perfiles", [])
        or []
    )
    result["num_perfiles_gestores"] = (
        len(perfiles) if isinstance(perfiles, list) else 0
    )

    # Cartas K15
    letters_path = fund_dir / "letters_data.json"
    if letters_path.exists():
        ldata = _safe_read_json(letters_path) or {}
        cartas = ldata.get("cartas", []) or []
        result["num_cartas_k15"] = sum(
            1
            for c in cartas
            if isinstance(c, dict)
            and (c.get("tesis_gestora") or c.get("decisiones_tomadas"))
        )
    else:
        result["num_cartas_k15"] = 0

    # Readings
    readings_path = fund_dir / "readings_data.json"
    if readings_path.exists():
        rdata = _safe_read_json(readings_path) or {}
        analisis = rdata.get("analisis_completos", []) or []
        result["num_readings"] = len(analisis) if isinstance(analisis, list) else 0
        result["has_readings"] = result["num_readings"] > 0
    else:
        result["num_readings"] = 0
        result["has_readings"] = False

    # Dashboard HTML
    dashboard_path = DASHBOARD_DIR / f"fund-{isin}.html"
    result["has_dashboard"] = dashboard_path.exists()
    if result["has_dashboard"]:
        st = dashboard_path.stat()
        result["dashboard_mtime"] = datetime.fromtimestamp(
            st.st_mtime, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M")
        result["dashboard_size_kb"] = round(st.st_size / 1024, 1)

    return result


def build_combined_entry(isin: str, tax: dict, analysis: dict) -> dict:
    """Combina datos de taxonomía + análisis en una única entry para el catálogo."""
    has_analysis = bool(analysis.get("has_qualitative_analysis"))

    # Identidad: prioridad analysis (más actualizado) sobre taxonomy
    nombre = (
        analysis.get("analysis_nombre")
        or tax.get("nombre")
        or ""
    )
    gestora = (
        analysis.get("analysis_gestora")
        or _infer_gestora(nombre)
        or ""
    )
    # Tipo: ES/INT (de análisis) o derivar del prefijo del ISIN
    tipo = analysis.get("analysis_tipo") or ("ES" if isin.startswith("ES") else "INT")

    entry = {
        "isin": isin,
        "nombre": nombre,
        "gestora": gestora,
        "tipo": tipo,
        # Taxonomía del usuario
        "clasificacion_user": tax.get("clasificacion_user", ""),
        "es_top": bool(tax.get("es_top")),
        "es_bueno": bool(tax.get("es_bueno")),
        "is_mapfre_top": bool(tax.get("is_mapfre_top")),
        "tipo_activo": tax.get("tipo_activo", ""),
        "categoria": tax.get("categoria", ""),
        "categoria_morningstar": tax.get("categoria_morningstar", ""),
        "geografia": tax.get("geografia", ""),
        "divisa": tax.get("divisa") or analysis.get("divisa_kpi") or "",
        "issuer": tax.get("issuer", ""),
        "estilo": tax.get("estilo", ""),
        "clase_comercial": tax.get("clase_comercial", ""),
        "opinion": tax.get("opinion", ""),
        "filosofia": tax.get("filosofia", ""),
        "objetivo": tax.get("objetivo", ""),
        "horizonte_temporal": tax.get("horizonte_temporal", ""),
        "brokers": tax.get("brokers", []),
        "tags": tax.get("tags", []),
        # Análisis cualitativo (si existe)
        "has_qualitative_analysis": has_analysis,
        "completitud_pct": analysis.get("completitud_pct", 0) if has_analysis else 0,
        "aum_meur": analysis.get("aum_meur") if has_analysis else tax.get("aum_meur"),
        "ter_pct": analysis.get("ter_pct") if has_analysis else tax.get("ter"),
        "num_posiciones": analysis.get("num_posiciones", 0),
        "num_perfiles_gestores": analysis.get("num_perfiles_gestores", 0),
        "num_cartas_k15": analysis.get("num_cartas_k15", 0),
        "num_readings": analysis.get("num_readings", 0),
        "has_readings": analysis.get("has_readings", False),
        "has_dashboard": analysis.get("has_dashboard", False),
        "dashboard_mtime": analysis.get("dashboard_mtime", ""),
        "ultima_actualizacion": analysis.get("ultima_actualizacion", ""),
        "anio_creacion": analysis.get("anio_creacion") or tax.get("anio_creacion"),
        "_in_taxonomy": bool(tax),
    }
    return entry


def _infer_gestora(nombre: str) -> str:
    """Heurística rápida para inferir gestora del nombre. Vacío si no se puede."""
    if not nombre:
        return ""
    # Patrones comunes en nombres de fondos ES
    n = nombre.upper()
    candidates = [
        ("MAGALLANES", "Magallanes Value Investors"),
        ("AZ VALOR", "AzValor Asset Management"),
        ("AZVALOR", "AzValor Asset Management"),
        ("COBAS", "Cobas Asset Management"),
        ("MYINVESTOR", "MyInvestor"),
        ("CARTESIO", "Cartesio Inversiones"),
        ("DUNAS", "Dunas Capital"),
        ("BESTINVER", "Bestinver"),
        ("GAMMA", "Gamma Capital Markets"),
        ("ROBECO", "Robeco"),
        ("DNCA", "DNCA Investments"),
        ("AMUNDI", "Amundi"),
        ("VANGUARD", "Vanguard"),
        ("ISHARES", "iShares (BlackRock)"),
        ("BLACKROCK", "BlackRock"),
        ("FIDELITY", "Fidelity"),
        ("PIMCO", "PIMCO"),
        ("DWS", "DWS"),
        ("UBAM", "UBP"),
        ("GROUPAMA", "Groupama"),
    ]
    for needle, name in candidates:
        if needle in n:
            return name
    return ""


def build_catalog(
    verbose: bool = True, analyses_only: bool = False
) -> dict:
    """Construye el catálogo completo combinando taxonomy + analyses."""

    def log(msg):
        if verbose:
            print(msg)

    # 1. Cargar taxonomía (universo del usuario)
    taxonomy = {} if analyses_only else load_taxonomy(log)

    # 2. Cargar análisis cualitativos (data/funds/{ISIN}/output.json)
    analyses: dict[str, dict] = {}
    skipped_pseudo = 0
    if FUNDS_DIR.exists():
        log(f"[BUILD_CATALOG] Recorriendo {FUNDS_DIR}")
        for fund_dir in sorted(FUNDS_DIR.iterdir()):
            if not fund_dir.is_dir():
                continue
            isin = fund_dir.name
            # Skip backups/test
            if (
                isin.startswith("_")
                or "TEST" in isin.upper()
                or ".bak" in isin
                or ".backup" in isin
            ):
                skipped_pseudo += 1
                continue
            if not ISIN_REGEX.match(isin.upper()):
                skipped_pseudo += 1
                continue
            isin_upper = isin.upper()
            entry = extract_analysis(isin_upper, fund_dir)
            if entry:
                analyses[isin_upper] = entry
        log(
            f"[BUILD_CATALOG] Análisis cualitativos cargados: {len(analyses)} "
            f"(skipped pseudos: {skipped_pseudo})"
        )

    # 3. Universo: union de taxonomy + analyses
    all_isins = set(taxonomy.keys()) | set(analyses.keys())
    log(f"[BUILD_CATALOG] Universo total: {len(all_isins)} ISINs únicos")

    # 4. Construir entries combinadas
    entries = []
    for isin in sorted(all_isins):
        tax = taxonomy.get(isin, {})
        analysis = analyses.get(isin, {})
        entry = build_combined_entry(isin, tax, analysis)
        entries.append(entry)

    # 5. Stats agregados
    n_total = len(entries)
    n_es = sum(1 for e in entries if e["tipo"] == "ES")
    n_int = sum(1 for e in entries if e["tipo"] != "ES")
    n_top = sum(1 for e in entries if e["es_top"])
    n_bueno = sum(1 for e in entries if e["es_bueno"])
    n_mapfre = sum(1 for e in entries if e["is_mapfre_top"])
    n_analyzed = sum(1 for e in entries if e["has_qualitative_analysis"])
    n_dashboard = sum(1 for e in entries if e["has_dashboard"])
    n_top_analyzed = sum(
        1 for e in entries if e["es_top"] and e["has_qualitative_analysis"]
    )
    n_bueno_analyzed = sum(
        1 for e in entries if e["es_bueno"] and e["has_qualitative_analysis"]
    )
    gestoras_unicas = len({e["gestora"] for e in entries if e["gestora"]})
    aum_total = sum(
        e.get("aum_meur") or 0
        for e in entries
        if isinstance(e.get("aum_meur"), (int, float))
    )

    catalog = {
        "version": "2.0",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "n_funds": n_total,
        "stats": {
            "n_es": n_es,
            "n_int": n_int,
            "n_top": n_top,
            "n_bueno": n_bueno,
            "n_mapfre_top": n_mapfre,
            "n_analyzed": n_analyzed,
            "n_dashboard": n_dashboard,
            "n_top_analyzed": n_top_analyzed,
            "n_top_pending": n_top - n_top_analyzed,
            "n_bueno_analyzed": n_bueno_analyzed,
            "n_bueno_pending": n_bueno - n_bueno_analyzed,
            "gestoras_unicas": gestoras_unicas,
            "aum_total_meur": round(aum_total, 1),
            "skipped_pseudo": skipped_pseudo,
        },
        "funds": entries,
    }

    log(
        f"[BUILD_CATALOG] OK: {n_total} fondos | "
        f"Top={n_top} (analyzed {n_top_analyzed}) | "
        f"Bueno={n_bueno} (analyzed {n_bueno_analyzed}) | "
        f"Total analyzed={n_analyzed}"
    )
    return catalog


def save_catalog(catalog: dict, pretty: bool = False, verbose: bool = True) -> Path:
    DASHBOARD_DIR.mkdir(exist_ok=True)
    indent = 2 if pretty else None
    separators = None if pretty else (",", ":")
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=indent, separators=separators)
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    if verbose:
        print(f"[BUILD_CATALOG] Guardado: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    return OUTPUT_FILE


def main():
    parser = argparse.ArgumentParser(
        description="Genera funds_catalog_analyzer.json para el visor"
    )
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--analyses-only",
        action="store_true",
        help="Solo data/funds/, sin taxonomía del usuario",
    )
    args = parser.parse_args()

    verbose = not args.quiet
    catalog = build_catalog(verbose=verbose, analyses_only=args.analyses_only)
    save_catalog(catalog, pretty=args.pretty, verbose=verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
