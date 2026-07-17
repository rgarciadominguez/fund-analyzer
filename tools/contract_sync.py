"""
contract_sync.py — Valida y conforma el export al contrato `contrato_sync.json`.

Regla de oro (del contrato): un valor que no está en la lista de su campo NO se emite:
se pone `null` y se REPORTA para acordarlo. Nunca se inventa. Los dos lados validan
contra el mismo fichero → las dos BDD no pueden divergir.

Este módulo:
  1. Carga contrato_sync.json.
  2. Transforma (mapea el vocabulario grueso mío al del contrato):
     - region/geografia: Global, World → ACWI
     - distribucion: Reparto → Distribución
     - estilo: 'Divisa cubierta' → 'Cubre divisa'; 'Indexado' → fuera (es categoria_activo)
     - tipo_activo: compone el granular (Fondo RV / Fondo RF <plazo> / High Yield /
       Floating / Fondo Mixto / Fondo Monetario / ETF*/REITs/Materias primas) desde
       tipo_activo grueso + plazo + benchmark + caracteristicas_especiales + nombre.
     - plazo: rellena para RF desde benchmark / categoria Morningstar.
  3. Valida cada campo `enum` contra su lista. Fuera de lista → null + report.
  4. Devuelve (activos_conformes, report).

El export queda como VISTA conforme al contrato; Supabase sigue con su vocabulario grueso.

CLI:
    python -m tools.contract_sync --dump   # imprime el report contra el export actual
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTRACT_EXT = Path(r"C:\Users\RafaelGarcía\horizonte-datos\contrato_sync.json")
CONTRACT_REPO = ROOT / "data" / "contrato_sync.json"
EXPORT = Path(r"C:\Users\RafaelGarcía\horizonte-datos\catalogo_supabase.json")

# benchmark: valores que SE NULIFICAN por redundantes con tipo_activo/region (contrato v2).
# Se CONSERVAN los índices reales y la asignación de mixtos (Cartera Permanente, 40/60...).
_BENCH_NULIFICAR = {
    "Renta Fija Corto Plazo", "Renta Fija Medio Plazo", "Renta Fija Largo Plazo",
    "Renta Fija High Yield", "REITs", "RV UK",
}

# campo del export -> campo del contrato (los enum se validan por este mapeo)
FIELD_MAP = {
    "geografia": "region",
    "divisa": "moneda",
    "srri": "riesgo_ucits",
    "ter_pct": "ter",
    "comision_gestion_pct": "comision_gestion_pct",
    "distribucion": "distribucion",
    "tipo_activo": "tipo_activo",
    "estilo": "estilo",
    "plazo": "plazo",
    "categoria_rf": "categoria_rf",
    "categoria_activo": "categoria_activo",
    "estrellas": "estrellas",
    "comision_suscripcion": "comision_suscripcion",
}

def load_contract() -> dict:
    """Lee el contrato de la carpeta de Horizonte; cae al copia del repo si no hay acceso."""
    for p in (CONTRACT_EXT, CONTRACT_REPO):
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError("contrato_sync.json no encontrado (ni externo ni en repo)")


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("["):
            try:
                return json.loads(s.replace("'", '"'))
            except Exception:
                return [s]
        return [s]
    return [v]


def _has(chars, *needles):
    joined = " ".join(str(x) for x in chars).lower()
    return any(n.lower() in joined for n in needles)


def derive_plazo(row) -> str | None:
    """Plazo para RF: existente → benchmark → categoría Morningstar. null si no se puede."""
    if row.get("plazo") in ("Corto", "Medio", "Largo"):
        return row["plazo"]
    b = row.get("benchmark") or ""
    if b in ("Renta Fija Corto Plazo", "Euribor"):
        return "Corto"
    if b == "Renta Fija Medio Plazo":
        return "Medio"
    if b == "Renta Fija Largo Plazo":
        return "Largo"
    ms = (row.get("categoria_morningstar") or "").lower()
    if "ultra short" in ms or "short-term" in ms or "short term" in ms or "money market" in ms:
        return "Corto"
    if "long" in ms and "short" not in ms:   # evita "Long/Short"
        return "Largo"
    return None


def compose_tipo_activo(row, report_new):
    """Compone el tipo_activo granular del contrato. null + propuesta si nada encaja."""
    coarse = row.get("tipo_activo")
    chars = _as_list(row.get("caracteristicas_especiales"))
    nombre = (row.get("nombre") or "").upper()
    estilo = row.get("estilo") or ""
    bench = row.get("benchmark") or ""
    is_etf = "ETF" in nombre or _has(chars, "Indexado/ETF") and "ETF" in nombre
    is_reit = _has(chars, "REIT") or bench == "REITs"
    is_oro = any(x in nombre for x in ("ORO", "GOLD"))

    if coarse == "RV":
        if is_reit:
            return ("ETF REITs" if is_etf else "REITs"), None
        return ("ETF RV" if is_etf else "Fondo RV"), None

    if coarse == "RF":
        if _has(chars, "ILS", "Catástrofe", "Catastrofe"):
            # decisión Rafa (cierre v2): ILS/catástrofe → Alternativos (no valor nuevo).
            # El detalle queda en caracteristicas + benchmark 'Bonos Catástrofe'.
            return "Alternativos", None
        if _has(chars, "Floating") or estilo == "Floating rate":
            return "Fondo RF Floating Rate", None
        if _has(chars, "High Yield") or bench == "Renta Fija High Yield":
            return "Fondo RF High Yield", None
        plazo = derive_plazo(row)
        if plazo == "Corto":
            return "Fondo RF Corto Plazo", None
        if plazo == "Medio":
            return "Fondo RF Medio Plazo", None
        if plazo == "Largo":
            return "Fondo RF Largo Plazo", None
        report_new.append((row["isin"], "tipo_activo", "RF sin plazo derivable"))
        return None, "RF sin plazo"

    if coarse == "Mixtos":
        return "Fondo Mixto", None
    if coarse == "Monetario":
        return "Fondo Monetario", None
    if coarse in ("Materias_primas", "Materias primas"):
        return ("ETF Oro" if is_oro else "Materias primas"), None
    if coarse == "Alternativos":
        return "Alternativos", None
    if coarse is None:
        return None, None
    # valor grueso desconocido
    report_new.append((row["isin"], "tipo_activo", f"tipo grueso no mapeado: {coarse!r}"))
    return None, coarse


def apply_contract(activos: list) -> tuple[list, dict]:
    C = load_contract()["campos"]
    enums = {f: set(spec["valores"]) for f, spec in C.items()
             if spec.get("tipo") == "enum"}

    out = []
    fuera = Counter()            # valores fuera de contrato puestos a null
    fuera_ej = {}
    propuestas = []              # valores nuevos a acordar
    bench_categoria = Counter()

    for a in activos:
        r = dict(a)

        # --- transformaciones de vocabulario ---
        if r.get("geografia") in ("Global", "World"):
            r["geografia"] = "ACWI"
        if r.get("distribucion") == "Reparto":
            r["distribucion"] = "Distribución"
        if r.get("estilo") == "Divisa cubierta":
            r["estilo"] = "Cubre divisa"
        if r.get("estilo") == "Indexado":
            r["estilo"] = None          # Indexado es categoria_activo, no estilo

        # plazo derivado (para RF), útil por sí mismo
        if r.get("tipo_activo") == "RF" and not r.get("plazo"):
            p = derive_plazo(r)
            if p:
                r["plazo"] = p

        # tipo_activo granular (compone). Idempotente: si ya es un valor granular válido
        # (p.ej. re-exportando sin grupo del que re-sourcear), se deja tal cual.
        if r.get("tipo_activo") not in enums["tipo_activo"]:
            granular, prop = compose_tipo_activo(r, propuestas)
            r["tipo_activo"] = granular

        # benchmark: nulifica lo redundante con tipo_activo/region (contrato v2).
        # Conserva índices reales + asignación de mixtos (Cartera Permanente, 40/60...).
        if r.get("benchmark") in _BENCH_NULIFICAR:
            bench_categoria[r["benchmark"]] += 1
            r["benchmark"] = None

        # --- validación enum: fuera de lista → null + report ---
        for ex_field, co_field in FIELD_MAP.items():
            if co_field not in enums:
                continue
            v = r.get(ex_field)
            if v is None:
                continue
            if v not in enums[co_field]:
                fuera[f"{ex_field}={v!r}"] += 1
                fuera_ej.setdefault(f"{ex_field}={v!r}", r["isin"])
                r[ex_field] = None

        out.append(r)

    report = {
        "n_filas": len(out),
        "valores_puestos_a_null_por_fuera_de_contrato": dict(fuera),
        "ejemplo_isin": fuera_ej,
        "propuestas_valor_nuevo": [
            {"isin": i, "campo": c, "motivo": m} for i, c, m in propuestas
        ],
        "benchmark_nulificado_redundante": dict(bench_categoria),
        "tipo_activo_resultante": dict(Counter(x.get("tipo_activo") for x in out)),
        "plazo_relleno": sum(1 for x in out if x.get("plazo")),
    }
    return out, report


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    doc = json.loads(EXPORT.read_text(encoding="utf-8"))
    out, rep = apply_contract(doc["activos"])
    print(json.dumps(rep, ensure_ascii=False, indent=1))
