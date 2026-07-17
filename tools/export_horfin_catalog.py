"""
export_horfin_catalog.py — Regenera `catalogo_supabase.json` para Horizonte.

Contrato (respuesta_a_fund_analyzer.md, punto A):
  - MISMA estructura: metadatos arriba + lista en la clave `activos`. No renombrar.
  - Cada fila mantiene sus 34 campos actuales, MISMOS NOMBRES.
  - Añade 6 columnas: benchmark, estrellas, comision_suscripcion, descripcion,
    categoria_activo, kid.
  - Actualiza 3 valores: comision_gestion_pct, ter_pct (2 decimales), encaje_texto.
  - Cruce por ISIN. HUECO = null, NUNCA "" ni "n.a." (su sync: vacío-no-pisa;
    un "" les pisaría un dato bueno).

Método (para no adivinar la lógica del join que produjo el export del 16/07):
  - Los 34 campos se PRESERVAN del export viejo (misma forma, derivados intactos:
    es_primario_del_grupo, clasificacion_origen, opinion_origen, grupo_analizado...).
  - Se REFRESCAN desde Supabase SOLO los campos con dato propietario que cambió:
    opinion_user (11 adoptadas + generadas), encaje_texto, ter_pct, comision_gestion_pct,
    benchmark, estrellas, comision_suscripcion, descripcion, categoria_activo, kid.
  - Filas nuevas en Supabase que no estaban en el export viejo se añaden con los 34
    campos que se puedan (el resto null).

CLI:
    python -m tools.export_horfin_catalog            # escribe el fichero
    python -m tools.export_horfin_catalog --check-isin IE0007987708
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

OLD = Path(r"C:\Users\RafaelGarcía\horizonte-datos\catalogo_supabase.json")
OUT = OLD  # sobrescribe el viejo, como piden

# Los 34 campos, en orden, tal como salen en el export viejo.
FIELDS_34 = [
    "isin", "horfin_id", "nombre", "gestora", "fund_group_id", "es_primario_del_grupo",
    "tipo_activo", "geografia", "categoria_rf", "plazo", "estilo", "tema_sector",
    "caracteristicas_especiales", "srri", "categoria_morningstar", "clasificacion_user",
    "clasificacion_origen", "opinion_user", "opinion_origen", "encaje_texto", "filosofia",
    "estrategia", "ter_pct", "comision_gestion_pct", "importe_minimo_eur", "divisa",
    "distribucion", "broker_disponible", "aum_meur", "anios_antiguedad",
    "has_qualitative_analysis", "grupo_analizado", "fecha_ultimo_analisis", "class_isins_known",
]
NEW_6 = ["benchmark", "estrellas", "comision_suscripcion", "descripcion", "categoria_activo", "kid"]

# Campos que se refrescan desde funds (dato propietario que pudo cambiar).
REFRESH_FROM_FUNDS = [
    "opinion_user", "encaje_texto", "ter_pct", "comision_gestion_pct",
    "benchmark", "estrellas", "comision_suscripcion", "descripcion", "categoria_activo", "kid",
]

# Semántica de vacío: null. NUNCA "" ni "n.a." (rompería el sync de Horizonte).
_EMPTY = {"", "n.a.", "N.A.", "na", "NA", "null", "None", "-"}


def clean(v):
    """Normaliza a null los vacíos. Deja 0 y false intactos (son datos, no huecos)."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s in _EMPTY else s
    return v


def r2(v):
    """2 decimales. null si no es número (nunca 0 para decir 'no sé')."""
    try:
        if v is None or v == "":
            return None
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _retry(fn, n=4, wait=3):
    for a in range(n):
        try:
            return fn()
        except Exception:
            if a == n - 1:
                raise
            time.sleep(wait)


def _fetch_all(client, table, cols="*", page=500):
    out, off = [], 0
    while True:
        chunk = _retry(lambda: client.table(table).select(cols)
                       .range(off, off + page - 1).execute()).data
        out += chunk
        if len(chunk) < page:
            break
        off += page
    return out


def build() -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()

    old = json.loads(OLD.read_text(encoding="utf-8"))
    old_rows = {r["isin"]: r for r in old.get("activos", [])}

    funds = {r["isin"]: r for r in _fetch_all(c, "funds")}
    print(f"funds en Supabase: {len(funds)} | filas en export viejo: {len(old_rows)}")

    activos = []
    # 1) todas las filas del export viejo, refrescadas
    for isin, base in old_rows.items():
        row = {k: base.get(k) for k in FIELDS_34}
        f = funds.get(isin, {})
        # nombre: refresco desde funds (corrige cruces ISIN<->nombre erróneos como IE0007987708)
        if f.get("nombre_clase"):
            row["nombre"] = f["nombre_clase"]
        # refresco dato propietario
        for k in REFRESH_FROM_FUNDS:
            if k in ("ter_pct", "comision_gestion_pct"):
                continue
            if k in f:
                row[k] = f.get(k)
        row["ter_pct"] = r2(f.get("ter_pct", base.get("ter_pct")))
        row["comision_gestion_pct"] = r2(f.get("comision_gestion_pct", base.get("comision_gestion_pct")))
        # 6 nuevas
        for k in NEW_6:
            row[k] = f.get(k)
        activos.append({k: clean(v) for k, v in row.items()})

    # 2) filas nuevas en Supabase que no estaban en el viejo
    nuevas = 0
    for isin, f in funds.items():
        if isin in old_rows:
            continue
        row = {k: None for k in FIELDS_34}
        row["isin"] = isin
        row["horfin_id"] = f.get("horfin_id")
        row["nombre"] = f.get("nombre_clase")
        row["fund_group_id"] = f.get("fund_group_id")
        for k in ("opinion_user", "encaje_texto", "divisa", "distribucion",
                  "broker_disponible", "importe_minimo_eur", "has_qualitative_analysis"):
            row[k] = f.get(k)
        row["ter_pct"] = r2(f.get("ter_pct"))
        row["comision_gestion_pct"] = r2(f.get("comision_gestion_pct"))
        for k in NEW_6:
            row[k] = f.get(k)
        activos.append({k: clean(v) for k, v in row.items()})
        nuevas += 1

    print(f"filas: {len(old_rows)} refrescadas + {nuevas} nuevas = {len(activos)}")

    # métricas honestas
    def filled(k):
        return sum(1 for a in activos if a.get(k) not in (None, "", [], {}))
    meta = {
        "generado": datetime.now(timezone.utc).isoformat(),
        "fuente": "supabase catalogo (funds x fund_groups) — fund-analyzer",
        "version": 3,
        "n_filas": len(activos),
        "n_con_horfin_id": filled("horfin_id"),
        "n_benchmark": filled("benchmark"),
        "n_estrellas": filled("estrellas"),
        "n_descripcion": filled("descripcion"),
        "n_categoria_activo": filled("categoria_activo"),
        "n_kid": filled("kid"),
        "nota": "6 columnas nuevas anadidas. Hueco = null (nunca vacio). Cruce por ISIN.",
    }
    return {**meta, "activos": activos}


def check_isin(isin: str):
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    f = _retry(lambda: c.table("funds").select("isin,nombre_clase,fund_group_id,benchmark")
               .eq("isin", isin).execute()).data
    print(f"{isin} en funds ->", f or "NO EN MI BASE")
    tax = json.loads((ROOT / "data" / "fund_taxonomy.json").read_text(encoding="utf-8"))["funds"]
    print(f"{isin} en taxonomy ->", tax.get(isin, {}).get("nombre", "(no)"))
    old = json.loads(OLD.read_text(encoding="utf-8"))
    hit = next((r for r in old["activos"] if r["isin"] == isin), None)
    print(f"{isin} en export viejo ->", (hit or {}).get("nombre", "(no)"),
          "| benchmark:", (hit or {}).get("benchmark"))


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--check-isin")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.check_isin:
        check_isin(a.check_isin)
    else:
        doc = build()
        if a.dry_run:
            print(json.dumps({k: v for k, v in doc.items() if k != "activos"},
                             ensure_ascii=False, indent=1))
            print("EJEMPLO fila:", json.dumps(doc["activos"][0], ensure_ascii=False)[:400])
        else:
            OUT.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
            print(f"escrito: {OUT}")
