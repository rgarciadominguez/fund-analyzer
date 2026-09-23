"""
build_asset_classes.py — Genera asset_classes_por_isin.json para el portal (endpoint sync-clases).

Una entrada por ISIN con su info de CLASE (la que difiere entre clases del mismo fondo) +
el vínculo al fondo (fund_group_id) + las clases hermanas. Cruce por ISIN.

Estructura:
  {
    "generado": "...", "n": N,
    "clases": {
      "ES0159259011": {
        "fund_group_id": "...", "nombre_clase": "...", "es_primario": true,
        "divisa": "EUR", "ter_pct": 0.54, "comision_gestion_pct": 0.88,
        "distribucion": "Acumulación", "fecha_creacion_clase": "...",
        "importe_minimo_eur": null, "clases_hermanas": ["ES0159259003", ...]
      }, ...
    }
  }

CLI:  python -m tools.build_asset_classes
"""

from __future__ import annotations

from tools.consume_inputs_rafa import parse_brokers as _pb

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from tools.paths import HORFIN_DIR

ROOT = Path(__file__).resolve().parent.parent
OUT = HORFIN_DIR / "asset_classes_por_isin.json"
EXPORT = HORFIN_DIR / "catalogo_supabase.json"


def build() -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()

    funds = []
    off = 0
    while True:
        b = c.table("funds").select(
            "isin,nombre_clase,fund_group_id,divisa,ter_pct,comision_gestion_pct,"
            "fecha_creacion_clase,importe_minimo_eur,distribucion,broker_disponible,kid").range(off, off + 999).execute().data
        if not b:
            break
        funds += b
        off += 1000
        if off > 20000:
            break

    # clases hermanas por grupo (class_isins_known + los ISIN presentes del grupo)
    from collections import defaultdict
    by_group = defaultdict(set)
    for f in funds:
        if f.get("fund_group_id"):
            by_group[str(f["fund_group_id"])].add(f["isin"])
    # fecha_proximo_analisis por grupo (para que el portal genere tareas de re-análisis)
    prox_por_grupo = {}
    for g in c.table("fund_groups").select(
            "fund_group_id,class_isins_known,fecha_proximo_analisis").execute().data:
        gid = str(g["fund_group_id"])
        for i in (g.get("class_isins_known") or []):
            by_group[gid].add(i)
        if g.get("fecha_proximo_analisis"):
            prox_por_grupo[gid] = g["fecha_proximo_analisis"]

    # es_primario desde el export (ya lo computa el contrato)
    primario = {}
    if EXPORT.exists():
        for a in json.loads(EXPORT.read_text(encoding="utf-8")).get("activos", []):
            primario[a["isin"]] = bool(a.get("es_primario_del_grupo"))

    clases = {}
    for f in funds:
        gid = str(f["fund_group_id"]) if f.get("fund_group_id") else None
        hermanas = sorted(by_group.get(gid, set()) - {f["isin"]}) if gid else []
        clases[f["isin"]] = {
            "fund_group_id": gid,
            "nombre_clase": f.get("nombre_clase"),
            "es_primario": primario.get(f["isin"], False),
            "divisa": f.get("divisa"),
            "ter_pct": f.get("ter_pct"),
            "comision_gestion_pct": f.get("comision_gestion_pct"),
            "distribucion": f.get("distribucion"),
            "fecha_creacion_clase": f.get("fecha_creacion_clase"),
            "importe_minimo_eur": f.get("importe_minimo_eur"),
            # broker: EL dato de la clase (cambia por clase). Lista de brokers donde está.
            # CSV, no lista: el portal guarda el campo como texto y al devolverlo lo parte por comas
            # (una lista serializada volvía como '["MyInvestor"', '"Renta4"]' → 197 filas rotas, 2026-09-17)
            "broker": ",".join(_pb(f.get("broker_disponible"))),
            "kid": f.get("kid"),
            "clases_hermanas": hermanas,
            # fecha del próximo re-análisis (del grupo) → el portal genera tareas de
            # "relanzar fondo" cuando esta fecha < hoy. Misma para todas las clases del grupo.
            "fecha_proximo_analisis": prox_por_grupo.get(gid),
        }
    return {"generado": datetime.now(timezone.utc).isoformat(), "n": len(clases), "clases": clases}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    d = build()
    OUT.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"asset_classes_por_isin.json: {d['n']} ISIN -> {OUT}")
