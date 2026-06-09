"""reconcile_catalog.py — Garantiza que lo que está en el ANÁLISIS (output.json)
quede reflejado en la TABLA del catálogo (Supabase fund_groups).

Petición Rafa (2026-06-09): "si se tiene el dato en el análisis debe quedar
reflejado en la tabla". El sync ya propaga al analizar/sincronizar, pero cuando
un dato se rellena DESPUÉS (p.ej. aum_pdf_recover rellena kpis.aum en output.json
sin re-sincronizar), la tabla se queda atrás. Este reconciliador cierra ese hueco
sin re-subir ficheros (ligero).

Campos reconciliados (fill-if-empty / fix-placeholder, NUNCA pisa un valor real):
  - aum_meur          ← output.kpis.aum_actual_meur
  - num_participes    ← output.kpis.num_participes
  - nombre_base       ← output.nombre (si tabla = placeholder ISIN)
  - gestora           ← output.gestora (si tabla = placeholder ISIN / vacía)
  - gestores_nombres  ← perfiles de analyst_synthesis.gestores (si tabla vacía)

CLI:
    python -m tools.reconcile_catalog            # DRY-RUN
    python -m tools.reconcile_catalog --apply    # escribe en Supabase
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"


def _load(isin: str) -> dict:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _gestores_from_output(j: dict) -> list[str]:
    a = j.get("analyst_synthesis", {})
    if not isinstance(a, dict):
        return []
    perfiles = (a.get("gestores", {}) or {}).get("perfiles", []) if isinstance(a.get("gestores"), dict) else []
    out = []
    for p in perfiles:
        if isinstance(p, dict) and p.get("nombre"):
            out.append(str(p["nombre"]))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Reconcilia análisis (output.json) → tabla (fund_groups)")
    ap.add_argument("--apply", action="store_true", help="Escribe en Supabase (default: dry-run)")
    ap.add_argument("--isin", help="Solo este ISIN")
    args = ap.parse_args(argv)

    from tools.supabase_client import get_client
    c = get_client()
    funds = c.table("funds").select("isin,fund_group_id").limit(2000).execute().data
    groups = {g["fund_group_id"]: g for g in
              c.table("fund_groups").select(
                  "fund_group_id,nombre_base,gestora,aum_meur,num_participes,gestores_nombres"
              ).limit(2000).execute().data}

    n = {"aum_meur": 0, "num_participes": 0, "nombre_base": 0, "gestora": 0, "gestores_nombres": 0}
    applied = 0
    for f in funds:
        isin = f["isin"]
        if args.isin and isin != args.isin:
            continue
        g = groups.get(f["fund_group_id"])
        if not g:
            continue
        j = _load(isin)
        if not j:
            continue
        kpis = j.get("kpis", {}) or {}
        nombre = (j.get("nombre") or "").strip()
        gestora = (j.get("gestora") or "").strip()
        isin_up = isin.upper()
        upd = {}
        # aum / participes: fill-if-empty (tabla null/0)
        if kpis.get("aum_actual_meur") and not g.get("aum_meur"):
            upd["aum_meur"] = kpis["aum_actual_meur"]
        if kpis.get("num_participes") and not g.get("num_participes"):
            upd["num_participes"] = kpis["num_participes"]
        # nombre / gestora: fix placeholder (== ISIN) o vacío
        if nombre and nombre.upper() != isin_up and (not g.get("nombre_base") or g["nombre_base"].upper() == isin_up):
            from tools.import_taxonomy import normalize_nombre_base
            try:
                upd["nombre_base"] = normalize_nombre_base(nombre)
            except Exception:
                upd["nombre_base"] = nombre
        if gestora and gestora.upper() != isin_up and (not g.get("gestora") or g["gestora"].upper() == isin_up):
            upd["gestora"] = gestora
        # gestores_nombres: fill-if-empty desde perfiles del analyst
        if not g.get("gestores_nombres"):
            gn = _gestores_from_output(j)
            if gn:
                upd["gestores_nombres"] = gn
        if not upd:
            continue
        for k in upd:
            n[k] += 1
        print(f"{isin}: {', '.join(f'{k}={str(v)[:40]}' for k, v in upd.items())}")
        if args.apply:
            try:
                c.table("fund_groups").update(upd).eq("fund_group_id", g["fund_group_id"]).execute()
                applied += 1
            except Exception as e:
                print(f"   [ERROR] {isin}: {str(e)[:100]}")

    print(f"\nReconciliaciones: {n}")
    print(f"{'APLICADO: '+str(applied)+' grupos' if args.apply else 'DRY-RUN — nada escrito. Usa --apply.'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
