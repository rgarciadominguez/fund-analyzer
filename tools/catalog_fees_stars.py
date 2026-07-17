"""
catalog_fees_stars.py — Rellena `estrellas` y comisiones del catálogo desde Morningstar.

Fuente: screener público de Morningstar (`tools/morningstar_quant`), sin login.

REGLAS DE DATO (acordadas con Horizonte):
  - Redondeo a 2 decimales SIEMPRE (llegaban valores tipo 0.8999999999999999).
  - `null` = "no lo sé".  `0` = "no la tiene".  NUNCA un 0 para decir "no sé".
  - `estrellas` null si el fondo no llega a 3 años (Morningstar no puntúa) — no es fallo.
  - No se pisa un valor existente con null (fill-if-empty para comisiones).

`--audit-zeros` revisa los comision_gestion_pct == 0 del catálogo: un fondo con 0% de
gestión es raro y suele ser un "no sé" escrito como 0. Los que Morningstar contradiga
se corrigen; los que no se puedan confirmar se pasan a null.

CLI:
    python -m tools.catalog_fees_stars --dry-run
    python -m tools.catalog_fees_stars --apply
    python -m tools.catalog_fees_stars --audit-zeros [--apply]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

REPORT = ROOT / "data" / "catalog_fees_stars_report.json"


def _r2(v):
    """Redondea a 2 decimales. None si no es número."""
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def collect(isins: list[str]) -> dict:
    from tools.morningstar_quant import fetch_quant
    out = {}
    for i, isin in enumerate(isins, 1):
        try:
            q = fetch_quant(isin) or {}
        except Exception:
            q = {}
        com = q.get("comisiones") or {}
        out[isin] = {
            "estrellas": q.get("rating_estrellas"),
            "ter_pct": _r2(com.get("ter_pct")),
            "comision_gestion_pct": _r2(com.get("comision_gestion_pct")),
            "_resp": bool(q),
        }
        if i % 25 == 0:
            print(f"  morningstar {i}/{len(isins)}")
    return out


def run(apply: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client

    c = get_client()
    rows = c.table("funds").select("isin,ter_pct,comision_gestion_pct").execute().data
    isins = [r["isin"] for r in rows]
    print(f"catálogo: {len(isins)} fondos")
    ms = collect(isins)

    plan = []
    stats = {"estrellas": 0, "ter_nuevo": 0, "ter_redondeo": 0, "gestion_nuevo": 0, "sin_resp": 0}
    for r in rows:
        isin = r["isin"]
        m = ms.get(isin, {})
        if not m.get("_resp"):
            stats["sin_resp"] += 1
        upd = {}

        if m.get("estrellas") is not None:
            upd["estrellas"] = int(m["estrellas"])
            stats["estrellas"] += 1

        # TER: Morningstar manda donde no tengamos; y redondeo del que ya haya
        cur_ter = r.get("ter_pct")
        if cur_ter is None and m.get("ter_pct") is not None:
            upd["ter_pct"] = m["ter_pct"]
            stats["ter_nuevo"] += 1
        elif cur_ter is not None and _r2(cur_ter) != cur_ter:
            upd["ter_pct"] = _r2(cur_ter)
            stats["ter_redondeo"] += 1

        # comisión gestión: fill-if-empty
        if r.get("comision_gestion_pct") is None and m.get("comision_gestion_pct") is not None:
            upd["comision_gestion_pct"] = m["comision_gestion_pct"]
            stats["gestion_nuevo"] += 1

        if upd:
            plan.append({"isin": isin, "update": upd})

    print(json.dumps(stats, indent=1))
    REPORT.write_text(json.dumps({"stats": stats, "plan": plan}, ensure_ascii=False, indent=1),
                      encoding="utf-8")
    if apply:
        for p in plan:
            c.table("funds").update(p["update"]).eq("isin", p["isin"]).execute()
        print(f"escritas {len(plan)} filas")
    else:
        print(f"(dry-run) filas a tocar: {len(plan)}")
    return {"stats": stats, "plan": plan}


def audit_zeros(apply: bool = False) -> None:
    """comision_gestion_pct == 0 -> ¿es real o es un 'no sé'?"""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    from tools.morningstar_quant import fetch_quant

    c = get_client()
    rows = c.table("funds").select("isin,nombre_clase,comision_gestion_pct") \
        .eq("comision_gestion_pct", 0).execute().data
    print(f"fondos con comision_gestion_pct == 0: {len(rows)}\n")
    fixes = []
    for r in rows:
        isin = r["isin"]
        try:
            q = fetch_quant(isin) or {}
        except Exception:
            q = {}
        ms = (q.get("comisiones") or {}).get("comision_gestion_pct")
        ms = _r2(ms)
        if ms is not None and ms > 0:
            verdict, new = f"Morningstar dice {ms}% -> era un 'no sé'", ms
        elif ms == 0:
            verdict, new = "Morningstar confirma 0% -> real, se deja", None
        else:
            verdict, new = "sin confirmar -> a null (0 significaría 'no la tiene')", "NULL"
        print(f"  {isin} {(r.get('nombre_clase') or '')[:38]:38s} {verdict}")
        if new is not None:
            fixes.append({"isin": isin, "value": None if new == "NULL" else new})
    if apply and fixes:
        for f in fixes:
            c.table("funds").update({"comision_gestion_pct": f["value"]}).eq("isin", f["isin"]).execute()
        print(f"\ncorregidos {len(fixes)}")
    else:
        print(f"\n(dry-run) a corregir: {len(fixes)}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--audit-zeros", action="store_true")
    a = ap.parse_args()
    if a.audit_zeros:
        audit_zeros(apply=a.apply)
    else:
        run(apply=a.apply)
