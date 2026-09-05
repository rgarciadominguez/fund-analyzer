"""align_fund_group.py — Contrato FONDO vs CLASE (CLAUDE.md §0.9): un ANÁLISIS por GRUPO,
TODAS las clases resuelven a él. Repunta `dashboard_storage_path` + `output_json_storage_path`
de todas las clases del grupo al PRIMARIO, para que "ver análisis" muestre SIEMPRE el último
bueno del activo, sea cual sea la clase que pulses (portal, fund-analyzer, Supabase, BDD).

Primario (CLAUDE.md §0.9): clase EUR con más track-record SI tiene ≥3 años; si no hay EUR o la
EUR <3 años → la clase con el histórico más largo. Sólo entre clases con análisis BUENO (síntesis
real). Se llama desde sync_to_supabase tras cada análisis (mantiene la alineación sola).

CLI: python -m tools.align_fund_group [ISIN]        (un grupo, el del ISIN)
     python -m tools.align_fund_group --all         (todos los grupos)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _quality(isin: str):
    """(pos, synth_chars, mtime) del output.json local. synth>500 = análisis bueno."""
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return (0, 0, 0.0)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        pos = len((d.get("posiciones") or {}).get("actuales") or [])
        syn = len(((d.get("analyst_synthesis") or {}).get("resumen") or {}).get("texto") or "")
        return (pos, syn, p.stat().st_mtime)
    except Exception:
        return (0, 0, 0.0)


def _years(fc) -> int:
    try:
        return 2026 - int(str(fc)[:4])
    except Exception:
        return 0


def _pick_primary(members: list[dict]) -> dict | None:
    """Elige el primario del grupo según CLAUDE.md §0.9 (misma regla que tools.build_class_map).
    Solo clases con análisis BUENO (síntesis real) y dashboard. None si ninguna cumple.

    §0.9: (1) EUR ≥3 años → la de más track. (2) Hay EUR <3 años → el EUR de más track salvo que
    otra divisa tenga ≥7 años Y ≥3 más (entonces esa, avisando). "No saltar de divisa por 1 año".
    (3) Sin EUR → histórico más largo.
    """
    rows = [(m, *_quality(m["isin"])) for m in members]
    rows = [t for t in rows if t[2] > 500 and t[0].get("dashboard_storage_path")]
    if not rows:
        return None
    qmap = {t[0]["isin"]: t for t in rows}           # isin → (m, pos, synth, mtime)
    cand = [t[0] for t in rows]
    yrs = lambda m: _years(m.get("fecha_creacion_clase"))
    is_eur = lambda m: (m.get("divisa") or "").upper() == "EUR"
    # desempate: más track, síntesis más rica, más reciente, isin estable
    tie = lambda m: (-yrs(m), -(qmap[m["isin"]][2] or 0), -(qmap[m["isin"]][3] or 0), m["isin"])

    eur = sorted([m for m in cand if is_eur(m)], key=tie)
    eur3 = [m for m in eur if yrs(m) >= 3]
    if eur3:
        return eur3[0]
    if eur:
        best_eur = eur[0]
        others = sorted([m for m in cand if not is_eur(m)], key=tie)
        if others and yrs(others[0]) >= 7 and yrs(others[0]) - yrs(best_eur) >= 3:
            return others[0]
        return best_eur
    return sorted(cand, key=tie)[0]


def align_group(isin: str, client=None, log=print) -> dict:
    """Alinea el grupo del ISIN: todas las clases → storage del primario. No crítico."""
    try:
        if client is None:
            from dotenv import load_dotenv
            load_dotenv(ROOT / ".env")
            from tools.supabase_client import get_client
            client = get_client()
        f = client.table("funds").select("fund_group_id").eq("isin", isin.upper()).execute().data
        if not f:
            return {"aligned": 0, "reason": "isin no en funds"}
        gid = f[0]["fund_group_id"]
        cols = "isin,fund_group_id,divisa,dashboard_storage_path,output_json_storage_path,fecha_creacion_clase"
        members = client.table("funds").select(cols).eq("fund_group_id", gid).execute().data or []
        if len(members) < 2:
            return {"aligned": 0, "reason": "grupo mono-clase"}
        prim = _pick_primary(members)
        if not prim:
            return {"aligned": 0, "reason": "sin análisis bueno en el grupo"}
        pdash = prim.get("dashboard_storage_path")
        poj = prim.get("output_json_storage_path")
        n = 0
        for m in members:
            if m["isin"] == prim["isin"]:
                continue
            if m.get("dashboard_storage_path") != pdash or m.get("output_json_storage_path") != poj:
                client.table("funds").update(
                    {"dashboard_storage_path": pdash, "output_json_storage_path": poj}
                ).eq("isin", m["isin"]).execute()
                n += 1
        if n:
            log(f"[ALIGN] grupo de {isin}: {n} clases → primario {prim['isin']} ({prim.get('divisa')})")
        return {"aligned": n, "primary": prim["isin"]}
    except Exception as e:  # noqa: BLE001
        log(f"[ALIGN] falló (no crítico): {str(e)[:100]}")
        return {"aligned": 0, "error": str(e)[:100]}


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = argv or sys.argv[1:]
    if "--all" in args:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        from collections import defaultdict
        c = get_client()
        funds = c.table("funds").select("isin,fund_group_id").limit(5000).execute().data
        groups = defaultdict(list)
        for f in funds:
            groups[f["fund_group_id"]].append(f["isin"])
        total = 0
        seen = set()
        for gid, isins in groups.items():
            if len(isins) < 2 or gid in seen:
                continue
            seen.add(gid)
            total += align_group(isins[0], client=c).get("aligned", 0)
        print(f"ALIGN --all: {total} clases repuntadas")
    elif args:
        print(json.dumps(align_group(args[0]), ensure_ascii=False))
    else:
        print("uso: python -m tools.align_fund_group [ISIN] | --all")


if __name__ == "__main__":
    main()
