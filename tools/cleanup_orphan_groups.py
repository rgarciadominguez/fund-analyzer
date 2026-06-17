"""Limpia grupos huérfanos en Supabase: filas de `fund_groups` a las que NO apunta
ningún `funds.fund_group_id`.

Aparecen como drift del proceso de relink/reconciliación de clases (una clase se
mueve a otro grupo y el viejo queda vacío). No producen filas duplicadas en el
catálogo (el catálogo une funds→groups), pero ensucian fund_groups y provocan
COLISIONES de nombre cuando renombras un fondo a uno que coincide con un huérfano.

Uso:
    python -m tools.cleanup_orphan_groups          # dry-run (solo lista)
    python -m tools.cleanup_orphan_groups --apply  # borra de verdad
"""
from __future__ import annotations

import sys
from pathlib import Path


def find_orphans(client) -> list[dict]:
    groups = client.table("fund_groups").select(
        "fund_group_id,nombre_base,gestora").limit(10000).execute().data
    funds = client.table("funds").select("fund_group_id").limit(10000).execute().data
    used = {f["fund_group_id"] for f in funds if f.get("fund_group_id")}
    return [g for g in groups if g["fund_group_id"] not in used]


def cleanup(apply: bool = False, log=print) -> dict:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    orphans = find_orphans(c)
    log(f"Grupos huérfanos (0 fondos): {len(orphans)}")
    for g in orphans:
        log(f"  {g['fund_group_id'][:8]}  {(g.get('nombre_base') or '')[:42]!r}  ({g.get('gestora') or ''})")
    if apply and orphans:
        for g in orphans:
            c.table("fund_groups").delete().eq("fund_group_id", g["fund_group_id"]).execute()
        log(f"[OK] {len(orphans)} grupos huérfanos borrados.")
    elif orphans:
        log("(dry-run — usa --apply para borrar)")
    return {"n_orphans": len(orphans), "deleted": apply, "ids": [g["fund_group_id"] for g in orphans]}


def main():
    cleanup(apply="--apply" in sys.argv)


if __name__ == "__main__":
    main()
