"""
cost_sync.py — Vuelca los agregados de coste a Supabase para que la WEB PÚBLICA los lea.

El log de coste (data/cost_log.jsonl) vive local. La web pública (workers.dev) no tiene
servidor que lo lea. Igual que el catálogo, el patrón es: agregar y subir a Supabase; la
web lee de Supabase. Dos rollups:

  cost_fund   (isin PK): coste de por vida por fondo  → nivel ANÁLISIS DE FONDO
  cost_month  (mes+categoria PK): coste por mes natural → nivel MENSUAL

DDL (pegar en Supabase SQL Editor una vez):
  ver data/migrations/2026-07-20_cost_tables.sql

CLI:
    python -m tools.cost_sync --apply           # rollup completo (fondos + meses)
    python -m tools.cost_sync --fund ES0112...   # upsert de un solo fondo (lo llama el sync)
    python -m tools.cost_sync --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CAT_A = "analisis_fondos"
CAT_I = "procesar_imagenes"


def _now():
    return datetime.now(timezone.utc).isoformat()


def rollup_funds(client, only_isin: str | None = None, apply: bool = False) -> int:
    from tools.cost_monitor import by_fund
    rows = by_fund()
    if only_isin:
        rows = [r for r in rows if r["isin"] == only_isin]
    n = 0
    for r in rows:
        cat = r.get("por_categoria", {})
        rec = {
            "isin": r["isin"],
            "cost_usd": round(r["cost_usd"], 4),
            "cost_analisis_usd": round(cat.get(CAT_A, 0.0), 4),
            "cost_imagenes_usd": round(cat.get(CAT_I, 0.0), 4),
            "n_calls": r["n_calls"],
            "updated_at": _now(),
        }
        if apply:
            client.table("cost_fund").upsert(rec, on_conflict="isin").execute()
        n += 1
    return n


def rollup_months(client, apply: bool = False) -> int:
    from tools.cost_monitor import by_month
    n = 0
    for m in by_month(months=120):
        for cat, cost in (m.get("por_categoria") or {}).items():
            rec = {
                "mes": m["mes"], "categoria": cat,
                "cost_usd": round(cost, 4), "n_calls": m["n_calls"],
                "updated_at": _now(),
            }
            if apply:
                client.table("cost_month").upsert(rec, on_conflict="mes,categoria").execute()
            n += 1
    return n


def sync(only_isin: str | None = None, apply: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    client = get_client() if apply else None
    nf = rollup_funds(client, only_isin=only_isin, apply=apply)
    nm = 0 if only_isin else rollup_months(client, apply=apply)
    out = {"fondos": nf, "meses_categoria": nm, "apply": apply}
    print(out)
    return out


def sync_fund_cost(isin: str) -> None:
    """Upsert del coste de UN fondo. Best-effort, para llamar desde sync_to_supabase."""
    try:
        sync(only_isin=isin, apply=True)
    except Exception as e:
        print(f"[cost_sync] {isin} falló (no crítico): {str(e)[:80]}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fund")
    a = ap.parse_args()
    sync(only_isin=a.fund, apply=a.apply)
