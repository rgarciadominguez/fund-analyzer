"""
quant_sync.py — Aplana el quant (JSONB por grupo) a tablas POR ISIN para la web pública.

PROBLEMA: el quant (rendimientos anuales, métricas) vive en `fund_groups` como JSONB y por
`fund_group_id`, NO por ISIN. Vuestra conexión viva cruza por ISIN y quiere tablas
normalizadas (`hf_asset_*`). Este sync hace el puente, igual que cost_sync:

  hf_asset_annual_returns  (isin, anio, rentab_pct)      ← rendimiento_jsonb.rentabilidades_anuales
  hf_asset_metrics         (isin, cagr, vol, maxdd, ...)  ← rendimiento_jsonb + portfolio_metrics_jsonb
  hf_asset_prices          (isin, fecha, nav)             ← serie diaria Morningstar (opcional, --prices)

CRUCE: cada ISIN del grupo (funds.isin del grupo + fund_groups.class_isins_known) hereda el
quant del grupo. Todas las clases de un fondo comparten métricas de grupo (salvo precio, que
es por ISIN real de Morningstar).

DDL: data/migrations/2026-07-23_hf_asset_tables.sql

CLI:
    python -m tools.quant_sync --apply            # métricas + rendimientos (barato, del JSONB)
    python -m tools.quant_sync --prices --apply    # + serie diaria (baja de Morningstar por ISIN)
    python -m tools.quant_sync --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _now():
    return datetime.now(timezone.utc).isoformat()


def _isins_of_group(funds_rows: list, group_ids: set, known: list) -> dict:
    """{fund_group_id: [isin,...]} uniendo funds.isin del grupo + class_isins_known."""
    from collections import defaultdict
    out = defaultdict(set)
    for f in funds_rows:
        g = f.get("fund_group_id")
        if g:
            out[g].add(f["isin"])
    for g, isins in known.items():
        for i in (isins or []):
            out[g].add(i)
    return {g: sorted(v) for g, v in out.items()}


def build_rollups(client):
    groups = client.table("fund_groups").select(
        "fund_group_id,rendimiento_jsonb,portfolio_metrics_jsonb,class_isins_known").execute().data
    funds = client.table("funds").select("isin,fund_group_id").execute().data
    known = {g["fund_group_id"]: g.get("class_isins_known") for g in groups}
    by_group = _isins_of_group(funds, {g["fund_group_id"] for g in groups}, known)

    ann_rows, met_rows = [], []
    for g in groups:
        gid = g["fund_group_id"]
        isins = by_group.get(gid, [])
        if not isins:
            continue
        rend = g.get("rendimiento_jsonb") or {}
        pm = g.get("portfolio_metrics_jsonb") or {}
        ms = (pm.get("morningstar") or {})
        mi = (pm.get("myinvestor") or {})
        anuales = rend.get("rentabilidades_anuales") or {}

        for isin in isins:
            # rendimientos anuales: una fila por (isin, año)
            for anio, pct in anuales.items():
                if pct is None:
                    continue
                ann_rows.append({"isin": isin, "anio": int(anio),
                                 "rentab_pct": round(float(pct), 4), "updated_at": _now()})
            # métricas: una fila por isin
            met_rows.append({
                "isin": isin,
                "cagr_desde_inicio": rend.get("cagr_desde_inicio"),
                "rentab_1a": rend.get("rentab_1a"), "rentab_3a": rend.get("rentab_3a"),
                "rentab_5a": rend.get("rentab_5a"), "rentab_10a": rend.get("rentab_10a"),
                "volatilidad": rend.get("volatilidad"),
                "volatilidad_3a": rend.get("volatilidad_3a"),
                "volatilidad_5a": rend.get("volatilidad_5a"),
                "max_drawdown": rend.get("max_drawdown"),
                "peor_anio": rend.get("peor_anio"), "mejor_anio": rend.get("mejor_anio"),
                "estrellas": ms.get("estrellas"), "medalist": ms.get("medalist"),
                "srri": mi.get("srri"), "mstar_rating": mi.get("mstar_rating"),
                "fuente": rend.get("_fuente") or "morningstar_daily",
                "updated_at": _now(),
            })
    return ann_rows, met_rows


def sync(apply: bool = False, prices: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    client = get_client()

    ann, met = build_rollups(client)
    print(f"rendimientos anuales: {len(ann)} filas | métricas: {len(met)} filas")

    if apply:
        for i in range(0, len(met), 200):
            client.table("hf_asset_metrics").upsert(met[i:i+200], on_conflict="isin").execute()
        # anual: borrar+insert por simplicidad (idempotente por (isin,anio))
        for i in range(0, len(ann), 200):
            client.table("hf_asset_annual_returns").upsert(
                ann[i:i+200], on_conflict="isin,anio").execute()
        print(f"upsert: {len(met)} métricas + {len(ann)} rendimientos")

    n_prices = 0
    if prices:
        n_prices = sync_prices(client, apply=apply)

    return {"anual": len(ann), "metrics": len(met), "prices": n_prices, "apply": apply}


def sync_prices(client, apply: bool = False) -> int:
    """Serie diaria por ISIN desde Morningstar → hf_asset_prices. Pesado (una llamada/ISIN)."""
    from tools.morningstar_daily import fetch_series
    from datetime import datetime as _dt, timezone as _tz
    isins = sorted({f["isin"] for f in client.table("funds").select("isin").execute().data})
    print(f"precios: bajando serie de {len(isins)} ISIN de Morningstar...")
    total = 0
    for k, isin in enumerate(isins, 1):
        try:
            s = fetch_series(isin)  # [(ts_ms, nav), ...]
        except Exception:
            s = []
        if not s:
            continue
        rows = [{"isin": isin,
                 "fecha": _dt.fromtimestamp(ts/1000, _tz.utc).date().isoformat(),
                 "nav": round(float(v), 6)} for ts, v in s]
        if apply:
            for i in range(0, len(rows), 500):
                client.table("hf_asset_prices").upsert(
                    rows[i:i+500], on_conflict="isin,fecha").execute()
        total += len(rows)
        if k % 25 == 0:
            print(f"  {k}/{len(isins)} | {total} puntos")
    print(f"precios: {total} puntos de {len(isins)} ISIN")
    return total


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--prices", action="store_true")
    a = ap.parse_args()
    print(sync(apply=a.apply, prices=a.prices))
