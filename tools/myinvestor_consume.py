"""Vuelca data/funds/{ISIN}/myinvestor_data.json (creado por la skill cowork
`myinvestor-enrich`) a Supabase.

- funds.distribucion  ← distribución robusta de MyInvestor (override del nombre)
- funds.broker_disponible  ← añade "MyInvestor" si el fondo está en su catálogo
- funds.importe_minimo_eur  ← min_initial si es numérico
- fund_groups.portfolio_metrics_jsonb  ← asset_allocation, sectores, docs, srri,
  rating Morningstar, categoría ES, rentab. por año natural

Uso: python -m tools.myinvestor_consume <ISIN>
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path


def _num(v):
    if v is None:
        return None
    m = re.search(r"[\d.]+", str(v).replace(",", "."))
    return float(m.group(0)) if m else None


def main():
    if len(sys.argv) < 2:
        print("uso: python -m tools.myinvestor_consume <ISIN>")
        return
    isin = sys.argv[1].upper().strip()
    root = Path(__file__).resolve().parent.parent
    jp = root / "data" / "funds" / isin / "myinvestor_data.json"
    if not jp.exists():
        print(f"no existe {jp}")
        return
    d = json.loads(jp.read_text(encoding="utf-8"))
    from dotenv import load_dotenv
    load_dotenv(root / ".env")
    from tools.supabase_client import get_client
    c = get_client()

    if not d.get("disponible_myinvestor"):
        print(f"{isin}: no disponible en MyInvestor (nada que volcar)")
        return

    # --- funds (clase) ---
    fu = c.table("funds").select("broker_disponible,distribucion,fund_group_id").eq("isin", isin).execute().data
    if not fu:
        print(f"{isin}: no está en funds")
        return
    f0 = fu[0]
    fupd = {}
    if d.get("distribucion"):
        fupd["distribucion"] = d["distribucion"]   # MyInvestor es la fuente robusta
    brokers = list(f0.get("broker_disponible") or [])
    if "MyInvestor" not in brokers:
        brokers.append("MyInvestor"); fupd["broker_disponible"] = brokers
    mn = _num(d.get("min_initial"))
    if mn is not None and mn > 1:
        fupd["importe_minimo_eur"] = mn
    if fupd:
        c.table("funds").update(fupd).eq("isin", isin).execute()

    # --- fund_groups (datos de cartera/identidad compartidos) ---
    gid = f0.get("fund_group_id")
    if gid:
        cur = c.table("fund_groups").select("portfolio_metrics_jsonb").eq("fund_group_id", gid).execute().data
        pm = (cur[0].get("portfolio_metrics_jsonb") or {}) if cur else {}
        pm["myinvestor"] = {k: d.get(k) for k in
                            ("asset_allocation", "top_sectors", "docs", "srri",
                             "mstar_rating", "categoria_morningstar_es", "rentab_anual")
                            if d.get(k) not in (None, {}, [])}
        c.table("fund_groups").update({"portfolio_metrics_jsonb": pm}).eq("fund_group_id", gid).execute()

    print(f"{isin}: MyInvestor volcado (distrib={fupd.get('distribucion')}, broker+MyInvestor, "
          f"min={fupd.get('importe_minimo_eur')}, allocation/sectores/docs en grupo)")


if __name__ == "__main__":
    main()
