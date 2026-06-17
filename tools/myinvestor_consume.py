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


def consume(isin: str, client=None, log=None) -> dict:
    """Vuelca myinvestor_data.json del ISIN a Supabase. Devuelve dict de cambios."""
    isin = (isin or "").upper().strip()
    root = Path(__file__).resolve().parent.parent
    jp = root / "data" / "funds" / isin / "myinvestor_data.json"
    if not jp.exists():
        return {}
    try:
        d = json.loads(jp.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if client is None:
        from dotenv import load_dotenv
        load_dotenv(root / ".env")
        from tools.supabase_client import get_client
        client = get_client()
    c = client
    if not d.get("disponible_myinvestor"):
        return {}

    # --- funds (clase) ---
    # La clase que enriquecemos es la que CASÓ en MyInvestor (matched_isin), que
    # puede ser una clase hermana distinta de la analizada. Si no, la objetivo.
    target = (d.get("matched_isin") or isin).upper().strip()
    fu = c.table("funds").select("broker_disponible,distribucion,fund_group_id").eq("isin", target).execute().data
    if not fu:
        # la clase que casó no está como fila en funds → cae a la objetivo para el grupo
        fu = c.table("funds").select("broker_disponible,distribucion,fund_group_id").eq("isin", isin).execute().data
        target = isin
    if not fu:
        return {}
    f0 = fu[0]

    # Audit MEDIO (2026-06-17): revalidar que matched_isin pertenece al MISMO grupo
    # que el fondo analizado. Si la skill casó un ISIN de OTRO fondo (mismo nombre,
    # fondo distinto), escribiríamos su distribución/mín./allocation en la fila/grupo
    # equivocados. Solo bloquea el caso claro (ambos en Supabase, grupos distintos).
    if target != isin and f0.get("fund_group_id"):
        own = c.table("funds").select("fund_group_id").eq("isin", isin).execute().data
        own_gid = own[0].get("fund_group_id") if own else None
        if own_gid and own_gid != f0.get("fund_group_id"):
            if log:
                log(f"[MYINVESTOR] matched_isin {target} en OTRO grupo que {isin} → descartado (match erróneo)")
            return {"skipped": "matched_isin_other_group", "matched": target}

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
        c.table("funds").update(fupd).eq("isin", target).execute()

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

    msg = (f"{isin}: MyInvestor volcado (distrib={fupd.get('distribucion')}, broker+MyInvestor, "
           f"min={fupd.get('importe_minimo_eur')}, allocation/sectores/docs en grupo)")
    if log:
        log(msg)
    return {"funds": fupd, "grupo_myinvestor": True}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m tools.myinvestor_consume <ISIN>")
        sys.exit(1)
    r = consume(sys.argv[1])
    print(f"{sys.argv[1].upper()}: {'volcado' if r else 'no disponible / sin json'}")
