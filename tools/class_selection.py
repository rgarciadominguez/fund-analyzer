"""Qué CLASES de un fondo se publican y con qué papel (regla Rafa 2026-09-23).

Regla: por cada fondo (grupo) y por cada DIVISA (y cobertura): UNA clase retail y UNA limpia.
  · retail  = la que está en MyInvestor (lo que compra un particular); si ninguna, la de mayor
              comisión de gestión no institucional.
  · limpia  = la que está en Mundo Asesoramiento Mapfre (clase limpia de asesoramiento); si
              ninguna, la de menor comisión distinta de la retail.
  · serie_larga = además, si en esa divisa/cobertura hay una clase con más track record que la
              retail y la limpia elegidas (≥2 años más), se publica también, identificada como
              "serie más larga (desde AAAA)".
  · analizada = la clase analizada por el fund-analyzer va siempre.
Determinista y explicable: cada clase sale con `motivo`. Fuentes: Supabase funds (divisa,
cobertura, comisión, fecha de creación, brokers), lista Mundo Asesoramiento (broker_availability),
caché MyInvestor, dashboard/_class_map.json (cobertura por nombre).

CLI: python -m tools.class_selection ISIN   → tabla de clases elegidas del grupo
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ORDER_ROL = {"analizada": 0, "retail": 1, "limpia": 2, "serie_larga": 3}
ORDER_CCY = {"EUR": 0, "USD": 1, "GBP": 2, "CHF": 3}
MAJOR_CCY = ("EUR", "USD", "GBP", "CHF")   # divisas con bucket retail/limpia; el resto solo si es la analizada


def _year(v) -> int | None:
    try:
        return int(str(v)[:4])
    except Exception:
        return None


def _is_dist(name: str) -> bool:
    return bool(re.search(r"\b(inc|dis|dist|minc|qinc|y dis|distribuci)\b", (name or "").lower()))


def group_classes(isin: str, client=None) -> list[dict]:
    """Todas las clases del grupo del ISIN con sus atributos normalizados."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    if client is None:
        from tools.supabase_client import get_client
        client = get_client()
    from tools.morningstar_daily import is_hedged_class
    from tools.consume_inputs_rafa import parse_brokers
    from tools.broker_availability import _mapfre_universe, _myinvestor_universe
    isin = (isin or "").upper()
    r = client.table("funds").select("fund_group_id").eq("isin", isin).execute().data
    if not r or not r[0].get("fund_group_id"):
        return []
    gid = r[0]["fund_group_id"]
    rows = client.table("funds").select(
        "isin,nombre_clase,divisa,divisa_hedge_bool,comision_gestion_pct,ter_pct,fecha_creacion_clase,"
        "importe_minimo_eur,broker_disponible,has_qualitative_analysis").eq("fund_group_id", gid).execute().data or []
    mapfre, _ = _mapfre_universe()
    mi_found, _ = _myinvestor_universe()
    out = []
    for f in rows:
        i = f["isin"].upper()
        name = f.get("nombre_clase") or ""
        brokers = parse_brokers(f.get("broker_disponible") or [])
        out.append({
            "isin": i, "nombre": name, "gid": gid,
            "divisa": (f.get("divisa") or "").upper() or "EUR",
            "hedge": bool(f.get("divisa_hedge_bool")) or is_hedged_class(name),
            "fee": f.get("comision_gestion_pct"), "ter": f.get("ter_pct"),
            "anio": _year(f.get("fecha_creacion_clase")),
            "minimo": f.get("importe_minimo_eur"),
            "dist": _is_dist(name),
            # por CLASE exacta: la caché del conector. El flag broker_disponible se hereda por grupo
            # (basta una clase para marcar el fondo) y NO distingue clases → solo si no hay caché.
            "myinvestor": (i in mi_found) if mi_found else ("MyInvestor" in brokers),
            "mundo": i in mapfre,
            "analizada": bool(f.get("has_qualitative_analysis")),
        })
    return out


def select(isin: str, client=None) -> list[dict]:
    """Clases a publicar del grupo del ISIN, con `roles` y `motivo`."""
    cls = group_classes(isin, client)
    if not cls:
        return []
    by = {c["isin"]: c for c in cls}
    roles: dict[str, list[str]] = {c["isin"]: [] for c in cls}
    motivo: dict[str, list[str]] = {c["isin"]: [] for c in cls}
    buckets: dict[tuple, list[dict]] = {}
    for c in cls:
        # Solo divisas principales: una clase CAD/SEK/JPY suelta no aporta a un cliente español y
        # multiplica filas (Carmignac: 7 clases publicadas por una CAD cubierta). La analizada va siempre.
        if c["divisa"] not in MAJOR_CCY:
            continue
        buckets.setdefault((c["divisa"], c["hedge"]), []).append(c)

    def _inst(c):   # institucional: mínimo ≥ 500k
        return (c.get("minimo") or 0) >= 500_000

    for key, lst in buckets.items():
        # retail
        cand = [c for c in lst if c["myinvestor"]]
        if cand:
            r = sorted(cand, key=lambda c: (c["dist"], c["anio"] or 9999))[0]
            motivo[r["isin"]].append("retail: disponible en MyInvestor")
        else:
            cand = [c for c in lst if not _inst(c) and c.get("fee") is not None] or [c for c in lst if c.get("fee") is not None] or lst
            r = sorted(cand, key=lambda c: (-(c.get("fee") or 0), c["dist"], c["anio"] or 9999))[0]
            motivo[r["isin"]].append("retail: mayor comisión de gestión (ninguna clase en MyInvestor)")
        roles[r["isin"]].append("retail")
        # limpia
        cand = [c for c in lst if c["mundo"] and c["isin"] != r["isin"]]
        if cand:
            l = sorted(cand, key=lambda c: (c["dist"], c["anio"] or 9999))[0]
            motivo[l["isin"]].append("limpia: en Mundo Asesoramiento Mapfre")
        else:
            cand = [c for c in lst if c["isin"] != r["isin"] and c.get("fee") is not None and not _inst(c)
                    and (r.get("fee") is None or (c["fee"] or 0) < (r["fee"] or 0))]
            l = sorted(cand, key=lambda c: ((c.get("fee") or 0), c["dist"], c["anio"] or 9999))[0] if cand else None
            if l:
                motivo[l["isin"]].append("limpia: menor comisión de gestión de la divisa")
        if l:
            roles[l["isin"]].append("limpia")
        # serie más larga
        chosen_years = [c["anio"] for c in (r, l) if c and c["anio"]]
        oldest = sorted([c for c in lst if c["anio"]], key=lambda c: c["anio"])
        if oldest and chosen_years and oldest[0]["anio"] <= min(chosen_years) - 2 and oldest[0]["isin"] not in ((r or {}).get("isin"), (l or {}).get("isin")):
            o = oldest[0]
            roles[o["isin"]].append("serie_larga")
            motivo[o["isin"]].append(f"serie más larga de la divisa (desde {o['anio']}, vs {min(chosen_years)} de la retail/limpia)")
    for c in cls:
        if c["analizada"]:
            roles[c["isin"]].insert(0, "analizada")
            motivo[c["isin"]].insert(0, "clase analizada por el fund-analyzer")
    out = []
    for c in cls:
        if roles[c["isin"]]:
            out.append({**c, "roles": roles[c["isin"]], "motivo": "; ".join(motivo[c["isin"]])})
    out.sort(key=lambda c: (ORDER_CCY.get(c["divisa"], 9), c["hedge"], min(ORDER_ROL.get(x, 9) for x in c["roles"])))
    return out


def etiqueta(c: dict) -> str:
    """Texto corto para identificar la clase en el fund-dashboard (className)."""
    rol = next((x for x in ("retail", "limpia", "serie_larga") if x in c["roles"]), "analizada")
    base = {"retail": "Retail", "limpia": "Limpia", "serie_larga": "Serie más larga", "analizada": "Analizada"}[rol]
    parts = [base, c["divisa"] + (" cubierta" if c["hedge"] else "")]
    if c["myinvestor"]:
        parts.append("MyInvestor")
    if c["mundo"]:
        parts.append("Mundo Asesoramiento")
    if "serie_larga" in c["roles"] and c.get("anio"):
        parts.append(f"desde {c['anio']}")
    return " · ".join(parts)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    for isin in sys.argv[1:]:
        for c in select(isin):
            print(f"{c['isin']} {c['divisa']:3}{'H' if c['hedge'] else ' '} fee={c['fee']} {c['anio']} {','.join(c['roles']):24} {etiqueta(c):45} | {c['nombre'][:40]} | {c['motivo']}")
