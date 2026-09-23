"""Listado de clases de un fondo (no-ES) desde el screener público de Morningstar.

Al buscar un ISIN en Morningstar sale el fondo y su tabla de clases (distintas
divisas, comisiones, año de inicio). Esta API pública (`lt.morningstar.com`, key
`klr5zyak8x`) NO está bloqueada (a diferencia de las páginas snapshot) y devuelve
JSON: 1) busca por ISIN → FundId; 2) filtra por FundId → todas las clases.

Listado COMPLETO de clases (incluidas las antiguas y de otras divisas): el filtro
por FundId agrupa TODAS las clases del fondo aunque sean de distinta divisa/año.
OJO (bug ago-2026): los dos puntos del filtro (`FundId:IN:`) deben ir URL-encoded
(`%3A`); con `:` crudos el endpoint CUELGA (timeout) y el código caía a 1 sola
clase — por eso a un fondo recién lanzado (clase nueva) le faltaba el track record
de sus clases antiguas. Ahora se conservan todas las clases de la divisa buscada y,
además, las de otras divisas que aporten MÁS track record (inicio anterior), para
que años_antiguedad y el histórico se anclen en la clase más veterana.

Devuelve lista compatible con `reconcile_fund_groups.populate_fund_classes`:
  [{isin, nombre_clase, divisa, comision_gestion_pct, ter_pct, fecha_inicio}]

Uso:
    python -m tools.morningstar_classes LU0168736675       # imprime clases
    python -m tools.morningstar_classes --all              # DRY-RUN no-ES analizados
    python -m tools.morningstar_classes --all --apply      # inserta en Supabase
"""
from __future__ import annotations

import re
import sys

import httpx

_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_BASE = "https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener"
_DP = "SecId|Name|Isin|FundId|CurrencyId|OngoingCharge|ManagementFee|InceptionDate|BrandingCompanyName|CategoryName"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
# Cap anti-ruido SOLO para fondos con muchísimas clases: por encima de esto se poda
# (divisa objetivo + la más antigua de cada otra divisa). Por debajo se conservan todas.
_HARD_CAP = 20


def _screener(params: str) -> dict:
    dp = _DP.replace("|", "%7C")
    url = (f"{_BASE}?page=1&pageSize=80&outputType=json&version=1"
           f"&universeIds=FOALL%24%24ALL&securityDataPoints={dp}&{params}")
    import time
    last = None
    for attempt in range(3):   # reintentos: el screener es flaky (1ª llamada lenta)
        try:
            r = httpx.get(url, headers=_UA, timeout=25, follow_redirects=True)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(1.0 * (attempt + 1))
    raise last


def _divisa(cur: str) -> str:
    return (cur or "")[-3:].upper()


def _row(row: dict) -> dict:
    return {
        "isin": (row.get("Isin") or "").upper(),
        "nombre_clase": row.get("Name") or "",
        "divisa": _divisa(row.get("CurrencyId")),
        "comision_gestion_pct": row.get("ManagementFee"),
        "ter_pct": row.get("OngoingCharge"),
        "fecha_inicio": (str(row.get("InceptionDate"))[:10] or None) if row.get("InceptionDate") else None,
        "gestora": row.get("BrandingCompanyName") or "",          # alta de fondos nuevos (import_portal_isins)
        "categoria_morningstar": row.get("CategoryName") or "",
    }


def _retener(clases: list[dict], isin: str) -> list[dict]:
    """Conserva el listado completo salvo fondos con clases masivas (>_HARD_CAP):
    ahí se queda con la divisa del ISIN buscado + la clase más antigua de cada otra
    divisa (para no perder track record ni cualitativo de las veteranas)."""
    if len(clases) <= _HARD_CAP:
        return clases
    tgt = next((c for c in clases if c["isin"] == isin), None)
    tgt_cur = (tgt or {}).get("divisa") or ""
    keep, oldest_by_cur = [], {}
    for c in clases:
        cur = c.get("divisa") or ""
        if cur == tgt_cur or c["isin"] == isin:
            keep.append(c)
            continue
        f = c.get("fecha_inicio") or "9999"
        if cur not in oldest_by_cur or f < (oldest_by_cur[cur].get("fecha_inicio") or "9999"):
            oldest_by_cur[cur] = c
    keep.extend(oldest_by_cur.values())
    seen, out = set(), []
    for c in keep:
        if c["isin"] not in seen:
            seen.add(c["isin"]); out.append(c)
    return out


def fetch_classes(isin: str) -> list[dict]:
    """Clases del fondo al que pertenece `isin` (listado completo). [] si no se encuentra."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return []
    try:
        hit = _screener(f"term={isin}")
    except Exception:
        return []
    rows = hit.get("rows") or []
    if not rows:
        return []
    fundid = rows[0].get("FundId")
    if not fundid:
        return [_row(rows[0])]
    # COLONS URL-ENCODED (%3A): con `:` crudos el endpoint cuelga (timeout) y solo
    # devolvía la clase buscada. Encoded devuelve TODAS las clases del FundId.
    try:
        allcl = _screener(f"filters=FundId%3AIN%3A{fundid}").get("rows") or []
    except Exception:
        allcl = rows
    if not allcl:
        allcl = rows
    out = [_row(r) for r in allcl if _ISIN.match((r.get("Isin") or "").upper())]
    # dedup por ISIN
    seen, dedup = set(), []
    for c in out:
        if c["isin"] not in seen:
            seen.add(c["isin"])
            dedup.append(c)
    # garantiza que el ISIN buscado esté presente aunque el filtro fallara
    if isin not in seen and rows:
        dedup.append(_row(rows[0]))
    return _retener(dedup, isin)


def _non_es_analyzed():
    """ISINs no-ES analizados localmente (con output.json)."""
    import glob
    import os
    out = []
    for f in glob.glob(os.path.join("data", "funds", "*", "output.json")):
        d = os.path.basename(os.path.dirname(f))
        if "." in d or not _ISIN.match(d.upper()) or d.upper().startswith("ES"):
            continue
        out.append(d.upper())
    return sorted(set(out))


def main():
    args = sys.argv[1:]
    if args and _ISIN.match(args[0].upper()):
        for c in fetch_classes(args[0]):
            print(c)
        return
    apply = "--apply" in args
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    from tools.reconcile_fund_groups import populate_fund_classes
    client = get_client()
    funds = _non_es_analyzed()
    print(f"{'APLICAR' if apply else 'DRY-RUN'} — {len(funds)} fondos no-ES analizados\n")
    import time
    total = 0
    for isin in funds:
        try:
            clases = fetch_classes(isin)
        except Exception as e:
            print(f"{isin}: ERROR {str(e)[:80]}")
            continue
        time.sleep(0.6)   # evita rate-limit del screener (flaky bajo ráfaga)
        if len(clases) <= 1:
            print(f"{isin}: {len(clases)} clase(s) — sin hermanas o no encontrado")
            continue
        print(f"{isin}: {len(clases)} clases")
        for c in clases:
            print(f"    {c['isin']}  {c['divisa']}  ter={c['ter_pct']}  gest={c['comision_gestion_pct']}  "
                  f"inc={c['fecha_inicio']}  {c['nombre_clase']}")
        total += populate_fund_classes(client, isin, clases, apply=apply)
        print()
    print(f"{'APLICADO' if apply else 'DRY-RUN'}: {total} clases {'insertadas' if apply else 'a insertar'}.")


if __name__ == "__main__":
    main()
