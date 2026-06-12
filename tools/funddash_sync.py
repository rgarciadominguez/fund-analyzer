"""funddash_sync.py — PUENTE fund-analyzer → fund-dashboard.

Empuja un fondo analizado en fund-analyzer a la tabla `funds` del fund-dashboard
(Supabase dcnvdaaexyuhqvyrrkob) con su CATEGORIZACIÓN (meta). Las `rows` (serie de
precios) se dejan VACÍAS a propósito: el fund-dashboard las baja de Morningstar por
ISIN y las guarda (Opción A acordada 2026-06-12).

Meta del fund-dashboard: {isin, name, className, category, assetType, geography,
currency, issuer}. Se DERIVA de output.json y se puede SOBREESCRIBIR por fondo con
data/funds/{ISIN}/funddash_meta.json (lo que pongas ahí manda).

CLI:
  python -m tools.funddash_sync --isin ES0112231008
  python -m tools.funddash_sync --all            # todos los output.json
  python -m tools.funddash_sync --isin X --dry-run
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from tools.benchmarks_to_funddash import SB_URL, SB_KEY, _post, _repo_isins  # reutiliza config+push

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"

# Taxonomía del fund-dashboard (DP_META_OPTS)
_GEO = {"estados unidos": "USA", "usa": "USA", "ee": "USA",
        "reino unido": "Europa", "francia": "Europa", "alemania": "Europa", "españa": "España",
        "italia": "Europa", "suiza": "Europa", "europa": "Europa", "zona euro": "Europa",
        "japón": "Asia", "japon": "Asia", "china": "Asia", "asia": "Asia", "pacífico": "Asia",
        "india": "Emergentes", "emergentes": "Emergentes", "brasil": "Latam", "méxico": "Latam",
        "global": "Global"}


def _bond_share(pos: list) -> float:
    if not pos:
        return 0.0
    import re
    rf = 0
    for p in pos:
        t = str(p.get("tipo", "")).upper()
        if any(k in t for k in ("RF", "FIJA", "BOND", "REPO")) or re.search(
                r"bond|bill|treasury|gilt|\bnote|\d{4}-\d{2}-\d{2}", str(p.get("nombre", "")), re.I):
            rf += 1
    return rf / len(pos)


def _derive_geography(j: dict) -> str:
    geo = j.get("geographic_allocation") or []
    regs = [str(g.get("region", "")).lower() for g in geo if g.get("peso_pct")]
    if not regs:
        return ""
    # si está repartido en >=4 regiones de >1 continente → Global; si no, la dominante
    if len(regs) >= 5:
        return "Global"
    top = regs[0]
    for k, v in _GEO.items():
        if k in top:
            return v
    return "Global"


def build_meta(isin: str) -> dict | None:
    op = FUNDS / isin / "output.json"
    if not op.exists():
        return None
    j = json.loads(op.read_text(encoding="utf-8"))
    pos = (j.get("posiciones", {}) or {}).get("actuales", []) or []
    rf = _bond_share(pos)
    asset = "RF LP" if rf > 0.7 else ("Mixto" if rf > 0.3 else "RV")
    name = j.get("nombre", isin)
    nlow = name.lower()
    if any(k in nlow for k in ("índice", "indice", "index", "indexado")):
        category = "Indexado"
    else:
        category = "Activo"
    cur = (j.get("kpis", {}) or {}).get("divisa") or "Euro"
    cur = {"EUR": "Euro", "USD": "USD", "GBP": "GBP"}.get(str(cur).upper(), cur or "Euro")
    meta = {
        "isin": isin,
        "name": name,
        "className": "",
        "category": category,
        "assetType": asset,
        "geography": _derive_geography(j),
        "currency": cur,
        "issuer": "Gubernamental" if (rf > 0.3 and "gobierno" in nlow) else "",
    }
    return meta  # solo DERIVADO (guess). El override del usuario se aplica aparte.


def _override(isin: str) -> dict:
    """Categorización EXPLÍCITA del usuario (data/funds/{ISIN}/funddash_meta.json).
    Lo que pongas aquí SIEMPRE manda (sobre el derivado y sobre lo que haya en el tool)."""
    ov = FUNDS / isin / "funddash_meta.json"
    if ov.exists():
        try:
            return {k: v for k, v in json.loads(ov.read_text(encoding="utf-8")).items() if v}
        except Exception:
            return {}
    return {}


def _get_meta(isin: str) -> dict:
    """Meta actual del fondo en el fund-dashboard (para no pisarla)."""
    import urllib.request
    req = urllib.request.Request(SB_URL + "/rest/v1/funds?isin=eq." + isin + "&select=meta",
                                 headers={"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=30))
        return (d[0].get("meta") or {}) if d else {}
    except Exception:
        return {}


def _patch_meta(isin: str, meta: dict) -> int:
    """Actualiza SOLO la columna meta (preserva rows/serie existente)."""
    import urllib.request
    req = urllib.request.Request(
        SB_URL + "/rest/v1/funds?isin=eq." + isin,
        data=json.dumps({"meta": meta, "updated_at": "2026-06-12T00:00:00Z"}).encode("utf-8"),
        headers={"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY,
                 "Content-Type": "application/json", "Prefer": "return=minimal"},
        method="PATCH")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def sync(isin: str, repo: set, dry: bool = False) -> bool:
    derived = build_meta(isin)
    if not derived:
        print(f"  SKIP {isin}: sin output.json")
        return False
    override = _override(isin)
    existe = isin in repo
    if existe:
        cur = {} if dry else _get_meta(isin)
        meta = dict(cur)
        for k, v in derived.items():
            if v and not meta.get(k):     # ADITIVO: el derivado solo rellena campos vacíos
                meta[k] = v
        meta.update(override)             # tu categorización explícita SIEMPRE manda
        meta["isin"] = isin
        accion = "PATCH aditivo (preserva serie+lo tuyo)"
    else:
        meta = {**derived, **override}
        accion = "NUEVO (rows vacías → MST)"
    line = (f"  {isin:14} [{accion:38}] cat={meta.get('category'):8} tipo={meta.get('assetType'):6} "
            f"geo={str(meta.get('geography')):10} {str(meta.get('name'))[:28]}")
    if dry:
        print("[dry] " + line)
        return True
    try:
        if existe:
            _patch_meta(isin, meta)
        else:
            _post({"isin": isin, "meta": meta, "rows": [], "updated_at": "2026-06-12T00:00:00Z"})
        print("[OK]  " + line)
        return True
    except Exception as e:
        print(f"  ERR {isin}: {str(e)[:70]}")
        return False


def main(argv=None) -> int:
    args = argv or sys.argv[1:]
    dry = "--dry-run" in args
    if "--all" in args:
        isins = sorted(d.name for d in FUNDS.iterdir()
                       if "." not in d.name and (d / "output.json").exists())
    elif "--isin" in args:
        isins = [args[args.index("--isin") + 1].strip().upper()]
    else:
        print("uso: python -m tools.funddash_sync --isin X | --all [--dry-run]")
        return 1
    repo = _repo_isins()   # lectura; permite distinguir PATCH (existe) vs NUEVO incluso en dry-run
    ok = sum(sync(i, repo, dry) for i in isins)
    print(f"\n{'(dry) ' if dry else ''}sincronizados: {ok}/{len(isins)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
