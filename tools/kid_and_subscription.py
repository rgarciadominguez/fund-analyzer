"""
kid_and_subscription.py — Consolida `kid` + `comision_suscripcion` desde MyInvestor.

ENTRADA: data/.myinvestor_raw_*.json (cosechados vía el conector MCP de MyInvestor,
         que el pipeline Python NO puede tocar — solo `claude -p` / subagentes).

`kid`:
    URL pública del KID (fundinfo, vía MyInvestor). REGLA DE HORIZONTE: "preferimos null
    a un enlace que no abre". Por eso NINGUNA url se escribe sin verificar antes que
    devuelve HTTP 200 y que el cuerpo empieza por %PDF. Las que fallen -> null.
    Aviso conocido: estas URL llevan un apiKey de fundinfo embebido; si rota, caen.
    Por eso `--verify-existing` re-chequea las ya guardadas (va en el health check diario).

`comision_suscripcion` (boolean):
    Semántica acordada: "¿te la cobra MyInvestor?", NO "¿el folleto la contempla?".
    Morningstar (MaxFrontEndLoad) da el máximo del folleto y miente para esto: marca 3%
    en fondos que en MyInvestor se compran a coste 0. Por eso la fuente es MyInvestor.
    pct > 0 -> True | pct == 0 -> False | sin dato -> null (nunca False por defecto).

CLI:
    python -m tools.kid_and_subscription --consume [--apply]
    python -m tools.kid_and_subscription --verify-existing
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

REPORT = ROOT / "data" / "kid_report.json"
_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def verify_url(url: str, timeout: float = 30.0) -> bool:
    """True solo si la URL devuelve 200 y el cuerpo es un PDF real."""
    if not url or not url.startswith(("http://", "https://")):
        return False
    try:
        r = httpx.get(url, headers=_UA, timeout=timeout, follow_redirects=True)
        return r.status_code == 200 and r.content[:4] == b"%PDF"
    except Exception:
        return False


def _merge_raw() -> tuple[dict, list]:
    found, missing = {}, []
    for p in sorted(ROOT.glob("data/.myinvestor_raw_*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        for f in d.get("encontrados", []):
            if f.get("isin"):
                found[f["isin"]] = f
        missing += d.get("no_encontrados", [])
    return found, missing


def consume(apply: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client

    found, missing = _merge_raw()
    print(f"MyInvestor: {len(found)} encontrados | {len(missing)} no están en su catálogo")

    cands = {i: f["kid"] for i, f in found.items() if f.get("kid")}
    print(f"con url_kiid: {len(cands)} -> verificando que abren (HTTP 200 + %PDF)...")
    with ThreadPoolExecutor(max_workers=12) as ex:
        oks = list(ex.map(lambda kv: (kv[0], verify_url(kv[1])), cands.items()))
    valid = {i: cands[i] for i, ok in oks if ok}
    broken = [i for i, ok in oks if not ok]
    print(f"  verificadas OK: {len(valid)} | caídas (-> null): {len(broken)}")

    c = get_client()
    catalog = {r["isin"] for r in c.table("funds").select("isin").execute().data}

    plan = []
    n_susc = {"si": 0, "no": 0, "null": 0}
    for isin, f in found.items():
        if isin not in catalog:
            continue  # de los 78: se aplicará al analizarlos
        upd = {}
        if isin in valid:
            upd["kid"] = valid[isin]
        pct = f.get("comision_suscripcion_pct")
        if pct is None:
            n_susc["null"] += 1
        else:
            b = float(pct) > 0
            upd["comision_suscripcion"] = b
            n_susc["si" if b else "no"] += 1
        if upd:
            plan.append({"isin": isin, "update": upd})

    rep = {
        "myinvestor_encontrados": len(found),
        "myinvestor_no_encontrados": len(missing),
        "kid_candidatos": len(cands),
        "kid_verificados_ok": len(valid),
        "kid_caidos": broken,
        "comision_suscripcion": n_susc,
        "filas_a_tocar": len(plan),
        "no_en_catalogo_aun": sorted(set(found) - catalog),
    }
    REPORT.write_text(json.dumps({**rep, "plan": plan}, ensure_ascii=False, indent=1),
                      encoding="utf-8")
    print(json.dumps(rep, ensure_ascii=False, indent=1)[:700])

    if apply:
        for p in plan:
            c.table("funds").update(p["update"]).eq("isin", p["isin"]).execute()
        print(f"escritas {len(plan)} filas")
    else:
        print(f"(dry-run) filas a tocar: {len(plan)}")
    return rep


def verify_existing() -> None:
    """Re-chequea los kid ya guardados. El apiKey de fundinfo puede rotar."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    rows = [r for r in c.table("funds").select("isin,kid").execute().data if r.get("kid")]
    print(f"kid guardados: {len(rows)}")
    with ThreadPoolExecutor(max_workers=12) as ex:
        res = list(ex.map(lambda r: (r["isin"], verify_url(r["kid"])), rows))
    bad = [i for i, ok in res if not ok]
    print(f"OK: {len(res)-len(bad)} | caídos: {len(bad)}")
    for i in bad:
        print(f"  CAÍDO {i}")
    return bad


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--consume", action="store_true")
    ap.add_argument("--verify-existing", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.verify_existing:
        verify_existing()
    else:
        consume(apply=a.apply)
