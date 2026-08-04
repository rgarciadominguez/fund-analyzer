"""
dep_autocure.py — Auto-diagnóstico/auto-cura de endpoints de Morningstar rotos.

Cuando el canario de la serie diaria falla, este módulo hace lo que resolvió el incidente
de 2026-03→08 A MANO: probar la MATRIZ de hosts × keys (siguiendo redirects) contra un
activo-testigo, y decir cuál devuelve datos válidos. Si el endpoint configurado está roto
pero otro funciona, PROPONE el cambio de URL (y opcionalmente lo aplica con guard) y AVISA.

"Mejor vacío que erróneo": nunca inventa; solo reporta combos que devuelven ≥30 puntos reales.

CLI:
    python -m tools.dep_autocure                 # diagnóstico (no toca nada)
    python -m tools.dep_autocure --apply         # aplica el fix al _URL si hay uno claro
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
PROPOSAL = ROOT / "data" / "dep_autocure_proposal.json"

TROJAN = "IE00B6T42S66"
_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Matriz conocida de la serie temporal de Morningstar (host × key).
_HOSTS = ["lt.morningstar.com", "tools.morningstar.es", "global.morningstar.com"]
_KEYS = ["klr5zyak8x", "2nhcdckzon"]
_PATH = "/api/rest.svc/timeseries_price/{key}"
_PARAMS = "?id={isin}&idtype=Isin&frequency=daily&startDate=1900-01-01&outputType=compactJSON"


def _try(host: str, key: str, isin: str) -> dict:
    url = f"https://{host}{_PATH.format(key=key)}{_PARAMS.format(isin=isin)}"
    r = {"host": host, "key": key, "url": url, "puntos": 0, "ok": False, "nota": ""}
    try:
        resp = httpx.get(url, headers=_UA, timeout=25, follow_redirects=True)
        r["status"] = resp.status_code
        if str(resp.url) != url and "timeseries_price" not in str(resp.url):
            r["nota"] = f"redirect→{str(resp.url)[:50]}"
            return r
        body = (resp.text or "").strip()
        if not body or body[:1] == "<":
            r["nota"] = "vacío/HTML"
            return r
        data = resp.json()
        pts = data if isinstance(data, list) else \
            data.get("TimeSeries", {}).get("Security", [{}])[0].get("HistoryDetail", [])
        r["puntos"] = len(pts)
        r["ok"] = len(pts) >= 30
        if not r["ok"]:
            r["nota"] = "pocos puntos"
    except Exception as e:
        r["nota"] = f"{type(e).__name__}: {str(e)[:50]}"
    return r


def diagnose(isin: str = TROJAN) -> dict:
    resultados = [_try(h, k, isin) for h in _HOSTS for k in _KEYS]
    buenos = [r for r in resultados if r["ok"]]
    # ¿qué usa hoy morningstar_daily?
    actual = _current_config()
    actual_ok = next((r["ok"] for r in resultados
                      if r["host"] in (actual.get("host") or "") and r["key"] == actual.get("key")), None)
    prop = None
    if buenos and actual_ok is False:
        b = max(buenos, key=lambda r: r["puntos"])
        prop = {"host": b["host"], "key": b["key"], "puntos": b["puntos"]}
    out = {
        "generado": datetime.now(timezone.utc).isoformat(),
        "isin": isin,
        "config_actual": actual,
        "config_actual_funciona": actual_ok,
        "combos_que_funcionan": [{"host": r["host"], "key": r["key"], "puntos": r["puntos"]} for r in buenos],
        "propuesta": prop,
        "matriz": resultados,
    }
    PROPOSAL.parent.mkdir(parents=True, exist_ok=True)
    PROPOSAL.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def _current_config() -> dict:
    """Extrae host+key del _URL de morningstar_daily."""
    try:
        from tools.morningstar_daily import _URL
        m = re.search(r"https://([^/]+)/api/rest\.svc/timeseries_price/(\w+)", _URL)
        if m:
            return {"host": m.group(1), "key": m.group(2)}
    except Exception:
        pass
    return {}


def apply_fix(prop: dict) -> bool:
    """Aplica el host/key propuesto al _URL de morningstar_daily (con backup). Guard:
    solo si la propuesta trae >=30 puntos reales."""
    if not prop or prop.get("puntos", 0) < 30:
        print("[autocure] sin propuesta fiable (>=30 puntos) — no se aplica")
        return False
    f = ROOT / "tools" / "morningstar_daily.py"
    src = f.read_text(encoding="utf-8")
    new = re.sub(r"https://[^/]+/api/rest\.svc/timeseries_price/\w+",
                 f"https://{prop['host']}/api/rest.svc/timeseries_price/{prop['key']}", src)
    if new == src:
        print("[autocure] no se encontró la URL para sustituir")
        return False
    (ROOT / "tools" / "morningstar_daily.py.bak_autocure").write_text(src, encoding="utf-8")
    f.write_text(new, encoding="utf-8")
    print(f"[autocure] APLICADO: host={prop['host']} key={prop['key']} "
          f"({prop['puntos']} puntos). Backup en morningstar_daily.py.bak_autocure")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--isin", default=TROJAN)
    a = ap.parse_args()
    d = diagnose(a.isin)
    print(f"=== AUTO-CURA Morningstar serie (testigo {a.isin}) ===")
    print(f"config actual: {d['config_actual']} → funciona: {d['config_actual_funciona']}")
    for r in d["matriz"]:
        mark = "OK " if r["ok"] else "XX "
        print(f"  {mark}{r['host']:24s} {r['key']:12s} puntos={r['puntos']:5d} {r['nota']}")
    if d["propuesta"]:
        print(f"\n>>> PROPUESTA: cambiar a host={d['propuesta']['host']} key={d['propuesta']['key']} "
              f"({d['propuesta']['puntos']} puntos)")
        if a.apply:
            apply_fix(d["propuesta"])
        else:
            print("    (corre con --apply para aplicarlo, o hazlo a mano)")
    elif d["config_actual_funciona"]:
        print("\nConfig actual funciona — nada que curar.")
    else:
        print("\nNingún combo devuelve datos — Morningstar caído del todo o cambió el esquema. AVISAR.")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
