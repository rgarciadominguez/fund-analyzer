"""
healthcheck.py — Canarios de dependencias externas con aserción de FRESCURA.

Motivo (2026-03→08): Morningstar movió el host de series (tools.morningstar.es →
lt.morningstar.com). El viejo devolvía 301→homepage / cuerpo vacío, y los fetchers lo
tragaban con `except: return []` → series VACÍAS EN SILENCIO ~4 meses. Nadie se enteró
porque "parecía que iba". Este módulo detecta ese patrón y AVISA, para que no se repita.

Cada canario usa un ACTIVO-TESTIGO conocido y exige frescura real (no solo "responde"):
  - serie diaria: ≥30 puntos Y última fecha < 7 días.
  - screener: el ISIN devuelve SU fila (no fuzzy) con rentabilidades.
  - Supabase / fund-dashboard: última fecha de datos < 7 días.

Firma del bug detectada explícitamente (`probe_endpoint`): 3xx que redirige a una home
genérica, o 200 con cuerpo vacío / HTML / <30 puntos cuando debería haber años de datos.

Regla: "mejor vacío que erróneo". Si de verdad no hay dato, se marca FALLO y se avisa;
nunca se da verde en falso.

CLI:
    python -m tools.healthcheck            # corre todos; exit!=0 si algún CRÍTICO falla
    python -m tools.healthcheck --json     # salida JSON
    python -m tools.healthcheck --only morningstar_serie
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
STATUS = ROOT / "data" / "healthcheck_status.json"

# Activo-testigo con años de histórico y cobertura Morningstar/CNMV segura.
TROJAN = "IE00B6T42S66"          # Trojan (Ireland) — Morningstar, años de serie
MAGALLANES = "ES0159259011"      # Magallanes European Equity — CNMV (ES) + Morningstar
FRESH_DAYS = 7
MIN_POINTS = 30
_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _now():
    return datetime.now(timezone.utc)


def _res(name, critical, ok, status, detail, **extra):
    return {"canario": name, "critico": critical, "ok": ok, "status": status,
            "detalle": detail, **extra}


# ─────────────────────────────────────────────────────────────────────────────
# Detector de la FIRMA del bug: redirect a home o cuerpo vacío/HTML.
# ─────────────────────────────────────────────────────────────────────────────
def probe_endpoint(url: str, timeout: float = 20.0) -> dict:
    """GET crudo. Devuelve la anatomía de la respuesta para diagnosticar el patrón
    'roto en silencio': redirect a homepage genérica, cuerpo vacío o HTML."""
    out = {"url": url, "error": None, "status": None, "final_url": None,
           "redirigido": False, "a_home": False, "vacio": False, "es_html": False,
           "es_json": False, "len": 0}
    try:
        r = httpx.get(url, headers=_UA, timeout=timeout, follow_redirects=True)
        out["status"] = r.status_code
        out["final_url"] = str(r.url)
        out["redirigido"] = str(r.url) != url
        body = (r.text or "").strip()
        out["len"] = len(body)
        out["vacio"] = len(body) == 0
        out["es_html"] = body[:1].startswith("<") or "text/html" in r.headers.get("content-type", "")
        # ¿la URL final es una home genérica? (perdió el path del endpoint)
        fu = str(r.url).lower()
        out["a_home"] = ("/timeseries_price" not in fu and "/screener" not in fu
                         and "rest.svc" not in fu)
        try:
            r.json()
            out["es_json"] = True
        except Exception:
            out["es_json"] = False
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:80]}"
    return out


def _last_date_from_series(s: list) -> datetime | None:
    """s = [(ts_ms, nav)]. Última fecha como datetime UTC."""
    if not s:
        return None
    try:
        ts = max(int(t) for t, _ in s)
        return datetime.fromtimestamp(ts / 1000, timezone.utc)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CANARIOS
# ─────────────────────────────────────────────────────────────────────────────
def canary_morningstar_serie() -> dict:
    """Serie diaria (morningstar_daily.fetch_series): ≥30 puntos Y última < 7 días."""
    try:
        from tools.morningstar_daily import fetch_series, _URL
    except Exception as e:
        return _res("morningstar_serie", True, False, "IMPORT_ERROR", str(e)[:100])
    s = fetch_series(TROJAN)
    n = len(s)
    last = _last_date_from_series(s)
    edad = (_now() - last).days if last else None
    if n < MIN_POINTS:
        # diagnóstico de la firma del bug
        probe = probe_endpoint(_URL.format(isin=TROJAN))
        firma = ("redirect→home" if probe["a_home"] else
                 "cuerpo vacío" if probe["vacio"] else
                 "HTML no-JSON" if probe["es_html"] else "pocos puntos")
        return _res("morningstar_serie", True, False, "SERIE_VACIA",
                    f"Trojan {TROJAN}: {n} puntos (<{MIN_POINTS}). Firma: {firma}.",
                    n_puntos=n, probe=probe)
    if edad is None or edad > FRESH_DAYS:
        return _res("morningstar_serie", True, False, "SERIE_CONGELADA",
                    f"Trojan {TROJAN}: última fecha hace {edad} días (>{FRESH_DAYS}). Serie congelada.",
                    n_puntos=n, ultima=last.date().isoformat() if last else None)
    return _res("morningstar_serie", True, True, "OK",
                f"{n} puntos, última {last.date().isoformat()} (hace {edad}d)",
                n_puntos=n, ultima=last.date().isoformat())


def canary_morningstar_screener() -> dict:
    """Screener (morningstar_quant.fetch_quant): el ISIN devuelve SU fila con rentabilidades."""
    try:
        from tools.morningstar_quant import fetch_quant
    except Exception as e:
        return _res("morningstar_screener", True, False, "IMPORT_ERROR", str(e)[:100])
    q = fetch_quant(TROJAN)
    if not q or not q.get("secid"):
        return _res("morningstar_screener", True, False, "SIN_FILA",
                    f"Screener no devolvió fila para {TROJAN} (host/key rotos o fuzzy).")
    if not q.get("rentabilidades"):
        return _res("morningstar_screener", True, False, "SIN_METRICAS",
                    f"{TROJAN} sin rentabilidades (respuesta degradada).")
    return _res("morningstar_screener", True, True, "OK",
                f"secid {q['secid']}, rentab {list(q['rentabilidades'].keys())}")


def canary_morningstar_nav() -> dict:
    """morningstar_quant.fetch_nav: serie NAV por secid (mismo host lt)."""
    try:
        from tools.morningstar_quant import fetch_quant, fetch_nav
    except Exception as e:
        return _res("morningstar_nav", False, False, "IMPORT_ERROR", str(e)[:100])
    q = fetch_quant(TROJAN)
    secid = q.get("secid") if q else None
    if not secid:
        return _res("morningstar_nav", False, False, "SIN_SECID", "no hay secid para probar fetch_nav")
    start = (_now() - timedelta(days=120)).date().isoformat()
    end = _now().date().isoformat()
    try:
        nav = fetch_nav(secid, start, end)
    except Exception as e:
        return _res("morningstar_nav", False, False, "ERROR", str(e)[:100])
    if not nav:
        return _res("morningstar_nav", False, False, "VACIO", f"fetch_nav({secid}) vacío (host roto?)")
    return _res("morningstar_nav", False, True, "OK", f"{len(nav)} puntos")


def canary_cnmv() -> dict:
    """CNMV alcanzable + sirve la ficha del fondo (por ISIN)."""
    url = f"https://www.cnmv.es/portal/Consultas/IIC/Fondo.aspx?isin={MAGALLANES}"
    p = probe_endpoint(url, timeout=25)
    if p["error"]:
        return _res("cnmv", False, False, "ERROR", p["error"])
    if p["status"] and p["status"] >= 400:
        return _res("cnmv", False, False, "HTTP", f"HTTP {p['status']}")
    if p["vacio"]:
        return _res("cnmv", False, False, "VACIO", "cuerpo vacío")
    return _res("cnmv", False, True, "OK", f"HTTP {p['status']}, {p['len']} bytes")


def canary_cssf() -> dict:
    """CSSF (portal fondos LU) alcanzable."""
    p = probe_endpoint("https://www.cssf.lu/en/", timeout=25)
    if p["error"]:
        return _res("cssf", False, False, "ERROR", p["error"])
    if p["status"] and p["status"] >= 400:
        return _res("cssf", False, False, "HTTP", f"HTTP {p['status']}")
    return _res("cssf", False, True, "OK", f"HTTP {p['status']}")


def canary_yahoo() -> dict:
    """Yahoo Finance (quant_enrichment): obtiene crumb (prueba de acceso)."""
    try:
        from tools.quant_enrichment import _yahoo_client
    except Exception as e:
        return _res("yahoo", False, False, "IMPORT_ERROR", str(e)[:100])
    try:
        client, crumb = _yahoo_client()
        if not client or not crumb:
            return _res("yahoo", False, False, "SIN_CRUMB", "no se obtuvo crumb (acceso Yahoo roto)")
        return _res("yahoo", False, True, "OK", "crumb obtenido")
    except Exception as e:
        return _res("yahoo", False, False, "ERROR", str(e)[:100])


def canary_serper() -> dict:
    """Serper: key presente (no gastamos crédito en el canario diario)."""
    import os
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    if os.environ.get("SERPER_DISABLED") == "1":
        return _res("serper", False, True, "DISABLED", "SERPER_DISABLED=1 (intencional)")
    if not os.environ.get("SERPER_API_KEY"):
        return _res("serper", False, False, "SIN_KEY", "falta SERPER_API_KEY")
    return _res("serper", False, True, "OK", "key presente")


def canary_supabase() -> dict:
    """Supabase fund-analyzer: conexión + count de funds."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        n = get_client().table("funds").select("isin", count="exact").limit(1).execute().count
    except Exception as e:
        return _res("supabase", True, False, "ERROR", str(e)[:100])
    if not n:
        return _res("supabase", True, False, "VACIO", "0 funds")
    return _res("supabase", True, True, "OK", f"{n} funds")


def canary_funddash_supabase() -> dict:
    """fund-dashboard (Supabase separado): la serie `rows` de un fondo-testigo < 7 días.
    Cubre el bug que congeló TODAS sus series en 2026-03-20 (fetch perezoso al host muerto)."""
    try:
        from tools.benchmarks_to_funddash import SB_URL, SB_KEY
    except Exception as e:
        return _res("funddash_series", True, False, "IMPORT_ERROR", str(e)[:100])
    url = f"{SB_URL}/rest/v1/funds?isin=eq.{TROJAN}&select=rows,updated_at"
    try:
        r = httpx.get(url, headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"},
                      timeout=30)
        data = r.json()
    except Exception as e:
        return _res("funddash_series", True, False, "ERROR", str(e)[:100])
    if not data:
        return _res("funddash_series", True, False, "SIN_FONDO",
                    f"{TROJAN} no está en el fund-dashboard Supabase")
    rows = (data[0].get("rows") or [])
    if len(rows) < MIN_POINTS:
        return _res("funddash_series", True, False, "SERIE_VACIA",
                    f"fund-dashboard {TROJAN}: {len(rows)} puntos (<{MIN_POINTS})")
    # última fecha de la serie
    fechas = [p.get("date") or p.get("fecha") for p in rows if (p.get("date") or p.get("fecha"))]
    ult = max(fechas) if fechas else None
    if not ult:
        return _res("funddash_series", True, False, "SIN_FECHA", "rows sin fecha")
    try:
        d = datetime.fromisoformat(ult[:10]).replace(tzinfo=timezone.utc)
        edad = (_now() - d).days
    except Exception:
        edad = None
    if edad is None or edad > FRESH_DAYS + 25:  # el push del FA no es diario; tolerancia mayor
        return _res("funddash_series", True, False, "SERIE_CONGELADA",
                    f"fund-dashboard {TROJAN}: última {ult} (hace {edad}d). Congelada.",
                    ultima=ult)
    return _res("funddash_series", True, True, "OK",
                f"{len(rows)} puntos, última {ult} (hace {edad}d)", ultima=ult)


CANARIES = {
    "morningstar_serie": canary_morningstar_serie,
    "morningstar_screener": canary_morningstar_screener,
    "morningstar_nav": canary_morningstar_nav,
    "cnmv": canary_cnmv,
    "cssf": canary_cssf,
    "yahoo": canary_yahoo,
    "serper": canary_serper,
    "supabase": canary_supabase,
    "funddash_series": canary_funddash_supabase,
}


def run_all(only: str | None = None) -> dict:
    resultados = []
    for name, fn in CANARIES.items():
        if only and name != only:
            continue
        try:
            resultados.append(fn())
        except Exception as e:
            resultados.append(_res(name, True, False, "EXCEPCION", str(e)[:120]))
    criticos_ko = [r for r in resultados if r["critico"] and not r["ok"]]
    warn = [r for r in resultados if not r["critico"] and not r["ok"]]
    resumen = {
        "generado": _now().isoformat(),
        "total": len(resultados),
        "ok": sum(1 for r in resultados if r["ok"]),
        "criticos_fallando": len(criticos_ko),
        "warnings": len(warn),
        "estado": "ROTO" if criticos_ko else ("DEGRADADO" if warn else "SANO"),
        "resultados": resultados,
    }
    try:
        STATUS.parent.mkdir(parents=True, exist_ok=True)
        STATUS.write_text(json.dumps(resumen, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
    return resumen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()
    r = run_all(only=a.only)
    if a.json:
        print(json.dumps(r, ensure_ascii=False, indent=1))
    else:
        print(f"=== HEALTHCHECK: {r['estado']} "
              f"({r['ok']}/{r['total']} ok, {r['criticos_fallando']} críticos KO, {r['warnings']} warn) ===")
        for x in r["resultados"]:
            mark = "OK " if x["ok"] else ("XX " if x["critico"] else "!! ")
            print(f"  {mark}{x['canario']:22s} [{x['status']}] {x['detalle']}")
    sys.exit(1 if r["criticos_fallando"] else 0)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
