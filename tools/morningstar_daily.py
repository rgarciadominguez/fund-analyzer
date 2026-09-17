"""Quant desde la serie diaria de NAV de Morningstar (por ISIN) — misma fuente que
el fund-dashboard. Calcula rentabilidades por año natural, CAGR, volatilidad,
max drawdown y rentab. por plazo, para tener UNA fuente cuantitativa consistente.

Uso: python -m tools.morningstar_daily <ISIN>
     python -m tools.morningstar_daily --all [--apply]   (vuelca a fund_groups.rendimiento_jsonb)
"""
from __future__ import annotations
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

import httpx

_UA = {"User-Agent": "Mozilla/5.0"}
# 2026-07-30: el host `tools.morningstar.es` (key 2nhcdckzon) dejó de servir la serie diaria
# (301 → homepage global). El endpoint timeseries_price se movió a `lt.morningstar.com`
# (key `klr5zyak8x`, la misma del screener/quant).
# 2026-09-01 (BUG CRÍTICO): pedir la serie por `idtype=Isin` es FUZZY — si el ISIN no está
# indexado, Morningstar devuelve el NAV de OTRO security (caso Carmignac LU1623762843 → CAGR/vol
# erróneos que no cuadraban con Morningstar/Finect). FIX ROBUSTO (como fund-dashboard/quant):
# resolver el SecId REAL vía screener con validación EXACTA de ISIN, y pedir la serie por SecId
# (idtype=Morningstar). Si el ISIN no matchea exacto → sin serie (mejor vacío que de otro fondo).
_SCR = "https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener"
_TS = "https://lt.morningstar.com/api/rest.svc/timeseries_price/klr5zyak8x"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def resolve_secid(isin: str) -> str | None:
    """ISIN → SecId de Morningstar con validación EXACTA (el screener `term=` es fuzzy y ante
    un ISIN no indexado devuelve el 'mejor match' = OTRO fondo). None si no hay match exacto."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return None
    from tools.http_retry import get_json
    dp = "SecId%7CName%7CIsin%7CFundId"
    for uni in ("FOALL%24%24ALL", "ETALL%24%24ALL", "CEALL%24%24ALL"):
        url = (f"{_SCR}?page=1&pageSize=10&outputType=json&version=1"
               f"&universeIds={uni}&securityDataPoints={dp}&term={isin}")
        try:
            rows = get_json(url, headers=_UA, timeout=20).get("rows") or []
        except Exception:
            rows = []
        for r in rows:
            if (r.get("Isin") or "").upper().strip() == isin:
                return r.get("SecId")
    return None


def fetch_series(isin: str) -> list:
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return []
    secid = resolve_secid(isin)
    if not secid:
        import sys as _sys
        print(f"[morningstar_daily] {isin}: SecId no resuelto (ISIN no indexado) → sin serie "
              f"(mejor vacío que de otro fondo)", file=_sys.stderr)
        return []
    end = datetime.now(timezone.utc).date().isoformat()
    url = (f"{_TS}?currencyId=EUR&idtype=Morningstar&frequency=daily"
           f"&id={secid}&startDate=1990-01-01&endDate={end}&outputType=COMPACTJSON")
    try:
        from tools.http_retry import get_json
        data = get_json(url, headers=_UA, timeout=25)
        return [(int(t), float(v)) for t, v in data if v]
    except Exception as _e:
        import sys as _sys
        print(f"[morningstar_daily] fetch_series({isin}) sin datos: {type(_e).__name__} "
              f"{str(_e)[:80]}", file=_sys.stderr)
        return []


# ─────────────────────────────────────────────────────────────────────────────────────────────
# SERIE DE UNA CLASE CON PREDECESOR (2026-09-17)
# BUG que motivó esto (MontLake IE000Z9YV312, clase EUR CUBIERTA): para "alargar" el track se usaba
# la serie de OTRA clase (FIEI, EUR sin cubrir) cuyo tramo previo a su lanzamiento es un relleno de
# Morningstar con los NAV en USD etiquetados como EUR, y cuyo tramo posterior lleva el riesgo
# EUR/USD que la clase cubierta NO tiene → 2025 salía −1,9% (real +8,13%) y un drawdown de −14%
# inexistente. Reglas, genéricas:
#   1. La serie PROPIA de una clase solo vale desde su fecha de lanzamiento (InceptionDate). Lo que
#      Morningstar trae antes es historia de otra clase/vehículo: nunca se atribuye a la clase.
#   2. El tramo PREDECESOR sale de la serie de referencia del linaje, en la divisa que corresponde:
#      clase CUBIERTA → divisa original de la estrategia (una clase cubierta replica el retorno en
#      divisa local menos el coste de cobertura); clase NO cubierta → convertida a la divisa de la clase.
#   3. Se empalma re-basando el predecesor al primer NAV real de la clase y se devuelve el corte,
#      para que dashboard y métricas puedan marcar qué parte es predecesor.
_HEDGE_RE = re.compile(r"(hedged|\bhdg\b|\bhgd\b|\(h\)|\bh[- ]?(eur|usd|chf|gbp|jpy|sek|nok|aud|cad)\b"
                       r"|\b(eur|usd|chf|gbp|jpy|sek|nok|aud|cad)[- ]?h\b"
                       r"|\bh (eur|usd|chf|gbp|jpy|sek|nok|aud|cad)\b)", re.I)


def is_hedged_class(name: str) -> bool:
    """¿La clase cubre divisa?, por su nombre ('… FIEHA H EUR Acc', 'EUR Hedged', 'Hdg')."""
    return bool(_HEDGE_RE.search(name or ""))


def resolve_security(isin: str) -> dict | None:
    """ISIN → {secid, name, currency, inception} con validación EXACTA de ISIN (ver resolve_secid)."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return None
    from tools.http_retry import get_json
    dp = "SecId%7CName%7CIsin%7CPriceCurrency%7CInceptionDate"
    for uni in ("FOALL%24%24ALL", "ETALL%24%24ALL", "CEALL%24%24ALL"):
        url = (f"{_SCR}?page=1&pageSize=10&outputType=json&version=1"
               f"&universeIds={uni}&securityDataPoints={dp}&term={isin}")
        try:
            rows = get_json(url, headers=_UA, timeout=20).get("rows") or []
        except Exception:
            rows = []
        for r in rows:
            if (r.get("Isin") or "").upper().strip() == isin and r.get("SecId"):
                return {"isin": isin, "secid": r["SecId"], "name": r.get("Name") or "",
                        "currency": (r.get("PriceCurrency") or "").upper() or None,
                        "inception": (r.get("InceptionDate") or "")[:10] or None}
    return None


def fetch_series_secid(secid: str, currency: str = "EUR") -> list:
    """Serie diaria por SecId en la divisa pedida (Morningstar convierte con `currencyId`)."""
    end = datetime.now(timezone.utc).date().isoformat()
    url = (f"{_TS}?currencyId={currency or 'EUR'}&idtype=Morningstar&frequency=daily"
           f"&id={secid}&startDate=1990-01-01&endDate={end}&outputType=COMPACTJSON")
    try:
        from tools.http_retry import get_json
        return [(int(t), float(v)) for t, v in get_json(url, headers=_UA, timeout=25) if v]
    except Exception:
        return []


def _ts_of(date_str: str) -> int | None:
    try:
        return int(datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        return None


def build_class_series(isin: str, pred_isin: str | None = None) -> dict:
    """Serie de la clase `isin` = tramo PREDECESOR (si hay linaje) + serie PROPIA desde su lanzamiento.
    Devuelve {points, own_start_ts, currency, hedged, pred:{isin,currency,from_ts,to_ts}|None}.
    `pred_isin`: clase de referencia del linaje (por defecto, la del registro de lineage_kb)."""
    out = {"isin": (isin or "").upper(), "points": [], "own_start_ts": None, "currency": None,
           "hedged": False, "pred": None}
    sec = resolve_security(isin)
    if not sec:
        return out
    cur = sec["currency"] or "EUR"
    out["currency"], out["hedged"] = cur, is_hedged_class(sec["name"])
    own = sorted(fetch_series_secid(sec["secid"], cur))
    inc = _ts_of(sec["inception"] or "")
    if inc:                                   # regla 1: lo anterior al lanzamiento no es de la clase
        own = [p for p in own if p[0] >= inc]
    if not own:
        return out
    out["own_start_ts"] = own[0][0]
    if pred_isin is None:
        try:
            from tools.lineage_kb import get_record
            tr = ((get_record(isin) or {}).get("track_record") or {})
            pred_isin = tr.get("pred_series_isin") or tr.get("quant_series_isin_usd") or tr.get("quant_series_isin")
        except Exception:
            pred_isin = None
    pts = own
    if pred_isin and pred_isin.upper() != out["isin"]:
        psec = resolve_security(pred_isin)
        if psec:
            pcur = (psec["currency"] or cur) if out["hedged"] else cur      # regla 2
            pser = [p for p in sorted(fetch_series_secid(psec["secid"], pcur)) if p[0] < own[0][0]]
            if len(pser) >= _MIN_PRED_POINTS:
                k = own[0][1] / pser[-1][1]                                  # regla 3: empalme
                pts = [(t, v * k) for t, v in pser] + own
                out["pred"] = {"isin": psec["isin"], "secid": psec["secid"], "currency": pcur,
                               "from_ts": pser[0][0], "to_ts": own[0][0]}
    out["points"] = pts
    return out


_MIN_PRED_POINTS = 20


def _year(ts):
    return datetime.fromtimestamp(ts / 1000, timezone.utc).year


def _ym(ts):
    d = datetime.fromtimestamp(ts / 1000, timezone.utc)
    return d.year, d.month


def monthly_returns_by_ym(s: list) -> dict:
    """Retornos mensuales {(año, mes): retorno} desde el último NAV de cada mes.
    Clave usada para alinear un fondo con el rf mes a mes (para el Sharpe)."""
    s = sorted(s)
    month_last = {}
    for ts, v in s:
        month_last[_ym(ts)] = v
    keys = sorted(month_last)
    out = {}
    for i in range(1, len(keys)):
        prev = month_last[keys[i - 1]]
        if prev:
            out[keys[i]] = month_last[keys[i]] / prev - 1
    return out


def compute_metrics(isin: str, rf_monthly: dict | None = None) -> dict:
    """Métricas desde la serie diaria. Si `rf_monthly` ({(año,mes): ret} de un
    monetario), añade sharpe_Na por exceso sobre ese rf en la ventana de cada plazo.

    LINEAGE (§0.9, track-record = serie NAV real más larga): si el fondo tiene un registro en
    data/fund_lineage.json con una clase cuya serie cubre MÁS histórico (incluye vehículo predecesor
    ya empalmado por Morningstar, p.ej. MontLake desde 2021 vs UCITS 2024), se usa esa serie y se
    marca `_lineage` con la etiqueta/caveat para que el dashboard avise."""
    lineage_note = None
    s = []
    try:
        from tools.lineage_kb import get_record
        rec = get_record(isin) or {}
        if rec.get("track_record"):
            # Serie PROPIA de la clase + tramo predecesor en la divisa correcta (build_class_series).
            cs = build_class_series(isin)
            if cs["points"] and cs.get("pred"):
                s = cs["points"]
                p = cs["pred"]
                lineage_note = {
                    "serie_de_clase": p["isin"],
                    "divisa_predecesor": p["currency"],
                    "desde": datetime.fromtimestamp(p["from_ts"] / 1000, timezone.utc).date().isoformat(),
                    "clase_propia_desde": datetime.fromtimestamp(p["to_ts"] / 1000, timezone.utc).date().isoformat(),
                    "nota": (f"Hasta {datetime.fromtimestamp(p['to_ts'] / 1000, timezone.utc).date().isoformat()}"
                             f" la serie es la del vehículo/clase predecesor ({p['isin']}, en {p['currency']});"
                             f" desde entonces, la serie real de esta clase."),
                    "caveat": rec.get("caveat_global"),
                }
    except Exception:
        pass
    if not s:
        s = fetch_series(isin)
    m = metrics_from_series(s, rf_monthly=rf_monthly)
    if lineage_note and m:
        m["_lineage"] = lineage_note
    return m


def metrics_from_series(s: list, rf_monthly: dict | None = None) -> dict:
    if len(s) < 30:
        return {}
    s = sorted(s)
    # Rentabilidad por año natural (primer vs último NAV del año)
    byyear = defaultdict(list)
    for ts, v in s:
        byyear[_year(ts)].append((ts, v))
    anuales = {}
    for y, pts in byyear.items():
        pts.sort()
        if len(pts) >= 2 and pts[0][1]:
            anuales[str(y)] = round((pts[-1][1] / pts[0][1] - 1) * 100, 2)
    # Volatilidad anualizada desde retornos MENSUALES (metodología Morningstar/Finect)
    mbym = monthly_returns_by_ym(s)
    ym_keys = sorted(mbym)
    mret = [mbym[k] for k in ym_keys]

    def _vol(rets):
        if len(rets) < 6:
            return None
        m = sum(rets) / len(rets)
        var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
        return round(math.sqrt(var) * math.sqrt(12) * 100, 2)

    vol = _vol(mret)
    vol_3y = _vol(mret[-36:])
    vol_5y = _vol(mret[-60:])
    # CAGR desde inicio
    t0, v0 = s[0]; t1, v1 = s[-1]
    años = (t1 - t0) / (1000 * 86400 * 365.25)
    cagr = round(((v1 / v0) ** (1 / años) - 1) * 100, 2) if años > 0.5 and v0 else None
    # Max drawdown (diario) + underwater (días por debajo del máximo previo)
    peak = -1e9; mdd = 0.0; uw_dias = 0; uw_racha = 0; uw_max = 0
    for _, v in s:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, v / peak - 1)
        if v < peak - 1e-12:
            uw_dias += 1; uw_racha += 1; uw_max = max(uw_max, uw_racha)
        else:
            uw_racha = 0
    underwater = {"dias_bajo_agua": uw_dias, "racha_max_bajo_agua_dias": uw_max,
                  "pct_tiempo_bajo_agua": round(100 * uw_dias / len(s), 1) if s else None}
    # Rentab. anualizada por plazo. Solo si la serie CUBRE la ventana: si el fondo
    # empezó hace 7 años, rentab_10a debe ser None, no anualizar 7 años como 10
    # (infravaloraba el dato y descuadraba la validación del portal, 2026-07-20).
    tol = int(35 * 86400 * 1000)   # 35 días de holgura (arranque/festivos)

    def _ret_period(years):
        cut = t1 - int(years * 365.25 * 86400 * 1000)
        if t0 > cut + tol:                       # no hay histórico suficiente
            return None
        base = next((v for ts, v in s if ts >= cut), None)
        if base and base > 0:
            return round(((v1 / base) ** (1 / years) - 1) * 100, 2)
        return None

    # Sharpe por plazo = exceso sobre el rf (monetario EUR) en la MISMA ventana.
    # Se alinea mes a mes con el rf: excess = ret_fondo - ret_rf de los meses comunes
    # de la ventana; sharpe = media(excess)/desv(excess)·√12. None si faltan datos del
    # fondo O del rf en esa ventana (2026-07-20, rf = FR0000989626 por defecto).
    def _sharpe(years):
        if not rf_monthly or t0 > _ret_cut(years) + tol:
            return None
        n = years * 12
        win = ym_keys[-n:] if len(ym_keys) >= 6 else ym_keys
        excess = [mbym[k] - rf_monthly[k] for k in win if k in rf_monthly]
        if len(excess) < 6:
            return None
        m = sum(excess) / len(excess)
        var = sum((x - m) ** 2 for x in excess) / (len(excess) - 1)
        sd = math.sqrt(var)
        if sd == 0:
            return None
        return round((m / sd) * math.sqrt(12), 2)

    def _ret_cut(years):
        return t1 - int(years * 365.25 * 86400 * 1000)

    out = {
        "_fuente": "morningstar_daily",
        "n_puntos": len(s),
        "rentabilidades_anuales": dict(sorted(anuales.items())),
        "cagr_desde_inicio": cagr,
        "volatilidad": vol, "volatilidad_3a": vol_3y, "volatilidad_5a": vol_5y,
        "max_drawdown": round(mdd * 100, 2),
        "underwater": underwater,
        "peor_anio": round(min(anuales.values()), 2) if anuales else None,
        "mejor_anio": round(max(anuales.values()), 2) if anuales else None,
        "rentab_1a": _ret_period(1), "rentab_3a": _ret_period(3),
        "rentab_5a": _ret_period(5), "rentab_10a": _ret_period(10),
    }
    if rf_monthly:
        out["sharpe_1a"] = _sharpe(1)
        out["sharpe_3a"] = _sharpe(3)
        out["sharpe_5a"] = _sharpe(5)
        out["sharpe_10a"] = _sharpe(10)
    return out


def main():
    args = sys.argv[1:]
    if args and _ISIN.match(args[0].upper()):
        import json
        print(json.dumps(compute_metrics(args[0]), indent=2, ensure_ascii=False))
        return
    apply = "--apply" in args
    import time
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    funds = c.table("funds").select("isin,fund_group_id,has_qualitative_analysis").limit(5000).execute().data
    # primario por grupo = la clase ANALIZADA (has_qualitative_analysis); si ninguna, la 1ª.
    # El track record se ancla luego en la clase con más histórico de la MISMA divisa que el
    # primario (resolve_track_record) — idéntico al sync por-análisis. Backfill consistente.
    from tools.track_record_isin import resolve_track_record
    prim = {}
    for f in sorted(funds, key=lambda x: not x.get("has_qualitative_analysis")):
        prim.setdefault(f["fund_group_id"], f["isin"])
    print(f"{'APLICAR' if apply else 'DRY-RUN'} — {len(prim)} grupos", flush=True)
    ok = 0
    for i, (gid, isin) in enumerate(prim.items(), 1):
        try:
            tr_isin, serie = resolve_track_record(c, isin)
            m = metrics_from_series(serie)
        except Exception as e:
            print(f"  [{i}] {isin} ERROR {str(e)[:60]}", flush=True); continue
        time.sleep(0.4)
        if not m:
            continue
        ok += 1
        if apply:
            c.table("fund_groups").update({"rendimiento_jsonb": m}).eq("fund_group_id", gid).execute()
        if i <= 8 or tr_isin.upper() != isin.upper():
            print(f"  [{i}] {isin} -> tr={tr_isin}: cagr={m.get('cagr_desde_inicio')} "
                  f"años={len(m.get('rentabilidades_anuales', {}))}", flush=True)
    print(f"{'APLICADO' if apply else 'DRY'}: {ok} grupos con métricas", flush=True)


if __name__ == "__main__":
    main()
