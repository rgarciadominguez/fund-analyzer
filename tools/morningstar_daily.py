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
    src_isin = isin
    lineage_note = None
    try:
        from tools.lineage_kb import get_record
        rec = get_record(isin) or {}
        tr = rec.get("track_record") or {}
        alt = tr.get("quant_series_isin")
        if alt and alt.upper() != (isin or "").upper():
            s_alt = fetch_series(alt)
            s_own = fetch_series(isin)
            # usar la serie con inicio más antiguo (más histórico real)
            if s_alt and (not s_own or s_alt[0][0] < s_own[0][0]):
                src_isin = alt
                lineage_note = {
                    "serie_de_clase": alt,
                    "desde": tr.get("quant_series_start"),
                    "nota": tr.get("quant_note") or "incluye histórico de vehículo predecesor",
                    "caveat": rec.get("caveat_global"),
                }
    except Exception:
        pass
    s = fetch_series(src_isin)
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
