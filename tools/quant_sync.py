"""
quant_sync.py — Métricas cuantitativas POR ISIN (cada clase su NAV → sus métricas).

FIX 2026-07-23 (Bug A + Bug B, feedback portal):
  Antes esto copiaba el JSONB de GRUPO (`fund_groups.rendimiento_jsonb`) a todas las clases
  → métricas idénticas entre clases del mismo fondo. MAL: cada share class tiene su propio NAV
  y, con distintas comisiones, distinta rentabilidad neta — es EL dato para elegir clase.

  Ahora: por CADA ISIN del catálogo (TODAS las clases de cada fund_group_id, no solo la
  principal) se baja su serie NAV de Morningstar y se computan SUS métricas
  (`morningstar_daily.metrics_from_series`), incluido underwater (dias_bajo_agua,
  racha_max_bajo_agua_dias, pct_tiempo_bajo_agua). Join por ISIN, nunca por grupo.

Salidas:
  hf_asset_metrics         1 fila por ISIN con SUS métricas (+ underwater)
  hf_asset_annual_returns  1 fila por (ISIN, año) con SU rentab
  hf_asset_prices          serie diaria por ISIN (--prices)
  metricas.json            fichero per-ISIN (para la carga del portal)

DDL: data/migrations/2026-07-23_hf_asset_tables.sql (+ columnas underwater).

CLI:
    python -m tools.quant_sync --apply            # métricas + rendimientos por ISIN
    python -m tools.quant_sync --prices --apply    # + serie diaria
    python -m tools.quant_sync --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
METRICAS_JSON = Path(r"C:\Users\RafaelGarcía\horizonte-datos\metricas.json")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _catalog_isins(client) -> list[str]:
    """TODOS los ISIN del catálogo (todas las clases de todos los grupos)."""
    isins = set()
    off = 0
    while True:
        b = client.table("funds").select("isin").range(off, off + 999).execute().data
        if not b:
            break
        isins.update(r["isin"] for r in b)
        off += 1000
        if off > 20000:
            break
    return sorted(isins)


def _group_rating(client) -> dict:
    """estrellas/medalist/srri por ISIN, heredados del grupo (no son NAV-derivados)."""
    groups = {g["fund_group_id"]: g for g in
              client.table("fund_groups").select(
                  "fund_group_id,portfolio_metrics_jsonb").execute().data}
    out = {}
    off = 0
    while True:
        b = client.table("funds").select("isin,fund_group_id").range(off, off + 999).execute().data
        if not b:
            break
        for f in b:
            g = groups.get(f.get("fund_group_id")) or {}
            pm = g.get("portfolio_metrics_jsonb") or {}
            ms = pm.get("morningstar") or {}
            mi = pm.get("myinvestor") or {}
            out[f["isin"]] = {"estrellas": ms.get("estrellas"), "medalist": ms.get("medalist"),
                              "srri": mi.get("srri"), "mstar_rating": mi.get("mstar_rating")}
        off += 1000
        if off > 20000:
            break
    return out


def build_per_isin(client, want_prices: bool = False):
    """Por cada ISIN sus PROPIAS métricas (difieren por clase). HÍBRIDO:
      - screener Morningstar (lt.morningstar.com, por SecId/clase): trailing rentab/vol/maxDD
        → SIEMPRE disponible, difiere por clase (fuente primaria).
      - serie diaria (morningstar_daily.fetch_series, host lt.morningstar.com): rentab por año natural + UNDERWATER + vol/cagr
        de por vida → cuando el endpoint responde (a veces rate-limita); si no, esos campos null.
    Devuelve (met_rows, ann_rows, price_rows, metricas_json, sin_metrica)."""
    from tools.morningstar_daily import fetch_series, metrics_from_series
    from tools.morningstar_quant import fetch_quant
    from datetime import datetime as _dt, timezone as _tz

    isins = _catalog_isins(client)
    rating = _group_rating(client)
    print(f"ISIN del catálogo (todas las clases): {len(isins)}")

    met_rows, ann_rows, price_rows = [], [], []
    metricas_json = {}
    sin_metrica = []
    n_serie = 0
    # circuit-breaker: si el endpoint de serie está caído (rate-limit/301), tras N fallos
    # seguidos dejamos de intentarlo (cada intento reintenta con backoff → muy lento).
    serie_ok = want_prices  # con --prices sí interesa insistir; sin, corta rápido
    serie_fallos = 0
    SERIE_CORTA = 12
    for k, isin in enumerate(isins, 1):
        # --- serie diaria (mejor: da anuales + underwater); best-effort ---
        s = []
        if serie_ok or serie_fallos < SERIE_CORTA:
            try:
                s = fetch_series(isin)
            except Exception:
                s = []
            if s:
                serie_fallos = 0
            else:
                serie_fallos += 1
                if serie_fallos == SERIE_CORTA and not want_prices:
                    print(f"  [circuit-breaker] serie diaria caída ({SERIE_CORTA} fallos) → "
                          f"solo screener el resto del run")
        m = metrics_from_series(s) if s and len(s) >= 30 else {}
        if m:
            n_serie += 1
        uw = m.get("underwater") or {}
        # --- screener por clase (siempre; rellena lo que la serie no dé) ---
        try:
            q = fetch_quant(isin) or {}
        except Exception:
            q = {}
        qr = q.get("rentabilidades") or {}
        risk = q.get("riesgo") or {}
        r3, r5 = (risk.get("3a") or {}), (risk.get("5a") or {})
        # max_drawdown: preferimos el de por vida (serie); si no, el de 5a/3a del screener
        maxdd = m.get("max_drawdown")
        if maxdd is None:
            maxdd = r5.get("max_drawdown") if r5.get("max_drawdown") is not None else r3.get("max_drawdown")

        def pick(serie_v, screener_v):
            return serie_v if serie_v is not None else screener_v

        row = {
            "isin": isin,
            "cagr_desde_inicio": m.get("cagr_desde_inicio"),
            "rentab_1a": pick(m.get("rentab_1a"), qr.get("1a")),
            "rentab_3a": pick(m.get("rentab_3a"), qr.get("3a")),
            "rentab_5a": pick(m.get("rentab_5a"), qr.get("5a")),
            "rentab_10a": pick(m.get("rentab_10a"), qr.get("10a")),
            "volatilidad": m.get("volatilidad"),
            "volatilidad_3a": pick(m.get("volatilidad_3a"), r3.get("volatilidad")),
            "volatilidad_5a": pick(m.get("volatilidad_5a"), r5.get("volatilidad")),
            "max_drawdown": maxdd,
            "peor_anio": m.get("peor_anio"), "mejor_anio": m.get("mejor_anio"),
            "dias_bajo_agua": uw.get("dias_bajo_agua"),
            "racha_max_bajo_agua_dias": uw.get("racha_max_bajo_agua_dias"),
            "pct_tiempo_bajo_agua": uw.get("pct_tiempo_bajo_agua"),
            "estrellas": (rating.get(isin, {}).get("estrellas") if rating.get(isin, {}).get("estrellas") is not None
                          else q.get("rating_estrellas")),
            "medalist": rating.get(isin, {}).get("medalist") or q.get("medalist_rating"),
            "srri": rating.get(isin, {}).get("srri"),
            "mstar_rating": rating.get(isin, {}).get("mstar_rating"),
            "n_puntos": m.get("n_puntos"),
            "fuente": "morningstar_daily+screener" if m else "screener",
            "updated_at": _now(),
        }
        # ¿tiene ALGUNA métrica útil? (rentab por clase). Si no: upsert con NAV-metrics NULL
        # para LIMPIAR posibles valores viejos de grupo (Bug B residual) y reportarlo.
        if row["rentab_3a"] is None and row["rentab_5a"] is None and row["rentab_1a"] is None:
            sin_metrica.append(isin)
            met_rows.append({
                "isin": isin,
                "cagr_desde_inicio": None, "rentab_1a": None, "rentab_3a": None,
                "rentab_5a": None, "rentab_10a": None, "volatilidad": None,
                "volatilidad_3a": None, "volatilidad_5a": None, "max_drawdown": None,
                "peor_anio": None, "mejor_anio": None,
                "estrellas": row.get("estrellas"), "medalist": row.get("medalist"),
                "srri": row.get("srri"), "mstar_rating": row.get("mstar_rating"),
                "fuente": "sin_datos", "updated_at": _now(),
            })
            continue
        # Omitir underwater/n_puntos si son null (serie bloqueada) → el upsert corre
        # aunque el ALTER de esas columnas aún no esté aplicado. Se rellenan cuando la
        # serie diaria vuelva + ALTER hecho.
        for _k in ("dias_bajo_agua", "racha_max_bajo_agua_dias", "pct_tiempo_bajo_agua", "n_puntos"):
            if row.get(_k) is None:
                row.pop(_k, None)
        met_rows.append(row)
        for anio, pct in (m.get("rentabilidades_anuales") or {}).items():
            if pct is not None:
                ann_rows.append({"isin": isin, "anio": int(anio),
                                 "rentab_pct": round(float(pct), 4), "updated_at": _now()})
        metricas_json[isin] = {kk: vv for kk, vv in row.items() if kk != "updated_at"}
        metricas_json[isin]["rentabilidades_anuales"] = m.get("rentabilidades_anuales") or {}
        if want_prices and s:
            for ts, v in s:
                price_rows.append({"isin": isin,
                                   "fecha": _dt.fromtimestamp(ts/1000, _tz.utc).date().isoformat(),
                                   "nav": round(float(v), 6)})
        if k % 25 == 0:
            print(f"  {k}/{len(isins)} | con métrica {len(met_rows)} | con serie diaria {n_serie} | sin métrica {len(sin_metrica)}")
    print(f"  serie diaria disponible en {n_serie}/{len(isins)} (el resto, métricas del screener)")
    return met_rows, ann_rows, price_rows, metricas_json, sin_metrica


def sync(apply: bool = False, prices: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    client = get_client()

    met, ann, price, mjson, sin_metrica = build_per_isin(client, want_prices=prices)
    print(f"\nmétricas POR ISIN: {len(met)} | rendimientos: {len(ann)} | "
          f"precios: {len(price)} | SIN métrica: {len(sin_metrica)}")
    if sin_metrica:
        print(f"  ISIN sin métrica en Morningstar (revisar): {sin_metrica[:20]}"
              f"{' …' if len(sin_metrica) > 20 else ''}")

    # metricas.json (para la carga del portal)
    METRICAS_JSON.write_text(json.dumps(
        {"generado": _now(), "n": len(mjson), "sin_metrica": sin_metrica, "metricas": mjson},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"escrito {METRICAS_JSON} ({len(mjson)} ISIN)")

    if apply:
        for i in range(0, len(met), 100):
            client.table("hf_asset_metrics").upsert(met[i:i+100], on_conflict="isin").execute()
        for i in range(0, len(ann), 200):
            client.table("hf_asset_annual_returns").upsert(ann[i:i+200], on_conflict="isin,anio").execute()
        print(f"upsert: {len(met)} métricas + {len(ann)} rendimientos (por ISIN)")
        if prices:
            for i in range(0, len(price), 500):
                client.table("hf_asset_prices").upsert(price[i:i+500], on_conflict="isin,fecha").execute()
            print(f"upsert precios: {len(price)} puntos")
    return {"metrics": len(met), "anual": len(ann), "prices": len(price),
            "sin_metrica": len(sin_metrica), "apply": apply}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--prices", action="store_true")
    a = ap.parse_args()
    print(sync(apply=a.apply, prices=a.prices))
