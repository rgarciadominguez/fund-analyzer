"""Volcado del feed CUANTITATIVO para el portal (fund-dashboard → portal).

Por cada ISIN de la lista del portal:
  - histórico diario de NAV (Morningstar por ISIN) → historico_diario.csv.gz
  - métricas derivadas (rentab/vol por plazo, CAGR, max DD, underwater, Sharpe por
    exceso sobre un monetario EUR) → metricas.json
  - los que Morningstar no cubre → faltan.json (null, no inventados)

Cada serie se descarga UNA vez y sirve para histórico y métricas.
Sharpe: exceso sobre rf (por defecto Groupama Trésorerie IC FR0000989626) en la
ventana de cada plazo.

Uso:
    python -m tools.export_quant_feed
    python -m tools.export_quant_feed --lista <ruta.json> --rf FR0000989626 --out <dir>
"""
from __future__ import annotations

import gzip
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from tools.morningstar_daily import fetch_series, metrics_from_series, monthly_returns_by_ym

_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
_DEFAULT_LISTA = Path(r"C:\Users\RafaelGarcía\horizonte-datos\mis_296_activos.json")
_DEFAULT_OUT = Path(r"C:\Users\RafaelGarcía\horizonte-datos")
_DEFAULT_RF = "FR0000989626"   # Groupama Trésorerie IC (monetario EUR)


def _fecha(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d")


def _load_isins(lista: Path) -> list:
    raw = json.loads(lista.read_text(encoding="utf-8"))
    rows = raw.get("activos") if isinstance(raw, dict) else raw
    out, seen = [], set()
    for r in rows:
        iv = (r.get("isin") or "").upper().strip()
        nombre = r.get("nombre") or ""
        if _ISIN.match(iv) and iv not in seen:
            seen.add(iv)
            out.append((iv, nombre))
    return out


def run(lista: Path, rf_isin: str, out_dir: Path) -> dict:
    isins = _load_isins(lista)
    print(f"[QUANT] {len(isins)} ISIN a volcar | rf={rf_isin}", flush=True)

    rf_series = fetch_series(rf_isin)
    rf_monthly = monthly_returns_by_ym(rf_series) if rf_series else None
    if not rf_monthly:
        print(f"[QUANT] AVISO: sin serie del rf {rf_isin} → Sharpe irá null", flush=True)

    hist_path = out_dir / "historico_diario.csv.gz"
    metricas, faltan = {}, []
    total_filas = 0
    import time

    with gzip.open(hist_path, "wt", encoding="utf-8", newline="") as gz:
        gz.write("isin,fecha,nav\n")
        for i, (isin, nombre) in enumerate(isins, 1):
            s = fetch_series(isin)
            time.sleep(0.35)
            if len(s) < 30:
                faltan.append({"isin": isin, "nombre": nombre,
                               "motivo": "sin_cobertura_morningstar" if not s else "serie_muy_corta"})
                if i % 20 == 0 or not s:
                    print(f"  [{i}/{len(isins)}] {isin} SIN DATOS", flush=True)
                continue
            s.sort()
            for ts, v in s:
                gz.write(f"{isin},{_fecha(ts)},{round(v, 4)}\n")
                total_filas += 1
            m = metrics_from_series(s, rf_monthly=rf_monthly)
            m["rango"] = {"desde": _fecha(s[0][0]), "hasta": _fecha(s[-1][0]), "n_puntos": len(s)}
            metricas[isin] = m
            if i % 20 == 0:
                print(f"  [{i}/{len(isins)}] {isin}  {len(s)} pts  ok", flush=True)

    meta = {
        "generado": datetime.now(timezone.utc).isoformat(),
        "fuente": "morningstar_daily (timeseries_price por ISIN)",
        "rf_isin": rf_isin,
        "rf_nombre": "Groupama Trésorerie IC" if rf_isin == _DEFAULT_RF else "",
        "unidades": "rentab/vol/sharpe en PORCENTAJE (5.0=5%); nav crudo en divisa del fondo",
        "sharpe": "exceso mensual sobre el rf, alineado por mes, en la ventana de cada plazo (1/3/5/10a)",
        "n_isin": len(isins), "n_con_datos": len(metricas), "n_faltan": len(faltan),
        "n_filas_historico": total_filas,
    }
    (out_dir / "metricas.json").write_text(
        json.dumps({"_meta": meta, "metricas": metricas}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    (out_dir / "faltan.json").write_text(
        json.dumps({"_meta": {"generado": meta["generado"], "n": len(faltan)}, "faltan": faltan},
                   ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n[QUANT] LISTO:", flush=True)
    print(f"  historico_diario.csv.gz  {total_filas} filas  "
          f"{hist_path.stat().st_size/1024/1024:.2f} MB", flush=True)
    print(f"  metricas.json            {len(metricas)} ISIN con métricas", flush=True)
    print(f"  faltan.json              {len(faltan)} sin cobertura", flush=True)
    return meta


def main():
    args = sys.argv[1:]
    def _arg(flag, default):
        return args[args.index(flag) + 1] if flag in args else default
    lista = Path(_arg("--lista", str(_DEFAULT_LISTA)))
    rf = _arg("--rf", _DEFAULT_RF)
    out = Path(_arg("--out", str(_DEFAULT_OUT)))
    run(lista, rf, out)


if __name__ == "__main__":
    main()
