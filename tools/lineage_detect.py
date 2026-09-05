"""lineage_detect.py — Detector DETERMINISTA (0 tokens LLM) de histórico predecesor.

Señal barata y potente (descubierta con MontLake): si la serie NAV REAL de una clase (Morningstar)
empieza MUCHO antes que su fecha de lanzamiento legal, es que hay un vehículo predecesor cuya historia
ya está empalmada en Morningstar → ese histórico está disponible GRATIS y hay que usarlo (CLAUDE.md
§0.9: track-record = serie NAV real más larga, no la fecha de inicio legal).

Uso:
  - Como TRIGGER barato en la prep: si `predecessor_signal` o el fondo es joven/con gap → lanzar el
    resolver agéntico (skill lineage-resolver-cowork) para NOMBRAR el predecesor + su narrativa + sus AR.
  - Como fuente de quant: `longest_series_isin` es la clase cuya serie cubre más histórico real.

detect(isin) -> {
  isin, group_classes:[{isin, divisa, legal_launch, series_start, points}],
  longest_series_isin, longest_series_start, legal_launch_primary,
  predecessor_signal(bool), gap_years, motivo
}
Best-effort; nunca lanza.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# umbral: la serie real empieza >= este nº de años antes del lanzamiento legal → predecesor probable
GAP_YEARS_THRESHOLD = 1


def _ts_to_date(ts_ms) -> str:
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""


def _year(s) -> int:
    try:
        return int(str(s)[:4])
    except Exception:
        return 0


def _group_classes(isin: str) -> list:
    """ISINs + divisa + fecha_creacion de las clases del grupo (Supabase). Fallback: solo el isin."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        c = get_client()
        f = c.table("funds").select("fund_group_id").eq("isin", isin.upper()).execute().data
        if not f:
            return [{"isin": isin.upper(), "divisa": None, "legal_launch": None}]
        gid = f[0]["fund_group_id"]
        rows = c.table("funds").select("isin,divisa,fecha_creacion_clase").eq(
            "fund_group_id", gid).execute().data or []
        return [{"isin": r["isin"], "divisa": (r.get("divisa") or "").upper() or None,
                 "legal_launch": r.get("fecha_creacion_clase")} for r in rows]
    except Exception:
        return [{"isin": isin.upper(), "divisa": None, "legal_launch": None}]


def detect(isin: str) -> dict:
    isin = (isin or "").upper().strip()
    out = {"isin": isin, "group_classes": [], "longest_series_isin": None,
           "longest_series_start": None, "legal_launch_primary": None,
           "predecessor_signal": False, "gap_years": 0, "motivo": ""}
    if isin.startswith("ES"):
        out["motivo"] = "ES (cartera CNMV, no aplica lineage multi-vehículo)"
        return out
    try:
        from tools.morningstar_daily import resolve_secid, fetch_series
    except Exception as e:
        out["motivo"] = f"morningstar_daily no disponible: {e}"
        return out

    classes = _group_classes(isin)
    earliest_start = None
    earliest_isin = None
    for cl in classes:
        start = None
        pts = 0
        try:
            if resolve_secid(cl["isin"]):
                s = fetch_series(cl["isin"])
                if s:
                    pts = len(s)
                    start = _ts_to_date(s[0][0])
        except Exception:
            pass
        cl["series_start"] = start
        cl["points"] = pts
        out["group_classes"].append(cl)
        if start and (earliest_start is None or start < earliest_start):
            earliest_start = start
            earliest_isin = cl["isin"]

    out["longest_series_isin"] = earliest_isin
    out["longest_series_start"] = earliest_start

    # lanzamiento legal de referencia = el más antiguo declarado entre las clases
    legal_years = [_year(cl.get("legal_launch")) for cl in classes if _year(cl.get("legal_launch"))]
    legal_min = min(legal_years) if legal_years else 0
    out["legal_launch_primary"] = legal_min or None

    if earliest_start and legal_min:
        gap = legal_min - _year(earliest_start)
        out["gap_years"] = gap
        if gap >= GAP_YEARS_THRESHOLD:
            out["predecessor_signal"] = True
            out["motivo"] = (f"serie NAV real desde {earliest_start} ({earliest_isin}) vs lanzamiento "
                             f"legal {legal_min} → {gap} años de histórico predecesor ya empalmado")
        else:
            out["motivo"] = f"serie real ~coincide con lanzamiento legal (gap {gap}a) — sin predecesor evidente"
    else:
        out["motivo"] = "sin serie Morningstar o sin fecha legal para comparar"
    return out


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    for isin in (argv or sys.argv[1:]) or []:
        print(json.dumps(detect(isin), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
