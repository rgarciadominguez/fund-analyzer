"""Calcula años_antiguedad de un fondo desde la fecha/año de inicio del análisis.

Fallo común: el análisis trae la fecha de inicio (kpis.anio_creacion,
kpis.fecha_registro, clases[].launch...) pero no se calculaban los años → el
grupo quedaba con años_antiguedad = None. Esta función lo deriva y lo rellena.

Prioridad de la fecha de inicio:
  kpis.anio_creacion → kpis.fecha_registro → kpis.fecha_creacion/fecha_inicio →
  fecha_inicio top-level → min(clases[].launch/año) → primer año de la serie diaria.
"""
from __future__ import annotations
import re
from datetime import datetime, timezone

_THIS_YEAR = 2026  # se recalcula en runtime abajo


def _year(v):
    if v is None:
        return None
    m = re.search(r"(19|20)\d{2}", str(v))
    return int(m.group(0)) if m else None


def extract_start_year(data: dict, rendimiento: dict | None = None) -> int | None:
    """Año de inicio del fondo (clase más antigua) desde el output.json."""
    k = data.get("kpis", {}) or {}
    for cand in (k.get("anio_creacion"), k.get("fecha_registro"), k.get("fecha_creacion"),
                 k.get("fecha_inicio"), data.get("fecha_creacion"), data.get("fecha_inicio")):
        y = _year(cand)
        if y and 1900 <= y <= _THIS_YEAR:
            return y
    # mínimo año entre las clases del folleto
    años = []
    for cl in (data.get("clases") or []):
        for key in ("launch", "fecha_inicio", "fecha_creacion", "año", "anio", "inception", "year"):
            y = _year(cl.get(key))
            if y and 1900 <= y <= _THIS_YEAR:
                años.append(y)
    if años:
        return min(años)
    # primer año de la serie diaria de rendimiento
    ra = (rendimiento or {}).get("rentabilidades_anuales") or {}
    yrs = [int(x) for x in ra if str(x).isdigit()]
    if yrs:
        return min(yrs)
    return None


def fill_anios(client, isin: str, output_data: dict | None = None, log=None) -> int | None:
    """Rellena fund_groups.años_antiguedad (+ fecha_creacion_fondo si falta) si está
    vacío y hay año de inicio. Devuelve los años calculados o None."""
    import json
    from pathlib import Path
    isin = (isin or "").upper().strip()
    if output_data is None:
        p = Path(__file__).resolve().parent.parent / "data" / "funds" / isin / "output.json"
        if not p.exists():
            return None
        try:
            output_data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    rg = client.table("funds").select("fund_group_id").eq("isin", isin).execute().data
    if not rg:
        return None
    gid = rg[0]["fund_group_id"]
    g = client.table("fund_groups").select(
        "años_antiguedad,fecha_creacion_fondo,rendimiento_jsonb").eq("fund_group_id", gid).execute().data
    g = g[0] if g else {}
    if g.get("años_antiguedad") is not None:
        return g["años_antiguedad"]
    now_year = datetime.now(timezone.utc).year
    globals()["_THIS_YEAR"] = now_year
    sy = extract_start_year(output_data, g.get("rendimiento_jsonb"))
    if not sy:
        return None
    anios = now_year - sy
    upd = {"años_antiguedad": anios}
    if not g.get("fecha_creacion_fondo"):
        upd["fecha_creacion_fondo"] = f"{sy}-01-01"
    client.table("fund_groups").update(upd).eq("fund_group_id", gid).execute()
    if log:
        log(f"[SYNC] años_antiguedad calculado: {anios} (inicio {sy})")
    return anios
