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
    """Año de inicio del fondo = la **clase MÁS ANTIGUA**. Toma el MÍNIMO entre TODAS
    las señales (fecha del fondo + año de cada clase del folleto + serie diaria), no
    la primera que aparezca: si analizas una clase de 2023 pero hay una de 2004, gana
    2004."""
    cands = []
    k = data.get("kpis", {}) or {}
    for v in (k.get("anio_creacion"), k.get("fecha_registro"), k.get("fecha_creacion"),
              k.get("fecha_inicio"), data.get("fecha_creacion"), data.get("fecha_inicio")):
        cands.append(_year(v))
    for cl in (data.get("clases") or []):
        for key in ("launch", "fecha_inicio", "fecha_creacion", "año", "anio", "inception", "year"):
            cands.append(_year(cl.get(key)))
    ra = (rendimiento or {}).get("rentabilidades_anuales") or {}
    cands += [int(x) for x in ra if str(x).isdigit()]
    valid = [y for y in cands if y and 1950 <= y <= _THIS_YEAR]
    return min(valid) if valid else None


def ensure_anio_creacion(data: dict) -> bool:
    """Fija kpis.anio_creacion al año de la CLASE MÁS ANTIGUA. Lo rellena si falta y
    también lo CORRIGE si el valor actual es de una clase más nueva que la más antigua
    conocida (analizas una clase de 2023 pero el fondo tiene una de 2004 → 2004).
    Devuelve True si lo modificó."""
    if not isinstance(data, dict):
        return False
    globals()["_THIS_YEAR"] = datetime.now(timezone.utc).year
    oldest = extract_start_year(data)
    if not oldest:
        return False
    kpis = data.setdefault("kpis", {})
    cur = _year(kpis.get("anio_creacion"))
    if cur is None or oldest < cur:
        kpis["anio_creacion"] = oldest
        return True
    return False


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
    now_year = datetime.now(timezone.utc).year
    globals()["_THIS_YEAR"] = now_year
    g = client.table("fund_groups").select(
        "años_antiguedad,fecha_creacion_fondo,rendimiento_jsonb").eq("fund_group_id", gid).execute().data
    g = g[0] if g else {}
    # Año de inicio = la CLASE MÁS ANTIGUA. Combina TODAS las señales y toma el mínimo:
    #  - año de inicio de cada clase del grupo en Supabase (funds.fecha_creacion_clase)
    #  - señales del output.json (fecha del fondo / clases del folleto / serie diaria)
    años = []
    sy = extract_start_year(output_data, g.get("rendimiento_jsonb"))
    if sy:
        años.append(sy)
    rows = client.table("funds").select("fecha_creacion_clase").eq("fund_group_id", gid).execute().data
    for r in rows:
        y = _year(r.get("fecha_creacion_clase"))
        if y and 1950 <= y <= now_year:
            años.append(y)
    if not años:
        return g.get("años_antiguedad")
    oldest = min(años)            # la clase más antigua
    anios = now_year - oldest
    # Solo actualiza si mejora (más antiguo) o si estaba vacío — nunca rejuvenece.
    cur = g.get("años_antiguedad")
    if cur is not None and anios <= cur:
        return cur
    upd = {"años_antiguedad": anios, "fecha_creacion_fondo": f"{oldest}-01-01"}
    client.table("fund_groups").update(upd).eq("fund_group_id", gid).execute()
    if log:
        log(f"[SYNC] años_antiguedad (clase más antigua {oldest}): {anios}")
    return anios
