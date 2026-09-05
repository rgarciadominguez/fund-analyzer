"""fund_age.py — Año de lanzamiento REAL de un fondo, robusto (output.json → Supabase → lineage).

Bug detectado (2026-09-06): muchos output.json INT NO tienen la fecha de inicio (kpis.* = None),
así que historico_gap/doc_completeness creían que el fondo tenía 1 año y NO disparaban el sourcing
multi-año (caso Royal London Global Bond Opps: 2015, pero su output.json no lo dice → nunca se
buscaban sus AR antiguos). Este helper centraliza la resolución con fallback a Supabase y al lineage.

launch_year(isin, output_data=None) -> int (0 si desconocido)
  Orden: (1) lineage strategy_inception (lanzamiento REAL, incl. predecesor);
         (2) output.json kpis.anio_creacion / fecha_* / anios_antiguedad;
         (3) Supabase funds.fecha_creacion_clase + fund_groups.fecha_creacion_fondo (el más antiguo).
"""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_CACHE: dict[str, int] = {}


def _y(v) -> int:
    m = re.search(r"(19|20)\d{2}", str(v or ""))
    return int(m.group(0)) if m else 0


def launch_year(isin: str, output_data: dict | None = None, use_lineage: bool = True) -> int:
    isin = (isin or "").upper().strip()
    if not isin:
        return 0
    if isin in _CACHE:
        return _CACHE[isin]

    # (1) lineage = lanzamiento REAL (incluye predecesor)
    if use_lineage:
        try:
            from tools.lineage_kb import get_record
            rec = get_record(isin) or {}
            y = _y(rec.get("strategy_inception"))
            if not y:
                for p in rec.get("predecessors") or []:
                    y = _y(p.get("from"))
                    if y:
                        break
            if y:
                _CACHE[isin] = y
                return y
        except Exception:
            pass

    # (2) output.json
    d = output_data
    if d is None:
        p = ROOT / "data" / "funds" / isin / "output.json"
        d = {}
        if p.exists():
            try:
                d = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                d = {}
    k = (d or {}).get("kpis") or {}
    y = _y(k.get("anio_creacion"))
    if not y:
        if k.get("anios_antiguedad"):
            try:
                y = date.today().year - int(k["anios_antiguedad"])
            except Exception:
                y = 0
    if not y:
        for v in (k.get("fecha_registro"), k.get("fecha_inicio"), k.get("fecha_creacion"),
                  (d or {}).get("fecha_creacion")):
            y = _y(v)
            if y:
                break
    if y:
        _CACHE[isin] = y
        return y

    # (3) Supabase (fecha_creacion_clase / fecha_creacion_fondo) — el más antiguo
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        c = get_client()
        f = c.table("funds").select("fund_group_id,fecha_creacion_clase").eq("isin", isin).execute().data
        if f:
            y = _y(f[0].get("fecha_creacion_clase"))
            gid = f[0].get("fund_group_id")
            if gid:
                g = c.table("fund_groups").select("fecha_creacion_fondo").eq("fund_group_id", gid).execute().data
                if g:
                    yf = _y(g[0].get("fecha_creacion_fondo"))
                    if yf:
                        y = min(y or 9999, yf)
    except Exception:
        y = y if 'y' in dir() else 0

    y = y if y and y != 9999 else 0
    if y:
        _CACHE[isin] = y
    return y


if __name__ == "__main__":
    import sys
    for isin in sys.argv[1:]:
        print(isin, "->", launch_year(isin))
