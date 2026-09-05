"""doc_completeness.py — Mide y conduce la COMPLETITUD DOCUMENTAL de un fondo (Goal 2, Rafa
2026-09-06): idealmente ≥1 AR + 1 SAR + 1 carta del gestor POR CADA AÑO de historia REAL del fondo,
todo accesible en la pestaña Documentos.

"Historia REAL" = desde el lanzamiento del vehículo actual, o desde el predecesor si existe lineage
(data/fund_lineage.json: strategy_inception). Así RL Global Bond Opps cuenta desde 2015 y MontLake
desde 2018 (aunque el UCITS sea 2024) — con el caveat de que los docs del predecesor pueden no ser
públicos (RAIF privado).

assess(isin) -> {
  isin, launch_year, years[], por_anio:{year:{ar,sar,letter}}, cobertura_ar/sar/carta (0..1),
  faltan_ar[], faltan_sar[], faltan_carta[], resumen
}

CLI:
  python -m tools.doc_completeness IE00BGSVCP50            # informe de un fondo
  python -m tools.doc_completeness --all                   # tabla de todos los fondos
  python -m tools.doc_completeness IE00BGSVCP50 --enqueue  # encola gaps de AR para sourcing
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
THIS_YEAR = date.today().year


def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _year_of(s) -> int:
    m = re.search(r"(19|20)\d{2}", str(s or ""))
    return int(m.group(0)) if m else 0


def _real_launch_year(isin: str, d: dict) -> int:
    """Año de lanzamiento REAL (lineage → output.json → Supabase). Centralizado en fund_age."""
    try:
        from tools.fund_age import launch_year
        return launch_year(isin, output_data=d)
    except Exception:
        k = d.get("kpis") or {}
        for v in (k.get("anio_creacion"), k.get("fecha_registro"), k.get("fecha_inicio"),
                  d.get("fecha_creacion")):
            y = _year_of(v)
            if y:
                return y
        return 0


def assess(isin: str) -> dict:
    isin = (isin or "").upper().strip()
    out = {"isin": isin, "launch_year": 0, "years": [], "por_anio": {},
           "cobertura_ar": 0.0, "cobertura_sar": 0.0, "cobertura_carta": 0.0,
           "faltan_ar": [], "faltan_sar": [], "faltan_carta": [], "resumen": ""}
    p = FUNDS / isin / "output.json"
    if not p.exists():
        out["resumen"] = "sin output.json"
        return out
    d = _load(p)
    ly = _real_launch_year(isin, d)
    out["launch_year"] = ly
    if not ly:
        out["resumen"] = "sin año de lanzamiento"
        return out
    years = list(range(ly, THIS_YEAR + 1))
    out["years"] = years

    docs = ((d.get("analyst_synthesis") or {}).get("documentos") or {})
    ar_years, sar_years, letter_years = set(), set(), set()
    for it in docs.get("informes_pdf") or []:
        if not isinstance(it, dict):
            continue
        t = (it.get("tipo") or "").lower()
        y = _year_of(it.get("periodo")) or _year_of(it.get("nombre")) or _year_of(it.get("url"))
        if not y:
            continue
        if t in ("annual_report", "informe_anual"):
            ar_years.add(y)
        elif t in ("semi_annual_report", "semiannual_report", "informe_semestral"):
            sar_years.add(y)
        elif t in ("quarterly_letter", "carta", "carta_gestor"):
            letter_years.add(y)
    # cartas_urls (cartas del gestor) — año del nombre/url
    for it in docs.get("cartas_urls") or []:
        y = _year_of(it if isinstance(it, str) else (it.get("periodo") or it.get("url") if isinstance(it, dict) else ""))
        if y:
            letter_years.add(y)

    for y in years:
        row = {"ar": y in ar_years, "sar": y in sar_years, "letter": y in letter_years}
        out["por_anio"][str(y)] = row
        if not row["ar"]:
            out["faltan_ar"].append(y)
        if not row["sar"]:
            out["faltan_sar"].append(y)
        if not row["letter"]:
            out["faltan_carta"].append(y)

    n = len(years) or 1
    out["cobertura_ar"] = round(len(ar_years & set(years)) / n, 2)
    out["cobertura_sar"] = round(len(sar_years & set(years)) / n, 2)
    out["cobertura_carta"] = round(len(letter_years & set(years)) / n, 2)
    out["resumen"] = (f"{ly}-{THIS_YEAR} ({n}a): AR {int(out['cobertura_ar']*100)}% · "
                      f"SAR {int(out['cobertura_sar']*100)}% · cartas {int(out['cobertura_carta']*100)}%")
    return out


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = argv or sys.argv[1:]
    if "--all" in args:
        rows = []
        for fd in sorted(FUNDS.iterdir()):
            if fd.is_dir() and (fd / "output.json").exists():
                a = assess(fd.name)
                if a["launch_year"]:
                    rows.append(a)
        rows.sort(key=lambda a: a["cobertura_ar"])
        print(f"{'ISIN':14} {'años':>5}  AR%  SAR% carta%  resumen")
        for a in rows:
            print(f"{a['isin']:14} {len(a['years']):>5}  {int(a['cobertura_ar']*100):>3} "
                  f"{int(a['cobertura_sar']*100):>4} {int(a['cobertura_carta']*100):>5}   "
                  f"faltanAR={a['faltan_ar'][:6]}")
        return
    enqueue = "--enqueue" in args
    for isin in [a for a in args if not a.startswith("--")]:
        a = assess(isin.upper())
        print(json.dumps(a, ensure_ascii=False, indent=1))
        if enqueue and a["faltan_ar"]:
            try:
                from tools.ar_sourcing_queue import enqueue as _enq
                _enq(isin.upper(), ar_count=len(a["years"]) - len(a["faltan_ar"]))
                print(f"[doc_completeness] {isin}: encolado para sourcing de AR (faltan {len(a['faltan_ar'])} años)")
            except Exception as e:
                print(f"[doc_completeness] enqueue falló: {e}")


if __name__ == "__main__":
    main()
