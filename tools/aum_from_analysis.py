"""aum_from_analysis.py — Recupera el AUM desde la NARRATIVA del análisis.

Idea (Rafa, 2026-06-10): cuando el extractor INT no rellena kpis.aum_actual_meur
(discovery no trajo el factsheet), el analyst-cowork muchas veces SÍ menciona el
patrimonio en el texto (de FT/Finect/ficha gestora). "Cogerlo directamente" de
ahí en vez de complicarlo. Determinista, sin API.

Conservador: solo acepta cifras junto a una etiqueta de patrimonio TOTAL
("patrimonio total", "fund size", "total net assets", "AUM", "tamaño del
fondo"...), normaliza a M€, y descarta lo que no tenga sentido. Si no hay señal
clara → None (no inventa; deja el campo vacío).

Uso programático:  aum_from_narrative(texto) -> [(meur, label, evidencia), ...]
CLI:
    python -m tools.aum_from_analysis            # DRY-RUN sobre fondos con kpis.aum None
    python -m tools.aum_from_analysis --apply    # escribe kpis.aum_actual_meur (fill-if-empty)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"

_LABEL = (r"(patrimonio total|patrimonio del fondo|fund size|total net assets|"
          r"tama[ñn]o del fondo|activos bajo gesti[oó]n|\bAUM\b|gestiona)")
_PAT = re.compile(
    _LABEL + r"[^.\d]{0,40}?(?:de|en torno a|aprox\.?|~|:)?\s*"
    r"(?:(?:EUR|USD|€|\$)\s?)?([\d][\d.,]{2,})\s*"
    r"(M€|MM€|millones|m€|mn|M EUR|bn|billion|mil millones|€|EUR|USD)?",
    re.IGNORECASE,
)


def _to_meur(num: str, unit: str) -> float | None:
    s = num.replace(" ", "")
    unit = (unit or "").lower()
    seps = [c for c in s if c in ".,"]
    try:
        if len(seps) >= 2:           # agrupado: 973.183.909 / 1,234,567
            val = float(re.sub(r"[.,]", "", s))
        elif seps:
            last = re.split(r"[.,]", s)[-1]
            if len(last) == 3 and unit in ("", "€", "eur", "usd"):
                val = float(re.sub(r"[.,]", "", s))   # separador de millar
            else:
                val = float(s.replace(".", "").replace(",", ".")) if "," in s else float(s)
        else:
            val = float(s)
    except ValueError:
        return None
    if unit in ("m€", "mm€", "millones", "mn", "m eur"):
        meur = val
    elif unit in ("bn", "billion", "mil millones"):
        meur = val * 1000
    else:
        meur = val / 1e6 if val > 100000 else val
    return round(meur, 2) if 0.5 <= meur <= 100000 else None


def aum_from_narrative(text: str) -> list[tuple]:
    """Lista de candidatos (meur, label, evidencia). El primero = más fiable."""
    out = []
    for m in _PAT.finditer(text or ""):
        meur = _to_meur(m.group(2), m.group(3) or "")
        if meur is not None:
            snip = (text[max(0, m.start() - 10):m.start() + 70] or "").replace("\n", " ")
            out.append((meur, m.group(1).lower(), snip))
    # prioriza etiquetas "total"/"fund size" sobre "gestiona"
    out.sort(key=lambda c: 0 if ("total" in c[1] or "fund size" in c[1] or "net assets" in c[1]) else 1)
    return out


def harvest(j: dict) -> tuple | None:
    """Devuelve (meur, evidencia) si lo encuentra en el analyst_synthesis."""
    a = j.get("analyst_synthesis", {})
    if not isinstance(a, dict):
        return None
    txt = " ".join(
        (a.get(s, {}) or {}).get("texto", "") or ""
        for s in ("resumen", "evolucion", "cartera", "estrategia")
        if isinstance(a.get(s), dict)
    )
    cands = aum_from_narrative(txt)
    return (cands[0][0], cands[0][2]) if cands else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Recupera AUM desde la narrativa del análisis (fill-if-empty)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--isin")
    args = ap.parse_args(argv)
    applied = 0
    for d in sorted(FUNDS_DIR.iterdir()):
        if not d.is_dir() or "." in d.name:
            continue
        if args.isin and d.name != args.isin:
            continue
        p = d / "output.json"
        if not p.exists():
            continue
        try:
            j = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if (j.get("kpis") or {}).get("aum_actual_meur") is not None:
            continue
        h = harvest(j)
        if not h:
            continue
        meur, ev = h
        print(f"{d.name}: AUM={meur} M€  | {ev[:80]}")
        if args.apply:
            from tools.output_merger import save_output, mark_manual_edit
            j.setdefault("kpis", {})["aum_actual_meur"] = meur
            mark_manual_edit(j, "kpis.aum_actual_meur")
            save_output(d.name, j)
            applied += 1
    print(f"\n{'APLICADO: '+str(applied) if args.apply else 'DRY-RUN — usa --apply'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
