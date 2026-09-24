"""Tipos de cambio a EUR por fecha, desde las series locales (data/benchmarks/EUR_USD.csv, formato
Investing: "Fecha,Último,..." con dd.mm.yyyy y coma decimal). Fallback: tipos fijos aproximados.

Por qué (2026-09-24, Baillie LTGG): el patrimonio de un sub-fondo con divisa base USD se publicaba
como M€ sin convertir (5.348 M$ → "5.348 M€"; real ≈ 4.548 M€ al cambio de sep-2025). El extractor
tenía un fallback fijo (0,92) que tampoco vale para fechas concretas.
"""
from __future__ import annotations

import csv
import re
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_FILES = {"USD": ROOT / "data" / "benchmarks" / "EUR_USD.csv"}      # EUR base: 1 EUR = x USD
_FALLBACK_EUR_PER_UNIT = {"USD": 0.90, "GBP": 1.16, "CHF": 1.06, "JPY": 0.0060, "SEK": 0.089, "NOK": 0.086, "DKK": 0.134}


@lru_cache(maxsize=8)
def _series(cur: str) -> list[tuple[date, float]]:
    p = _FILES.get(cur)
    if not p or not p.exists():
        return []
    out = []
    with open(p, encoding="utf-8-sig", newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            # Investing exporta cada línea como UNA celda entrecomillada con comillas dobladas dentro:
            # "03.01.2000,""1,0262"",..." → segundo pase de csv sobre el contenido.
            cells = row if len(row) > 1 else next(csv.reader([row[0]]))
            cells = [c.strip().strip('"') for c in cells]
            m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", cells[0] if cells else "")
            if not m or len(cells) < 2:
                continue
            try:
                d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                v = float(cells[1].replace(".", "").replace(",", "."))
            except ValueError:
                continue
            if v > 0:
                out.append((d, v))
    out.sort()
    return out


def _to_date(when) -> date | None:
    if isinstance(when, date):
        return when
    s = str(when or "")
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            d = datetime.strptime(s[:len(fmt.replace("%Y", "2000").replace("%m", "01").replace("%d", "01"))], fmt).date()
            return d if fmt == "%Y-%m-%d" else (d.replace(day=28) if fmt == "%Y-%m" else d.replace(month=12, day=31))
        except ValueError:
            continue
    return None


def eur_per_unit(cur: str, when=None) -> tuple[float, str]:
    """(euros por 1 unidad de `cur` en la fecha, fuente). Fecha: último dato disponible ≤ fecha."""
    cur = (cur or "EUR").upper()
    if cur == "EUR":
        return 1.0, "EUR"
    ser = _series(cur)
    d = _to_date(when)
    if ser:
        if d is None:
            return 1.0 / ser[-1][1], f"EUR_USD.csv {ser[-1][0]}"
        prev = [x for x in ser if x[0] <= d]
        if prev:
            return 1.0 / prev[-1][1], f"EUR_USD.csv {prev[-1][0]}"
        return 1.0 / ser[0][1], f"EUR_USD.csv {ser[0][0]} (primer dato)"
    return _FALLBACK_EUR_PER_UNIT.get(cur, 1.0), f"fallback fijo {cur}"


def to_eur(amount: float, cur: str, when=None) -> tuple[float, str]:
    r, src = eur_per_unit(cur, when)
    return amount * r, src


if __name__ == "__main__":
    import sys
    for arg in sys.argv[1:] or ["2025-09-30", "2026-03-31"]:
        print(arg, "USD→EUR:", eur_per_unit("USD", arg))
