"""benchmark_data.py — Carga las series históricas de índices benchmark
(data/benchmarks/, CSVs de Investing.com en formato español) y las normaliza a
base 100 para superponerlas al fondo en el dashboard.

Formato CSV: cols 'Fecha,Último,...' con fecha DD.MM.YYYY y números en formato
español ('17.963,62' = 17963.62; period=miles, coma=decimal). Dos estilos de
comillas (RV/oro: todo entrecomillado; bonos: fecha sin comillas) — ambos OK vía csv.

Uso:
  from tools.benchmark_data import pick_benchmark, load_base100
  key = pick_benchmark(fund_data)              # 'MSCI World EUR'
  serie = load_base100(key, desde='2018-01-01')  # [{'fecha','valor'}] base 100
CLI: python -m tools.benchmark_data --list
"""
from __future__ import annotations

import csv
import glob
import sys
from datetime import date, datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "data" / "benchmarks" / "Benchmark"

# Registro: clave amigable -> patrón glob del CSV (versión EUR por defecto, que
# es la divisa del dashboard). USD disponible cambiando el sufijo.
REGISTRY = {
    "MSCI World EUR": "Renta Variable/MSCI World Gross EUE - euros.csv",
    "MSCI ACWI EUR": "Renta Variable/MSCI ACWI Gross - euros.csv",
    "MSCI ACWI ex USA EUR": "Renta Variable/MSCI ACWI ex USA Gross - euros.csv",
    "MSCI EM EUR": "Renta Variable/MSCI EM Gross - euros.csv",
    "MSCI Europe EUR": "Renta Variable/MSCI Europe Gross EUR - euros.csv",
    "MSCI Pacific EUR": "Renta Variable/MSCI Pacific (Net) - euros.csv",
    "MSCI World ex USA EUR": "Renta Variable/MSCI World ex USA Gross - euros.csv",
    "MSCI World Small Cap EUR": "Renta Variable/MSCI World Small Cap (Price version) - euros.csv",
    "MSCI US Small EUR": "Renta Variable/MSCI US Small Gross - euros.csv",
    "MSCI USA EUR": "Renta Variable/MSCI United States Broad Gross - euros.csv",
    "S&P 500 EUR": "Renta Variable/SP500 Gross - euros.csv",
    "Nasdaq 100 EUR": "Renta Variable/Nasdaq 100 TR  - euros.csv",
    "Nasdaq Composite EUR": "Renta Variable/Nasdaq Composite TR - euros.csv",
    "REITs Europa EUR": "Renta Variable/REITs Europa - euros.csv",
    "Small caps Europa EUR": "Renta Variable/Small caps Europa (STOXX Small EUR NR) - euros.csv",
    "Oro EUR": "Oro/Bloomberg Gold TR - euros.csv",
    "Eurozona Gov 10Y": "Bonos Gubernamentales/Datos históricos del TR Eurozone 10 Years Government Benchmark (1).csv",
    "Eurozona Gov 5Y": "Bonos Gubernamentales/Datos históricos del TR Eurozone 5 Years Government Benchmark (1).csv",
    "US Gov 10Y EUR": "Bonos Gubernamentales/Datos históricos del TR US 10 Year Government Benchmark (2)_convertido_EUR_desde_2000.csv",
}


def _parse_num(s: str) -> float | None:
    s = (s or "").strip().strip('"').replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> date | None:
    s = (s or "").strip().strip('"')
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _resolve(key_or_path: str) -> Path | None:
    rel = REGISTRY.get(key_or_path, key_or_path)
    p = BENCH / rel
    if p.exists():
        return p
    hits = glob.glob(str(BENCH / "**" / rel), recursive=True) or glob.glob(str(BENCH / "**" / f"*{rel}*"), recursive=True)
    return Path(hits[0]) if hits else None


def load_series(key_or_path: str) -> list[tuple[date, float]]:
    """[(fecha, valor)] ascendente por fecha. Vacío si no se encuentra."""
    p = _resolve(key_or_path)
    if not p:
        return []
    out = []
    with open(p, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)  # cabecera
        for row in reader:
            # Algunos CSVs vienen doble-comillados (toda la línea en un campo con
            # comillas internas dobladas) → csv.reader devuelve 1 campo; re-parsear.
            if len(row) == 1 and "," in row[0]:
                row = next(csv.reader([row[0]]), row)
            if len(row) < 2:
                continue
            d, v = _parse_date(row[0]), _parse_num(row[1])
            if d and v is not None:
                out.append((d, v))
    out.sort(key=lambda x: x[0])
    return out


def load_base100(key_or_path: str, desde: str | None = None) -> list[dict]:
    """Serie normalizada a base 100 desde la primera fecha >= `desde` (YYYY-MM-DD)."""
    serie = load_series(key_or_path)
    if not serie:
        return []
    start = _parse_date(desde) if desde else None
    if start:
        serie = [(d, v) for d, v in serie if d >= start] or serie
    base = serie[0][1]
    if not base:
        return []
    return [{"fecha": d.isoformat(), "valor": round(100 * v / base, 2)} for d, v in serie]


def pick_benchmark(fund_data: dict) -> str | None:
    """Elige el benchmark más adecuado por geografía/tipo del fondo. Heurística;
    se puede sobreescribir con data/benchmarks_map.json (ISIN->clave)."""
    import json
    isin = fund_data.get("isin", "")
    mp = ROOT / "data" / "benchmarks_map.json"
    if mp.exists():
        try:
            m = json.loads(mp.read_text(encoding="utf-8"))
            if m.get(isin):
                return m[isin]
        except Exception:
            pass
    # señales
    geo = " ".join(str(g.get("region", "")) for g in (fund_data.get("geographic_allocation") or [])).lower()
    pos = (fund_data.get("posiciones", {}) or {}).get("actuales", []) or []
    rf = sum(1 for p in pos if any(k in str(p.get("tipo", "")).upper() for k in ("RF", "BOND", "FIJA"))) / max(1, len(pos))
    nombre = (fund_data.get("nombre", "") + " " + str(fund_data.get("kpis", {}).get("benchmark", ""))).lower()
    if rf > 0.5 or "bond" in nombre or "renta fija" in nombre:
        return "Eurozona Gov 10Y"
    if "emerg" in nombre or "emerging" in nombre or "india" in nombre or "em " in geo:
        return "MSCI EM EUR"
    if "nasdaq" in nombre:
        return "Nasdaq 100 EUR"
    if "s&p" in nombre or "sp500" in nombre or "500" in nombre:
        return "S&P 500 EUR"
    if "pacíf" in nombre or "pacific" in nombre or "jap" in nombre or "japan" in geo:
        return "MSCI Pacific EUR"
    if "europe" in nombre or "europ" in nombre or "europe" in geo:
        return "MSCI Europe EUR"
    if "small" in nombre:
        return "MSCI World Small Cap EUR"
    return "MSCI World EUR"  # global por defecto


def main(argv=None) -> int:
    args = argv or sys.argv[1:]
    if "--list" in args:
        print(f"{'clave':26} filas  rango")
        for k in REGISTRY:
            s = load_series(k)
            rng = f"{s[0][0]} → {s[-1][0]}" if s else "NO ENCONTRADO"
            print(f"  {k:26} {len(s):>5}  {rng}")
        return 0
    print("uso: python -m tools.benchmark_data --list")
    return 0


if __name__ == "__main__":
    sys.exit(main())
