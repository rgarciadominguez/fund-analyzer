"""benchmarks_to_funddash.py — Sube los índices/indexados de data/benchmarks/ al
fund-dashboard (Supabase propia de esa herramienta: tabla `funds {isin, meta, rows}`),
para que se muestren como un fondo cuantitativo más (Categoría=Indexado).

Taxonomía del fund-dashboard (DP_META_OPTS):
  categoria: Indexado/Activo/Smart Beta/Mixto   -> "Indexado"
  tipoActivo: RV/RF MP/RF LP/Mixto/Alternativo/Monetario
  geografia: Global/Europa/USA/España/Asia/Emergentes/Latam
  divisa: Euro/USD/...   issuer: Gubernamental/...

rows = [{date:'YYYY-MM-DD', nav:float}] ; upsert por isin (Prefer merge-duplicates).

CLI:
  python -m tools.benchmarks_to_funddash --dry-run          # no escribe, lista lo que haría
  python -m tools.benchmarks_to_funddash --test             # sube solo 2 (MSCI World + SP500)
  python -m tools.benchmarks_to_funddash --benchmark-only   # solo carpeta Benchmark/
  python -m tools.benchmarks_to_funddash                    # todo
"""
from __future__ import annotations

import json
import re
import sys
import urllib.request
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
BDIR = ROOT / "data" / "benchmarks"

SB_URL = "https://dcnvdaaexyuhqvyrrkob.supabase.co"
SB_KEY = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6"
          "ImRjbnZkYWFleHl1aHF2eXJya29iIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQxOTQ1"
          "MDUsImV4cCI6MjA4OTc3MDUwNX0.MXp-zIRWIMfKfprSbzvIe3Z52o4swdNW2RK77SZHL-o")


def _geo(name: str) -> str:
    n = name.lower()
    if any(k in n for k in ("emerging", " em ", "em gross", "emergent")):
        return "Emergentes"
    if any(k in n for k in ("world", "acwi", "global", "developed")):
        return "Global"
    if any(k in n for k in ("europe", "europa", "stoxx", "eurozone")):
        return "Europa"
    if any(k in n for k in ("pacific", "japan", "japón", "asia")):
        return "Asia"
    if any(k in n for k in ("sp500", "s&p", "nasdaq", "us ", "united states", "us small", "us 3", "us 5", "us 10", "us 30")):
        return "USA"
    return "Global"


def _asset_issuer(name: str) -> tuple[str, str]:
    n = name.lower()
    if any(k in n for k in ("government", "gov ", "bond", "gilt", "treasury", "oat", "btp", "eurozone")):
        # plazo por nombre
        if any(k in n for k in ("3 year", "5 year", "3 years", "5 years", "floating", "short", "corto", "medio")):
            return "RF MP", "Gubernamental"
        return "RF LP", "Gubernamental"
    if any(k in n for k in ("gold", "oro", "futuros oro")):
        return "Alternativo", ""
    if "reit" in n:
        return "RV", ""  # REITs cotizan como RV en el dashboard
    return "RV", ""


def _currency(name: str) -> str:
    n = name.lower()
    if "convertido_eur" in n or "euros" in n:
        return "Euro"
    if "dolar" in n or "usd" in n:
        return "USD"
    if " us " in f" {n} " and ("government" in n or "year" in n):
        return "USD"  # índice TR de bonos US en puntos USD (sin convertir)
    return "Euro"


_ISIN_RE = re.compile(r"\b([A-Z]{2}[A-Z0-9]{9}\d)\b")


def derive_meta(path: Path, indexado: bool) -> dict:
    stem = path.stem
    m = _ISIN_RE.search(stem)
    isin = m.group(1) if m else None
    # nombre legible
    name = re.sub(r"^\d{4}\s*-\s*", "", stem)               # quita '2017 - '
    name = _ISIN_RE.sub("", name).strip(" -")               # quita ISIN del nombre
    name = re.sub(r"\s*-\s*(euros|dolares)$", "", name, flags=re.I).strip()
    # limpiar nombres de bonos ('Datos históricos del TR ... Benchmark (1)')
    name = re.sub(r"Datos hist[oó]ricos del TR\s*", "", name, flags=re.I)
    name = re.sub(r"\s*Government Benchmark", " Gov", name, flags=re.I)
    name = re.sub(r"\s*Benchmark\b", "", name, flags=re.I)
    name = re.sub(r"\s*\(\d+\)", "", name)
    name = re.sub(r"_convertido_EUR_desde_\d+", "", name, flags=re.I)
    name = re.sub(r"\s+", " ", name).strip(" -")
    asset, issuer = _asset_issuer(stem)
    cur = _currency(stem)
    if not isin:
        # ID sintético para índices puros
        slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()[:36]
        isin = f"IDX_{slug}_{cur}"
        display = f"{name} ({cur})"
    else:
        display = name
    return {
        "isin": isin,
        "name": display,
        "className": "Índice",
        "category": "Índice",    # categoría = Índice (filtrable en el selector de repo)
        "assetType": asset,
        "geography": _geo(stem),
        "currency": cur,
        "issuer": issuer,
    }


def build_rows(path: Path) -> list[dict]:
    """Serie a fin de mes (mensual) para mantener el payload ligero — el
    fund-dashboard calcula vol/drawdown sobre cierres mensuales. El último dato
    disponible (sea cual sea su día) se conserva siempre."""
    from tools.benchmark_data import load_series
    serie = load_series(str(path))
    if not serie:
        return []
    by_month = {}
    for d, v in serie:                       # serie viene ascendente
        by_month[(d.year, d.month)] = (d, v)  # se queda el último día de cada mes
    # el datapack del fund-dashboard lee r.value; incluimos value (+nav por compat)
    out = [{"date": d.isoformat(), "value": v, "nav": v} for d, v in by_month.values()]
    out.sort(key=lambda x: x["date"])
    return out


def _post(payload: dict) -> int:
    req = urllib.request.Request(
        SB_URL + "/rest/v1/funds",
        data=json.dumps(payload).encode("utf-8"),
        headers={"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY,
                 "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"},
        method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def _repo_isins() -> set:
    req = urllib.request.Request(SB_URL + "/rest/v1/funds?select=isin",
                                 headers={"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY})
    return {f["isin"] for f in json.load(urllib.request.urlopen(req, timeout=120))}


def iter_csvs(benchmark_only: bool):
    for p in sorted((BDIR / "Benchmark").rglob("*.csv")):
        yield p, False
    if not benchmark_only:
        for p in sorted((BDIR / "Indexados").rglob("*.csv")):
            yield p, True


def main(argv=None) -> int:
    args = argv or sys.argv[1:]
    dry = "--dry-run" in args
    test = "--test" in args
    bonly = "--benchmark-only" in args
    new_only = "--indexados-new" in args

    if new_only:
        items = [(p, True) for p in sorted((BDIR / "Indexados").rglob("*.csv"))]
        repo = _repo_isins()
        print(f"(repo tiene {len(repo)} ISINs; subo solo Indexados que NO estén)")
    else:
        items = list(iter_csvs(bonly))
        repo = set()
    if test:
        items = [(p, ix) for p, ix in items if re.search(r"MSCI World Gross EUE|SP500 Gross - euros", p.name)][:2]

    ok = skip = err = 0
    seen = set()
    for p, indexado in items:
        meta = derive_meta(p, indexado)
        isin = meta["isin"]
        if isin in seen:
            continue
        seen.add(isin)
        if new_only and (isin in repo or isin.startswith("IDX_")):
            continue  # ya existe en repo, o sin ISIN real (sintético) → no es "de los 8 nuevos"
        rows = build_rows(p)
        if len(rows) < 30:
            print(f"  SKIP (pocos datos {len(rows)}): {p.name}")
            skip += 1
            continue
        payload = {"isin": isin, "meta": meta, "rows": rows, "updated_at": "2026-06-11T00:00:00Z"}
        if dry:
            print(f"  [{meta['category']}/{meta['assetType']}/{meta['geography']}/{meta['currency']}] {isin:32} {meta['name'][:40]:40} rows={len(rows)}")
            ok += 1
            continue
        try:
            _post(payload)
            print(f"  OK {isin:32} {meta['name'][:38]:38} rows={len(rows)}")
            ok += 1
        except Exception as e:
            print(f"  ERR {isin}: {str(e)[:80]}")
            err += 1
    print(f"\n{'(dry-run) ' if dry else ''}procesados OK={ok} skip={skip} err={err}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
