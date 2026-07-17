"""
benchmark_vocab.py — Vocabulario canónico de `benchmark` (categorización Horizonte).

El campo `benchmark` NO es "el índice del folleto": es la CATEGORIZACIÓN de Horizonte.
Su vocabulario sale de `Activos y Bancos.csv` (columna Benchmark, 263 valores / 38 grafías).
Aquí se normaliza a ~30 valores canónicos (un concepto = una grafía) y se deja abierto:
el clasificador puede acuñar uno nuevo con wording similar si ningún canónico encaja.

Reglas de normalización aplicadas (documentadas, no silenciosas):
  - Case/typos:  EURIBOR→Euribor, sp500→SP500, MSCI Europa→MSCI Europe,
                 "Renta FIja"→"Renta Fija", "Meedio"→"Medio"
  - Mixtos: se canoniza a estilo RV-primero con "y"  →  "40% RV y 60% RF".
            Así "60% RF 40% RV" y "60% RF y 40% RV" colapsan en el mismo concepto
            (son la misma cartera escrita al revés).
  - Sinónimos: "Small Caps Europe"→"MSCI Europe Small Caps", "Renta Variable UK"→"RV UK"

CLI:
    python -m tools.benchmark_vocab            # imprime el vocabulario canónico
    python -m tools.benchmark_vocab --rebuild  # relee el CSV y regenera el JSON
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "benchmark_vocabulary.json"
CSV_HORFIN = Path(r"C:\Users\RafaelGarcía\horizonte-datos\Activos y Bancos (1).csv")

# grafía cruda (lower, sin espacios extra) -> canónica
ALIASES = {
    "euribor": "Euribor",
    "sp500": "SP500",
    "msci europa": "MSCI Europe",
    "msci europe": "MSCI Europe",
    "renta fija corto plazo": "Renta Fija Corto Plazo",
    "renta fija medio plazo": "Renta Fija Medio Plazo",
    "renta fija meedio plazo": "Renta Fija Medio Plazo",
    "renta fija largo plazo": "Renta Fija Largo Plazo",
    "renta fija high yield": "Renta Fija High Yield",
    "small caps europe": "MSCI Europe Small Caps",
    "msci europe small caps": "MSCI Europe Small Caps",
    "renta variable uk": "RV UK",
    "rv uk": "RV UK",
    # acuñados por el clasificador (se normalizan igual que los de Horizonte:
    # un concepto = una grafía, aunque el valor sea nuevo)
    "cat bonds": "Bonos Catástrofe",
    "bonos catástrofe": "Bonos Catástrofe",
    "bonos catastrofe": "Bonos Catástrofe",
    "renta fija nordics": "Renta Fija Nórdica",
    "renta fija nórdica": "Renta Fija Nórdica",
    # mixtos -> RV primero, separador "y"
    "75% rv y 25% rf": "75% RV y 25% RF",
    "25% rv 75% rf": "25% RV y 75% RF",
    "75% rf y 25% rv": "25% RV y 75% RF",
    "75% rf 25% rv": "25% RV y 75% RF",
    "40% rv y 60% rf": "40% RV y 60% RF",
    "60% rf 40% rv": "40% RV y 60% RF",
    "60% rf y 40% rv": "40% RV y 60% RF",
}


def canon(raw: str) -> str | None:
    """Normaliza una grafía cruda a la canónica. None si vacío."""
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return None
    return ALIASES.get(s.lower(), s)


def build(csv_path: Path = CSV_HORFIN) -> dict:
    with open(csv_path, encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    raw = [r["Benchmark"].strip() for r in rows if (r.get("Benchmark") or "").strip()]
    cnt = Counter(c for c in (canon(v) for v in raw) if c)
    return {
        "version": 1,
        "generated_at": "2026-07-17",
        "source": csv_path.name,
        "nota": (
            "Vocabulario CERRADO-PREFERENTE, no cerrado-estricto: el clasificador usa "
            "estos valores; si ninguno encaja acuña uno nuevo con wording similar."
        ),
        "n_valores_crudos": len(raw),
        "n_grafias_crudas": len(set(raw)),
        "n_canonicos": len(cnt),
        "vocabulario": [{"valor": v, "n_horfin": n} for v, n in cnt.most_common()],
        "aliases": ALIASES,
    }


def load() -> list[str]:
    """Lista de valores canónicos (para el clasificador)."""
    if not OUT.exists():
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(build(), ensure_ascii=False, indent=1), encoding="utf-8")
    d = json.loads(OUT.read_text(encoding="utf-8"))
    return [x["valor"] for x in d["vocabulario"]]


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    d = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{d['n_valores_crudos']} valores crudos -> {d['n_grafias_crudas']} grafías -> "
          f"{d['n_canonicos']} canónicos")
    print(f"escrito: {OUT}")
    for x in d["vocabulario"]:
        print(f"  {x['n_horfin']:3d}  {x['valor']}")
