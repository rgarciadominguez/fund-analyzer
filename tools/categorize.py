"""categorize.py — Paso de CATEGORIZACIÓN manual de un fondo (fund-analyzer +
fund-dashboard) en un único sitio.

Marcas todos los campos relevantes (con el valor auto-derivado como default y las
opciones válidas a la vista), se guarda en data/funds/{ISIN}/funddash_meta.json
(que SIEMPRE manda sobre el auto-derivado) y se empuja al fund-dashboard.

Dos modos:
  - INTERACTIVO:  python -m tools.categorize --isin LU0203975437
       (te pregunta campo a campo; Enter = mantener el default mostrado)
  - POR FLAGS:    python -m tools.categorize --isin X --category Activo --assetType RV \
                     --geography Europa --currency Euro --issuer "" --className Limpia
  Añade --no-push para no sincronizar al fund-dashboard al terminar.

Campos (taxonomía exacta del fund-dashboard):
  category   : Activo | Indexado | Smart Beta | Mixto
  assetType  : RV | RF MP | RF LP | Mixto | Alternativo | Monetario
  geography  : Global | Europa | USA | España | Asia | Emergentes | Latam
  currency   : Euro | USD | GBP | JPY | Otra
  issuer     : Gubernamental | Corporativa | Mixto | High Yield | Inflación   (RF)
  className  : Limpia | Retail | Institucional | Otro
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.funddash_sync import build_meta, FUNDS

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

OPTS = {
    "category": ["Activo", "Indexado", "Smart Beta", "Mixto"],
    "assetType": ["RV", "RF MP", "RF LP", "Mixto", "Alternativo", "Monetario"],
    "geography": ["Global", "Europa", "USA", "España", "Asia", "Emergentes", "Latam"],
    "currency": ["Euro", "USD", "GBP", "JPY", "Otra"],
    "issuer": ["", "Gubernamental", "Corporativa", "Mixto", "High Yield", "Inflación"],
    "className": ["", "Limpia", "Retail", "Institucional", "Otro"],
}
FIELDS = ["name", "category", "assetType", "geography", "currency", "issuer", "className"]


def categorize(isin: str, flags: dict, interactive: bool) -> dict:
    derived = build_meta(isin) or {"isin": isin, "name": isin}
    ov_path = FUNDS / isin / "funddash_meta.json"
    current = {}
    if ov_path.exists():
        try:
            current = json.loads(ov_path.read_text(encoding="utf-8"))
        except Exception:
            current = {}
    result = {"isin": isin}
    for f in FIELDS:
        default = current.get(f) or derived.get(f) or ""
        if f in flags and flags[f] is not None:
            val = flags[f]
        elif interactive:
            opts = OPTS.get(f)
            tag = f"  [{'/'.join(o or '—' for o in opts)}]" if opts else ""
            raw = input(f"  {f:10} (def: {default!r}){tag}: ").strip()
            val = raw if raw else default
        else:
            val = default
        if f in OPTS and val and val not in OPTS[f]:
            print(f"  ⚠ '{val}' no es opción válida de {f} {OPTS[f]} — se guarda igual (custom).")
        result[f] = val
    # guardar funddash_meta.json (solo campos no vacíos, isin siempre)
    ov_path.parent.mkdir(parents=True, exist_ok=True)
    save = {k: v for k, v in result.items() if v or k == "isin"}
    ov_path.write_text(json.dumps(save, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--isin", required=True)
    for f in FIELDS:
        p.add_argument(f"--{f}")
    p.add_argument("--no-push", action="store_true")
    a = p.parse_args(argv)
    isin = a.isin.strip().upper()
    flags = {f: getattr(a, f) for f in FIELDS}
    interactive = not any(v is not None for v in flags.values())

    print(f"\n== Categorizar {isin} ==" + ("  (interactivo; Enter=default)" if interactive else "  (por flags)"))
    res = categorize(isin, flags, interactive)
    print("\nGuardado en funddash_meta.json:")
    for f in FIELDS:
        print(f"  {f:10} = {res.get(f)!r}")

    if not a.no_push:
        from tools.funddash_sync import sync, _repo_isins
        print("\nEmpujando al fund-dashboard…")
        sync(isin, _repo_isins(), dry=False)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
