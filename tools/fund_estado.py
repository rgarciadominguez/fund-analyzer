"""
fund_estado.py — Estado `cerrado`/`pendiente` por fondo (contrato v2, regla permanente).

Regla que Rafa quiere fija PARA SIEMPRE:
  Un fondo analizado está CERRADO. Un reanálisis general NO cambia nada de un fondo cerrado
  (ni clasificación, ni textos, ni benchmark). La revisión anual la hace Rafa a mano; volver
  a pasar el análisis nunca recategoriza solo.

Implementación:
  - Registro `data/fund_estado.json`: {isin: "cerrado" | "pendiente"}.
  - Default (si el ISIN no está en el registro): `cerrado` si el fondo está analizado
    (tiene análisis cualitativo o grupo analizado), `pendiente` si aún no.
  - Rafa puede marcar un fondo `pendiente` a mano (en el registro) para AUTORIZAR que el
    siguiente reanálisis lo recategorice. Es el único modo de reabrir un fondo cerrado.
  - El pipeline consulta `is_cerrado(isin)` y SALTA la re-generación para cerrados.
  - El export expone el campo `estado` para que Rafa vea sobre qué fondos revisa.

CLI:
    python -m tools.fund_estado --seed          # siembra el registro desde el análisis
    python -m tools.fund_estado --set ISIN pendiente
    python -m tools.fund_estado --show ISIN
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "data" / "fund_estado.json"


def _load() -> dict:
    if REGISTRY.exists():
        try:
            return json.loads(REGISTRY.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(d: dict) -> None:
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY.write_text(json.dumps(d, ensure_ascii=False, indent=1, sort_keys=True),
                        encoding="utf-8")


def default_estado(row: dict) -> str:
    """Default cuando el ISIN no está en el registro: analizado → cerrado."""
    analizado = bool(row.get("has_qualitative_analysis") or row.get("grupo_analizado"))
    return "cerrado" if analizado else "pendiente"


def get_estado(isin: str, row: dict | None = None) -> str:
    reg = _load()
    if isin in reg:
        return reg[isin]
    return default_estado(row or {})


def is_cerrado(isin: str, row: dict | None = None) -> bool:
    """True si el fondo está cerrado → el reanálisis NO debe recategorizarlo."""
    return get_estado(isin, row) == "cerrado"


def is_cerrado_explicit(isin: str) -> bool:
    """True SOLO si el registro marca 'cerrado' explícitamente.

    Para el GUARD del sync: un fondo nuevo (no en el registro) o marcado 'pendiente'
    debe poder enriquecerse en su PRIMER análisis. Solo se salta si Rafa/seed lo cerró.
    Distinto de is_cerrado(), que aplica el default analizado→cerrado (para el export).
    """
    return _load().get(isin) == "cerrado"


def set_estado(isin: str, estado: str) -> None:
    assert estado in ("cerrado", "pendiente"), estado
    reg = _load()
    reg[isin] = estado
    _save(reg)


def seed_from_rows(rows: list[dict]) -> int:
    """Siembra el registro con el default de cada fila (no pisa lo ya puesto a mano)."""
    reg = _load()
    n = 0
    for r in rows:
        isin = r.get("isin")
        if isin and isin not in reg:
            reg[isin] = default_estado(r)
            n += 1
    _save(reg)
    return n


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", action="store_true")
    ap.add_argument("--set", nargs=2, metavar=("ISIN", "ESTADO"))
    ap.add_argument("--show", metavar="ISIN")
    a = ap.parse_args()
    if a.seed:
        exp = Path(r"C:\Users\RafaelGarcía\horizonte-datos\catalogo_supabase.json")
        rows = json.loads(exp.read_text(encoding="utf-8"))["activos"]
        n = seed_from_rows(rows)
        reg = _load()
        import collections
        print(f"sembrados {n} nuevos | total {len(reg)} |",
              dict(collections.Counter(reg.values())))
    elif a.set:
        set_estado(a.set[0], a.set[1])
        print(f"{a.set[0]} -> {a.set[1]}")
    elif a.show:
        print(a.show, "->", get_estado(a.show))
    else:
        ap.print_help()
