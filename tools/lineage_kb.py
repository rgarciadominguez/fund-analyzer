"""lineage_kb.py — Acceso a data/fund_lineage.json (lineage/predecesor de fondos).

Un fondo puede existir ANTES de su vehículo legal actual (patrón MontLake: AMC 2018 → RAIF 2021 →
UCITS 2024). Este KB guarda esa cadena para track-record completo (quant), narrativa e histórico de
cartera. Ver [[portal-embeds-worker-fund-url]] no; ver CLAUDE.md §0.9 (track-record = serie NAV real
más larga, no la fecha de inicio legal).

API:
  get_record(isin) -> dict | {}          registro de lineage del fondo (o {} si no hay)
  has_predecessor(isin) -> bool
  predecessor_isins(isin) -> list[str]   ISINs de vehículos predecesores
  quant_series_isin(isin) -> str|None    clase cuya serie NAV cubre el histórico más largo
  upsert(record) -> None                 escribe/actualiza un registro (por el resolver)
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KB_PATH = ROOT / "data" / "fund_lineage.json"


def _load() -> dict:
    try:
        d = json.loads(KB_PATH.read_text(encoding="utf-8"))
    except Exception:
        d = {}
    d.setdefault("funds", {})
    d.setdefault("isins_map", {})
    return d


def _save(d: dict) -> None:
    KB_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")


def get_record(isin: str) -> dict:
    """Registro de lineage del fondo. Resuelve también por ISIN de un predecesor (isins_map)."""
    d = _load()
    isin = (isin or "").upper()
    if isin in d["funds"]:
        return d["funds"][isin]
    tgt = d["isins_map"].get(isin)
    return d["funds"].get(tgt, {}) if tgt else {}


def has_predecessor(isin: str) -> bool:
    return bool((get_record(isin) or {}).get("predecessors"))


def predecessor_isins(isin: str) -> list:
    rec = get_record(isin) or {}
    return [p.get("isin") for p in (rec.get("predecessors") or []) if p.get("isin")]


def quant_series_isin(isin: str):
    rec = get_record(isin) or {}
    return (rec.get("track_record") or {}).get("quant_series_isin")


def upsert(record: dict) -> None:
    """Inserta/actualiza un registro de lineage y su isins_map. `record` debe traer 'isin'."""
    isin = (record.get("isin") or "").upper()
    if not isin:
        raise ValueError("record sin 'isin'")
    d = _load()
    d["funds"][isin] = record
    for p in (record.get("predecessors") or []):
        pi = (p.get("isin") or "").upper()
        if pi:
            d["isins_map"][pi] = isin
    _save(d)


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) > 1:
        print(json.dumps(get_record(sys.argv[1]), ensure_ascii=False, indent=1))
    else:
        d = _load()
        print(f"{len(d['funds'])} fondos con lineage:")
        for i, r in d["funds"].items():
            n = len(r.get("predecessors") or [])
            print(f"  {i}: {r.get('strategy_name','?')} — {n} predecesor(es), "
                  f"track desde {(r.get('track_record') or {}).get('quant_series_start','?')}")
