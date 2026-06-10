"""ar_sourcing_queue.py — Cola de fondos que necesitan sourcing de AR por AGENTE.

Estrategia (Rafa, 2026-06-10): el agente Opus de sourcing es el canal de MÁXIMO
rendimiento (navega web gestora + Finect + swissfunddata + Wayback) pero caro
→ se usa UNA vez por fondo nuevo y se cachea en `known_annual_reports.json`.
Las vías baratas (KB + Finect) cubren los re-runs.

El extractor, tras intentar KB+Finect, si un fondo sigue con <2 AR locales,
lo ENCOLA aquí. Luego (sesión Claude o scheduled) se procesan los pendientes
lanzando el agente, que rellena la KB. No spawnea agentes desde el pipeline
(coste/latencia); solo flaggea.

CLI:
  python -m tools.ar_sourcing_queue            # lista pendientes
  python -m tools.ar_sourcing_queue --done ISIN  # marca resuelto
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUEUE = ROOT / "data" / "_ar_sourcing_queue.json"

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _load() -> dict:
    try:
        return json.loads(QUEUE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(d: dict) -> None:
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    QUEUE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")


def enqueue(isin: str, fund_name: str = "", gestora: str = "", ar_count: int = 0) -> None:
    """Marca que el fondo necesita sourcing de AR por agente (idempotente)."""
    d = _load()
    d[isin] = {"fund_name": fund_name, "gestora": gestora, "ar_count": ar_count,
               "status": "pending"}
    _save(d)


def mark_done(isin: str) -> None:
    d = _load()
    if isin in d:
        d[isin]["status"] = "done"
        _save(d)


def list_pending() -> list:
    return [{"isin": k, **v} for k, v in _load().items() if v.get("status") == "pending"]


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Cola de sourcing de AR por agente")
    ap.add_argument("--done", help="marca un ISIN como resuelto")
    args = ap.parse_args(argv)
    if args.done:
        mark_done(args.done.strip().upper())
        print(f"{args.done}: marcado done")
        return 0
    pend = list_pending()
    print(f"Pendientes de sourcing por agente: {len(pend)}")
    for p in pend:
        print(f"  {p['isin']}  {p.get('fund_name','')[:40]:40}  AR locales={p.get('ar_count')}  gestora={p.get('gestora','')[:25]}")
    if pend:
        print("\nPara resolver: lanzar agente Opus de sourcing por cada uno → rellenar")
        print("data/known_annual_reports.json, luego: python -m tools.ar_sourcing_queue --done ISIN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
