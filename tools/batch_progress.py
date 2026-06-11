"""batch_progress.py — Panel de progreso del re-run del batch INT/prioritarios.

Muestra, por fondo: estado del pipeline (hecho / corriendo / pendiente) y el
veredicto del estándar (PASS / ISSUES) vía validate_int_standard.

Uso:  python -m tools.batch_progress
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
LOGS = ROOT / "logs"

# Batch a re-correr. ★ = prioritario (captura Rafa 2026-06-11).
BATCH = [
    ("LU0203975437", "Robeco BP Global Premium (template)", False),
    ("LU0153585137", "Vontobel European Equity", True),
    ("LU0234681749", "Goldman Sachs Euro CORE Equity", True),
    ("IE00B8BPMF80", "Wellington Strategic European Eq", True),
    ("IE00BF5GGB04", "GAM Star Cat Bond", False),
    ("IE00BDR0JY05", "Ashoka WhiteOak India", True),
    ("LU3038481936", "HAMCO Global Value", False),
    ("LU0933684283", "EQUAM Global Value", True),
    ("ES0114104039", "Bankinter Índice Japón (ES)", True),
    ("LU0329355670", "Robeco QI (1)", True),
    ("LU1048590381", "Robeco QI (2)", True),
    ("LU1654173217", "Robeco QI (3)", True),
    ("LU1694789378", "DNCA Alpha Bonds", False),
    ("LU0168736675", "Sifter Fund Global", False),
    ("IE00B6T42S66", "Trojan Fund Ireland", False),
    ("LU0289214628", "JPM Europe Equity Plus", False),
    ("FR001400CEG4", "Sextant Quality Focus A", False),
    ("FR001400CEK6", "Sextant Quality Focus F", False),
]


def _state(isin: str) -> tuple[str, str]:
    """(estado_pipeline, veredicto)."""
    op = FUNDS / isin / "output.json"
    log = LOGS / f"batch_{isin}.log"
    running = log.exists() and "bat fin" not in log.read_text(encoding="utf-8", errors="ignore")
    if not op.exists():
        return ("🔄 corriendo" if running else "⏳ pendiente", "—")
    try:
        j = json.loads(op.read_text(encoding="utf-8"))
    except Exception:
        return ("⚠ output ilegible", "—")
    has_new = bool((j.get("analyst_synthesis", {}).get("estrategia", {}) or {}).get("resumen_general"))
    if not has_new:
        return ("🔄 corriendo" if running else "⏳ pendiente", "—")
    # re-run hecho → validar
    try:
        from tools.validate_int_standard import validate
        r = validate(isin)
        verdict = "✅ PASS" if r["pass"] else "⚠ " + "; ".join(
            i.split(":", 1)[-1].strip()[:34] for i in r["issues"][:2])
        return ("✓ hecho", verdict)
    except Exception as exc:
        return ("✓ hecho", f"? {str(exc)[:30]}")


def main() -> int:
    done = pas = 0
    print(f"\n  {'#':>2}  {'★':1} {'ISIN':14} {'fondo':34} {'pipeline':13} veredicto")
    print("  " + "─" * 92)
    for i, (isin, name, prio) in enumerate(BATCH, 1):
        st, vd = _state(isin)
        if st.startswith("✓"):
            done += 1
        if vd.startswith("✅"):
            pas += 1
        star = "★" if prio else " "
        print(f"  {i:>2}  {star} {isin:14} {name[:34]:34} {st:13} {vd}")
    print("  " + "─" * 92)
    print(f"  Hechos: {done}/{len(BATCH)}   ·   PASS: {pas}/{len(BATCH)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
