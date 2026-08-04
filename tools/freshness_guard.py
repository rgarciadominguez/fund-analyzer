"""
freshness_guard.py — Aserción de frescura de la serie NAV tras un análisis.

Formaliza lo que antes solo se logueaba como `rows=0`: si un fondo cubierto por Morningstar
sale con serie VACÍA o CONGELADA (última fecha vieja), el run NO es "verde": se marca
`completed_with_warnings` con motivo claro, visible en data/analysis_warnings.json (y el
catálogo lo puede mostrar). Regla: mejor avisar que dar por bueno un dato inexistente.

Uso desde el pipeline (sync_to_supabase):
    from tools.freshness_guard import check_and_record
    check_and_record(isin, log=log)
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WARNINGS = ROOT / "data" / "analysis_warnings.json"
FRESH_DAYS = 10          # tras análisis toleramos algo más que el canario (7d)
MIN_POINTS = 30


def check_serie(isin: str) -> dict:
    """{ok, motivo, n_puntos, ultima}. ok=False SOLO si el fondo está cubierto por Morningstar
    (screener devuelve secid) pero la serie sale vacía/congelada — el bug real. Si no está
    cubierto, ok=True (no es un fallo, es que Morningstar no lo tiene). Best-effort."""
    try:
        from tools.morningstar_quant import fetch_quant
        cubierto = bool((fetch_quant(isin) or {}).get("secid"))
    except Exception:
        cubierto = True  # ante la duda, chequeamos (no silenciamos)
    if not cubierto:
        return {"ok": True, "motivo": "no cubierto por Morningstar", "n_puntos": 0, "ultima": None}
    try:
        from tools.morningstar_daily import fetch_series
        s = fetch_series(isin)
    except Exception as e:
        return {"ok": False, "motivo": f"error fetch: {str(e)[:60]}", "n_puntos": 0, "ultima": None}
    n = len(s)
    if n < MIN_POINTS:
        return {"ok": False, "motivo": f"serie NAV vacía ({n} puntos)", "n_puntos": n, "ultima": None}
    try:
        ts = max(int(t) for t, _ in s)
        last = datetime.fromtimestamp(ts / 1000, timezone.utc)
        edad = (datetime.now(timezone.utc) - last).days
    except Exception:
        return {"ok": False, "motivo": "serie sin fecha válida", "n_puntos": n, "ultima": None}
    if edad > FRESH_DAYS:
        return {"ok": False, "motivo": f"serie NAV congelada (última hace {edad}d)",
                "n_puntos": n, "ultima": last.date().isoformat()}
    return {"ok": True, "motivo": "fresca", "n_puntos": n, "ultima": last.date().isoformat()}


def _load() -> dict:
    if WARNINGS.exists():
        try:
            return json.loads(WARNINGS.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(d: dict):
    WARNINGS.parent.mkdir(parents=True, exist_ok=True)
    WARNINGS.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")


def check_and_record(isin: str, log=None) -> dict:
    """Chequea la serie y registra/limpia el warning. Devuelve el resultado de check_serie."""
    r = check_serie(isin)
    reg = _load()
    if r["ok"]:
        # fresca → quitar warning previo si lo había
        if isin in reg:
            reg.pop(isin, None)
            _save(reg)
    else:
        reg[isin] = {
            "run_status": "completed_with_warnings",
            "motivo": r["motivo"],
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        _save(reg)
        msg = f"[SYNC] WARNING frescura: {isin} → {r['motivo']} (run NO verde)"
        if log:
            log(msg)
        else:
            print(msg)
    return r


def get_warning(isin: str) -> dict | None:
    return _load().get(isin)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    isin = sys.argv[1] if len(sys.argv) > 1 else "IE00B6T42S66"
    print(json.dumps(check_and_record(isin), ensure_ascii=False, indent=1))
