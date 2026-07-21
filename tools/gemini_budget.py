"""
gemini_budget.py — Tope de gasto de Google/Gemini con auto-reversión a Haiku.

Petición de Rafa (2026-07-20): usar Gemini Flash para la extracción masiva (barata),
PERO con un tope de gasto Google; "si vemos que no es como dices, hay que volver atrás".
Este módulo es el freno automático: si el gasto Gemini del mes supera el tope, o no hay
GOOGLE_API_KEY, `use_gemini_for_extraction()` devuelve False y el pipeline cae a Haiku.

Config (.env):
  GEMINI_EXTRACTION=1        # activa Gemini para extracción (default 0 = todo Haiku)
  GEMINI_MONTHLY_CAP_USD=5   # tope de gasto Gemini/mes; superado → Haiku (default 5)
  GOOGLE_API_KEY=...         # imprescindible; si falta, siempre Haiku

Decisión de routing (una función, uso en gemini_wrapper.extract_fast):
  use_gemini_for_extraction() -> bool
"""

from __future__ import annotations

import os


def _month_gemini_spend_usd() -> float:
    """Gasto Gemini (modelos 'gemini*') del mes en curso, desde el cost_monitor."""
    try:
        from datetime import date
        from tools.cost_monitor import _read_entries
        first = date.today().replace(day=1)
        tot = 0.0
        for e in _read_entries(since=first):
            if str(e.get("model", "")).lower().startswith("gemini"):
                tot += e.get("cost_usd", 0.0)
        return tot
    except Exception:
        return 0.0


def cap_usd() -> float:
    try:
        return float(os.environ.get("GEMINI_MONTHLY_CAP_USD", "5") or "5")
    except ValueError:
        return 5.0


def status() -> dict:
    key = bool(os.environ.get("GOOGLE_API_KEY", "").strip())
    enabled = os.environ.get("GEMINI_EXTRACTION", "0").strip() == "1"
    spent = _month_gemini_spend_usd()
    cap = cap_usd()
    over = spent >= cap
    return {
        "gemini_extraction_enabled": enabled,
        "google_api_key": key,
        "spent_month_usd": round(spent, 4),
        "cap_usd": cap,
        "over_cap": over,
        "usaria_gemini": enabled and key and not over,
        "motivo_haiku": (
            None if (enabled and key and not over)
            else ("GEMINI_EXTRACTION!=1" if not enabled
                  else "sin GOOGLE_API_KEY" if not key
                  else f"tope superado (${spent:.2f} >= ${cap:.2f})")
        ),
    }


def use_gemini_for_extraction() -> bool:
    """True solo si: activado + hay key + bajo el tope. Si no, el caller usa Haiku."""
    s = status()
    return bool(s["usaria_gemini"])


if __name__ == "__main__":
    import json
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    print(json.dumps(status(), ensure_ascii=False, indent=2))
