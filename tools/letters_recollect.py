"""Vuelve a recolectar las cartas del gestor de un fondo SIN repetir la preparación completa.

Uso:  python -m tools.letters_recollect ISIN

Por qué (25-sep-2026): la skill letters-sourcing-cowork registra cartas nuevas en la base de conocimiento y
ensure_kb_letters las descarga como documentos de discovery, pero LettersCollector ya había corrido en la
prep (paso 1) y letters_data.json no las incluía hasta el run siguiente. Este comando ejecuta solo el
colector (que fusiona con lo existente y nunca pierde cartas) con los mismos datos de identidad que usa el
orchestrator: nombre, gestora y año de creación desde output.json / cnmv_data.json / intl_data.json.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _identity(isin: str) -> tuple[str, str, int | None]:
    fd = ROOT / "data" / "funds" / isin
    nombre, gestora, anio = "", "", None
    for name in ("output.json", "cnmv_data.json", "intl_data.json", "cssf_data.json"):
        p = fd / name
        if not p.exists():
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        nombre = nombre or d.get("nombre") or d.get("nombre_oficial") or ""
        gestora = gestora or d.get("gestora") or d.get("gestora_oficial") or ""
        anio = anio or (d.get("kpis") or {}).get("anio_creacion")
    if not anio:
        try:
            from tools.fund_age import launch_year  # type: ignore
            anio = launch_year(isin)
        except Exception:
            anio = None
    return nombre, gestora, anio


def recollect(isin: str) -> dict:
    # Sin el .env no hay ANTHROPIC_API_KEY: con GEMINI_DISABLED=1 el killswitch se queda sin
    # fallback y _extract_commentary devuelve [] para TODAS las cartas (silencioso: solo un WARN
    # por carta). Se cargaba en el orchestrator, pero este comando se lanza suelto.
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except Exception:
        pass
    isin = isin.upper().strip()
    nombre, gestora, anio = _identity(isin)
    from agents.letters_collector import LettersCollector
    collector = LettersCollector(isin, fund_name=nombre, gestora=gestora, anio_creacion=anio)
    return asyncio.run(collector.run())


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 2:
        print("uso: python -m tools.letters_recollect ISIN")
        raise SystemExit(2)
    out = recollect(sys.argv[1])
    cartas = out.get("cartas") or []
    print(f"[letters_recollect] {sys.argv[1].upper()}: {len(cartas)} cartas · periodos "
          f"{sorted(c.get('periodo') or '' for c in cartas)} · sin carta: {out.get('anos_sin_carta')}")
