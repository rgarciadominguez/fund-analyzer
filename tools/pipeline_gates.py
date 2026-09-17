"""pipeline_gates.py — decide, de forma DETERMINISTA e independiente del modo, si el bat debe
correr (o puede saltar) el extract y el analyst en un re-análisis con --resume.

Objetivo (petición de Rafa): al re-analizar hay que integrar SOLO lo nuevo SIN rehacer todo ni
ignorar docs nuevos. El bug de MontLake: el aportado estaba en pending_extraction pero el extract
se SALTÓ (por leer scope=full), así que el doc nunca se procesó.

Reglas:
  extract debe correr  → si hay tasks en pending_extraction.json SIN su extracted/{id}.json.
  analyst debe correr  → si el extracted/*.json más reciente es POSTERIOR a la síntesis previa
                          (analyst_synthesis_cowork.json / output.json) → hay datos nuevos que sintetizar.

CLI (para el bat):
  python -m tools.pipeline_gates --isin X --check extract   # imprime 1 (correr) / 0 (saltar)
  python -m tools.pipeline_gates --isin X --check analyst
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _fd(isin: str) -> Path:
    return ROOT / "data" / "funds" / isin.upper()


def pending_unextracted(isin: str) -> list[str]:
    """IDs de tasks del manifiesto que NO tienen su extracted/{id}.json (docs pendientes de extraer)."""
    fd = _fd(isin)
    pe = fd / "pending_extraction.json"
    ex = fd / "extracted"
    if not pe.exists():
        return []
    try:
        tasks = (json.loads(pe.read_text(encoding="utf-8")).get("tasks") or [])
    except Exception:
        return []
    out = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = t.get("id")
        if not tid:
            continue
        if not (ex / f"{tid}.json").exists():
            out.append(tid)
    return out


def aporte_sin_integrar(isin: str) -> bool:
    """True si hay un doc en raw/aportados/ cuyo dato aún NO está integrado en el análisis (su
    extract falta o es más nuevo que la síntesis). Se basa en la CARPETA (no en pending_extraction)
    para poder decidir el modo ANTES del reconcile/prep. Sirve para FORZAR modo=aporte (complementar)
    en un re-análisis, pase lo que pase con el scope — así un doc aportado nunca se ignora ni dispara
    un rehacer-desde-cero. Cuando ya está integrado, devuelve 0 (un re-análisis posterior es normal)."""
    fd = _fd(isin)
    apo = fd / "raw" / "aportados"
    if not apo.exists():
        return False
    # Id canónico (incluye la VERSIÓN del esquema): si el esquema cambió, el extract vigente no
    # existe todavía → "sin integrar" → se re-extrae y re-sintetiza solo.
    from tools.aportados import task_id_for
    ref_mtime = None
    for ref_name in ("analyst_synthesis_cowork.json", "output.json"):
        ref = fd / ref_name
        if ref.exists():
            ref_mtime = ref.stat().st_mtime
            break
    for p in apo.glob("*.pdf"):
        exf = fd / "extracted" / f"{task_id_for(p.name)}.json"
        if not exf.exists():
            return True  # aportado sin extraer → no integrado
        if ref_mtime is None or exf.stat().st_mtime > ref_mtime + 1:
            return True  # extracto del aportado más nuevo que la síntesis → no integrado
    return False


def analyst_stale(isin: str) -> bool:
    """True si hay extractos MÁS NUEVOS que la síntesis previa (datos nuevos sin sintetizar)."""
    fd = _fd(isin)
    ex = fd / "extracted"
    if not ex.exists():
        return False
    exts = [p for p in ex.glob("*.json") if p.name != "extraction_complete.json"]
    if not exts:
        return False
    newest = max(p.stat().st_mtime for p in exts)
    # referencia de "última síntesis": el cowork json si existe, si no el output.json
    for ref_name in ("analyst_synthesis_cowork.json", "output.json"):
        ref = fd / ref_name
        if ref.exists():
            return newest > ref.stat().st_mtime + 1  # +1s de margen
    return True  # hay extractos pero no hay síntesis → hay que sintetizar


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    ap.add_argument("--check", required=True, choices=["extract", "analyst", "aporte"])
    a = ap.parse_args(argv)
    if a.check == "extract":
        run = len(pending_unextracted(a.isin)) > 0
    elif a.check == "aporte":
        run = aporte_sin_integrar(a.isin)
    else:
        run = analyst_stale(a.isin)
    print("1" if run else "0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
