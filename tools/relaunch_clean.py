"""Limpieza para --relaunch (corte a mitad): retira SOLO lo que puede haber quedado a medias.

Antes el .bat hacía `rmdir /s /q extracted\\` y borraba manager_profile/letters/síntesis: tras un
cuelgue del watchdog en un annual_update (BNY 23-sep) eso rehacía horas de extracción (y cuota)
de un análisis bueno. Cada extract se escribe ENTERO al terminar, así que un corte solo deja, como
mucho, un JSON truncado: se eliminan los que no parsean y la skill de extracción (2b) salta los
que existen. Las salidas LLM de una fase (manager/letters/síntesis) se rehacen igual que antes.

Uso: python -m tools.relaunch_clean ISIN
"""
from __future__ import annotations

import glob
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main(isin: str) -> int:
    fd = ROOT / "data" / "funds" / isin.upper()
    n_bad = 0
    for f in glob.glob(str(fd / "extracted" / "*.json")):
        try:
            json.load(open(f, encoding="utf-8"))
        except Exception:
            try:
                os.remove(f)
                n_bad += 1
                print(f"[RELAUNCH] extract corrupto eliminado: {os.path.basename(f)}")
            except Exception:
                pass
    # Solo la salida LLM de la FASE FINAL. manager_profile.json y letters_data.json son salidas de la
    # PREP (manager_profiler / letters_collector): si se borran y el relaunch salta la prep
    # ("RESUME-SKIP prep ya hecho"), el fondo se queda sin cartas (BNY 23-sep: bundle re-export
    # "letters_data.json not found" y análisis publicado sin cartas).
    for name in ("analyst_synthesis_cowork.json",):
        p = fd / name
        if p.exists():
            try:
                p.unlink()
                print(f"[RELAUNCH] {name} eliminado (se rehace)")
            except Exception:
                pass
    n_ok = len(glob.glob(str(fd / "extracted" / "*.json")))
    print(f"[RELAUNCH] extractos conservados: {n_ok} | corruptos retirados: {n_bad}")
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else ""))
