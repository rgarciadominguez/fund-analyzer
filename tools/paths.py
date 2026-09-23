"""Rutas del sistema que NO deben depender de la máquina (2026-09-23).

Por qué: el pipeline corre en el servidor (usuario `Usuario`) pero muchas tools llevaban rutas
fijas de la Surface (`C:\\Users\\RafaelGarcía\\...`). En el servidor eso apunta a un perfil que no
sincroniza con OneDrive → el Excel espejo, el catálogo del portal y el feed quant se escribían
donde nadie los ve (o fallaban). Regla: TODO se deriva del repo o de variables de entorno.

  ROOT        raíz del repo (…\\Asesoría Financiera\\fund-analyzer)
  AF_DIR      carpeta "Asesoría Financiera" (padre del repo) — Operativa, Mapfre, BDD…
  BDD_DIR     AF_DIR\\Operativa\\BDD          (Excel espejo, listados)
  HORFIN_DIR  datos de intercambio con el portal (catálogo, clases, métricas). Orden:
              $HORFIN_DATA_DIR → ~\\horizonte-datos si existe → ROOT\\data\\horfin (se crea).
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AF_DIR = ROOT.parent
BDD_DIR = AF_DIR / "Operativa" / "BDD"


def _horfin_dir() -> Path:
    env = os.environ.get("HORFIN_DATA_DIR")
    if env:
        p = Path(env); p.mkdir(parents=True, exist_ok=True); return p
    home = Path.home() / "horizonte-datos"
    if home.exists():
        return home
    legacy = Path(r"C:\Users\RafaelGarcía\horizonte-datos")
    if legacy.exists():
        return legacy
    p = ROOT / "data" / "horfin"
    p.mkdir(parents=True, exist_ok=True)
    return p


HORFIN_DIR = _horfin_dir()
