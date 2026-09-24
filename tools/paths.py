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


def _logs_local_dir() -> Path:
    """Logs de servicios que escriben SIN PARAR (poller, guardián, web_server): FUERA de OneDrive.
    Dentro de OneDrive, cada append compite con la sincronización y OneDrive fabrica copias de
    conflicto ("portal_analyze_worker-DESKTOP-0256PEV-454.log"): 470 copias y 3,6 GB en 4 semanas,
    que además retrasaban los ficheros que sí importan (queue_state, SKILL.md de tareas).
    Orden: FA_LOGS_DIR > %LOCALAPPDATA%/fund-analyzer/logs > <repo>/logs (último recurso)."""
    env = os.environ.get("FA_LOGS_DIR")
    if env:
        return Path(env)
    la = os.environ.get("LOCALAPPDATA")
    if la:
        return Path(la) / "fund-analyzer" / "logs"
    return ROOT / "logs"


LOGS_LOCAL_DIR = _logs_local_dir()


def rotate_if_big(path: Path, max_mb: int = 10, keep: int = 3) -> None:
    """Rotación con BORRADO: path → path.1 → … → path.keep; lo que sobra se elimina."""
    try:
        if not path.exists() or path.stat().st_size < max_mb * 1024 * 1024:
            return
        for i in range(keep, 0, -1):
            src = path.with_name(f"{path.name}.{i - 1}") if i > 1 else path
            dst = path.with_name(f"{path.name}.{i}")
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
    except Exception:
        pass
