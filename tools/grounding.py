"""Verificación de 'grounding': que los GESTORES del análisis aparezcan en alguna
fuente cruda (no inventados por el LLM).

El anti-invención previo era solo prompt + un regex de auto-confesión: si el LLM
inventaba un gestor con confianza, nada lo detectaba. Esto cruza cada nombre de
`analyst_synthesis.gestores.perfiles` contra el texto de TODAS las fuentes crudas
(manager_profile, letters, readings, cnmv, gestora_resources). Un gestor que no
aparece en ninguna es 'no grounded' → probable invención.

Conservador (evita falsos positivos): un nombre está grounded si su nombre de pila
Y un apellido aparecen en el texto (acento-insensible). Solo gestores; es donde más
invención documentada hay.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"

_SOURCE_FILES = ("manager_profile.json", "letters_data.json", "readings_data.json",
                 "cnmv_data.json", "gestora_resources.json", "intl_data.json")


def _strip(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def _raw_text(isin: str) -> str:
    fd = FUNDS_DIR / isin
    parts = []
    for fn in _SOURCE_FILES:
        p = fd / fn
        if p.exists():
            try:
                parts.append(p.read_text(encoding="utf-8"))
            except Exception:
                pass
    return _strip(" \n ".join(parts))


_GENERIC_FIRST = {"equipo", "team", "gestores", "departamento", "comite", "grupo",
                  "the", "los", "las", "equation", "personal"}


def _is_person_name(name: str) -> bool:
    """Descarta descriptores genéricos ('Equipo de High Yield de Insight') que no
    son una persona a la que verificar."""
    toks = [t for t in re.split(r"[^a-záéíóúñ]+", _strip(name)) if len(t) > 1]
    if not toks:
        return False
    return toks[0] not in _GENERIC_FIRST


def _name_grounded(name: str, raw: str) -> bool:
    toks = [t for t in re.split(r"[^a-záéíóúñ]+", _strip(name)) if len(t) > 2]
    if not toks:
        return True  # sin tokens útiles → no juzgar
    if len(toks) == 1:
        return toks[0] in raw
    # nombre de pila (primero) Y algún apellido (resto) presentes
    given = toks[0]
    surnames = toks[1:]
    return given in raw and any(s in raw for s in surnames)


def ungrounded_gestores(isin: str, data: dict | None = None) -> list:
    """Nombres de gestores del análisis que NO aparecen en ninguna fuente cruda."""
    isin = (isin or "").upper().strip()
    if data is None:
        p = FUNDS_DIR / isin / "output.json"
        if not p.exists():
            return []
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
    try:
        from tools.output_accessor import get_perfiles
        perfiles = get_perfiles(data) or []
    except Exception:
        perfiles = (data.get("analyst_synthesis", {}).get("gestores", {}) or {}).get("perfiles", []) or []
    names = [p.get("nombre", "").strip() for p in perfiles
             if isinstance(p, dict) and p.get("nombre") and _is_person_name(p.get("nombre"))]
    if not names:
        return []
    raw = _raw_text(isin)
    if not raw:
        return []  # sin fuentes que cruzar → no marcar (no penalizar falta de datos)
    return [n for n in names if not _name_grounded(n, raw)]


def main():
    import sys
    if len(sys.argv) < 2:
        print("uso: python -m tools.grounding <ISIN>")
        return
    isin = sys.argv[1].upper()
    ung = ungrounded_gestores(isin)
    if ung:
        print(f"✗ {isin}: gestores NO grounded (¿inventados?): {ung}")
    else:
        print(f"✓ {isin}: todos los gestores aparecen en fuentes crudas")


if __name__ == "__main__":
    main()
