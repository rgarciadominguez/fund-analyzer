"""
Output Accessor — funciones canónicas para LEER campos de output.json.

Resuelve el bug observado el 2026-04-26:
  - diagnose_funds.py miraba `output.gestores.equipo` (vacío)
  - Los datos reales estaban en `output.analyst_synthesis.gestores.perfiles`
  - Resultado: dije "0 gestores" cuando había 4

Cada getter devuelve el path canónico, con fallback a paths secundarios para
compatibilidad con datos antiguos. Esto es la SINGLE SOURCE OF TRUTH para
"dónde leer X" en el sistema entero.

Si añades un campo nuevo: añade un getter aquí. Si ves código en otros
ficheros leyendo paths complejos en output.json, refactorízalos a usar
estos getters.

Uso:
    from tools.output_accessor import get_perfiles, get_kpis, get_posiciones
    perfiles = get_perfiles(output)
    aum = get_kpis(output).get("aum_actual_meur")
"""
from typing import Any


def _get(d: dict, *paths) -> Any:
    """Devuelve el primer valor no-vacío encontrado por los paths dados."""
    if not isinstance(d, dict):
        return None
    for path in paths:
        cur = d
        for part in path.split("."):
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(part)
        if cur is not None and cur not in ("", [], {}):
            return cur
    return None


# ── METADATA ────────────────────────────────────────────────────────────────
def get_isin(output: dict) -> str:
    return _get(output, "isin") or ""


def get_nombre(output: dict) -> str:
    """Nombre oficial del fondo (último PDF semestral CNMV)."""
    return _get(output, "nombre", "analyst_synthesis.nombre") or ""


def get_gestora(output: dict) -> str:
    return _get(output, "gestora", "analyst_synthesis.gestora") or ""


def get_tipo(output: dict) -> str:
    return _get(output, "tipo") or ""


def get_ultima_actualizacion(output: dict) -> str:
    return _get(output, "ultima_actualizacion") or ""


# ── KPIS ────────────────────────────────────────────────────────────────────
def get_kpis(output: dict) -> dict:
    """KPIs cuantitativos del fondo (AUM, TER, partícipes, etc.)."""
    return _get(output, "kpis", "analyst_synthesis.kpis") or {}


# ── GESTORES (el caso que más sufre) ────────────────────────────────────────
def get_perfiles(output: dict) -> list:
    """Perfiles de gestores con detalle (trayectoria, cargo, etc.).
    PATH CANÓNICO: analyst_synthesis.gestores.perfiles
    Es donde el analyst escribe los perfiles ricos. El path top-level
    output.gestores.equipo contiene SOLO los nombres crudos del manager_profiler."""
    return _get(output, "analyst_synthesis.gestores.perfiles", "gestores.perfiles") or []


def get_gestor_principal(output: dict) -> str:
    """Nombre del gestor principal (lead manager)."""
    perfiles = get_perfiles(output)
    if perfiles and isinstance(perfiles[0], dict):
        return perfiles[0].get("nombre", "")
    return _get(output, "analyst_synthesis.gestores.principal") or ""


def get_gestores_equipo_raw(output: dict) -> list:
    """Lista de NOMBRES crudos del manager_profiler (sin perfiles ricos).
    Útil para validar continuidad. Para info rica, usar get_perfiles()."""
    return _get(output, "gestores.equipo", "gestores.equipo_detalle_web") or []


# ── POSICIONES ──────────────────────────────────────────────────────────────
def get_posiciones_actuales(output: dict) -> list:
    """Posiciones actuales del fondo (cartera detallada)."""
    return _get(output, "posiciones.actuales") or []


def get_posiciones_historicas(output: dict) -> list:
    return _get(output, "posiciones.historicas") or []


# ── CUANTITATIVO ────────────────────────────────────────────────────────────
def get_serie_aum(output: dict) -> list:
    return _get(output, "cuantitativo.serie_aum") or []


def get_serie_ter(output: dict) -> list:
    return _get(output, "cuantitativo.serie_ter") or []


def get_serie_rentabilidad(output: dict) -> list:
    return _get(output, "cuantitativo.serie_rentabilidad") or []


def get_serie_vl_base100(output: dict) -> list:
    return _get(output, "cuantitativo.serie_vl_base100") or []


def get_mix_activos(output: dict) -> list:
    return _get(output, "cuantitativo.mix_activos_historico") or []


# ── SECCIONES ANALYST ────────────────────────────────────────────────────────
def get_section_resumen(output: dict) -> dict:
    return _get(output, "analyst_synthesis.resumen") or {}


def get_section_historia(output: dict) -> dict:
    return _get(output, "analyst_synthesis.historia") or {}


def get_section_estrategia(output: dict) -> dict:
    return _get(output, "analyst_synthesis.estrategia") or {}


def get_section_cartera(output: dict) -> dict:
    return _get(output, "analyst_synthesis.cartera") or {}


# ── FUENTES + EVIDENCIA ─────────────────────────────────────────────────────
def get_fuentes(output: dict) -> dict:
    return _get(output, "fuentes") or {}


def get_documentos(output: dict) -> dict:
    return _get(output, "analyst_synthesis.documentos") or {}


# ── VALIDACIÓN ─────────────────────────────────────────────────────────────
def detect_drift(output: dict) -> list:
    """Detecta inconsistencias entre paths duplicados (top-level vs analyst_synthesis).
    Devuelve lista de drifts encontrados, vacía si todo coherente."""
    drifts = []
    asy = output.get("analyst_synthesis") or {}

    pairs = [
        ("nombre", asy.get("nombre")),
        ("gestora", asy.get("gestora")),
        ("isin", asy.get("isin")),
    ]
    for top_key, asy_val in pairs:
        top_val = output.get(top_key)
        if top_val and asy_val and top_val != asy_val:
            drifts.append({
                "field": top_key,
                "top_level": str(top_val)[:50],
                "analyst_synthesis": str(asy_val)[:50],
            })
    return drifts


if __name__ == "__main__":
    import json, sys
    if len(sys.argv) < 2:
        print("Uso: python -m tools.output_accessor <ISIN>")
        sys.exit(1)
    isin = sys.argv[1].strip().upper()
    from pathlib import Path
    p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
    output = json.loads(p.read_text(encoding="utf-8"))
    print(f"ISIN: {get_isin(output)}")
    print(f"Nombre: {get_nombre(output)}")
    print(f"Gestora: {get_gestora(output)}")
    print(f"AUM: {get_kpis(output).get('aum_actual_meur')}")
    print(f"Perfiles: {len(get_perfiles(output))}")
    print(f"Posiciones: {len(get_posiciones_actuales(output))}")
    print(f"Serie AUM: {len(get_serie_aum(output))} puntos")
    drifts = detect_drift(output)
    if drifts:
        print(f"\n⚠ Drifts detectados: {drifts}")
