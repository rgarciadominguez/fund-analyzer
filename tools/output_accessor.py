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


# ── KPIs específicos (atajos) ───────────────────────────────────────────────
def get_kpi_aum(output: dict):
    """AUM actual en M€."""
    return get_kpis(output).get("aum_actual_meur")


def get_kpi_participes(output: dict):
    return get_kpis(output).get("num_participes")


def get_kpi_ter(output: dict):
    return get_kpis(output).get("ter_pct")


def get_kpi_ter_efectivo(output: dict):
    return get_kpis(output).get("ter_efectivo_pct")


def get_kpi_coste_gestion(output: dict):
    return get_kpis(output).get("coste_gestion_pct")


def get_kpi_volatilidad(output: dict):
    return get_kpis(output).get("volatilidad_pct")


def get_kpi_clasificacion(output: dict) -> str:
    return get_kpis(output).get("clasificacion") or ""


def get_kpi_perfil_riesgo(output: dict):
    return get_kpis(output).get("perfil_riesgo")


def get_kpi_depositario(output: dict) -> str:
    return get_kpis(output).get("depositario") or ""


def get_kpi_divisa(output: dict) -> str:
    return get_kpis(output).get("divisa") or "EUR"


def get_kpi_fecha_registro(output: dict) -> str:
    return get_kpis(output).get("fecha_registro") or ""


def get_kpi_max_drawdown(output: dict):
    return get_kpis(output).get("max_drawdown_pct")


def get_kpi_rotacion(output: dict):
    return get_kpis(output).get("rotacion_cartera_pct")


def get_kpi_rating_morningstar(output: dict):
    return get_kpis(output).get("rating_morningstar")


def get_kpi_srri(output: dict):
    """SRRI / Perfil riesgo escala UCITS."""
    return get_kpis(output).get("srri") or get_kpis(output).get("perfil_riesgo")


# ── GESTORES (el caso que más sufre) ────────────────────────────────────────
def get_perfiles(output: dict) -> list:
    """Perfiles de gestores con detalle (trayectoria, cargo, etc.).
    PATH CANÓNICO ÚNICO: analyst_synthesis.gestores.perfiles

    NO hace fallback al top-level — el top-level output.gestores.perfiles
    contiene info CRUDA por fuente web (1 entry = 1 URL) del manager_deep_agent,
    no perfiles de personas. Mezclarlos confunde al lector.

    Si esta lista está vacía, significa: el analyst no encontró/generó perfiles
    ricos. Mostrar "Sin perfiles disponibles" honestamente."""
    val = _get(output, "analyst_synthesis.gestores.perfiles")
    return val if isinstance(val, list) else []


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


def get_serie_participes(output: dict) -> list:
    return _get(output, "cuantitativo.serie_participes") or []


def get_serie_ter_por_clase(output: dict) -> list:
    return _get(output, "cuantitativo.serie_ter_por_clase") or []


def get_serie_comisiones_por_clase(output: dict) -> list:
    return _get(output, "cuantitativo.serie_comisiones_por_clase") or []


def get_serie_rotacion(output: dict) -> list:
    return _get(output, "cuantitativo.serie_rotacion") or []


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


# ── CUALITATIVO + ANALYST EXTRA ─────────────────────────────────────────────
def get_cualitativo(output: dict) -> dict:
    """Datos cualitativos del fondo (filosofía, estrategia, objetivos)."""
    return _get(output, "cualitativo", "analyst_synthesis.cualitativo") or {}


def get_section_evolucion(output: dict) -> dict:
    return _get(output, "analyst_synthesis.evolucion") or {}


def get_section_fuentes_externas(output: dict) -> dict:
    return _get(output, "analyst_synthesis.fuentes_externas") or {}


def get_hechos_relevantes(output: dict) -> list:
    return _get(output, "hechos_relevantes", "analyst_synthesis.hechos_relevantes") or []


def get_lecturas_externas(output: dict) -> dict:
    return _get(output, "lecturas_externas") or {}


def get_analisis_consistencia(output: dict) -> dict:
    return _get(output, "analisis_consistencia") or {}


def get_comision_exito(output: dict) -> dict:
    return _get(output, "comision_exito") or {}


def get_anio_creacion(output: dict):
    return _get(output, "kpis.anio_creacion", "analyst_synthesis.kpis.anio_creacion")


def get_clases_info(output: dict) -> list:
    """Lista de clases del fondo con su info (divisa, inversion_minima, etc.)."""
    return _get(output, "cuantitativo.serie_clases_info", "clases_info") or []


# ── INT-específicos (cache interno del extractor v3) ────────────────────────
def get_int_clases(output: dict) -> list:
    """Lista de clases del extractor INT (clases con NAV per share)."""
    return _get(output, "_int_clases", "clases") or []


def get_int_gestores(output: dict) -> list:
    """Cache interno gestores INT (raw del extractor)."""
    return _get(output, "_int_gestores") or []


def get_int_cualitativo(output: dict) -> dict:
    """Cache interno cualitativo INT."""
    return _get(output, "_int_cualitativo") or {}


def get_economia_fondo(output: dict) -> dict:
    """Datos económicos INT (management fees, net result, etc.)."""
    return _get(output, "economia_fondo") or {}


def get_clases(output: dict) -> list:
    """Clases canónicas (top-level, INT). Para ES usar get_clases_info()."""
    return _get(output, "clases") or []


# ── Helpers compuestos (texto de secciones) ────────────────────────────────
def get_resumen_texto(output: dict) -> str:
    return get_section_resumen(output).get("texto", "") or ""


def get_historia_texto(output: dict) -> str:
    return get_section_historia(output).get("texto", "") or ""


def get_estrategia_texto(output: dict) -> str:
    return get_section_estrategia(output).get("texto", "") or ""


def get_cartera_texto(output: dict) -> str:
    return get_section_cartera(output).get("texto", "") or ""


def get_gestores_texto(output: dict) -> str:
    """Texto narrativo de la sección gestores (no perfiles)."""
    return _get(output, "analyst_synthesis.gestores.texto") or ""


def get_evolucion_texto(output: dict) -> str:
    return get_section_evolucion(output).get("texto", "") or ""


# ── Modo strict para tests ─────────────────────────────────────────────────
class CanonicalPathEmpty(KeyError):
    """Raised por getters strict cuando path canónico está vacío."""
    pass


def get_perfiles_strict(output: dict) -> list:
    """Versión strict de get_perfiles. Falla si vacío. Para tests."""
    val = get_perfiles(output)
    if not val:
        raise CanonicalPathEmpty("analyst_synthesis.gestores.perfiles vacío")
    return val


def get_kpi_aum_strict(output: dict):
    val = get_kpi_aum(output)
    if val is None:
        raise CanonicalPathEmpty("kpis.aum_actual_meur vacío")
    return val


# ── Helper de inspección ───────────────────────────────────────────────────
def audit_output(output: dict) -> dict:
    """Devuelve resumen de TODOS los campos canónicos con valores resumidos.
    Útil para diagnóstico interactivo y CLI."""
    perfiles = get_perfiles(output)
    return {
        "isin": get_isin(output),
        "nombre": get_nombre(output),
        "gestora": get_gestora(output),
        "tipo": get_tipo(output),
        "ultima_actualizacion": get_ultima_actualizacion(output),
        "kpis_aum_meur": get_kpi_aum(output),
        "kpis_participes": get_kpi_participes(output),
        "kpis_ter": get_kpi_ter(output),
        "kpis_clasificacion": get_kpi_clasificacion(output),
        "kpis_perfil_riesgo": get_kpi_perfil_riesgo(output),
        "kpis_depositario": get_kpi_depositario(output),
        "kpis_divisa": get_kpi_divisa(output),
        "kpis_anio_creacion": get_anio_creacion(output),
        "n_perfiles": len(perfiles),
        "perfiles_nombres": [p.get("nombre", "?") for p in perfiles[:5]],
        "n_posiciones_actuales": len(get_posiciones_actuales(output)),
        "n_posiciones_historicas": len(get_posiciones_historicas(output)),
        "n_serie_aum": len(get_serie_aum(output)),
        "n_serie_rentabilidad": len(get_serie_rentabilidad(output)),
        "n_serie_ter": len(get_serie_ter(output)),
        "n_serie_participes": len(get_serie_participes(output)),
        "n_clases_info": len(get_clases_info(output)),
        "n_int_clases": len(get_int_clases(output)),
        "n_int_gestores": len(get_int_gestores(output)),
        "resumen_chars": len(get_resumen_texto(output)),
        "historia_chars": len(get_historia_texto(output)),
        "estrategia_chars": len(get_estrategia_texto(output)),
        "cartera_chars": len(get_cartera_texto(output)),
        "gestores_chars": len(get_gestores_texto(output)),
        "evolucion_chars": len(get_evolucion_texto(output)),
        "n_hechos_relevantes": len(get_hechos_relevantes(output)),
        "manual_edits": output.get("_manual_edits", []),
    }


# ── VALIDACIÓN reforzada (Fase C) ──────────────────────────────────────────
def detect_drift(output: dict) -> list:
    """Detecta inconsistencias entre paths duplicados / solapados.
    Devuelve lista de drifts encontrados, vacía si todo coherente.

    Checks:
    - Metadata duplicada (nombre/gestora/isin) entre top-level y analyst_synthesis
    - KPI AUM duplicado entre kpis y analyst_synthesis.kpis
    - Perfiles inconsistentes: top-level perfiles VS analyst_synthesis.perfiles con
      contenido distinto (caso del 2026-04-27)
    - _int_X cache poblado pero analyst_synthesis no procesó
    """
    drifts = []
    asy = output.get("analyst_synthesis") or {}

    # 1. Metadata pairs
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

    # 2. KPIs AUM duplicado (top.kpis.aum vs analyst_synthesis.kpis.aum)
    top_aum = (output.get("kpis") or {}).get("aum_actual_meur")
    asy_aum = (asy.get("kpis") or {}).get("aum_actual_meur")
    if top_aum and asy_aum and abs(float(top_aum) - float(asy_aum)) / max(float(top_aum), 0.1) > 0.05:
        drifts.append({
            "field": "kpis.aum_actual_meur",
            "top_level": top_aum,
            "analyst_synthesis": asy_aum,
            "diff_pct": round(abs(float(top_aum) - float(asy_aum)) / float(top_aum) * 100, 1),
        })

    # 3. Perfiles inconsistentes (caso 2026-04-27)
    top_perfiles = (output.get("gestores") or {}).get("perfiles") or []
    asy_perfiles = (asy.get("gestores") or {}).get("perfiles") or []
    # top-level perfiles suele ser cache web (1 entry/URL), no perfiles personas.
    # Drift solo si TOP tiene perfiles personas (con nombre persona) Y son distintos del analyst.
    if top_perfiles and asy_perfiles:
        top_names = {(p.get("nombre") or "").strip() for p in top_perfiles
                     if isinstance(p, dict) and p.get("nombre")}
        asy_names = {(p.get("nombre") or "").strip() for p in asy_perfiles
                     if isinstance(p, dict) and p.get("nombre")}
        only_top = top_names - asy_names
        only_asy = asy_names - top_names
        if only_top:  # nombres en top NO en analyst → posible drift de manager_profiler
            drifts.append({
                "field": "gestores.perfiles",
                "issue": f"top tiene {len(only_top)} nombres que analyst NO procesó: {list(only_top)[:3]}",
                "advice": "Re-ejecutar analyst para sintetizar estos perfiles, o revisar manualmente",
            })

    # 4. _int_X cache poblado pero analyst no procesó
    int_gestores = output.get("_int_gestores") or []
    if int_gestores and not asy_perfiles:
        drifts.append({
            "field": "_int_gestores",
            "issue": f"_int_gestores tiene {len(int_gestores)} pero analyst_synthesis.gestores.perfiles vacío",
            "advice": "Re-ejecutar analyst — pass INT no completado",
        })

    return drifts


def audit_all_funds() -> dict:
    """Recorre todos los fondos en data/funds/ y devuelve dict {isin: {audit + drifts}}."""
    from pathlib import Path
    import json
    root = Path(__file__).parent.parent / "data" / "funds"
    results = {}
    for fd in sorted(root.iterdir()):
        if not fd.is_dir():
            continue
        out = fd / "output.json"
        if not out.exists():
            continue
        try:
            d = json.loads(out.read_text(encoding="utf-8"))
            results[fd.name] = {
                "audit": audit_output(d),
                "drifts": detect_drift(d),
            }
        except Exception as exc:
            results[fd.name] = {"error": str(exc)}
    return results


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = sys.argv[1:]
    if "--audit-all" in args:
        all_audit = audit_all_funds()
        any_drift = False
        for isin, info in all_audit.items():
            if "error" in info:
                print(f"{isin}: ERROR {info['error']}")
                continue
            audit = info["audit"]
            drifts = info["drifts"]
            mark = " ⚠" if drifts else ""
            print(f"{isin}: {audit['nombre'][:40]:<40} | "
                  f"AUM={audit['kpis_aum_meur']} | "
                  f"perfiles={audit['n_perfiles']} | "
                  f"pos={audit['n_posiciones_actuales']}{mark}")
            for d in drifts:
                any_drift = True
                print(f"   DRIFT: {d}")
        if any_drift:
            print("\n⚠ Drifts encontrados — revisar manualmente.")
        else:
            print("\n✓ Sin drifts en ningún fondo.")
    elif "--audit" in args:
        idx = args.index("--audit")
        if idx + 1 >= len(args):
            print("Uso: python -m tools.output_accessor --audit <ISIN>")
            sys.exit(1)
        isin = args[idx + 1].strip().upper()
        p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
        output = json.loads(p.read_text(encoding="utf-8"))
        audit = audit_output(output)
        print(json.dumps(audit, ensure_ascii=False, indent=2))
        drifts = detect_drift(output)
        if drifts:
            print(f"\n⚠ Drifts detectados:")
            for d in drifts:
                print(f"  {d}")
    elif args:
        # Backward compat: <ISIN> sin flag = como antes
        isin = args[0].strip().upper()
        p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
        output = json.loads(p.read_text(encoding="utf-8"))
        print(f"ISIN: {get_isin(output)}")
        print(f"Nombre: {get_nombre(output)}")
        print(f"Gestora: {get_gestora(output)}")
        print(f"AUM: {get_kpi_aum(output)}")
        print(f"Perfiles: {len(get_perfiles(output))}")
        print(f"Posiciones: {len(get_posiciones_actuales(output))}")
        print(f"Serie AUM: {len(get_serie_aum(output))} puntos")
        drifts = detect_drift(output)
        if drifts:
            print(f"\n⚠ Drifts detectados:")
            for d in drifts:
                print(f"  {d}")
    else:
        print("Uso:")
        print("  python -m tools.output_accessor <ISIN>             # info básica")
        print("  python -m tools.output_accessor --audit <ISIN>     # audit detallado JSON")
        print("  python -m tools.output_accessor --audit-all        # tabla resumen + drifts")
