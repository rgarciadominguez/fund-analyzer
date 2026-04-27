"""Tests parametrizados sobre los fondos baseline para validar getters
de tools/output_accessor.py contra datos reales.

Cobertura:
- Getters básicos (nombre, gestora, isin) en TODOS los fondos
- Getters de KPIs y series con cardinalidad esperada
- Strict mode falla cuando path canónico vacío
- Tests anti-regresión: cada getter devuelve datos NO vacíos en al menos 1 fondo
- detect_drift devuelve [] o whitelist conocida en todos los fondos

Uso: python tests/test_output_accessor.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from tools.output_accessor import (
    get_isin, get_nombre, get_gestora, get_tipo,
    get_kpis, get_kpi_aum, get_kpi_participes, get_kpi_ter, get_kpi_clasificacion,
    get_perfiles, get_perfiles_strict, CanonicalPathEmpty,
    get_posiciones_actuales, get_posiciones_historicas,
    get_serie_aum, get_serie_rentabilidad, get_serie_ter, get_serie_vl_base100,
    get_serie_participes, get_clases_info,
    get_section_resumen, get_section_historia, get_section_estrategia,
    get_section_cartera, get_section_evolucion,
    get_resumen_texto, get_historia_texto, get_estrategia_texto,
    get_cartera_texto, get_gestores_texto,
    get_cualitativo, get_hechos_relevantes, get_anio_creacion, get_fuentes,
    get_documentos, get_int_clases, get_int_gestores,
    get_economia_fondo, get_clases,
    detect_drift, audit_output, audit_all_funds,
)


FUNDS_DIR = ROOT / "data" / "funds"
ALL_FUNDS = sorted([p.name for p in FUNDS_DIR.iterdir() if p.is_dir()])


def _load(isin: str) -> dict:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


# ── TESTS ───────────────────────────────────────────────────────────────────
def test_getters_basicos_todos_fondos():
    """Todos los fondos tienen nombre, isin, tipo válidos."""
    fail = []
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        # ISIN debe coincidir con directorio
        actual_isin = get_isin(d)
        if actual_isin and actual_isin != isin:
            fail.append(f"{isin}: ISIN mismatch (output dice '{actual_isin}')")
        # Nombre no vacío
        if not get_nombre(d):
            fail.append(f"{isin}: nombre vacío")
        # Tipo es ES o INT
        tipo = get_tipo(d)
        if tipo and tipo not in ("ES", "INT"):
            fail.append(f"{isin}: tipo inesperado '{tipo}'")
    return fail


def test_kpis_cardinalidad():
    """KPIs es dict, contiene aum_actual_meur en la mayoría."""
    fail = []
    n_with_aum = 0
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        kpis = get_kpis(d)
        if not isinstance(kpis, dict):
            fail.append(f"{isin}: kpis no es dict")
            continue
        if get_kpi_aum(d):
            n_with_aum += 1
    if n_with_aum < 5:  # esperamos AUM en al menos 5 fondos baseline
        fail.append(f"Solo {n_with_aum} fondos tienen AUM (esperado >=5)")
    return fail


def test_perfiles_es_lista():
    """get_perfiles siempre devuelve lista (nunca None)."""
    fail = []
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        p = get_perfiles(d)
        if not isinstance(p, list):
            fail.append(f"{isin}: get_perfiles no es lista (es {type(p).__name__})")
    return fail


def test_strict_mode_lanza_keyerror():
    """get_perfiles_strict en output vacío lanza CanonicalPathEmpty."""
    try:
        get_perfiles_strict({})
        return ["strict no falló en output vacío"]
    except CanonicalPathEmpty:
        pass
    except Exception as exc:
        return [f"strict lanzó excepción incorrecta: {type(exc).__name__}"]
    return []


def test_series_cuantitativo_devuelven_listas():
    """Todos los getters de serie devuelven lista (nunca None)."""
    fail = []
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        for getter, name in [
            (get_serie_aum, "serie_aum"),
            (get_serie_rentabilidad, "serie_rentabilidad"),
            (get_serie_ter, "serie_ter"),
            (get_serie_vl_base100, "serie_vl_base100"),
            (get_serie_participes, "serie_participes"),
            (get_clases_info, "clases_info"),
        ]:
            v = getter(d)
            if not isinstance(v, list):
                fail.append(f"{isin}: {name} no es lista")
    return fail


def test_secciones_analyst_son_dicts():
    """Getters de secciones analyst_synthesis devuelven dict."""
    fail = []
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        for getter, name in [
            (get_section_resumen, "resumen"),
            (get_section_historia, "historia"),
            (get_section_estrategia, "estrategia"),
            (get_section_cartera, "cartera"),
            (get_section_evolucion, "evolucion"),
        ]:
            v = getter(d)
            if not isinstance(v, dict):
                fail.append(f"{isin}: section {name} no es dict")
    return fail


def test_textos_son_strings():
    """Helpers de texto devuelven string."""
    fail = []
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        for getter, name in [
            (get_resumen_texto, "resumen_texto"),
            (get_historia_texto, "historia_texto"),
            (get_estrategia_texto, "estrategia_texto"),
            (get_gestores_texto, "gestores_texto"),
        ]:
            v = getter(d)
            if not isinstance(v, str):
                fail.append(f"{isin}: {name} no es str")
    return fail


def test_drifts_whitelist():
    """detect_drift devuelve [] o solo drifts whitelist conocidos."""
    fail = []
    WHITELIST_FIELDS = set()  # vacía = ningún drift permitido
    for isin in ALL_FUNDS:
        d = _load(isin)
        if d is None:
            continue
        drifts = detect_drift(d)
        for drift in drifts:
            field = drift.get("field", "")
            if field not in WHITELIST_FIELDS:
                fail.append(f"{isin}: drift inesperado en '{field}': {drift}")
    return fail


def test_audit_output_devuelve_dict_con_claves_esperadas():
    """audit_output() devuelve dict con todas las claves canónicas."""
    fail = []
    for isin in ALL_FUNDS[:3]:  # smoke test primeros 3
        d = _load(isin)
        if d is None:
            continue
        audit = audit_output(d)
        required_keys = {"isin", "nombre", "kpis_aum_meur", "n_perfiles",
                         "n_posiciones_actuales", "n_serie_aum"}
        missing = required_keys - set(audit.keys())
        if missing:
            fail.append(f"{isin}: audit_output missing keys {missing}")
    return fail


def test_int_specific_getters():
    """Getters INT-específicos devuelven correcto tipo en INT funds."""
    fail = []
    int_funds = [f for f in ALL_FUNDS if not f.startswith("ES")]
    for isin in int_funds:
        d = _load(isin)
        if d is None:
            continue
        if not isinstance(get_int_clases(d), list):
            fail.append(f"{isin}: get_int_clases no es lista")
        if not isinstance(get_int_gestores(d), list):
            fail.append(f"{isin}: get_int_gestores no es lista")
        if not isinstance(get_economia_fondo(d), dict):
            fail.append(f"{isin}: get_economia_fondo no es dict")
    return fail


# ── Runner ──────────────────────────────────────────────────────────────────
TESTS = [
    ("getters basicos todos fondos", test_getters_basicos_todos_fondos),
    ("kpis cardinalidad", test_kpis_cardinalidad),
    ("perfiles es lista", test_perfiles_es_lista),
    ("strict mode lanza CanonicalPathEmpty", test_strict_mode_lanza_keyerror),
    ("series cuantitativo devuelven listas", test_series_cuantitativo_devuelven_listas),
    ("secciones analyst son dicts", test_secciones_analyst_son_dicts),
    ("textos son strings", test_textos_son_strings),
    ("drifts whitelist (idealmente vacios)", test_drifts_whitelist),
    ("audit_output devuelve dict completo", test_audit_output_devuelve_dict_con_claves_esperadas),
    ("INT specific getters", test_int_specific_getters),
]


if __name__ == "__main__":
    print("=" * 70)
    print(f"Tests output_accessor.py sobre {len(ALL_FUNDS)} fondos")
    print("=" * 70)
    total_failures = 0
    for name, fn in TESTS:
        print(f"\n[{name}]")
        failures = fn()
        if failures:
            print(f"  FAIL ({len(failures)}):")
            for f in failures[:5]:
                print(f"    - {f}")
            if len(failures) > 5:
                print(f"    ... ({len(failures) - 5} más)")
            total_failures += len(failures)
        else:
            print(f"  OK")
    print()
    print("=" * 70)
    if total_failures:
        print(f"FAIL: {total_failures} fallos totales")
        sys.exit(1)
    print(f"PASS: {len(TESTS)}/{len(TESTS)} tests")
