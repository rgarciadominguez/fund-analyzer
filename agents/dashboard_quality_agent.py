"""
Dashboard Quality Agent — motor de reglas declarativo.

Lee `data/quality_rules.json` y evalúa el output.json del fondo contra cada regla.
Genera `quality_report.json` con la lista de fallos detectados.

NO usa scores ni umbrales. Solo emite fallos. Cada fallo lleva:
  - seccion
  - problema (texto humano-legible)
  - agente_responsable (para que el orchestrator sepa a quién reagentar)
  - accion (qué hacer para corregirlo)

Las reglas son declarativas (JSON). Cada regla tiene un `check_type` que el motor
sabe interpretar. Para añadir reglas nuevas: editar `data/quality_rules.json`,
no este código.

Usage:
    python -m agents.dashboard_quality_agent ES0112231008
"""
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()
ROOT = Path(__file__).parent.parent
RULES_PATH = ROOT / "data" / "quality_rules.json"


# ═══════════════════════════════════════════════════════════════
# Helpers de acceso a campos anidados con notación tipo "a.b.c[0].d"
# ═══════════════════════════════════════════════════════════════

_INDEX_RX = re.compile(r"^([^\[]+)\[(\d+)\]$")


def _get_nested(data: dict, path: str):
    """Obtiene un valor anidado de un dict usando notación 'a.b.c[0].d'.
    Devuelve None si cualquier paso del camino no existe."""
    if not path:
        return None
    cur = data
    for part in path.split("."):
        if cur is None:
            return None
        m = _INDEX_RX.match(part)
        if m:
            key = m.group(1)
            idx = int(m.group(2))
            if not isinstance(cur, dict) or key not in cur:
                return None
            arr = cur.get(key)
            if not isinstance(arr, list) or idx >= len(arr):
                return None
            cur = arr[idx]
        else:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
    return cur


# ═══════════════════════════════════════════════════════════════
# Validadores por check_type
# ═══════════════════════════════════════════════════════════════

# Regex para "cifras concretas" en texto (porcentajes, importes, conteos)
_CIFRAS_RX = re.compile(r"\d+[.,]\d+\s*%|\d+\s*M€|\d+\s*partícipes|\d+\s*posiciones|\d+\s*años")


def _check_min_chars(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto en `field_path` debe tener al menos `value` caracteres."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    actual = len(text)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_count_array(rule: dict, data: dict) -> tuple[bool, dict]:
    """Lista en `field_path` debe tener al menos `value` elementos."""
    arr = _get_nested(data, rule["field_path"]) or []
    if not isinstance(arr, list):
        arr = []
    actual = len(arr)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_bold_headers(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto debe tener al menos `value` headers en negrita (líneas con **...**).
    Cuenta líneas que son completamente **bold**, no solo apariciones de **."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    headers = 0
    for para in text.split("\n\n"):
        ps = para.strip()
        if ps.startswith("**") and ps.endswith("**") and ps.count("**") == 2 and len(ps) > 4:
            headers += 1
        # También contar headers inline al inicio de párrafo
        elif ps.startswith("**") and "**" in ps[2:]:
            # patrón "**Título**: contenido"
            close = ps.find("**", 2)
            if close > 2:
                headers += 1
    actual = headers
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_cifras(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto debe contener al menos `value` cifras concretas (regex)."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    matches = _CIFRAS_RX.findall(text)
    actual = len(matches)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_must_contain_fund_name(rule: dict, data: dict) -> tuple[bool, dict]:
    """El texto debe mencionar el nombre del fondo (o su primera parte antes de coma)."""
    text = _get_nested(data, rule["field_path"]) or ""
    fund_name = data.get("nombre", "") or ""
    if not isinstance(text, str) or not fund_name:
        return True, {"fund_name": fund_name}  # no podemos comprobar
    short = fund_name.split(",")[0].strip().lower()
    text_l = text.lower()
    ok = (short and short in text_l) or fund_name.lower() in text_l
    return ok, {"fund_name": fund_name}


def _check_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """El campo `field_path` existe y no está vacío."""
    val = _get_nested(data, rule["field_path"])
    ok = bool(val) and val not in ("", [], {}, None)
    return ok, {}


def _check_nested_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """Igual que field_present, pero formatea con info del lead manager."""
    val = _get_nested(data, rule["field_path"])
    ok = bool(val) and val not in ("", [], {}, None)
    # Para problema_template con {lead_name}
    perfiles = _get_nested(data, "analyst_synthesis.gestores.perfiles") or []
    lead_name = perfiles[0].get("nombre", "") if perfiles else ""
    return ok, {"lead_name": lead_name}


def _check_nested_array_min(rule: dict, data: dict) -> tuple[bool, dict]:
    """Lista anidada (ej. perfiles[0].decisiones_clave) debe tener N items."""
    val = _get_nested(data, rule["field_path"]) or []
    if not isinstance(val, list):
        val = []
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_any_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """Al menos uno de los `field_paths` debe estar presente y no vacío."""
    for path in rule.get("field_paths", []):
        val = _get_nested(data, path)
        if val and val not in ("", [], {}, None):
            return True, {}
    return False, {}


def _check_no_bold_headers(rule: dict, data: dict) -> tuple[bool, dict]:
    """Text must NOT have standalone **bold** headers (subsections). Pure prose only."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        return True, {"actual": 0}
    headers = 0
    for para in text.split("\n\n"):
        ps = para.strip()
        if ps.startswith("**") and ps.endswith("**") and ps.count("**") == 2 and len(ps) > 4:
            headers += 1
    ok = headers <= rule.get("value", 0)  # value = max allowed headers (usually 0)
    return ok, {"actual": headers, "expected": rule.get("value", 0)}


def _check_min_chars_nested(rule: dict, data: dict) -> tuple[bool, dict]:
    """Nested field (e.g. perfiles[0].trayectoria) must have min chars."""
    val = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(val, str):
        val = ""
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    # Get name for template
    perfiles = _get_nested(data, "analyst_synthesis.gestores.perfiles") or []
    lead_name = perfiles[0].get("nombre", "") if perfiles else ""
    return ok, {"actual": actual, "expected": expected, "lead_name": lead_name}


def _check_has_quotes(rule: dict, data: dict) -> tuple[bool, dict]:
    """Section must have quotes array with min items."""
    val = _get_nested(data, rule["field_path"]) or []
    if not isinstance(val, list):
        val = []
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_has_field_in_hitos(rule: dict, data: dict) -> tuple[bool, dict]:
    """Each hito in array must have specific fields (e.g. contexto_mercado, decisiones, resultado)."""
    hitos = _get_nested(data, rule["field_path"]) or []
    if not isinstance(hitos, list) or not hitos:
        return False, {"actual": 0, "expected": rule.get("value", 1)}
    required_field = rule.get("required_field", "")
    count_with = sum(1 for h in hitos if isinstance(h, dict) and h.get(required_field))
    ok = count_with >= rule.get("value", 1)
    return ok, {"actual": count_with, "expected": rule.get("value", 1)}


def _check_serie_vl_valid(rule: dict, data: dict) -> tuple[bool, dict]:
    """La serie VL debe tener valores decimales realistas, no enteros repetidos ni saltos imposibles.
    Detecta corrupción: VLs todos iguales, VLs enteros 1.0/2.0/3.0, rentabilidad total >300% o <-80%."""
    vl_series = _get_nested(data, rule.get("field_path", "cuantitativo.serie_vl_base100")) or []
    if not isinstance(vl_series, list) or len(vl_series) < 2:
        return True, {"actual": "no_data"}  # scarcity rule handles this

    vls = [float(v.get("vl", 0) or 0) for v in vl_series if isinstance(v, dict)]
    if not vls or all(v == 0 for v in vls):
        return False, {"reason": "todos_ceros", "actual": "VLs=0"}

    # Patrón de corrupción 1: todos los VLs son el mismo entero (1.0 repetido)
    unique_vals = set(vls)
    if len(unique_vals) <= 2 and all(v == int(v) for v in unique_vals if v > 0):
        return False, {"reason": "enteros_repetidos", "actual": f"VLs={sorted(unique_vals)}"}

    # Patrón de corrupción 2: primer VL muy alto o muy bajo (corrupto)
    first_vl = vls[0]
    if first_vl > 200 or (first_vl > 0 and first_vl < 0.1):
        return False, {"reason": "primer_vl_anomalo", "actual": f"VL inicial={first_vl}"}

    # Patrón 3: rentabilidad total irreal (ningún fondo normal multiplica x3 en pocos años
    # ni cae más del 80% sin que sea noticia)
    base100 = [float(v.get("base100", 0) or 0) for v in vl_series if isinstance(v, dict)]
    if base100 and base100[0] > 0:
        final_ret = (base100[-1] / base100[0] - 1) * 100
        n_years = len(base100)
        # CAGR implícito
        if n_years > 1 and base100[-1] > 0:
            cagr = (base100[-1] / base100[0]) ** (1/n_years) - 1
            cagr_pct = cagr * 100
            # Detecta CAGR absurdo: >40% o <-30% sostenido
            if cagr_pct > 40 or cagr_pct < -30:
                return False, {"reason": "cagr_irreal", "actual": f"CAGR={cagr_pct:.1f}% en {n_years} años"}

    return True, {"actual": f"{len(vls)} VLs OK"}


def _check_equipo_not_generic(rule: dict, data: dict) -> tuple[bool, dict]:
    """El equipo gestor no debe ser un placeholder genérico tipo 'Equipo {gestora}'.
    Debe contener nombres reales de personas."""
    equipo = _get_nested(data, rule.get("field_path", "gestores.equipo")) or []
    if not isinstance(equipo, list) or not equipo:
        return True, {"actual": "empty"}  # scarcity rule handles this

    gestora = (data.get("gestora", "") or "").lower()
    for item in equipo:
        s = str(item).lower().strip()
        # Detecta patrones genéricos
        if s.startswith("equipo ") and gestora and gestora.split(",")[0] in s:
            return False, {"reason": "equipo_generico", "actual": str(item)}
        if s in ("equipo gestor", "gestor", "equipo"):
            return False, {"reason": "equipo_placeholder", "actual": str(item)}

    # Check si hay perfiles reales en synthesis que contradigan el equipo genérico
    perfiles = _get_nested(data, "analyst_synthesis.gestores.perfiles") or []
    if perfiles and equipo:
        # Si hay perfiles con nombres reales pero equipo no los refleja
        nombres_perfiles = [p.get("nombre", "") for p in perfiles if isinstance(p, dict)]
        nombres_perfiles = [n for n in nombres_perfiles if n]
        if nombres_perfiles:
            # equipo debería contener al menos uno de los nombres de los perfiles
            equipo_str = " ".join(str(e) for e in equipo).lower()
            if not any(n.split()[0].lower() in equipo_str for n in nombres_perfiles if n.split()):
                return False, {"reason": "equipo_desconectado_de_perfiles",
                                "actual": f"equipo={equipo} pero perfiles={nombres_perfiles}"}

    return True, {"actual": str(equipo)}


def _check_mix_activos_sum_100(rule: dict, data: dict) -> tuple[bool, dict]:
    """Cada periodo de mix_activos_historico debe sumar 95-105%.
    Tolerancia de 5pp para redondeos. Detecta extracción duplicada de subtotales."""
    mix = _get_nested(data, rule.get("field_path", "cuantitativo.mix_activos_historico")) or []
    if not isinstance(mix, list) or not mix:
        return True, {"actual": "no_data"}

    bad_periods = []
    for item in mix:
        if not isinstance(item, dict):
            continue
        periodo = item.get("periodo", "")
        rv = float(item.get("rv_pct", 0) or item.get("renta_variable_pct", 0) or 0)
        rf = float(item.get("renta_fija_pct", 0) or 0)
        liq = float(item.get("liquidez_pct", 0) or 0)
        otros = float(item.get("otros_pct", 0) or 0)
        dep = float(item.get("depositos_pct", 0) or 0)
        total = rv + rf + liq + otros + dep
        if total > 0 and (total < 95 or total > 105):
            bad_periods.append(f"{periodo}={total:.0f}%")

    if bad_periods:
        return False, {"reason": "suma_incorrecta",
                       "actual": ", ".join(bad_periods[:5]),
                       "count": len(bad_periods)}
    return True, {"actual": f"{len(mix)} periodos OK"}


def _check_mix_activos_no_over_100(rule: dict, data: dict) -> tuple[bool, dict]:
    """Ningún componente individual (RF, RV, LIQ) puede superar 100% del patrimonio.
    Detecta duplicación de filas (suma de subtotales + totales)."""
    mix = _get_nested(data, rule.get("field_path", "cuantitativo.mix_activos_historico")) or []
    if not isinstance(mix, list) or not mix:
        return True, {"actual": "no_data"}

    violations = []
    for item in mix:
        if not isinstance(item, dict):
            continue
        periodo = item.get("periodo", "")
        for comp_name, comp_keys in [
            ("RF", ["renta_fija_pct"]),
            ("RV", ["rv_pct", "renta_variable_pct"]),
            ("LIQ", ["liquidez_pct"]),
            ("OTROS", ["otros_pct"]),
        ]:
            val = 0
            for k in comp_keys:
                v = item.get(k)
                if v is not None:
                    val = float(v)
                    break
            if val > 105:  # 5pp tolerance
                violations.append(f"{periodo} {comp_name}={val:.0f}%")

    if violations:
        return False, {"reason": "componente_sobre_100",
                       "actual": ", ".join(violations[:5]),
                       "count": len(violations)}
    return True, {"actual": f"{len(mix)} periodos OK"}


def _check_posiciones_nombre_limpio(rule: dict, data: dict) -> tuple[bool, dict]:
    """El campo 'nombre' de cada posición no debe contener separadores tipo '|' o varios campos concatenados."""
    posiciones = _get_nested(data, rule.get("field_path", "posiciones.actuales")) or []
    if not isinstance(posiciones, list) or not posiciones:
        return True, {"actual": "no_data"}

    dirty = 0
    examples = []
    for p in posiciones:
        if not isinstance(p, dict):
            continue
        nombre = str(p.get("nombre", "") or "")
        # Pipe separator indica concatenación de campos
        if "|" in nombre:
            dirty += 1
            if len(examples) < 3:
                examples.append(nombre[:60])

    threshold = rule.get("value", 5)  # Max % de nombres sucios permitidos
    pct_dirty = (dirty / len(posiciones)) * 100 if posiciones else 0
    if pct_dirty > threshold:
        return False, {"reason": "nombres_con_separadores",
                       "actual": f"{dirty}/{len(posiciones)} ({pct_dirty:.0f}%) - ejemplos: {examples}",
                       "count": dirty}
    return True, {"actual": f"{len(posiciones)-dirty}/{len(posiciones)} OK"}


def _check_posiciones_sectores(rule: dict, data: dict) -> tuple[bool, dict]:
    """Al menos value% de posiciones deben tener campo 'sector' con valor real (no '?', '', 'N/A')."""
    posiciones = _get_nested(data, rule.get("field_path", "posiciones.actuales")) or []
    if not isinstance(posiciones, list) or not posiciones:
        return True, {"actual": "no_data"}

    with_sector = 0
    for p in posiciones:
        if not isinstance(p, dict):
            continue
        sector = str(p.get("sector", "") or "").strip()
        if sector and sector not in ("?", "-", "N/A", "n/a", "Unknown"):
            with_sector += 1

    threshold_pct = rule.get("value", 80)
    pct = (with_sector / len(posiciones)) * 100 if posiciones else 0
    if pct < threshold_pct:
        return False, {"reason": "sectores_ausentes",
                       "actual": f"{with_sector}/{len(posiciones)} ({pct:.0f}%) con sector",
                       "count": len(posiciones) - with_sector}
    return True, {"actual": f"{pct:.0f}% con sector"}


def _check_posiciones_historicas_cobertura(rule: dict, data: dict) -> tuple[bool, dict]:
    """Debe haber posiciones históricas para al menos value% de los años con datos AUM."""
    historicas = _get_nested(data, "posiciones.historicas") or []
    aum_series = _get_nested(data, "cuantitativo.serie_aum") or []

    if not aum_series:
        return True, {"actual": "no_aum_data"}

    aum_years = set(str(a.get("periodo", ""))[:4] for a in aum_series if isinstance(a, dict))
    aum_years.discard("")
    if not aum_years:
        return True, {"actual": "no_aum_years"}

    hist_years = set(str(h.get("periodo", ""))[:4] for h in historicas if isinstance(h, dict))
    hist_years.discard("")

    coverage = (len(hist_years & aum_years) / len(aum_years)) * 100 if aum_years else 0
    threshold = rule.get("value", 70)

    if coverage < threshold:
        missing = sorted(aum_years - hist_years)
        return False, {"reason": "cobertura_insuficiente",
                       "actual": f"{len(hist_years)}/{len(aum_years)} años ({coverage:.0f}%) - faltan: {missing[:8]}"}
    return True, {"actual": f"{coverage:.0f}% cobertura"}


def _check_fortalezas_no_contradict_riesgos(rule: dict, data: dict) -> tuple[bool, dict]:
    """Si los riesgos mencionan 'falta de datos/información histórica', las fortalezas
    NO deben citar cifras o datos de esos mismos aspectos. Detecta incoherencia interna."""
    fortalezas = _get_nested(data, "analyst_synthesis.resumen.fortalezas") or []
    riesgos = _get_nested(data, "analyst_synthesis.resumen.riesgos") or []
    if not fortalezas or not riesgos:
        return True, {"actual": "sin_datos"}

    # Detecta riesgos que admiten falta de datos
    data_gap_patterns = [
        "falta de información",
        "falta información",
        "falta de datos",
        "sin información",
        "no se ha facilitado",
        "no se ha proporcionado",
        "información histórica completa",
        "no hay datos",
        "datos no disponibles",
    ]

    riesgos_text = " ".join(str(r).lower() for r in riesgos)
    admits_gap = any(p in riesgos_text for p in data_gap_patterns)

    if not admits_gap:
        return True, {"actual": "no_gaps_admitted"}

    # Si admite gap, las fortalezas no deberían citar cifras precisas ni rentabilidades
    # (porque el analyst dijo que faltan datos)
    contradictions = []
    for f in fortalezas:
        f_str = str(f).lower()
        if re.search(r"\d+[,.]?\d*\s*%", f_str) and ("rentabilidad" in f_str or "retorno" in f_str or "rendimiento" in f_str):
            contradictions.append(str(f)[:80])

    if contradictions:
        return False, {"reason": "fortalezas_contradicen_riesgos",
                       "actual": f"Riesgos admiten falta de datos pero fortalezas citan rentabilidad: {contradictions[:2]}"}
    return True, {"actual": "coherente"}


def _check_clasificacion_vs_cartera(rule: dict, data: dict) -> tuple[bool, dict]:
    """La clasificación del fondo debe ser coherente con el mix real de la cartera actual.
    Si clasifica como RV pero RV<40% en cartera → contradicción."""
    clasif = str(_get_nested(data, "kpis.clasificacion") or "").lower()
    if not clasif:
        return True, {"actual": "no_clasificacion"}

    mix = _get_nested(data, "cuantitativo.mix_activos_historico") or []
    if not mix:
        return True, {"actual": "no_mix"}

    # Buscar el periodo más reciente
    latest = None
    latest_year = ""
    for item in mix:
        if isinstance(item, dict):
            periodo = str(item.get("periodo", ""))[:4]
            if periodo > latest_year:
                latest_year = periodo
                latest = item
    if not latest:
        return True, {"actual": "no_latest"}

    rv = float(latest.get("rv_pct", 0) or latest.get("renta_variable_pct", 0) or 0)
    rf = float(latest.get("renta_fija_pct", 0) or 0)

    # Normalizar si supera 100% (datos corruptos)
    total = rv + rf + float(latest.get("liquidez_pct", 0) or 0) + float(latest.get("otros_pct", 0) or 0)
    if total > 120:
        # Datos demasiado corruptos para verificar clasificación
        return True, {"actual": "mix_corrupto_omitido"}

    # Detectar palabras clave en clasificación
    is_rv_pure = ("renta variable" in clasif) and ("mixta" not in clasif and "mixto" not in clasif)
    is_rf_pure = ("renta fija" in clasif) and ("mixta" not in clasif and "mixto" not in clasif)
    is_mixta_rv = "renta variable mixta" in clasif or "rv mixta" in clasif
    is_mixta_rf = "renta fija mixta" in clasif or "rf mixta" in clasif

    if is_rv_pure and rv < 70:
        return False, {"reason": "rv_pure_sin_rv",
                       "actual": f"Clasificación '{clasif}' pero RV={rv:.0f}% en cartera"}
    if is_rf_pure and rf < 70:
        return False, {"reason": "rf_pure_sin_rf",
                       "actual": f"Clasificación '{clasif}' pero RF={rf:.0f}% en cartera"}
    if is_mixta_rv and rv < 30:
        return False, {"reason": "mixta_rv_con_poco_rv",
                       "actual": f"Clasificación '{clasif}' pero RV solo {rv:.0f}% (esperado >=30%)"}
    if is_mixta_rf and rf < 30:
        return False, {"reason": "mixta_rf_con_poco_rf",
                       "actual": f"Clasificación '{clasif}' pero RF solo {rf:.0f}% (esperado >=30%)"}

    return True, {"actual": "coherente"}


def _check_text_returns_match_data(rule: dict, data: dict) -> tuple[bool, dict]:
    """Detecta alucinaciones en el texto: si el texto menciona rentabilidades concretas
    (ej. '+15,4% en 2023'), deben cuadrar con serie_rentabilidad o serie_vl_base100.
    Fallo si el texto afirma una rentabilidad que no existe en los datos."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str) or len(text) < 100:
        return True, {"actual": "no_text"}

    # Extraer menciones "X% en YYYY" o "YYYY: X%"
    pattern1 = re.compile(r"(?:rentabilidad|rendimiento|retorno|subida|caída|bajada)[^.]{0,60}?([+-]?\d{1,3}[,.]?\d{0,2})\s*%[^.]{0,40}?(\b20\d{2}\b)", re.IGNORECASE)
    pattern2 = re.compile(r"\b(20\d{2})\b[^.]{0,40}?(?:rentabilidad|rendimiento|retorno|subida|caída|bajada)[^.]{0,40}?([+-]?\d{1,3}[,.]?\d{0,2})\s*%", re.IGNORECASE)

    mentions = []
    for m in pattern1.finditer(text):
        pct, year = m.group(1), m.group(2)
        mentions.append((year, pct.replace(",", ".")))
    for m in pattern2.finditer(text):
        year, pct = m.group(1), m.group(2)
        mentions.append((year, pct.replace(",", ".")))

    if not mentions:
        return True, {"actual": "no_mentions"}

    # Buscar rentabilidades reales en los datos
    real_returns = {}
    serie_rent = _get_nested(data, "cuantitativo.serie_rentabilidad") or []
    for item in serie_rent:
        if isinstance(item, dict):
            periodo = str(item.get("periodo", ""))
            rent = item.get("rentabilidad_pct")
            if periodo and rent is not None:
                real_returns[periodo[:4]] = float(rent)

    # Si no hay serie_rentabilidad, calcular de base100
    if not real_returns:
        vl_series = _get_nested(data, "cuantitativo.serie_vl_base100") or []
        base_by_year = {str(v.get("periodo", ""))[:4]: float(v.get("base100", 0) or 0)
                        for v in vl_series if isinstance(v, dict)}
        years_sorted = sorted(base_by_year.keys())
        for i in range(1, len(years_sorted)):
            y = years_sorted[i]
            prev_b = base_by_year[years_sorted[i-1]]
            curr_b = base_by_year[y]
            if prev_b > 0:
                real_returns[y] = (curr_b / prev_b - 1) * 100

    if not real_returns:
        return True, {"actual": "no_real_data_to_compare"}

    # Verificar menciones vs datos reales
    discrepancies = []
    for year, pct_str in mentions:
        try:
            claimed = float(pct_str)
        except ValueError:
            continue
        real = real_returns.get(year)
        if real is None:
            continue
        # Tolerancia: 3 puntos porcentuales
        if abs(claimed - real) > 3:
            discrepancies.append(f"{year}: texto dice {claimed}%, real {real:.1f}%")

    if discrepancies:
        return False, {"reason": "rentabilidades_inventadas",
                       "actual": " | ".join(discrepancies[:3])}

    return True, {"actual": f"{len(mentions)} menciones verificadas"}


# Registro de validadores
CHECK_REGISTRY = {
    "min_chars": _check_min_chars,
    "min_count_array": _check_min_count_array,
    "no_bold_headers": _check_no_bold_headers,
    "min_chars_nested": _check_min_chars_nested,
    "has_quotes": _check_has_quotes,
    "has_field_in_hitos": _check_has_field_in_hitos,
    "min_bold_headers": _check_min_bold_headers,
    "min_cifras": _check_min_cifras,
    "must_contain_fund_name": _check_must_contain_fund_name,
    "field_present": _check_field_present,
    "nested_field_present": _check_nested_field_present,
    "nested_array_min": _check_nested_array_min,
    "any_field_present": _check_any_field_present,
    "serie_vl_valid": _check_serie_vl_valid,
    "equipo_not_generic": _check_equipo_not_generic,
    "text_returns_match_data": _check_text_returns_match_data,
    "mix_activos_sum_100": _check_mix_activos_sum_100,
    "mix_activos_no_over_100": _check_mix_activos_no_over_100,
    "posiciones_nombre_limpio": _check_posiciones_nombre_limpio,
    "posiciones_sectores": _check_posiciones_sectores,
    "posiciones_historicas_cobertura": _check_posiciones_historicas_cobertura,
    "fortalezas_no_contradict_riesgos": _check_fortalezas_no_contradict_riesgos,
    "clasificacion_vs_cartera": _check_clasificacion_vs_cartera,
}


# ═══════════════════════════════════════════════════════════════
# Excepciones (applies_when)
# ═══════════════════════════════════════════════════════════════

def _rule_applies(rule: dict, data: dict) -> bool:
    """Evalúa la condición `applies_when` de una regla. Si no hay condición, aplica."""
    cond = rule.get("applies_when")
    if not cond:
        return True

    fund_name = (data.get("nombre", "") or "").lower()

    # fund_name_contains: aplica solo si el nombre contiene alguno de estos
    if "fund_name_contains" in cond:
        if not any(s.lower() in fund_name for s in cond["fund_name_contains"]):
            return False

    # fund_name_not_contains: NO aplica si el nombre contiene alguno
    if "fund_name_not_contains" in cond:
        if any(s.lower() in fund_name for s in cond["fund_name_not_contains"]):
            return False

    return True


# ═══════════════════════════════════════════════════════════════
# Agente principal
# ═══════════════════════════════════════════════════════════════

class DashboardQualityAgent:

    def __init__(self, isin: str):
        self.isin = isin.strip().upper()
        self.fund_dir = ROOT / "data" / "funds" / self.isin
        self.rules = self._load_rules()

    def _load_rules(self) -> dict:
        if not RULES_PATH.exists():
            console.print(f"[red]ERROR: no existe {RULES_PATH}[/red]")
            return {"rules": [], "sections": []}
        return json.loads(RULES_PATH.read_text(encoding="utf-8"))

    def run(self) -> dict:
        """Evalúa output.json contra todas las reglas. Devuelve report."""
        output_path = self.fund_dir / "output.json"
        if not output_path.exists():
            return {
                "fund": self.isin,
                "nombre": "",
                "fallos": [{
                    "seccion": "global",
                    "problema": "No existe output.json",
                    "agente_responsable": "orchestrator",
                    "accion": "Ejecutar pipeline completo"
                }],
                "secciones_evaluadas": [],
                "evaluado_at": datetime.now().isoformat(),
            }

        data = json.loads(output_path.read_text(encoding="utf-8"))
        fallos = []

        for rule in self.rules.get("rules", []):
            if not _rule_applies(rule, data):
                continue
            check_type = rule.get("check_type")
            checker = CHECK_REGISTRY.get(check_type)
            if not checker:
                console.print(f"[yellow]WARN: check_type desconocido: {check_type} (regla {rule.get('id')})[/yellow]")
                continue

            try:
                ok, ctx = checker(rule, data)
            except Exception as exc:
                console.print(f"[red]ERROR ejecutando regla {rule.get('id')}: {exc}[/red]")
                continue

            if not ok:
                # Formatear problema y accion con el contexto del check
                fmt_ctx = dict(ctx)
                fmt_ctx.setdefault("section", rule.get("section", ""))
                problema = rule.get("problema_template", "Fallo en regla {id}").format(
                    id=rule.get("id", ""), **fmt_ctx)
                accion = rule.get("accion_template", "").format(
                    id=rule.get("id", ""), **fmt_ctx)
                fallos.append({
                    "regla_id": rule.get("id"),
                    "seccion": rule.get("section", "global"),
                    "fail_type": rule.get("fail_type", "estructura"),
                    "problema": problema,
                    "agente_responsable": rule.get("agente_responsable", "analyst_agent"),
                    "accion": accion,
                })

        # Compute scoring metrics
        total_reglas = len([r for r in self.rules.get("rules", []) if _rule_applies(r, data)])
        fallos_estructura = sum(1 for f in fallos if f.get("fail_type") in ("estructura", "content"))
        fallos_scarcity = sum(1 for f in fallos if f.get("fail_type") == "scarcity")
        reglas_ok = total_reglas - len(fallos)
        aceptable = fallos_estructura == 0
        score_display = f"{reglas_ok}/{total_reglas} reglas OK"
        if fallos_scarcity > 0:
            score_display += f" ({fallos_scarcity} pendientes de datos)"

        report = {
            "fund": self.isin,
            "nombre": data.get("nombre", ""),
            "fallos": fallos,
            "fallos_estructura": fallos_estructura,
            "fallos_scarcity": fallos_scarcity,
            "total_reglas": total_reglas,
            "reglas_ok": reglas_ok,
            "aceptable": aceptable,
            "score_display": score_display,
            "secciones_evaluadas": self.rules.get("sections", []),
            "evaluado_at": datetime.now().isoformat(),
        }

        # Persistir y mostrar
        report_path = self.fund_dir / "quality_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        self._print_report(report)

        return report

    def _print_report(self, report: dict):
        """Tabla rich con count de fallos por sección."""
        # Agrupar fallos por sección
        per_section: dict[str, int] = {}
        for f in report["fallos"]:
            sec = f.get("seccion", "global")
            per_section[sec] = per_section.get(sec, 0) + 1

        table = Table(
            title=f"Quality Report — {report['nombre']} ({report['fund']})",
            show_header=True,
            border_style="cyan",
        )
        table.add_column("Sección", width=22)
        table.add_column("Fallos", width=8, justify="right")
        table.add_column("Estado", width=30)

        for sec in report.get("secciones_evaluadas", []):
            n = per_section.get(sec, 0)
            estado = "[green]OK" if n == 0 else "[yellow]REVISAR"
            table.add_row(sec, str(n), estado)

        # Sección 'global' (fallos sin sección concreta)
        if per_section.get("global", 0):
            table.add_row("global", str(per_section["global"]), "[red]ERROR")

        table.add_row("", "", "")
        total = len(report["fallos"])
        total_color = "green" if total == 0 else "yellow"
        table.add_row("[bold]TOTAL", f"[bold {total_color}]{total}",
                      f"[bold {total_color}]{'OK' if total == 0 else 'REVISAR'}")

        # Show scoring summary
        aceptable = report.get("aceptable", False)
        score_display = report.get("score_display", "")
        accept_color = "green" if aceptable else "yellow"
        table.add_row(
            f"[bold {accept_color}]Score",
            f"[bold {accept_color}]{score_display}",
            f"[bold {accept_color}]{'ACEPTABLE' if aceptable else 'NO ACEPTABLE'}",
        )

        console.print(table)

        if report["fallos"]:
            console.print(f"\n[bold]Fallos ({len(report['fallos'])}):[/bold]")
            for f in report["fallos"][:15]:
                console.print(
                    f"  [yellow]•[/yellow] [{f['seccion']}] {f['problema'][:90]}"
                )
                console.print(
                    f"    [dim]→ {f['agente_responsable']}: {f['accion'][:90]}[/dim]"
                )


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
    agent = DashboardQualityAgent(isin)
    agent.run()
