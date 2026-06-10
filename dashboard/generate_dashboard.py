"""
Dashboard HTML Generator — reads output.json for ANY fund and produces HTML dashboard.
Applies all formatting rules learned from Avantage Fund pattern.
Usage: python generate_dashboard.py [ISIN]

NOTA (2026-04-27): para LECTURA de campos de output.json se recomienda usar
tools/output_accessor.py (get_perfiles, get_kpis, etc.) en vez de paths
hardcoded como data["analyst_synthesis"]["gestores"]["perfiles"]. El accessor
centraliza dónde leer cada campo, evita el bug histórico de leer del path
equivocado (top-level vs analyst_synthesis duplicado).
"""
import json
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
ISIN = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
FUND_DIR = ROOT / "data" / "funds" / ISIN
OUTPUT = Path(__file__).parent / f"fund-{ISIN}.html"

# Accessor canónico para reads de output.json
sys.path.insert(0, str(ROOT))
try:
    from tools.output_accessor import (
        get_perfiles, get_kpis, get_posiciones_actuales,
        get_serie_aum, get_serie_rentabilidad, get_serie_ter,
        get_serie_vl_base100, get_serie_participes, get_serie_ter_por_clase,
        get_serie_comisiones_por_clase, get_serie_rotacion,
        get_mix_activos, get_clases_info, get_int_clases, get_int_gestores,
        get_economia_fondo, get_clases,
        get_nombre, get_gestora, get_isin, get_tipo, get_ultima_actualizacion,
        get_kpi_aum, get_kpi_participes, get_kpi_ter, get_kpi_ter_efectivo,
        get_kpi_coste_gestion, get_kpi_volatilidad, get_kpi_clasificacion,
        get_kpi_perfil_riesgo, get_kpi_depositario, get_kpi_divisa,
        get_kpi_fecha_registro, get_kpi_max_drawdown, get_kpi_rotacion,
        get_kpi_rating_morningstar, get_kpi_srri,
        get_section_resumen, get_section_historia, get_section_estrategia,
        get_section_cartera, get_section_evolucion, get_section_fuentes_externas,
        get_resumen_texto, get_historia_texto, get_estrategia_texto,
        get_cartera_texto, get_gestores_texto, get_evolucion_texto,
        get_cualitativo, get_hechos_relevantes, get_lecturas_externas,
        get_analisis_consistencia, get_comision_exito, get_anio_creacion,
        get_fuentes, get_documentos, get_posiciones_historicas,
    )
    _ACCESSOR_AVAILABLE = True
except Exception:
    _ACCESSOR_AVAILABLE = False


def load_data():
    with open(FUND_DIR / "output.json", encoding="utf-8") as f:
        data = json.load(f)

    # ── Data resilience: fill gaps from available data ──
    cuant = data.setdefault("cuantitativo", {})

    # Si serie_vl_base100 ya existe con datos ricos (VLs decimales reales del PDF CNMV),
    # NO sobrescribir con serie_aum.vl (que puede ser entero redondeado).
    existing_vl = cuant.get("serie_vl_base100", [])
    existing_has_decimals = bool(existing_vl) and any(
        v.get("vl") and float(v["vl"]) != int(float(v["vl"]))
        for v in existing_vl if isinstance(v, dict)
    )

    if not existing_has_decimals:
        # Construir desde serie_aum solo si no hay mejor fuente
        aum_series = cuant.get("serie_aum", [])
        valid_vl = [s for s in aum_series if s.get("vl") and 1 < s["vl"] < 100000]
        # Filtrar periodos parciales (ej "202506")
        valid_vl = [s for s in valid_vl if len(str(s.get("periodo", ""))) <= 7]

        if valid_vl and len(valid_vl) > len(existing_vl):
            first_vl = valid_vl[0]["vl"]
            cuant["serie_vl_base100"] = [
                {"periodo": s.get("periodo", ""), "vl": s["vl"], "base100": round(s["vl"] / first_vl * 100, 1)}
                for s in valid_vl
            ]

    # ── Detect corrupted VL base 100 series ──
    # Detectar serie VL corrupta. VLs válidos:
    #  - Decimales reales (ej 12.5432, 1249.8637) o pequeños (1-500 EUR)
    #  - Fondos con base 1000€: VLs pueden ser >500 perfectamente
    # Corrupción real:
    #  - Todos enteros idénticos tipo [1.0, 1.0, 2.0] (AUM/participes parseados como VL)
    #  - Años parseados como VL (valor == 20XX)
    #  - Rentabilidad implícita absurda (>300% o <-80%)
    serie_vl = cuant.get("serie_vl_base100", [])
    vl_corrupta = False
    if serie_vl and len(serie_vl) >= 3:
        vls = [float(v.get("vl", 0) or 0) for v in serie_vl if isinstance(v, dict)]
        unique_vals = set(vls)
        # 1. Todos valores únicos son enteros pequeños (1.0, 2.0) → corrupto
        if len(unique_vals) <= 3 and all(v == int(v) for v in unique_vals if v > 0) and max(unique_vals) < 20:
            vl_corrupta = True
        # 2. Detección de "VL = año": periodo y vl coinciden (ej periodo=2020, vl=2020)
        #    Solo marca corrupto si la MAYORÍA de entries tienen periodo==round(vl)
        if not vl_corrupta:
            year_matches = sum(
                1 for v in serie_vl
                if isinstance(v, dict) and str(v.get("periodo","")).isdigit()
                and abs(int(v["periodo"]) - round(float(v.get("vl", 0) or 0))) <= 1
            )
            if year_matches >= len(serie_vl) * 0.5:
                vl_corrupta = True
        # 3. Base100 del segundo punto < 50 (primer VL era anómalo)
        if not vl_corrupta and len(serie_vl) >= 2:
            second_base = serie_vl[1].get("base100", 100)
            if second_base < 50:
                vl_corrupta = True
    data["serie_vl_corrupta"] = vl_corrupta

    # Also clean serie_aum: filter anomalous entries
    aum_series_clean = cuant.get("serie_aum", [])
    if aum_series_clean:
        cuant["serie_aum"] = [s for s in aum_series_clean if len(str(s.get("periodo", ""))) <= 7]

    # If gestora is empty, try cnmv_data
    if not data.get("gestora"):
        cnmv_path = FUND_DIR / "cnmv_data.json"
        if cnmv_path.exists():
            try:
                cnmv = json.loads(cnmv_path.read_text(encoding="utf-8"))
                data["gestora"] = cnmv.get("gestora", "") or cnmv.get("gestora_pdf", "")
                # Also fill kpis if missing
                for k, v in cnmv.get("kpis", {}).items():
                    if v is not None and not data.get("kpis", {}).get(k):
                        data.setdefault("kpis", {})[k] = v
            except Exception:
                pass

    # If equipo is empty, try cnmv_data or manager_profile
    if not data.get("gestores", {}).get("equipo"):
        mgr_path = FUND_DIR / "manager_profile.json"
        if mgr_path.exists():
            try:
                mgr = json.loads(mgr_path.read_text(encoding="utf-8"))
                equipo = mgr.get("equipo_gestor", [])
                if equipo:
                    data.setdefault("gestores", {})["equipo"] = equipo
            except Exception:
                pass

    return data


import re as _re

def build_classes_table(data):
    """Build classes table dynamically from cuantitativo data.
    Shows only CURRENT classes (latest period in serie_comisiones_por_clase).
    Includes: Inicio (first year class appears), Com. Gestión, TER, Com. Éxito."""
    cuant = data.get("cuantitativo", {})
    com_series = cuant.get("serie_comisiones_por_clase", [])
    ter_series = cuant.get("serie_ter_por_clase", [])
    isin = data.get("isin", "")

    # Find first year each class appears (for "Inicio" column)
    clases_inicio = {}
    for s in com_series:
        per = str(s.get("periodo", ""))
        for cls in s.get("clases", {}):
            if cls not in clases_inicio:
                clases_inicio[cls] = per

    # Get ALL classes across ALL periods (historical + current)
    clases = {}

    # Collect all classes from comisiones history
    current_clases = set()
    if com_series:
        # Last period = current classes
        current_clases = set(com_series[-1].get("clases", {}).keys())
        # All history
        for s in com_series:
            for cls, val in s.get("clases", {}).items():
                if cls not in clases:
                    clases[cls] = {}

    # Latest comision per class (most recent value available)
    for s in reversed(com_series):
        for cls, val in s.get("clases", {}).items():
            if "com_gestion" not in clases.get(cls, {}):
                clases.setdefault(cls, {})["com_gestion"] = val

    # Mark active vs closed
    for cls in clases:
        clases[cls]["activa"] = cls in current_clases

    # TER por clase: leer serie_ter_por_clase (tiene valor real de cada clase),
    # NO serie_ter que es el agregado del fondo (típicamente coincide con la
    # clase A institucional). Bug: si usabas el agregado, TER < com_gestión
    # para B/C/D y la coherencia check de líneas ~347-352 blanqueaba la com.
    ter_por_clase_latest: dict = {}
    if ter_series:
        # Última entry de serie_ter_por_clase = más reciente
        last_ter = ter_series[-1] if isinstance(ter_series, list) else {}
        if isinstance(last_ter, dict):
            ter_por_clase_latest = last_ter.get("clases", {}) or {}

    # Fallback al TER agregado para clases SIN valor por clase
    global_ter = None
    for t in cuant.get("serie_ter", []):
        if t.get("ter_pct"):
            global_ter = t["ter_pct"]

    for cls in clases:
        if not clases[cls].get("activa"):
            continue
        # 1) preferir TER específico de la clase
        cls_ter = ter_por_clase_latest.get(cls)
        if cls_ter is None:
            # match insensible: las keys pueden venir mayús/min mezcladas
            for k, v in ter_por_clase_latest.items():
                if str(k).lower() == str(cls).lower():
                    cls_ter = v
                    break
        if cls_ter is not None:
            clases[cls]["ter"] = cls_ter
        elif global_ter:
            # 2) si no hay valor por clase, fallback al agregado
            clases[cls]["ter"] = global_ter

    # Fix bug "todas las filas con mismo ISIN": propagar ISIN por clase desde
    # serie_clases_info (lo escribe cnmv_agent en cnmv_data.cualitativo, ya
    # capturado por el fix heurístico 2-fases). La última entry es la más
    # reciente. Si una clase no tiene ISIN ahí, se mantiene el fallback al
    # ISIN principal en línea ~338.
    serie_clases_info = cuant.get("serie_clases_info", []) or []
    if serie_clases_info and isinstance(serie_clases_info, list):
        last_entry = serie_clases_info[-1] if serie_clases_info else {}
        if isinstance(last_entry, dict):
            for cls_name, cls_data in last_entry.items():
                if cls_name == "periodo":
                    continue
                if not isinstance(cls_data, dict):
                    continue
                cls_isin = cls_data.get("isin")
                if cls_isin and cls_name in clases:
                    clases[cls_name]["isin"] = cls_isin

    # Fallback INT: reducir tabla a retail + limpia + 1 extra (máx 3 filas).
    # Prioridad: retail EUR, limpia EUR, y si falta alguna, otra EUR significativa.
    if not clases:
        k = data.get("kpis", {})
        global_ter = k.get("ter_pct")
        principal_country = isin[:2].upper() if isin else ""

        # A) retail + limpia EUR (ya hay helper)
        eur_pair = _hdr_collect_eur_retail_and_clean(data)
        picked_isins = set()

        def _add_class(label, cls_info, fallback_code=""):
            if not cls_info:
                return False
            c_isin = cls_info.get("isin") or ""
            if c_isin in picked_isins:
                return False
            # Filtrar wrappers de otra jurisdicción
            if principal_country and c_isin and c_isin[:2].upper() != principal_country:
                return False
            code = _hdr_clean_class_code(cls_info.get("code") or fallback_code)
            if not code:
                code = fallback_code
            # Scrape FT summary para TER/OCF y launch_date
            ft_sum = _hdr_ft_summary_scrape(c_isin, "EUR") if c_isin else {}
            # Para la clase PRINCIPAL, usar kpis.coste_gestion_pct y kpis.ter_pct
            is_principal = (c_isin == isin)
            com = k.get("coste_gestion_pct") if is_principal else None
            ter_v = (k.get("ter_pct") if is_principal else None) \
                    or ft_sum.get("ocf_pct") or global_ter
            clases[code] = {
                "com_gestion": com, "ter": ter_v, "activa": True,
                "currency": "EUR", "isin": c_isin,
                "label": label,  # 'retail' | 'limpia' | 'extra'
            }
            ld = ft_sum.get("launch_date", "")
            import re as _re_ld
            m_ld = _re_ld.match(r"\d{1,2}\s+[A-Za-z]+\s+(\d{4})", ld or "")
            clases_inicio[code] = m_ld.group(1) if m_ld else str(k.get("anio_creacion", "—"))
            picked_isins.add(c_isin)
            return True

        # Primero las retail + limpia oficiales
        _add_class("retail", eur_pair.get("retail"), "Clase retail")
        _add_class("limpia", eur_pair.get("limpia"), "Clase limpia")

        # Si solo hay 1 clase (sin la pareja) añadir una 3ª significativa
        if len(clases) < 2:
            try:
                ft_siblings = _hdr_ft_search_sibling_classes(
                    data.get("nombre", "") or "", data.get("isin", "")
                )
            except Exception:
                ft_siblings = []
            for c in ft_siblings:
                if (c.get("currency") or "").upper() != "EUR":
                    continue
                c_isin = c.get("isin", "")
                if c_isin in picked_isins:
                    continue
                if principal_country and c_isin[:2].upper() != principal_country:
                    continue
                _add_class("extra", c)
                if len(clases) >= 3:
                    break

    # Último recurso: si seguimos sin clases pero hay coste_gestion_pct, usar
    # code REAL inferido del nombre del fondo o de _int_clases (no "A" fijo).
    if not clases:
        k = data.get("kpis", {})
        if k.get("coste_gestion_pct") or k.get("ter_pct"):
            int_cls = data.get("_int_clases", []) or []
            if int_cls and isinstance(int_cls[0], dict) and int_cls[0].get("code"):
                code_real = _hdr_clean_class_code(int_cls[0].get("code"))
            else:
                import re as _re_name
                m = _re_name.search(
                    r"\b([A-Z](?:-[A-Z])?)\s+(EUR|USD|GBP|CHF)\s*(Acc|Inc|Accumulation|Income)?\b",
                    data.get("nombre", "") or "", _re_name.I,
                )
                code_real = f"Class {m.group(1).upper()}" if m else "Clase principal"
            clases[code_real] = {
                "com_gestion": k.get("coste_gestion_pct"),
                "ter": k.get("ter_pct"),
                "activa": True,
            }
            clases_inicio[code_real] = str(k.get("anio_creacion", "—"))

    if not clases:
        return '<p class="pr" style="color:var(--ink-4);font-style:italic;">Información de clases no disponible.</p>'

    # Comisión de éxito — ESTRUCTURA del fondo (parámetro KID/folleto), no importes cobrados.
    # Ej: "5% sobre beneficios" → estructura que define cuánto puede cobrar la gestora cada año.
    # Se mantiene estable: aunque varíe el importe cobrado año a año, la regla es la misma.
    com_exito = data.get("comision_exito", {})
    tiene_exito = com_exito.get("existe", False)
    # Parámetro teórico del folleto/KID (ej: 5% sobre beneficios positivos)
    exito_teorico_pct = (data.get("kpis", {}).get("comision_exito_pct")
                         or com_exito.get("pct_teorico"))
    # Base de cálculo: "mixta" → s/patrimonio+resultados, "resultados" → solo s/beneficios, etc.
    base_calculo = (com_exito.get("base_comision") or "").lower()
    # Textualizar la base de forma humana
    if "mixta" in base_calculo:
        base_texto = "s/resultados"
    elif "result" in base_calculo:
        base_texto = "s/resultados"
    elif "benchmark" in base_calculo or "referencia" in base_calculo:
        base_texto = "s/exceso vs benchmark"
    else:
        base_texto = "s/resultados"  # default
    # Fallback: si no tenemos teórico pero sí hay serie histórica, intentar detectar
    exito_por_clase = {}
    exito_ultimo_anio = None
    for entry in sorted(com_exito.get("serie_historica", []), key=lambda x: str(x.get("periodo",""))):
        periodo = entry.get("periodo", "")
        for cls, val in entry.get("exito", {}).items():
            if val is not None:
                exito_por_clase[cls] = val
                exito_ultimo_anio = periodo

    rows = ""
    # Sort: active classes first, then closed
    sorted_classes = sorted(clases.keys(), key=lambda c: (0 if clases[c].get("activa") else 1, c))
    for cls_name in sorted_classes:
        cls_data = clases[cls_name]
        com = cls_data.get("com_gestion")
        ter = cls_data.get("ter")
        # Coherencia: TER debe ser >= Com.Gestión. Si TER < com → fuentes
        # mezcladas (típico INT sin serie_ter_por_clase). Blanquear com para
        # no mostrar una paradoja al usuario.
        if com is not None and ter is not None:
            try:
                if float(ter) < float(com):
                    com = None
            except (ValueError, TypeError):
                pass
        inicio = clases_inicio.get(cls_name, "—")
        activa = cls_data.get("activa", True)
        # ISIN de la clase: usar el específico si existe; si no, mostrar "—"
        # en lugar de repetir el ISIN principal (que daría falsa impresión de
        # que todas las clases tienen el mismo ISIN — bug previo).
        # Para la clase principal del fondo (típicamente A), sí usar el
        # ISIN del fondo si no hay específico.
        if cls_data.get("isin"):
            cls_isin = cls_data["isin"]
        elif cls_name.strip().upper() in ("A", "BASE", "MAIN"):
            cls_isin = isin
        else:
            cls_isin = "—"

        # Status badge
        estado = '<span style="color:var(--pos);font-size:10px;">Activa</span>' if activa else '<span style="color:var(--ink-4);font-size:10px;">Cerrada</span>'

        if tiene_exito:
            if exito_teorico_pct:
                exito_cell = f'<strong>{p(exito_teorico_pct)}</strong> {base_texto}'
            else:
                exito_cell = f'{base_texto} <span style="color:var(--ink-4);font-size:10px;">(% pendiente)</span>'
        else:
            exito_cell = '<span style="color:var(--pos);">No cobra</span>'

        row_style = ' style="opacity:0.5;"' if not activa else ''

        # Normalizar nombre: si ya empieza con "Class", no añadir "Clase"
        cls_lower = cls_name.lower()
        if cls_lower.startswith("class ") or cls_lower.startswith("clase "):
            display_name = cls_name
        elif len(cls_name) <= 4:  # p.ej. "A", "O", "I EUR"
            display_name = f"Clase {cls_name}"
        else:
            display_name = cls_name

        rows += (
            f'<tr{row_style}><td><strong>{display_name}</strong></td><td>{cls_isin}</td>'
            f'<td>{p(com)}</td><td>{p(ter) if activa else "—"}</td>'
            f'<td style="font-family:\'Source Sans 3\';font-size:12px;">{exito_cell}</td>'
            f'<td style="font-family:\'Source Code Pro\';font-size:11px;">{inicio}</td>'
            f'<td>{estado}</td></tr>'
        )

    exito_note = ""
    if tiene_exito:
        # Nota explicativa adaptada: incluye último importe cobrado si lo hay, como contexto
        ultimo_val = next(iter(exito_por_clase.values()), None)
        ctx_ultimo = ""
        if ultimo_val is not None and exito_ultimo_anio:
            ctx_ultimo = f' Último año aplicado ({exito_ultimo_anio}): <strong>{p(ultimo_val)}</strong> s/patrimonio cobrado.'
        exito_note = (
            f'<p style="font-size:10px;color:var(--ink-4);margin-top:4px;font-style:italic;">'
            f'* Estructura: parámetro fijo del fondo que define el máximo aplicable.'
            f'{ctx_ultimo}</p>'
        )

    return f"""<table class="rt mb20">
    <thead><tr><th>Clase</th><th>ISIN</th><th>Com. Gestión</th><th>TER</th><th>Com. Éxito</th><th>Inicio</th><th>Estado</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
  {exito_note}"""


def render_narrative_inline(text, fund_name=""):
    """Convert analyst_synthesis text (with **bold** markdown) to HTML.

    Supported markdown patterns inside a paragraph block (split by blank line):
    - `**Header**` standalone → subtle subsection (.subsec): bold, slightly
      larger than body, no uppercase, no border. Replaces the old .sr label
      that fragmented narrative reading. (2026-05-04)
    - `- item` or `* item` lines → <ul class="nl"> with <li> items. Lists
      were previously rendered as literal text with the dash visible.
    - Plain prose with inline `**bold**` → <p class="pr"> with <strong>.

    Skips redundant title headers (e.g. 'RESUMEN EJECUTIVO: FONDO X') and
    avoids two consecutive subsec headers (would mean an empty subsection).
    """
    if not text:
        return '<p class="pr" style="color:var(--ink-4);font-style:italic;">Sección pendiente de análisis. Ejecutar analyst_agent.</p>'

    fund_lower = (fund_name or "").lower().split(",")[0].strip()
    paragraphs = text.split("\n\n")
    html = ""
    prev_was_header = False

    def _bold(s):
        return _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', s)

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # 1. Pure bold standalone → subtle subsec
        is_header = (para.startswith("**") and para.endswith("**")
                     and para.count("**") == 2 and "\n" not in para)
        if is_header:
            header_text = para.strip("*").strip()
            header_lower = header_text.lower()
            if fund_lower and fund_lower in header_lower:
                continue
            if any(skip in header_lower for skip in ("resumen ejecutivo", "informe analítico", "informe para comité")):
                continue
            if prev_was_header:
                continue
            html += f'<p class="subsec">{_bold(header_text)}</p>'
            prev_was_header = True
            continue

        # 1b. Párrafo que empieza con "**AÑO ... :** texto" (cronología de Historia)
        # → bloque de AÑO indentado y elegante (sangría + borde), no negrita suelta
        # a mismo nivel que los headers de sección.
        ym = _re.match(r'^\*\*\s*((?:19|20)\d{2}[^*]{0,60}?)\*\*\s*[:\-—]?\s*(.+)', para, _re.DOTALL)
        if ym:
            label = ym.group(1).strip().rstrip(":-—— ").strip()
            rest = _bold(ym.group(2).strip().replace("\n", " "))
            # Entrada de año: bullet cuadrado + sangría francesa (el año lidera,
            # las líneas siguientes alinean bajo el texto).
            html += (f'<p class="pr" style="margin:6px 0 6px 20px;text-indent:-18px;">'
                     f'<span style="color:var(--gold,#b48020);font-size:8px;vertical-align:1px;">▪</span> '
                     f'<strong>{label}.</strong> {rest}</p>')
            prev_was_header = False
            continue

        # 2. Mixed prose + list inside the block: emit interleaved <p>/<ul>.
        # Lines starting with `- ` or `* ` form a contiguous <ul>; the rest
        # accumulates into a <p> until a list line breaks the run (or v.v.).
        buf_text: list[str] = []
        buf_list: list[str] = []

        def _flush_text():
            if buf_text:
                joined = " ".join(buf_text).strip()
                if joined:
                    html_parts.append(f'<p class="pr">{_bold(joined)}</p>')
                buf_text.clear()

        def _flush_list():
            if buf_list:
                items = "".join(f'<li>{_bold(it)}</li>' for it in buf_list)
                html_parts.append(f'<ul class="nl">{items}</ul>')
                buf_list.clear()

        html_parts: list[str] = []
        for ln in para.split("\n"):
            if not ln.strip():
                continue
            m = _re.match(r'^\s*[-*]\s+(.*)', ln)
            if m:
                _flush_text()
                buf_list.append(m.group(1).strip())
            else:
                _flush_list()
                buf_text.append(ln.strip())
        _flush_text()
        _flush_list()
        html += "".join(html_parts)
        prev_was_header = False

    return html


def _build_class_selector(data):
    """Build <option> tags for the commission chart class selector.

    Cascada:
    1. serie_comisiones_por_clase (formato CNMV/ES) — todas las clases históricas.
    1b. (Bug 3, 2026-04-27) serie_ter_por_clase si serie_comisiones_por_clase vacía.
    2. `_int_clases` (INT) — clases del extractor.
    3. FT search siblings (INT) — clases descubiertas.
    4. Default: una opción con el nombre de clase real inferido del nombre del fondo.

    Default seleccionado = clase con más años de historia (más puntos en la serie).
    """
    import re as _re
    cuant = data.get("cuantitativo", {})
    com_series = cuant.get("serie_comisiones_por_clase", [])
    ter_clase_series = cuant.get("serie_ter_por_clase", [])

    # 1) ES / CNMV — preferir serie_comisiones_por_clase, fallback a serie_ter_por_clase
    primary_series = com_series or ter_clase_series
    if primary_series:
        # Contar puntos por clase para elegir default = clase con más historia
        per_class_count = {}
        all_clases = set()
        for s in primary_series:
            for cls in s.get("clases", {}).keys():
                per_class_count[cls] = per_class_count.get(cls, 0) + 1
                all_clases.add(cls)
        # Clases activas en el último periodo
        current_clases = set(primary_series[-1].get("clases", {}).keys())
        if all_clases:
            # Default = la de más puntos (desempata: activa actual primero, luego alfa)
            default_cls = max(
                all_clases,
                key=lambda c: (per_class_count.get(c, 0), 1 if c in current_clases else 0, c),
            )
            # Orden visual: default primero, luego activas, luego cerradas
            def _sort_key(c):
                if c == default_cls:
                    return (0, c)
                if c in current_clases:
                    return (1, c)
                return (2, c)
            sorted_cls = sorted(all_clases, key=_sort_key)
            # Last period TER per class for label (Bug 1 Fase G — clase con TER visible)
            last_ter = primary_series[-1].get("clases", {}) if primary_series else {}
            opts = ""
            for cls in sorted_cls:
                sel = " selected" if cls == default_cls else ""
                ter_v = last_ter.get(cls)
                ter_str = f" (TER {str(ter_v).replace('.', ',')}%)" if ter_v is not None else ""
                base = cls if cls in current_clases else f"{cls} (cerrada)"
                opts += f'<option value="{cls}"{sel}>{base}{ter_str}</option>'
            return opts

    # 2-3) INT: _int_clases + FT siblings
    options = []
    seen_codes = set()
    for c in (data.get("_int_clases", []) or []):
        if isinstance(c, dict):
            code = c.get("code") or c.get("nombre") or ""
            if code:
                code_clean = _hdr_clean_class_code(code)
                if code_clean not in seen_codes:
                    options.append(code_clean)
                    seen_codes.add(code_clean)
    try:
        for c in _hdr_ft_search_sibling_classes(data.get("nombre", ""), data.get("isin", "")):
            if (c.get("currency") or "").upper() != "EUR":
                continue
            code_clean = _hdr_clean_class_code(c.get("code") or c.get("fund_name") or "")
            if code_clean and code_clean not in seen_codes:
                options.append(code_clean)
                seen_codes.add(code_clean)
    except Exception:
        pass

    # 4) Default desde nombre del fondo: "Trojan ... O EUR ACC" → "O"
    if not options:
        nombre = data.get("nombre", "") or ""
        m = _re.search(r"\b([A-Z](?:-[A-Z])?)\s+(EUR|USD|GBP|CHF)\b", nombre, _re.I)
        if m:
            options = [f"{m.group(1).upper()} {m.group(2).upper()}"]
        else:
            options = ["—"]

    opts = ""
    for i, cls in enumerate(options):
        sel = " selected" if i == 0 else ""
        opts += f'<option value="{cls}"{sel}>{cls}</option>'
    return opts


def f(val, d=0, s=""):
    """Spanish number format"""
    if val is None or val == "": return "—"
    if isinstance(val, str):
        try: val = float(val)
        except ValueError: return val
    r = f"{val:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return r + s


def p(val):
    return f(val, 1, "%") if val is not None else "—"


# ═══════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════

CSS = """<style>
:root{--ink:#0e0e0e;--ink-2:#2a2a2a;--ink-3:#555;--ink-4:#888;--ink-5:#bbb;--rule:#d0d0d0;--rule-light:#e8e8e8;--paper:#fafaf8;--paper-2:#f3f3f0;--paper-3:#ececea;--white:#fff;--navy:#0c2340;--navy-mid:#1a3a5c;--navy-pale:#e8eef5;--pos:#1a4d2e;--neg:#6b1a1a;--pos-bg:#f0f7f2;--neg-bg:#fdf2f2;}
[data-theme="dark"]{--ink:#e8e4dc;--ink-2:#c8c4bc;--ink-3:#908c84;--ink-4:#5c5850;--ink-5:#3c3830;--rule:#2e2c28;--rule-light:#252320;--paper:#111110;--paper-2:#181816;--paper-3:#1e1d1b;--white:#111110;--navy:#6a9ec8;--navy-mid:#4a7ea8;--navy-pale:#141e28;--pos:#2a7a44;--neg:#c04040;--pos-bg:#0e1a12;--neg-bg:#1a0e0e;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}html{font-size:14px;}
body{font-family:'Source Sans 3',sans-serif;background:var(--paper);color:var(--ink);-webkit-font-smoothing:antialiased;line-height:1.6;}
/* HEADER */
.lh{background:var(--navy);}
.lh-top{display:flex;align-items:stretch;border-bottom:1px solid rgba(255,255,255,0.10);}
.lh-left{padding:16px 28px;border-right:1px solid rgba(255,255,255,0.10);min-width:320px;display:flex;flex-direction:column;gap:4px;}
.lh-fund{font-family:'EB Garamond',serif;font-size:20px;font-weight:500;color:#fff;line-height:1.2;}
.lh-meta-line{font-size:10px;color:rgba(255,255,255,0.38);letter-spacing:0.3px;}
.lh-meta-line strong{color:rgba(255,255,255,0.7);font-weight:500;}
.lh-center{flex:1;display:flex;flex-direction:column;justify-content:center;padding:12px 24px;gap:3px;}
.lh-cv{font-family:'Source Code Pro',monospace;font-size:11px;color:rgba(255,255,255,0.65);display:flex;gap:6px;align-items:center;}
.lh-cl{font-size:9px;color:rgba(255,255,255,0.28);text-transform:uppercase;letter-spacing:0.8px;min-width:52px;}
.lh-right{display:flex;align-items:center;gap:24px;padding:14px 36px 14px 24px;border-left:1px solid rgba(255,255,255,0.07);margin-left:auto;}
.lh-rd{display:flex;flex-direction:column;align-items:center;gap:2px;}
.lh-rd-v{font-family:'Source Code Pro',monospace;font-size:11.5px;color:rgba(255,255,255,0.75);}
.lh-rd-l{font-size:8px;color:rgba(255,255,255,0.25);text-transform:uppercase;letter-spacing:0.8px;}
.lh-aum{text-align:right;}.lh-aum-v{font-family:'Source Code Pro',monospace;font-size:20px;color:#fff;letter-spacing:-0.5px;line-height:1;}
.lh-aum-l{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:rgba(255,255,255,0.28);margin-top:4px;}
.lh-pubs{margin-top:8px;border-top:1px solid rgba(255,255,255,0.08);padding-top:6px;font-family:'Source Code Pro',monospace;font-size:9.5px;line-height:1.4;color:rgba(255,255,255,0.55);}
.lh-pubs .lh-pub-row{display:flex;justify-content:space-between;gap:8px;}
.lh-pubs .lh-pub-lbl{color:rgba(255,255,255,0.30);text-transform:uppercase;letter-spacing:0.6px;}
.lh-pubs .lh-pub-val{color:rgba(255,255,255,0.78);}
.lh-pubs .lh-pub-next{color:rgba(255,255,255,0.45);font-style:italic;}
.lh-pubs .lh-pub-soon{color:#f4a261;font-weight:600;}
.srri-pips{display:flex;gap:2px;}.srri-pip{width:9px;height:9px;border:1px solid rgba(255,255,255,0.20);}.srri-pip.on{background:rgba(255,255,255,0.65);border-color:rgba(255,255,255,0.65);}
.srri-l{font-size:8px;color:rgba(255,255,255,0.25);text-transform:uppercase;letter-spacing:0.8px;margin-top:2px;}
.theme-toggle{background:none;border:1px solid rgba(255,255,255,0.15);color:rgba(255,255,255,0.40);font-family:'Source Sans 3';font-size:11px;padding:5px 11px;cursor:pointer;white-space:nowrap;margin-left:12px;}
.theme-toggle:hover{color:rgba(255,255,255,0.75);border-color:rgba(255,255,255,0.30);}
/* TABS */
.tabbar{background:var(--navy);padding:0 28px;display:flex;border-top:1px solid rgba(255,255,255,0.06);overflow-x:auto;}.tabbar::-webkit-scrollbar{display:none;}
.tb{background:none;border:none;border-bottom:2px solid transparent;padding:9px 16px 8px;font-family:'Source Sans 3';font-size:11.5px;color:rgba(255,255,255,0.35);cursor:pointer;white-space:nowrap;transition:color 0.15s;}
.tb:hover{color:rgba(255,255,255,0.65);}.tb.on{color:rgba(255,255,255,0.88);border-bottom-color:rgba(255,255,255,0.55);}
/* BODY */
.body{max-width:1600px;margin:0 auto;padding:36px 48px 72px;}.pane{display:none;}.pane.on{display:block;}
.pane-header{display:flex;align-items:baseline;justify-content:space-between;padding-bottom:12px;margin-bottom:24px;border-bottom:2px solid var(--ink);}
.pane-h1{font-family:'EB Garamond',serif;font-size:26px;font-weight:400;color:var(--ink);letter-spacing:-0.3px;line-height:1;}
.pane-dl{font-size:10.5px;color:var(--ink-4);text-transform:uppercase;letter-spacing:0.8px;}
.sr{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--ink-4);margin:24px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--rule);}.sr:first-child{margin-top:0;}
/* Inline subsection inside analyst_synthesis narratives (used by render_narrative_inline).
   Subtle: no border, no uppercase, slightly larger than body, kept close to next paragraph. */
.subsec{font-size:15px;font-weight:600;color:var(--ink);margin:22px 0 6px;letter-spacing:0.1px;line-height:1.35;text-decoration:underline dotted var(--rule);text-decoration-thickness:1px;text-underline-offset:5px;}.subsec:first-child{margin-top:0;}
.pr+.subsec{margin-top:22px;}
details.acc{margin:8px 0;border-bottom:1px solid var(--rule);}
details.acc>summary{cursor:pointer;font-size:15px;font-weight:600;color:var(--ink);padding:9px 0;list-style:none;line-height:1.35;}
details.acc>summary::-webkit-details-marker{display:none;}
details.acc>summary::after{content:'＋';float:right;color:var(--ink-4);font-weight:400;font-size:14px;}
details.acc[open]>summary::after{content:'−';}
details.acc>summary:hover{color:var(--gold,#b48020);}
.acc-b{padding:0 0 12px;}
.subtabs{margin:4px 0 8px;}
.subtab-nav{display:flex;flex-wrap:wrap;gap:2px;border-bottom:1px solid var(--rule);margin-bottom:16px;}
.subtab-btn{cursor:pointer;background:none;border:none;padding:8px 13px;font-size:12.5px;color:var(--ink-4);font-family:inherit;border-bottom:2px solid transparent;margin-bottom:-1px;white-space:nowrap;}
.subtab-btn:hover{color:var(--ink-2);}
.subtab-btn.active{color:var(--ink-1);font-weight:600;border-bottom-color:var(--gold,#b48020);}
.subtab-panel{display:none;}
.subtab-panel.active{display:block;}
.pr{font-size:13.5px;line-height:1.78;color:var(--ink-2);}.pr+.pr{margin-top:10px;}.pr strong{color:var(--ink);font-weight:600;}
/* Inline narrative list (markdown - or * inside analyst_synthesis text). */
.nl{margin:6px 0 12px 0;padding-left:22px;}.nl li{font-size:13.5px;line-height:1.78;color:var(--ink-2);margin-bottom:4px;}.nl li strong{color:var(--ink);font-weight:600;}.subsec+.nl{margin-top:6px;}
.col2{display:grid;grid-template-columns:1fr 1fr;gap:20px;}.col3{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;}
.mb16{margin-bottom:16px;}.mb20{margin-bottom:20px;}.mb24{margin-bottom:24px;}
hr.hr{border:none;border-top:1px solid var(--rule);margin:24px 0;}
/* KPI */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);border-top:2px solid var(--rule);border-bottom:1px solid var(--rule);margin-bottom:20px;}
.kpi-cell{padding:12px 18px;border-right:1px solid var(--rule);}.kpi-cell:first-child{padding-left:0;}.kpi-cell:last-child{border-right:none;}
.kpi-label{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--ink-4);margin-bottom:4px;}
.kpi-value{font-family:'Source Code Pro',monospace;font-size:20px;font-weight:400;color:var(--ink);letter-spacing:-0.5px;line-height:1;}
.kpi-value.pos{color:var(--pos);}.kpi-value.neg{color:var(--neg);}.kpi-sub{font-size:10px;color:var(--ink-4);margin-top:4px;}
/* TABLE */
.rt{width:100%;border-collapse:collapse;font-size:13px;}.rt thead tr{border-bottom:1px solid var(--ink);}
.rt th{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--ink-3);padding:0 10px 7px;text-align:right;}.rt th:first-child{text-align:left;padding-left:0;}
.rt td{padding:8px 10px;text-align:right;font-family:'Source Code Pro',monospace;font-size:12px;color:var(--ink-2);border-bottom:1px solid var(--rule-light);}
.rt td:first-child{font-family:'Source Sans 3';font-size:13px;font-weight:500;text-align:left;color:var(--ink);padding-left:0;}
.rt tbody tr:hover td{background:var(--paper-2);}.pos-v{color:var(--pos);}.neg-v{color:var(--neg);}
/* PRINCIPLES */
.prin{margin-top:8px;}.prin-i{display:grid;grid-template-columns:20px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid var(--rule-light);align-items:baseline;}
.prin-i:last-child{border-bottom:none;}.prin-n{font-family:'Source Code Pro';font-size:10px;color:var(--ink-4);}
.prin-b{font-size:12.5px;color:var(--ink-2);line-height:1.5;}.prin-b strong{color:var(--ink);font-weight:600;}
/* TIMELINE — dashboard original style */
.timeline{position:relative;padding-left:32px;}
.timeline::before{content:'';position:absolute;left:9px;top:10px;bottom:10px;width:1px;background:var(--rule);}
.tl-item{position:relative;margin-bottom:28px;}
.tl-dot{position:absolute;left:-28px;top:4px;width:14px;height:14px;border-radius:50%;background:var(--paper);border:2.5px solid var(--navy-mid);z-index:1;}
.tl-dot.dot-hito{border-color:var(--pos);}.tl-dot.dot-strat{border-color:var(--navy);}.tl-dot.dot-market{border-color:var(--navy-mid);}.tl-dot.dot-crisis{border-color:var(--neg);}.tl-dot.dot-reg{border-color:var(--ink-4);}
.tl-date{font-family:'Source Code Pro',monospace;font-size:11px;color:var(--ink-4);margin-bottom:5px;}
.tl-tag{display:inline-block;font-size:9px;font-weight:600;letter-spacing:0.5px;padding:2px 8px;border-radius:4px;margin-bottom:6px;text-transform:uppercase;}
.tag-hito{background:var(--pos-bg);color:var(--pos);}.tag-strat{background:var(--paper-2);color:var(--navy);border:1px solid var(--rule-light);}.tag-market{background:var(--navy-pale);color:var(--navy-mid);}.tag-crisis{background:var(--neg-bg);color:var(--neg);}.tag-reg{background:var(--paper-3);color:var(--ink-4);}
[data-theme="dark"] .tag-hito{background:#0a2a1c;color:#4ecf99;}[data-theme="dark"] .tag-strat{background:#2a200a;color:#e8c56c;}[data-theme="dark"] .tag-market{background:#141e28;color:#6a9ec8;}[data-theme="dark"] .tag-crisis{background:#1a0e0e;color:#c04040;}[data-theme="dark"] .tag-reg{background:#1e1d1b;color:#908c84;}
.tl-title{font-size:14px;font-weight:600;color:var(--ink);margin-bottom:5px;}.tl-desc{font-size:13px;color:var(--ink-3);line-height:1.65;}
/* MANAGER */
.mgr{display:grid;grid-template-columns:260px 1fr;border-top:1px solid var(--rule);padding:22px 0;gap:4px;}
.mgr:last-of-type{border-bottom:1px solid var(--rule);}
.mgr-s{padding-right:24px;border-right:1px solid var(--rule);}
.mgr-av{width:52px;height:52px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:'EB Garamond',serif;font-size:20px;color:#fff;margin-bottom:10px;}
.mgr-nm{font-family:'EB Garamond',serif;font-size:18px;color:var(--ink);line-height:1.2;margin-bottom:3px;}
.mgr-rl{font-size:10px;color:var(--ink-4);text-transform:uppercase;letter-spacing:0.4px;line-height:1.4;margin-bottom:10px;}
.mgr-cv{font-size:11.5px;color:var(--ink-3);line-height:1.55;padding-left:14px;list-style:disc;}
.mgr-cv li{margin-bottom:5px;}
.mgr-b{padding-left:24px;}
/* STRATEGY MATRIX */
.strat-row{display:grid;grid-template-columns:100px 1fr 1fr 1fr;border-top:1px solid var(--rule-light);}
.strat-row:first-of-type{border-top:1px solid var(--rule);}
.strat-yr{padding:12px 12px;font-family:'Source Code Pro';font-size:12px;font-weight:500;color:var(--navy);border-right:1px solid var(--rule-light);background:var(--navy-pale);white-space:nowrap;}
.strat-c{padding:12px 10px;font-size:12px;color:var(--ink-2);line-height:1.6;border-right:1px solid var(--rule-light);}
.strat-c:last-child{border-right:none;}.strat-c strong{color:var(--ink);font-weight:600;}
/* PORTFOLIO TABLE */
.pt-wrap{overflow-x:auto;}.pt{width:100%;border-collapse:collapse;white-space:nowrap;}
.pt thead tr{border-bottom:2px solid var(--ink);}
.pt th{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:var(--ink-2);padding:0 8px 8px;text-align:left;}
.pt td{padding:7px 8px;font-family:'Source Code Pro';font-size:11px;color:var(--ink-2);text-align:right;border-bottom:1px solid var(--rule-light);}
.pt td:first-child{font-family:'Source Sans 3';font-size:12.5px;font-weight:500;color:var(--ink);text-align:left;padding-left:0;}
.pt tbody tr:hover td{background:var(--paper-2);}
/* Variant pt-flex: para tablas con columnas de comentario/descripción que wrap */
.pt.pt-flex{white-space:normal;table-layout:fixed;}
.pt.pt-flex td{white-space:normal;text-align:left;vertical-align:top;line-height:1.5;}
.pt.pt-flex td.num{text-align:right;white-space:nowrap;font-family:'Source Code Pro';}
.pt.pt-flex td:first-child{font-family:'Source Sans 3';font-size:11.5px;color:var(--ink-3);font-weight:400;}
.wbar{display:flex;align-items:center;gap:5px;}.wfill{height:4px;border-radius:1px;}
.tp-badge{display:inline-block;padding:3px 10px;border-radius:3px;font-size:9.5px;font-weight:600;letter-spacing:0.4px;font-family:'Source Sans 3';text-transform:uppercase;white-space:nowrap;line-height:1.4;min-width:110px;text-align:center;}
.tp-rv{background:#1a3a5c;color:#fff;}
.tp-rf{background:#8c3214;color:#fff;}
.tp-gold{background:#b48020;color:#fff;}
.tp-cash{background:#4a7a5a;color:#fff;}
.tp-etf{background:#3d5a80;color:#fff;}
.tp-comm{background:#6b3fa0;color:#fff;}
.tp-der{background:#5c5850;color:#fff;}
.tp-otro{background:#888;color:#fff;}
.delta-new{font-size:9px;font-weight:600;color:var(--pos);background:var(--pos-bg);padding:1px 5px;border-radius:2px;}
.delta-up{color:var(--pos);}.delta-down{color:var(--neg);}
/* SOURCES — card style */
.src-card{background:var(--paper-2);border:1px solid var(--rule);border-radius:8px;padding:18px 22px;margin-bottom:14px;}
.src-card:hover{border-color:var(--navy-mid);}
.src-head{display:flex;align-items:center;gap:14px;margin-bottom:10px;}
.src-logo{width:40px;height:40px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-family:'Source Sans 3';font-size:13px;font-weight:700;color:#fff;flex-shrink:0;}
.src-info{flex:1;}
.src-o{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;color:var(--navy);line-height:1.3;}
.src-author{font-size:11px;color:var(--ink-4);margin-top:1px;}
.src-date{font-family:'Source Code Pro';font-size:10px;color:var(--ink-4);flex-shrink:0;}
.src-t{font-size:14px;font-weight:600;color:var(--ink);margin-bottom:8px;line-height:1.35;}
.exp-btn{background:none;border:none;cursor:pointer;color:var(--navy);font-size:11px;font-family:'Source Sans 3';padding:0;display:flex;align-items:center;gap:4px;margin-bottom:6px;}
.exp-body{display:none;font-size:12.5px;color:var(--ink-3);line-height:1.65;background:var(--paper-3);border-radius:6px;padding:12px 14px;margin-bottom:10px;}
.exp-body.open{display:block;}
.src-lnk{display:inline-flex;align-items:center;gap:4px;font-size:11px;color:var(--navy);text-decoration:none;border:1px solid var(--navy);border-radius:4px;padding:4px 10px;transition:background 0.15s;}
.src-lnk:hover{background:var(--navy-pale);}
/* DOCS */
.doc-grp{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1.2px;color:var(--ink-3);padding:18px 0 5px;border-bottom:1px solid var(--rule);}
.doc-grp:first-child{padding-top:0;}
.doc-r{display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid var(--rule-light);}
.doc-r:hover{background:var(--paper-2);}
.doc-ext{font-family:'Source Code Pro';font-size:8px;font-weight:600;color:var(--ink-4);background:var(--paper-3);padding:2px 4px;min-width:26px;text-align:center;flex-shrink:0;}
.doc-nm{font-size:12px;color:var(--ink);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.doc-mt{font-family:'Source Code Pro';font-size:9px;color:var(--ink-4);}
.doc-a{font-size:10px;color:var(--navy);text-decoration:none;flex-shrink:0;}
/* CHART */
.ch-b{margin-bottom:20px;padding:12px 8px 8px;}.ch-l{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1.1px;color:var(--ink-4);margin-bottom:14px;border-bottom:1px solid var(--rule-light);padding-bottom:5px;}
.ch-h{height:180px;position:relative;padding:4px 0;}.ch-hm{height:220px;position:relative;padding:4px 0;}
/* CHART SELECTOR */
.ch-sel{display:inline-flex;align-items:center;gap:6px;float:right;font-size:10px;color:var(--ink-4);}
.ch-sel select{background:var(--paper-2);border:1px solid var(--rule);padding:2px 6px;font-size:10px;font-family:'Source Sans 3';color:var(--ink);}
/* RESPONSIVE */
@media(max-width:900px){.lh-top{flex-wrap:wrap;}.lh-center{display:none;}.body{padding:20px 16px;}.col2,.col3{grid-template-columns:1fr;}.kpi-row{grid-template-columns:1fr 1fr;}.mgr{grid-template-columns:1fr;}.strat-row{grid-template-columns:60px 1fr;}}
</style>"""


# ═══════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════

def format_date(date_str):
    """Convert '31/07/2014' or '22/09/2017' to 'Julio 2014' or 'Septiembre 2017'"""
    months = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
              7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    if not date_str:
        return "—"
    try:
        parts = date_str.split("/")
        if len(parts) == 3:
            return f"{months.get(int(parts[1]), parts[1])} {parts[2]}"
    except Exception:
        pass
    return date_str


def _hdr_download_pdf(url, isin, fname):
    """Descarga un PDF a raw/discovery/{fname}. Devuelve True si OK."""
    # P3 (2026-05-19): respect env flag DASHBOARD_SKIP_ENRICH para escapes rápidos
    import os as _os
    if _os.environ.get("DASHBOARD_SKIP_ENRICH") == "1":
        return False
    try:
        import httpx
        base = ROOT / "data" / "funds" / isin / "raw" / "discovery"
        base.mkdir(parents=True, exist_ok=True)
        target = base / fname
        with httpx.Client(timeout=15, follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
            resp = c.get(url)
            if resp.status_code == 200 and len(resp.content) > 5000 and resp.headers.get("content-type", "").startswith("application/pdf"):
                target.write_bytes(resp.content)
                return True
    except Exception:
        pass
    return False


def _hdr_ddg_search_pdfs(query, max_results=10):
    """Busca PDFs públicos vía DuckDuckGo HTML (no requiere API key).
    Devuelve lista de URLs PDF encontradas, priorizando las que contienen
    keywords de documento financiero en la ruta."""
    import re as _re
    # P3: respect skip-enrich
    import os as _os
    if _os.environ.get("DASHBOARD_SKIP_ENRICH") == "1":
        return []
    try:
        import httpx
        url = "https://html.duckduckgo.com/html/"
        r = httpx.post(url, timeout=8, follow_redirects=True,
                       headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                       data={"q": query})
        if r.status_code != 200:
            return []
        # Extraer todas las URLs .pdf
        pdfs = list(set(_re.findall(r'https?://[^\s"\'<>]+?\.pdf[^\s"\'<>]*', r.text)))
        # Clean uddg= wrapper de DDG si existe
        cleaned = []
        for p in pdfs:
            # DDG redirige via /l/?uddg=<url_encoded>
            m = _re.search(r"uddg=([^&]+)", p)
            if m:
                from urllib.parse import unquote
                cleaned.append(unquote(m.group(1)))
            else:
                cleaned.append(p)
        return cleaned[:max_results]
    except Exception:
        return []


def _hdr_try_download_kid(isin, gestora=""):
    """Busca un KID online y lo descarga a raw/discovery/ (UNIVERSAL).

    Cascada:
    1. Si ya existe KID local → return.
    2. Fundsquare (rápido para LU): intenta leer el link KID directo.
    3. DuckDuckGo HTML search: query ISIN + keywords "KID PDF" /
       "key investor information". Filtra los PDFs que contengan el ISIN
       en la URL o keywords de documento regulatorio (KID/KIID/PRIIP/PRIIPS/
       key-information) en la ruta.
    4. Descarga el primer candidato válido.

    Devuelve path del KID descargado o None.
    """
    import re as _re
    base = ROOT / "data" / "funds" / isin / "raw" / "discovery"
    if base.exists():
        for p in base.glob("*.pdf"):
            name_l = p.name.lower()
            if "kid" in name_l or "kiid" in name_l or "priip" in name_l:
                return p

    # 2) Fundsquare para LU (más rápido)
    if isin.startswith("LU"):
        try:
            import httpx
            url = f"https://www.fundsquare.net/security/summary?idInstr={isin}"
            with httpx.Client(timeout=8, follow_redirects=True,
                              headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
                r = c.get(url)
                if r.status_code == 200:
                    m = _re.search(r'href="([^"]+?(?:KID|KIID)[^"]*?\.pdf)"', r.text, _re.I)
                    if m:
                        pdf_url = m.group(1)
                        if pdf_url.startswith("/"):
                            pdf_url = "https://www.fundsquare.net" + pdf_url
                        if _hdr_download_pdf(pdf_url, isin, "kid_from_fundsquare.pdf"):
                            return base / "kid_from_fundsquare.pdf"
        except Exception:
            pass

    # 3) DDG search UNIVERSAL (cualquier jurisdicción)
    doc_keywords = ("kid", "kiid", "priip", "priips", "key-information",
                    "informacion-clave", "informations-cles", "basisinformation")
    queries = [
        f'"{isin}" KID PDF',
        f'{isin} KID PDF',
        f'"{isin}" key investor information',
        f'{isin} PRIIP PDF',
    ]
    seen = set()
    for q in queries:
        for pdf_url in _hdr_ddg_search_pdfs(q, max_results=15):
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            url_l = pdf_url.lower()
            # PRIORIDAD: el PDF debe incluir el ISIN en la URL O keyword de KID en la ruta
            has_isin = isin.lower() in url_l
            has_kid_kw = any(kw in url_l for kw in doc_keywords)
            if not (has_isin or has_kid_kw):
                continue
            # Inferir nombre de archivo desde la URL
            fname = pdf_url.split("/")[-1].split("?")[0][:80]
            if not fname.lower().endswith(".pdf"):
                fname = f"kid_{isin}.pdf"
            if _hdr_download_pdf(pdf_url, isin, fname):
                return base / fname
    return None


def _hdr_try_download_ar(isin, gestora=""):
    """Descarga Annual Report desde DDG search. UNIVERSAL."""
    import re as _re
    base = ROOT / "data" / "funds" / isin / "raw" / "discovery"
    if base.exists():
        for p in base.glob("*.pdf"):
            name_l = p.name.lower()
            if "annual" in name_l or "jaarverslag" in name_l or "year-end" in name_l:
                return p
    doc_keywords = ("annual", "yearend", "year-end", "rapport-annuel",
                    "informe-anual", "jahresbericht", "geschaftsbericht")
    queries = [
        f'"{isin}" annual report PDF',
        f'{isin} annual report',
        f'{gestora} annual report {isin}' if gestora else None,
    ]
    queries = [q for q in queries if q]
    seen = set()
    for q in queries:
        for pdf_url in _hdr_ddg_search_pdfs(q, max_results=15):
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            url_l = pdf_url.lower()
            if not (isin.lower() in url_l or any(kw in url_l for kw in doc_keywords)):
                continue
            fname = pdf_url.split("/")[-1].split("?")[0][:80]
            if not fname.lower().endswith(".pdf"):
                fname = f"ar_{isin}.pdf"
            if _hdr_download_pdf(pdf_url, isin, fname):
                return base / fname
    return None


def _hdr_enrich_if_missing(data):
    """Detecta gaps críticos del header y trata de rellenarlos.
    Para gaps BARATOS (download URL público) → los ejecuta inline.
    Para gaps CAROS (correr agente con LLM) → devuelve sugerencias.

    Devuelve dict {enriched: [cosas hechas], suggestions: [comandos sugeridos]}
    """
    isin = data.get("isin", "")
    if not isin:
        return {"enriched": [], "suggestions": []}
    enriched = []
    suggestions = []
    gestora = data.get("gestora", "") or ""

    pdfs = _hdr_discovery_pdfs(isin)

    # 1. KID missing → descarga universal (Fundsquare LU + DDG search cualquier jurisdicción)
    if not pdfs["kid"]:
        kid_path = _hdr_try_download_kid(isin, gestora)
        if kid_path:
            enriched.append(f"KID descargado: {kid_path.name}")
            # Limpiar cache para forzar re-scan con nuevo PDF
            for k in list(_HDR_PDF_CACHE.keys()):
                if isin in k:
                    del _HDR_PDF_CACHE[k]
        else:
            suggestions.append(
                f"KID no encontrado → python -m agents.intl_discovery_agent {isin} --target=kid"
            )

    # 2. AR missing → descarga universal via DDG
    if not pdfs["annual_report"]:
        ar_path = _hdr_try_download_ar(isin, gestora)
        if ar_path:
            enriched.append(f"Annual report descargado: {ar_path.name}")
            for k in list(_HDR_PDF_CACHE.keys()):
                if isin in k:
                    del _HDR_PDF_CACHE[k]
        else:
            suggestions.append(
                f"AR no encontrado → python -m agents.intl_discovery_agent {isin} --target=annual_report"
            )

    # 3. manager_profile ausente o vacío
    mgr = _hdr_load_side(isin, "manager_profile.json")
    if not mgr.get("equipo"):
        suggestions.append(
            f"Equipo gestor sin perfiles → python -m agents.manager_deep_agent {isin}"
        )

    # 4. readings muy escasos (<3 fuentes)
    readings = _hdr_load_side(isin, "readings_data.json")
    n_readings = len(readings.get("analisis_completos", []) + readings.get("otros_readings", []))
    if n_readings < 3:
        suggestions.append(
            f"Readings escasos ({n_readings}/3) → python -m agents.readings_agent {isin}"
        )

    return {"enriched": enriched, "suggestions": suggestions}


def _hdr_load_side(isin, filename):
    """Load a side JSON file (manager_profile, cssf_data, cbi_data, readings) if exists."""
    try:
        p = ROOT / "data" / "funds" / isin / filename
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _hdr_discovery_pdfs(isin):
    """Lista los PDFs en raw/discovery/ del fondo. Los clasifica por tipo
    usando keywords en el nombre del fichero (KID/KIID, AR/Annual Report,
    factsheet, interim, letter, prospectus). Devuelve dict {tipo: [paths]}."""
    base = ROOT / "data" / "funds" / isin / "raw" / "discovery"
    result = {"kid": [], "annual_report": [], "interim_report": [],
              "factsheet": [], "letter": [], "prospectus": []}
    if not base.exists():
        return result
    for p in sorted(base.glob("*.pdf")):
        name = p.name.lower()
        if "kiid" in name or "kid" in name.replace("kids", ""):
            result["kid"].append(p)
        elif "prospectus" in name or "prospecto" in name:
            result["prospectus"].append(p)
        elif "interim" in name or "semi" in name or "half" in name:
            result["interim_report"].append(p)
        elif "annual" in name or "jaarverslag" in name or "year-end" in name:
            result["annual_report"].append(p)
        elif "factsheet" in name or "fact-sheet" in name or "fact_sheet" in name:
            result["factsheet"].append(p)
        elif "letter" in name or "newsletter" in name:
            result["letter"].append(p)
    return result


def _hdr_pdf_text(pdf_path, max_pages=8):
    """Extrae texto de las primeras N páginas del PDF. None si falla."""
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages[:max_pages]
            txt = ""
            for p in pages:
                try:
                    txt += (p.extract_text() or "") + "\n"
                except Exception:
                    continue
            return txt
    except Exception:
        return None


# Caché en memoria durante una ejecución del generador (evita releer el mismo PDF)
_HDR_PDF_CACHE = {}


def _hdr_extract_srri_from_kid(isin):
    """Extrae SRRI/SRI 1-7. Formatos soportados:
    - UCITS IV KIID: 'The fund has been classed as N' / 'SRRI N'
    - PRIIPS KID ES: 'Hemos clasificado este Producto en la clase de riesgo N'
                     'es la clase de riesgo N en una escala de 7'
    - PRIIPS KID EN: 'We have classified this product as N out of 7'
    - PRIIPS KID FR: 'Nous avons classé ce produit dans la classe de risque N sur 7'
    - PRIIPS KID DE: 'Wir haben dieses Produkt als N eingestuft'
    - PRIIPS KID IT: 'Abbiamo classificato questo prodotto come N su 7'
    Escanea TODOS los PDFs de raw/discovery/ (no solo los clasificados como KID)
    porque los nombres de archivo son heterogéneos (p.ej. 'bucket-files.pdf'
    puede ser un KID real).
    """
    import re as _re
    cache_key = f"srri::{isin}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    # TODOS los PDFs de discovery, no solo los clasificados como KID
    base = ROOT / "data" / "funds" / isin / "raw" / "discovery"
    if not base.exists():
        _HDR_PDF_CACHE[cache_key] = None
        return None
    all_pdfs = sorted(base.glob("*.pdf"))

    # Priorizar los que parezcan KID/factsheet primero (más rápido)
    pdfs_classified = _hdr_discovery_pdfs(isin)
    priority = pdfs_classified["kid"] + pdfs_classified["factsheet"]
    rest = [p for p in all_pdfs if p not in priority]
    candidates = priority + rest

    patterns = [
        # UCITS IV
        r"(?i)(?:fund|sub-?fund|class)\s+has\s+been\s+(?:classed|rated)\s+as\s+(\d)\b",
        r"(?i)\b(?:SRRI|SRI)\s*[:=\-]?\s*(\d)\s*(?:/|out\s+of|on\s+a\s+scale)?\s*7",
        r"(?i)synthetic\s+risk\s+(?:and\s+reward)?\s*indicator\s*[:=\-]?\s*(\d)",
        # PRIIPS ES
        r"(?i)hemos\s+clasificado\s+(?:este\s+)?(?:producto|fondo)[^\n.]{0,80}?clase\s+de\s+riesgo\s+(\d)",
        r"(?i)es\s+la\s+clase\s+de\s+riesgo\s+(\d)\s+en\s+una\s+escala\s+de\s+7",
        r"(?i)clasificado\s+este\s+producto\s+en\s+la\s+clase\s+de\s+riesgo\s+(\d)",
        r"(?i)indicador\s+de\s+riesgo[^\n]{0,30}?(\d)\s*(?:/|en|de|sobre)\s*7",
        r"(?i)indicador\s+de\s+riesgo\s*[:=\-]?\s*(\d)",
        # PRIIPS EN
        r"(?i)we\s+have\s+classifi(?:ed|cated)\s+this\s+(?:product|fund)[^\n.]{0,80}?(\d)\s+out\s+(?:of\s+)?7",
        r"(?i)classified\s+this\s+(?:product|fund)[^\n.]{0,80}?(?:as\s+)?a?\s*(\d)\s+out\s+(?:of\s+)?7",
        r"(?i)classified\s+this\s+product[^\n.]{0,80}?risk\s+class\s+(\d)",
        r"(?i)risk\s+class[:\s]+(\d)\b",
        # PRIIPS FR
        r"(?i)nous\s+avons\s+class[eé]\s+ce\s+produit\s+dans\s+la\s+classe\s+de\s+risque\s+(\d)(?:\s+sur\s+7)?",
        r"(?i)class[eé]\s+(?:ce|le)\s+produit\s+(?:dans|comme)\s+(?:la\s+)?classe\s+(?:de\s+risque\s+)?(\d)",
        # PRIIPS DE
        r"(?i)wir\s+haben\s+dieses\s+produkt\s+als\s+(\d)\s+eingestuft",
        r"(?i)risikoklasse\s*[:=]?\s*(\d)",
        # PRIIPS IT
        r"(?i)abbiamo\s+classificato\s+questo\s+prodotto\s+come\s+(\d)\s+su\s+7",
        r"(?i)classe\s+di\s+rischio\s+(\d)\s+su\s+7",
    ]
    for pdf_path in candidates:
        txt = _hdr_pdf_text(pdf_path, max_pages=8)
        if not txt:
            continue
        for pat in patterns:
            m = _re.search(pat, txt)
            if m:
                try:
                    val = int(m.group(1))
                    if 1 <= val <= 7:
                        _HDR_PDF_CACHE[cache_key] = val
                        return val
                except Exception:
                    pass
    _HDR_PDF_CACHE[cache_key] = None
    return None


def _hdr_extract_depositario_from_ar(isin):
    """Extrae Depositario / Custodian del AR o prospecto. Soporta 3 formatos:
    A) Inline: 'Depositary: <Name>' o 'Depositary <Name>'
    B) Multi-campo con nombre en línea siguiente (DNCA/SICAV luxemburguesas):
       'Administrative Agent, Depositary, Domiciliary Agent, ...\\n<Name>'
    C) Bloque 'Depositary\\n<Name>\\n<dirección>'
    Sufijos institucionales válidos: Mellon|Trust|Bank|SA|PLC|Ltd|GmbH|Corp|
       Services|Branch|Depositary|Custodian|Paribas|Mellon|State Street.
    Devuelve string o None.
    """
    import re as _re
    cache_key = f"deposit::{isin}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    pdfs = _hdr_discovery_pdfs(isin)
    candidates = pdfs["annual_report"] + pdfs["interim_report"] + pdfs["kid"]

    # A) Inline
    inline_pattern = _re.compile(
        r"(?i)(?:depositary|depositari[oa]|custodian)\s*[:\-]\s+"
        r"(The\s+)?([A-Z][\w&\-\.'\s]{5,70}?"
        r"(?:Mellon|Trust|Bank|S\.?A\.?\b|PLC|Ltd|GmbH|Corp|Services|Branch|"
        r"Depositary|Custodian|Paribas|State\s+Street)"
        r"[\w&\-\.'\s]{0,30})"
    )

    # Blacklist: textos que contienen estos fragmentos NO son depositarios válidos.
    # Son líneas legales del KID/prospectus (FCA register, texto regulatorio, etc.)
    # que casualmente contienen palabras como "Services" pegadas sin espacios.
    _DEPOSIT_BLACKLIST = _re.compile(
        r"(?i)(?:Register\s*No|RegisterNo|"
        r"FinancialServicesRegister|"
        r"Authority[,.]|Prudential\s*Regulation|"
        r"Conduct\s*Authority|FinancialConduct|"
        r"regulated\s+by|authori[sz]ed\s+by|"
        r"No\.?\s*\d{4,})"
    )

    def _is_valid_deposit_name(name):
        """Candidato válido: tiene espacios entre palabras, no contiene texto
        regulatorio, y no es una lista de entidades pegadas."""
        if not name or len(name) < 8 or len(name) > 100:
            return False
        # Debe tener al menos 1 espacio real entre palabras ≥3 chars
        tokens = [t for t in name.split() if len(t) >= 3]
        if len(tokens) < 2:
            return False
        # No contener patrones blacklisted
        if _DEPOSIT_BLACKLIST.search(name):
            return False
        # No tener >3 comas (suele ser lista legal, no un solo nombre)
        if name.count(",") > 2:
            return False
        # No tener ratio alto de mayúsculas pegadas sin espacio (ej. FinancialServicesRegister)
        # detectar más de 2 transiciones lower→Upper dentro de una palabra de ≥15 chars
        long_words = [t for t in name.split() if len(t) >= 15]
        for w in long_words:
            transitions = sum(1 for i in range(1, len(w)) if w[i-1].islower() and w[i].isupper())
            if transitions >= 2:
                return False
        return True

    # B) / C) Multi-campo: encontrar la etiqueta + línea siguiente con nombre institucional
    # El patrón requiere que (1) aparezca "Depositary" como parte de una lista o título
    # y (2) la primera línea posterior sea un nombre institucional.
    def _b_c_capture(txt):
        # Cualquier línea que CONTENGA "Depositary" (sin dos puntos) como cabecera
        for m in _re.finditer(r"(?mi)^.*depositary.*$", txt):
            label_line = m.group(0).strip()
            # Solo aceptar si la etiqueta NO es un párrafo largo (debe ser header)
            if len(label_line) > 160:
                continue
            # Obtener las 5 líneas siguientes
            rest = txt[m.end():]
            lines = [l.strip() for l in rest.split("\n") if l.strip()]
            for cand in lines[:5]:
                # Parecer un nombre institucional: empezar con mayúscula,
                # acabar con sufijo típico, ≤100 chars
                if _re.match(
                    r"^[A-Z][\w&\-\.'\s,]{4,95}"
                    r"(?:Mellon|Trust|Bank|S\.?A\.?|PLC|Ltd|GmbH|Corp|Services|Branch|"
                    r"Depositary|Custodian|Paribas|State\s+Street|Luxembourg\s+Branch)",
                    cand):
                    clean = _re.sub(r"\s{2,}", " ", cand.rstrip(" .,;"))
                    # Validar antes de aceptar — rechaza texto legal del KID
                    if _is_valid_deposit_name(clean):
                        return clean
        return None

    for pdf_path in candidates:
        txt = _hdr_pdf_text(pdf_path, max_pages=12)
        if not txt:
            continue
        # A) Try inline first
        m = inline_pattern.search(txt)
        if m:
            prefix = m.group(1) or ""
            name = (prefix + m.group(2)).strip().rstrip(" .,;")
            name = _re.sub(r"\s{2,}", " ", name)
            name = _re.split(r"\s+(?:Shareholders|Board|Investment|Administrator|Manager|Sub-Custodian)\s+", name)[0].strip()
            if _is_valid_deposit_name(name):
                _HDR_PDF_CACHE[cache_key] = name
                return name
        # B/C) Multi-campo
        name_b = _b_c_capture(txt)
        if name_b:
            _HDR_PDF_CACHE[cache_key] = name_b
            return name_b
    _HDR_PDF_CACHE[cache_key] = None
    return None


def _hdr_enrich_gestora_from_ar(isin, short_name):
    """Si la gestora actual es escueta (p.ej. 'GAM', 'DNCA'), busca en los
    PDFs cached (AR/KID/prospectus) el nombre institucional completo
    ({short_name} {sufijo institucional}). Devuelve el nombre largo si lo
    encuentra, o None."""
    import re as _re
    if not short_name or len(short_name) > 40:
        return None
    cache_key = f"gestora_enrich::{isin}::{short_name}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    pdfs = _hdr_discovery_pdfs(isin)
    candidates = pdfs["annual_report"] + pdfs["interim_report"] + pdfs["kid"] + pdfs.get("prospectus", [])
    # Ampliar con cualquier PDF en raw/ que contenga el short_name o "prospectus"/"annual"
    import pathlib
    raw_dir = pathlib.Path("data") / "funds" / isin / "raw"
    if raw_dir.exists():
        for pdf in raw_dir.rglob("*.pdf"):
            name_l = pdf.name.lower()
            if any(kw in name_l for kw in ("prospect", "annual", "kid", "kiid", "web_")):
                if str(pdf) not in candidates:
                    candidates.append(str(pdf))
    short_re = _re.escape(short_name.strip())
    # Patrón: {GAM} {palabras en misma línea} {sufijo institucional}.
    # Usar [^\n] en vez de \s para NO cruzar saltos de línea y evitar matchear
    # basura multilínea del PDF (ej. headers de tabla pegados).
    pat = _re.compile(
        rf"\b{short_re}[ \t]+"
        rf"((?:[A-Z][\w&\-\.]+[ \t]+){{0,5}}"
        rf"(?:Fund\s+Management|Fund\s+Managers?|Management|Investments?|Capital|"
        rf"Asset\s+Management|Partners|Holdings?|Advisors?)"
        rf"(?:[ \t]+(?:Limited|Ltd\.?|Plc|PLC|S\.?A\.?|SGIIC|GmbH|AG|BV|N\.?V\.?|LLC|Inc\.?))?)"
    )

    def _is_valid_gestora_full(full):
        """Rechaza fragmentos con fechas pegadas, palabras sin espacios, etc."""
        if not full or "\n" in full or len(full) < 15 or len(full) > 100:
            return False
        # Sin dígitos de 4 en fila (fecha)
        if _re.search(r"\d{4}", full):
            return False
        # Ninguna palabra larga con transiciones lower→Upper (camelCase pegado)
        for w in full.split():
            if len(w) >= 15:
                transitions = sum(1 for i in range(1, len(w)) if w[i-1].islower() and w[i].isupper())
                if transitions >= 1:
                    return False
        # Debe tener el short_name al principio o como palabra
        if short_name.lower() not in full.lower():
            return False
        return True

    candidates_found = []
    for pdf_path in candidates:
        txt = _hdr_pdf_text(pdf_path, max_pages=15)
        if not txt:
            continue
        for m in pat.finditer(txt):
            full = f"{short_name} {m.group(1).strip()}"
            full = _re.sub(r"\s{2,}", " ", full).strip(" .,;")
            if _is_valid_gestora_full(full) and full.lower() != short_name.lower():
                candidates_found.append(full)

    if not candidates_found:
        _HDR_PDF_CACHE[cache_key] = None
        return None

    # Ranking: priorizar candidatos con patrón "Management" + sufijo legal (Ltd/Plc/SA).
    # Son los nombres oficiales de la gestora (no productos/fondos).
    def _score(name):
        s = 0
        n = name.lower()
        if "fund management" in n or "fund managers" in n:
            s += 10
        if _re.search(r"\b(?:management|investments?|capital|asset\s+management)\b", n):
            s += 5
        if _re.search(r"\b(?:limited|ltd|plc|sa|sgiic|gmbh|ag|bv|llc|inc)\b\.?", n):
            s += 3
        # Penalizar candidatos demasiado cortos o largos
        if len(name) < 20 or len(name) > 70:
            s -= 1
        return s

    best = max(candidates_found, key=_score)
    _HDR_PDF_CACHE[cache_key] = best
    return best


def _hdr_resolve_gestora(data):
    """Cascade gestora: data.gestora → kpis.gestora → manager_profile.gestora →
    cssf.gestora_oficial → intl_data.gestora. Autoridad: versión más completa
    (con sufijo institucional como Ltd/Plc/SA/GmbH) si hay varios candidatos
    compatibles. Una gestora escueta como 'GAM' pierde frente a 'GAM Fund
    Management Ltd' si ambos están disponibles."""
    import re as _re
    k = data.get("kpis", {})
    candidates = [
        data.get("gestora"),
        k.get("gestora"),
    ]
    isin = data.get("isin", "")
    mgr = _hdr_load_side(isin, "manager_profile.json")
    candidates.append(mgr.get("gestora"))
    cssf = _hdr_load_side(isin, "cssf_data.json")
    candidates.append(cssf.get("gestora_oficial") or cssf.get("gestora"))
    cbi = _hdr_load_side(isin, "cbi_data.json")
    candidates.append(cbi.get("gestora_oficial") or cbi.get("gestora"))

    # Limpiar: solo strings truthy
    clean = []
    for c in candidates:
        if c and str(c).strip():
            clean.append(str(c).strip())
    if not clean:
        return ""

    # Patrón de sufijos institucionales (gestora real completa)
    inst_suffix = _re.compile(
        r"\b(?:Ltd\.?|Limited|Plc|PLC|S\.?A\.?|S\.?L\.?|SGIIC|"
        r"GmbH|AG|BV|N\.?V\.?|LLC|Inc\.?|"
        r"Management|Investments?|Capital|Partners|Asset\s+Management|"
        r"Fund\s+Managers?)\b",
        _re.I,
    )

    # Separar por tipo: con sufijo institucional vs sin
    with_suffix = [c for c in clean if inst_suffix.search(c)]
    if with_suffix:
        return max(with_suffix, key=len)

    # Ningún candidato tiene sufijo institucional. Intentar enriquecer desde
    # los PDFs buscando "{nombre_corto} {Fund Management Limited|Ltd|Plc|etc.}".
    base = clean[0]
    if len(base) < 25:  # solo enriquecer si el valor actual es escueto
        enriched = _hdr_enrich_gestora_from_ar(isin, base)
        if enriched and inst_suffix.search(enriched):
            return enriched
    return base


def _hdr_resolve_gestores(data):
    """Cascade gestores del header — misma prioridad que el quality check."""
    sources = [
        data.get("gestores", {}).get("equipo", []),
        data.get("_int_gestores", []),
        data.get("analyst_synthesis", {}).get("gestores", {}).get("equipo", []),
        data.get("analyst_synthesis", {}).get("gestores", {}).get("perfiles", []),
        data.get("cualitativo", {}).get("gestores", []),
    ]
    for src in sources:
        if not src:
            continue
        # Aceptar lista de dicts o lista de strings
        names = []
        for item in src:
            if isinstance(item, dict):
                n = item.get("nombre") or item.get("name")
                if n:
                    names.append(n.strip())
            elif isinstance(item, str) and item.strip():
                names.append(item.strip())
        if names:
            return names
    return []


def _hdr_resolve_depositario(data):
    """Cascade depositario: kpis.depositario → cssf.depositario → cbi.depositario
    → buscar en readings texto 'depositary/custodian [Bank]'."""
    k = data.get("kpis", {})
    if k.get("depositario"):
        return k["depositario"]
    isin = data.get("isin", "")
    for fname in ("cssf_data.json", "cbi_data.json"):
        side = _hdr_load_side(isin, fname)
        for key in ("depositario", "custodian", "depositary"):
            if side.get(key):
                return side[key]
    # Scan readings for explicit depositary mention
    readings = _hdr_load_side(isin, "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    import re as _re
    for r in all_r:
        txt = (r.get("resumen", "") or "") + " " + (r.get("titulo", "") or "")
        m = _re.search(r"(?i)(depositari[oa]|custodian|depositary)[:\s]+([A-Z][\w &\.,\-]{3,60})", txt)
        if m:
            return m.group(2).strip(" .,")
    # Último recurso: extraer del AR/prospecto en raw/discovery/
    return _hdr_extract_depositario_from_ar(isin) or ""


def _hdr_map_category_to_spanish(raw_category: str, data: dict) -> str:
    """Mapea categoría a uno de 4 BUCKETS base + refinamiento:
       1) Mixto
       2) Renta Fija
       3) Renta Variable
       4) Alternativo (oro, commodities, cat bonds, event-driven, macro,
          market-neutral, long/short, volatility, insurance-linked)

    Refinamiento dentro de cada bucket:
    - Mixto: Flexible / Conservador / Moderado / Agresivo / Global / Alternativo
    - Renta Fija: Retorno Absoluto / Flexible / Corporativa / Corto Plazo /
                  Gubernamental / Emergentes / Alta Rentabilidad
    - Renta Variable: Global / Europea / USA / Emergentes / Sectorial /
                      Retorno Absoluto / Long/Short
    - Alternativo: Cat Bonds / Oro / Commodities / Macro Global /
                   Market Neutral / Event-Driven / Multi-estrategia /
                   Volatilidad / Arbitraje / Infraestructura / Inmobiliario

    La detección mira:
    - Categoría Morningstar / FT textual.
    - Benchmark del fondo (ej. '€STER+2%' → Retorno Absoluto).
    - Nombre del fondo (ej. 'Alpha', 'Absolute', 'L/S').
    - Posiciones reales (oro, commodities → Alternativo).
    """
    import re as _re
    if not raw_category:
        raw_category = ""
    c = raw_category.lower()

    # Mirar benchmark del fondo y nombre
    benchmark = (data.get("kpis", {}) or {}).get("benchmark", "") or ""
    nombre_fondo = (data.get("nombre", "") or "").lower()
    bench_l = benchmark.lower()
    absolute_return = bool(
        _re.search(r"(?:€ster|€str|estr|€str|ester|euribor|libor|sofr|sonia|cash)\s*[+\-]", bench_l)
        or "absolute return" in bench_l or "retorno absoluto" in bench_l
        or "absolute return" in c or "retorno absoluto" in c
        or "unconstrained" in c
        or ("alpha" in nombre_fondo and "bond" in c)
    )

    # ── BUCKET 4: ALTERNATIVO ────────────────────────────────────
    # Detección por nombre / categoría / posiciones
    narrativa_txt = " ".join([
        str(data.get("_int_estrategia", "") or ""),
        str(data.get("_int_filosofia", "") or ""),
        str(data.get("_int_tipo_activos", "") or ""),
    ]).lower()
    text_check = c + " " + nombre_fondo + " " + narrativa_txt
    alt_keywords = {
        "Alternativo Cat Bonds": ["catastrophe bond", "cat bond", "cat bonds", "insurance-linked", "ils ", "reinsurance"],
        "Alternativo Oro": ["gold fund", "oro fund", "precious metal"],
        "Alternativo Commodities": ["commodity", "commodities"],
        "Alternativo Macro": ["global macro", "macro fund"],
        "Alternativo Market Neutral": ["market neutral", "market-neutral"],
        "Alternativo Event-Driven": ["event driven", "event-driven", "merger arbitrage", "risk arbitrage"],
        "Alternativo Long/Short": ["long/short", "long-short", "long short"],
        "Alternativo Volatilidad": ["volatility", "volatilidad"],
        "Alternativo Arbitraje": ["arbitrage fund", "arbitraje"],
        "Alternativo Infraestructura": ["infrastructure fund", "infraestructura"],
        "Alternativo Inmobiliario": ["real estate", "reit", "inmobiliario"],
        "Alternativo Multi-estrategia": ["multistrategy", "multi-strategy", "multi-estrategia"],
    }
    for label, kws in alt_keywords.items():
        if any(kw in text_check for kw in kws):
            # Refinamiento dentro de alt con retorno absoluto
            return label
    # Perfil activos: si hay oro + commodities > 15% → Alternativo Multi-activo
    tipos_cartera = set()
    peso_gold = 0
    peso_commodity = 0
    for pos in (data.get("posiciones", {}).get("actuales", []) or []):
        t = (pos.get("asset_type") or pos.get("sector") or "").lower()
        nombre_pos = (pos.get("nombre") or "").lower()
        peso = float(pos.get("peso_pct", 0) or 0)
        if "gold" in t or "oro" in t or "gold" in nombre_pos:
            peso_gold += peso
            tipos_cartera.add("gold")
        if "commodit" in t or "commodit" in nombre_pos:
            peso_commodity += peso
            tipos_cartera.add("commodity")
        for marker, cat in [("inflation","inflation"),("linker","inflation"),("tips","inflation")]:
            if marker in t or marker in nombre_pos:
                tipos_cartera.add("inflation")
    if "gold" in narrativa_txt or "oro" in narrativa_txt:
        tipos_cartera.add("gold")
    if "inflation" in narrativa_txt or "linker" in narrativa_txt or "ligados a la inflación" in narrativa_txt:
        tipos_cartera.add("inflation")

    # Detect equity-heavy / bond-heavy puros primero
    if _re.search(r"\b(equity|renta\s+variable|stock)\b", c):
        if absolute_return: return "Renta Variable Retorno Absoluto"
        if "global" in c: return "Renta Variable Global"
        if "europ" in c: return "Renta Variable Europea"
        if "emerg" in c: return "Renta Variable Emergentes"
        if _re.search(r"\b(us|america|american)\b", c): return "Renta Variable USA"
        return "Renta Variable"
    if _re.search(r"\b(bond|renta\s+fija|fixed\s+income|deuda)\b", c):
        if absolute_return: return "Renta Fija Retorno Absoluto"
        if "corporat" in c: return "Renta Fija Corporativa"
        if "short" in c or "corto" in c: return "Renta Fija Corto Plazo"
        if "govern" in c or "gobierno" in c: return "Renta Fija Gubernamental"
        if "emerg" in c: return "Renta Fija Emergentes"
        if "flexible" in c or "flex" in c: return "Renta Fija Flexible"
        return "Renta Fija"

    # Detect allocation profile
    allocation_map = [
        (r"cautious|conservative|defensive|conservad|defensiv", "Mixto Conservador"),
        (r"moderate|moderad|balanced|equilibrad", "Mixto Moderado"),
        (r"aggressive|agresiv|growth", "Mixto Agresivo"),
        (r"flexible|flexibl|unconstrained", "Mixto Flexible"),
    ]
    mapped = None
    for pat, spanish in allocation_map:
        if _re.search(pat, c):
            mapped = spanish
            break

    # Refinar: fondo flexible con oro/gold + bonos ligados inflación → Mixto Alternativo
    # (tipos_cartera ya calculado arriba)
    if mapped == "Mixto Flexible" and len(tipos_cartera) >= 2:
        return "Mixto Alternativo"

    if mapped:
        return mapped

    # Sin allocation clara — último recurso
    if "multi-asset" in c or "mixto" in c or "multiasset" in c:
        if tipos_cartera:
            return "Mixto Alternativo" if "gold" in tipos_cartera else "Mixto Conservador"
        return "Mixto"

    # Caso CNMV "Global" (clasificación genérica de Renta 4/otros): refinar por mix_activos
    if c == "global" or c == "":
        mix = (data.get("cuantitativo", {}) or {}).get("mix_activos_historico", []) or []
        if mix:
            last = mix[-1] if isinstance(mix[-1], dict) else {}
            rv = float(last.get("renta_variable_pct", 0) or 0)
            rf = float(last.get("renta_fija_pct", 0) or 0)
            liq = float(last.get("liquidez_pct", 0) or 0)
            if rv >= 70:
                return "Renta Variable Global"
            if rf >= 70:
                return "Renta Fija Global"
            if rv + rf > 0:
                if rv >= 50:
                    return "Mixto Global"
                if rv >= 30:
                    return "Mixto Moderado"
                return "Mixto Conservador"
        # Alternativa: inferir de posiciones reales
        posiciones = data.get("posiciones", {}).get("actuales", []) or []
        if posiciones:
            tipos = [(p.get("tipo") or p.get("asset_type") or "").upper() for p in posiciones]
            peso_rv = sum(float(p.get("peso_pct", 0) or 0)
                          for p in posiciones if (p.get("tipo") or p.get("asset_type") or "").upper() in ("ACCIONES", "EQUITY", "RV"))
            peso_rf = sum(float(p.get("peso_pct", 0) or 0)
                          for p in posiciones if (p.get("tipo") or p.get("asset_type") or "").upper() in ("BONO", "BOND", "RF"))
            if peso_rv + peso_rf > 20:
                if peso_rv >= 70: return "Renta Variable Global"
                if peso_rf >= 70: return "Renta Fija Global"
                if peso_rv >= 50: return "Mixto Global"
                return "Mixto Moderado"
        return "Global"

    # Si no reconocemos, devolver tal cual limpio
    return raw_category.strip().title() if raw_category else ""


def _hdr_resolve_categoria(data):
    """Cascade categoria → siempre devuelve categoría de industria española.
    1) Extractor si devuelve algo específico (≥2 palabras no genéricas).
    2) Morningstar/FT de readings → mapear al español.
    3) Inferir del perfil del fondo (mix activos) si nada más.
    """
    k = data.get("kpis", {})
    raw = (k.get("clasificacion") or "").strip()
    generic = {"", "—", "global", "multi-asset", "multiasset", "mixto", "fondo", "other"}
    # Paso 1: extractor si específico
    if raw and raw.lower() not in generic and len(raw.split()) >= 2:
        # Pasar por el mapper también por si está en inglés
        mapped = _hdr_map_category_to_spanish(raw, data)
        return mapped or raw

    # Paso 2a: scrape FT summary del ISIN (fuente más fiable)
    isin = data.get("isin", "")
    ft = _hdr_ft_summary_scrape(isin, "EUR") if isin else {}
    cat_ft = ft.get("category_ms", "")
    if cat_ft:
        mapped = _hdr_map_category_to_spanish(cat_ft, data)
        if mapped:
            return mapped

    # Paso 2b: readings Morningstar/FT
    readings = _hdr_load_side(isin, "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    import re as _re
    cat_patterns = [
        r"(?i)categor[íi]a\s+de?\s+([A-ZÁÉÍÓÚa-záéíóú][\w\s\-áéíóú]{5,50}?)\s+por\s+Morningstar",
        r"(?i)clasificado\s+en\s+la\s+categor[íi]a\s+de?\s+([A-Za-záéíóú][\w\s\-áéíóú]{5,50}?)\s+por",
        r"(?i)Morningstar\s+categor[yi]\s*[:\-]?\s*([A-Z][\w\s\-]{5,50})",
    ]
    for r in all_r:
        # Filtrar: solo readings del mismo ISIN (evita mezclar sub-fondos)
        if isin and isin.lower() not in (r.get("url", "") or "").lower():
            continue
        txt = r.get("resumen", "") or ""
        for pat in cat_patterns:
            m = _re.search(pat, txt)
            if m:
                cat_raw = m.group(1).strip().rstrip(".,;")
                mapped = _hdr_map_category_to_spanish(cat_raw, data)
                if mapped:
                    return mapped
    # Paso 3: inferir del perfil + benchmark
    inferred = _hdr_map_category_to_spanish(raw, data)
    return inferred or raw or ""


def _hdr_oldest_class_date(data):
    """Recorre todas las clases del fondo (data.clases / _int_clases) y devuelve
    la fecha MÁS ANTIGUA de inception/launch declarada explícitamente. La fecha
    de inicio del fondo es la de la primera clase creada, NO la de la clase R
    EUR (típicamente la más reciente).

    IMPORTANTE: solo usa campos `inception_date`/`launch_date`/`first_nav_date`
    declarados explícitamente. Los `nav_total_snapshots` NO se usan porque un
    snapshot solo dice 'la clase ya existía a esa fecha', no cuándo fue creada.
    Para esos casos, cae a la cascada FT/extractor.

    Devuelve string YYYY-MM-DD o None.
    """
    import re as _re
    clases = (data.get("clases") or data.get("_int_clases") or []) + (
        data.get("intl_data", {}).get("clases", []) if isinstance(data.get("intl_data"), dict) else []
    )
    candidates = []
    for c in clases:
        if not isinstance(c, dict):
            continue
        for fld in ("inception_date", "launch_date", "fecha_lanzamiento", "first_nav_date"):
            v = c.get(fld)
            if v and isinstance(v, str):
                candidates.append(v)
    if not candidates:
        return None
    # Normalizar a YYYY-MM-DD para comparar
    parsed = []
    for s in candidates:
        s = str(s).strip()
        m = _re.match(r"^(\d{4})-(\d{1,2})(?:-(\d{1,2}))?", s)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), (m.group(3) or "01").zfill(2)
            parsed.append(f"{y}-{mo}-{d}")
            continue
        m = _re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            parsed.append(f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}")
            continue
        m = _re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", s)
        if m:
            mn = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
                  "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}.get(
                      m.group(2).lower()[:3])
            if mn:
                parsed.append(f"{m.group(3)}-{mn}-{m.group(1).zfill(2)}")
    return min(parsed) if parsed else None


def _hdr_resolve_fecha_inicio(data):
    """Devuelve fecha con MES+AÑO si se puede. Cascada:
    0) Clases del fondo: usar la fecha MÁS ANTIGUA de inception/launch/snapshots
       de TODAS las clases (la fecha del fondo es la de la primera clase, NO la
       de la clase R EUR retail que suele ser la más reciente).
    1) fecha_registro CNMV (DD/MM/YYYY) → formatea.
    2) launch_date del FT summary del ISIN (fiable, específico del sub-fondo).
    3) inception_date / fecha_lanzamiento INT (YYYY-MM-DD) → formatea.
    4) Scan texto de `_int_historia`/`historia_fondo`/readings.
    5) anio_creacion (solo año — último recurso, puede ser del umbrella).
    """
    import re as _re
    months_num_to_es = {"01":"Enero","02":"Febrero","03":"Marzo","04":"Abril",
                        "05":"Mayo","06":"Junio","07":"Julio","08":"Agosto",
                        "09":"Septiembre","10":"Octubre","11":"Noviembre","12":"Diciembre"}
    # 0) Clase más antigua entre todas las clases del fondo
    oldest = _hdr_oldest_class_date(data)
    if oldest:
        m = _re.match(r"^(\d{4})-(\d{2})", oldest)
        if m:
            return f"{months_num_to_es.get(m.group(2), m.group(2))} {m.group(1)}"
    months_en_to_es = {
        "january":"Enero","february":"Febrero","march":"Marzo","april":"Abril",
        "may":"Mayo","june":"Junio","july":"Julio","august":"Agosto",
        "september":"Septiembre","october":"Octubre","november":"Noviembre",
        "december":"Diciembre",
        "jan":"Enero","feb":"Febrero","mar":"Marzo","apr":"Abril",
        "jun":"Junio","jul":"Julio","aug":"Agosto","sep":"Septiembre",
        "oct":"Octubre","nov":"Noviembre","dec":"Diciembre",
        "enero":"Enero","febrero":"Febrero","marzo":"Marzo","abril":"Abril",
        "mayo":"Mayo","junio":"Junio","julio":"Julio","agosto":"Agosto",
        "septiembre":"Septiembre","octubre":"Octubre","noviembre":"Noviembre",
        "diciembre":"Diciembre",
    }
    k = data.get("kpis", {})
    # 1) Formato CNMV DD/MM/YYYY
    fr = k.get("fecha_registro") or ""
    if fr and "/" in fr:
        return format_date(fr)
    # 2) FT summary launch_date (sub-fondo específico, no umbrella)
    isin = data.get("isin", "")
    if isin:
        ft = _hdr_ft_summary_scrape(isin, "EUR")
        ld = ft.get("launch_date", "") if ft else ""
        # Formatos FT típicos: "09 Mar 2020" / "14 Dec 2017"
        m = _re.match(r"\d{1,2}\s+([A-Za-z]+)\s+(\d{4})", ld or "")
        if m:
            month = months_en_to_es.get(m.group(1).lower()[:3], m.group(1).title())
            return f"{month} {m.group(2)}"
    # 3) inception_date INT (YYYY-MM-DD)
    inc = k.get("inception_date") or k.get("fecha_lanzamiento") or ""
    if inc:
        m = _re.match(r"(\d{4})-(\d{2})", str(inc))
        if m:
            months_num = {"01":"Enero","02":"Febrero","03":"Marzo","04":"Abril",
                          "05":"Mayo","06":"Junio","07":"Julio","08":"Agosto",
                          "09":"Septiembre","10":"Octubre","11":"Noviembre","12":"Diciembre"}
            return f"{months_num.get(m.group(2), m.group(2))} {m.group(1)}"
        m = _re.match(r"(\d{4})", str(inc))
        if m:
            return m.group(1)
    # 3) Scan textos del extractor + readings
    texts = [
        data.get("_int_historia", "") or "",
        (data.get("cualitativo", {}) or {}).get("historia_fondo", "") or "",
    ]
    readings = _hdr_load_side(data.get("isin", ""), "readings_data.json")
    for r in readings.get("analisis_completos", []) + readings.get("otros_readings", []):
        texts.append(r.get("resumen", "") or "")
    # Patterns:
    #   EN: "launched on 13 February 2012" | "inception February 2012"
    #   ES: "lanzado el 13 de febrero de 2012" | "lanzamiento febrero 2012"
    pattern = _re.compile(
        r"(?i)(?:launched|launch\s+date|inception|lanzad[oa]|fue\s+lanzad[oa]|lanzamiento|launched\s+on)"
        r"\s*(?:el|on|:)?\s*"
        r"(?:(\d{1,2})\s+(?:de\s+)?)?"
        r"(January|February|March|April|May|June|July|August|September|October|November|December|"
        r"enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"
        r"\s*(?:de\s+)?(\d{4})"
    )
    for text in texts:
        if not text:
            continue
        m = pattern.search(text)
        if m:
            month = months_en_to_es.get(m.group(2).lower(), m.group(2).title())
            year = m.group(3)
            return f"{month} {year}"
    # 4) Solo año
    ac = k.get("anio_creacion")
    if ac:
        return str(ac)
    return ""


def _hdr_resolve_divisa(data):
    """Detecta divisa + hedged.

    **Regla hedged (universal):**
    1. Nombre oficial UCITS del extractor → las clases hedged llevan marca
       SIEMPRE en el nombre completo: 'Hedged', '(H)', '-H-', 'H-A', 'HC'.
    2. Nombre completo del FT search — a veces el nombre en output.json está
       simplificado y no dice "Hedged" pero el nombre real en FT sí lo incluye.
    3. Cruce con KID: frases 'Currency hedged' / 'Share class hedged' = hedged;
       'Designation Risk' / 'when not hedged' = NOT hedged.
    4. Si ninguna señal positiva → NOT hedged.

    Divisa: regex del nombre → kpis.divisa → _int_clases[0].currency → EUR.
    """
    import re as _re
    nombre = data.get("nombre", "") or ""
    isin = data.get("isin", "")

    hedge_regex = r"\b(hedged|hgd|[-\s]H[-\s]|\(H\)|H-[AIWCM]|HC\b|HA\b|HI\b|HW\b)"

    # 1) Nombre corto del extractor
    hedged = bool(_re.search(hedge_regex, nombre, _re.I))

    # 2) Nombre COMPLETO del FT search del ISIN objetivo (p.ej. GAM Cat Bond R EUR Hedged)
    if not hedged and isin:
        for c in _hdr_ft_search_sibling_classes(nombre, isin):
            if c.get("isin") == isin:
                full_name = c.get("fund_name", "") or ""
                if _re.search(hedge_regex, full_name, _re.I):
                    hedged = True
                break

    # 3) Cruce con KID/factsheet local si el nombre no es claro
    if not hedged and isin:
        pdfs = _hdr_discovery_pdfs(isin)
        for pdf_path in (pdfs.get("kid", []) + pdfs.get("factsheet", []))[:2]:
            txt = _hdr_pdf_text(pdf_path, max_pages=4) or ""
            if not txt:
                continue
            if _re.search(r"(?i)\b(?:share\s+class|class|clase)\s+(?:is\s+)?(?:currency[-\s]+)?hedged\b", txt):
                hedged = True
                break
            if _re.search(r"(?i)\bcurrency\s+hedged\s+(?:to|against)\b", txt):
                hedged = True
                break
            if _re.search(r"(?i)\b(?:designation\s+risk|when\s+not\s+hedged|unhedged\s+share)", txt):
                hedged = False
                break

    # Divisa: primero del nombre, luego kpis, luego _int_clases
    curr_match = _re.search(r"\b(EUR|USD|GBP|CHF|JPY|CAD|AUD|SEK|NOK|DKK)\b", nombre.upper())
    if curr_match:
        curr = curr_match.group(1)
    else:
        k = data.get("kpis", {})
        curr = (k.get("divisa") or "").upper()
        if not curr:
            clases = data.get("_int_clases", []) or []
            if clases and isinstance(clases[0], dict):
                curr = (clases[0].get("currency") or "").upper()
        if not curr:
            curr = "EUR"
    return f"{curr} (hedged)" if hedged else curr


def _hdr_resolve_srri(data):
    """SRRI real o None. NUNCA inventar un valor por defecto.
    Prioridad: kpis.perfil_riesgo → kpis.srri → kpis.sri → buscar en readings."""
    k = data.get("kpis", {})
    for key in ("perfil_riesgo", "srri", "sri", "risk_indicator", "sri_level"):
        v = k.get(key)
        if v:
            try:
                iv = int(v)
                if 1 <= iv <= 7:
                    return iv
            except Exception:
                pass
    # Readings fallback (patrón "SRRI 4", "risk indicator 3")
    isin = data.get("isin", "")
    readings = _hdr_load_side(isin, "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    import re as _re
    for r in all_r:
        txt = (r.get("resumen", "") or "") + " " + (r.get("titulo", "") or "")
        m = _re.search(r"(?i)\b(?:SRRI|SRI|risk\s+indicator|indicador\s+de\s+riesgo)\s*[:\-]?\s*(\d)\b", txt)
        if m:
            iv = int(m.group(1))
            if 1 <= iv <= 7:
                return iv
    # Último recurso: extraer directamente del KID/factsheet en raw/discovery/
    return _hdr_extract_srri_from_kid(isin)


_RETAIL_CLASS_PREFIXES = {"A", "O", "N", "R", "P", "F", "B"}
_CLEAN_CLASS_PREFIXES = {"I", "X", "W", "Z", "S", "C", "Q"}

# Palabras inequívocas (prioridad sobre prefijo letra — convención GAM/M&G/otras)
_CLEAN_CLASS_WORDS = [
    "institutional", "institucional", "clean", "cleanshare", "clean share",
    "wholesale", "no-trail", "no trail", "no-load", "superinstitutional",
    "super institutional", "z share", "z-share",
]
_RETAIL_CLASS_WORDS = [
    "ordinary", "retail", "minorista", "advisor", "advisory",
    "investor", "individual", "private", "particular",
]


def _hdr_clean_class_code(code: str) -> str:
    """Limpia un código de clase eliminando el prefijo del paraguas.
    Ej: 'GAM Star Fund plc - GAM Swiss Re Cat Bond Class R Accumulation EUR Hedged'
        → 'Class R Accumulation EUR Hedged'
    Ej: 'DNCA Invest Alpha Bonds Class I EUR' → 'Class I EUR'
    Si no hay 'Class', devuelve los últimos segmentos más distintivos
    (Currency + Acc/Inc + Hedged).
    """
    if not code:
        return ""
    import re as _re
    # Buscar 'Class X ...' o 'Classe X ...' — tomar desde ahí al final
    m = _re.search(r"\bClass[e]?\s+[A-Z][-A-Z0-9]{0,6}[\s\S]*", code, _re.I)
    if m:
        return m.group(0).strip()
    # Si no encuentra 'Class', truncar al último segmento después de ' - '
    if " - " in code:
        code = code.split(" - ")[-1].strip()
    # Máximo 60 chars
    if len(code) > 60:
        return code[:57].rstrip() + "..."
    return code


def _hdr_classify_class(code: str) -> str:
    """Clasifica clase como 'retail' | 'limpia' | 'other'.

    Prioridad:
    1. PALABRAS inequívocas ('Institutional', 'Ordinary', 'Clean', 'Retail'...)
       Son convenciones universales que cualquier gestora respeta (GAM, M&G,
       Vanguard suelen usar 'Institutional Shares' vs 'Ordinary Shares').
    2. PREFIJO letra UCITS clásico (A/O/N → retail; I/X/W → limpia).
    """
    if not code:
        return "other"
    import re as _re
    code_l = code.lower()
    # 1) Word-based (highest precedence)
    for w in _CLEAN_CLASS_WORDS:
        if w in code_l:
            return "limpia"
    for w in _RETAIL_CLASS_WORDS:
        if w in code_l:
            return "retail"
    # 2) Prefijo letra UCITS
    s = _re.sub(r"(?i)\bclass\b", "", code).strip()
    m = _re.search(r"[A-Z]", s)
    if not m:
        return "other"
    letter = m.group(0).upper()
    if letter in _RETAIL_CLASS_PREFIXES:
        return "retail"
    if letter in _CLEAN_CLASS_PREFIXES:
        return "limpia"
    return "other"


def _hdr_ft_search_sibling_classes(fund_name, target_isin):
    """Descubre ISINs hermanos usando FT search.
    URL: https://markets.ft.com/data/search?query=<nombre>&searchCategory=funds
    FT devuelve TODAS las clases del fondo con patrón consistente:
    '/data/funds/tearsheet/summary?s=ISIN:CCY">Nombre Fondo CODE CURR'
    Filtra a las que pertenecen al mismo sub-fondo (contienen el nombre base).
    Devuelve [{isin, code, currency, fund_name}].
    """
    import re as _re
    if not fund_name:
        return []
    cache_key = f"ft_search::{target_isin}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    # Palabras "paraguas" del vehículo legal que NO son distintivas del sub-fondo
    # Evita falsos positivos: 'GAM Star Cat Bond' no debe matchear con 'Nordea Stars Bond'.
    UMBRELLA_WORDS = {
        "star", "stars", "fund", "funds", "sicav", "plc", "investment", "investments",
        "invest", "capital", "asset", "management", "ucits", "oeic", "sub-fund", "fcp",
        "ltd", "limited", "gmbh", "ireland", "luxembourg", "global", "international",
        "the", "and", "ag", "etf", "select", "open", "access",
    }

    # Extraer nombre base del sub-fondo (sin clase/divisa/Accumulation)
    base = fund_name.lower()
    base = _re.sub(r"\b(class|clase)\s+[a-z][\-a-z0-9]{0,6}\b", "", base, flags=_re.I)
    base = _re.sub(r"\b(eur|usd|gbp|chf|jpy|cad|aud)\s+(acc|inc|accumulation|income|distribution|dist)?\b", "", base, flags=_re.I)
    base = _re.sub(r"[-–—]\s*", " ", base).strip()
    words = [w for w in _re.split(r"\s+", base) if w and not w.isdigit()]
    # Excluir palabras paraguas Y palabras cortas (≤2 chars)
    distinctive = [w for w in words if len(w) > 2 and w not in UMBRELLA_WORDS]
    # Tomar hasta las primeras 3 palabras DISTINTIVAS
    keywords = distinctive[:3] if distinctive else words
    fund_query = " ".join(keywords) if keywords else fund_name.split(" Class")[0]

    url = f"https://markets.ft.com/data/search?query={fund_query.replace(' ', '%20')}&searchCategory=funds"
    try:
        import httpx
        with httpx.Client(timeout=8, follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
            resp = c.get(url)
            if resp.status_code != 200:
                _HDR_PDF_CACHE[cache_key] = []
                return []
            html = resp.text
    except Exception:
        _HDR_PDF_CACHE[cache_key] = []
        return []

    # Extraer: ISIN + CCY + nombre del resultado
    matches = _re.findall(
        r'/data/funds/tearsheet/summary\?s=([A-Z]{2}[A-Z0-9]{9}[0-9]):([A-Z]{3})[^>]*>([^<]{5,150})',
        html, _re.I
    )
    # Las keywords distintivas ya han sido filtradas (sin palabras paraguas).
    # Usar >2 chars para conservar "cat" (importante en "cat bond").
    base_keywords_lc = [k.lower() for k in keywords if len(k) > 2]

    def _build_results(strict):
        """Construye resultados filtrados. strict=True requiere TODAS las
        keywords. strict=False requiere ≥60% de las keywords (fallback)."""
        min_match = len(base_keywords_lc) if strict else max(1, int(len(base_keywords_lc) * 0.6))
        out = []
        for isin_f, ccy, name in matches:
            name_lc = name.strip().lower()
            hits = sum(1 for kw in base_keywords_lc if kw in name_lc)
            if hits < min_match:
                continue
            code_m = _re.search(
                r"\b([A-Z][-A-Z0-9]{0,6})\s+(EUR|USD|GBP|CHF|JPY|CAD|AUD|SGD)\b",
                name, _re.I
            )
            if code_m:
                class_letter = code_m.group(1).upper()
                code_str = f"Class {class_letter} {ccy}"
            else:
                code_str = name.strip()
            out.append({"isin": isin_f, "currency": ccy.upper(),
                        "code": code_str, "fund_name": name.strip()})
        return out

    # Paso 1: filtro ESTRICTO (todas las keywords). Default para evitar falsos positivos.
    results = _build_results(strict=True)

    # Paso 2: FALLBACK relajado SOLO si:
    #   - Filtro estricto devolvió 0 resultados, Y
    #   - El fondo no tiene AR ni prospectus locales (hemos agotado las vías principales)
    if not results:
        base_dir = ROOT / "data" / "funds" / target_isin / "raw" / "discovery"
        has_ar = False
        if base_dir.exists():
            for p in base_dir.glob("*.pdf"):
                name_l = p.name.lower()
                if "annual" in name_l or "prospect" in name_l or "interim" in name_l:
                    has_ar = True
                    break
        if not has_ar:
            results = _build_results(strict=False)

    _HDR_PDF_CACHE[cache_key] = results
    return results


def _hdr_extract_fund_classes_from_ar(isin):
    """Extrae clases del fondo con ISINs del Annual Report (o prospecto).
    Los AR suelen tener una tabla 'Share Classes' o 'Statistics' con todas las
    clases del sub-fondo y sus ISINs. Busca filas con:
    - Class letter + currency + (ISIN pattern)
    - Ex: 'Class A EUR  LU1694789451' o 'I EUR Acc — LU1694789378'.
    Devuelve [{code, isin, currency}] deduped.
    """
    import re as _re
    cache_key = f"ar_classes::{isin}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    pdfs = _hdr_discovery_pdfs(isin)
    candidates = pdfs["annual_report"] + pdfs["interim_report"] + pdfs["prospectus"] if pdfs.get("prospectus") else pdfs["annual_report"] + pdfs["interim_report"]
    found = {}
    for pdf_path in candidates[:2]:
        txt = _hdr_pdf_text(pdf_path, max_pages=30) or ""
        if not txt:
            continue
        # Pattern: <CLASS-CODE> <CURRENCY> ... <ISIN 12-char>
        # Ejemplo real AR: "Class A EUR Accumulation ... LU1694789451"
        # ISIN VÁLIDO: 2 letras código país ISO + 9 alfanuméricos + 1 dígito check
        _ISIN_COUNTRIES = (
            "LU", "IE", "FR", "GB", "DE", "ES", "IT", "NL", "BE", "AT", "CH",
            "SE", "NO", "DK", "FI", "PT", "US", "CA", "AU", "JP", "HK", "SG",
            "KY", "BM", "JE", "GG", "IM", "VG",
        )
        for m in _re.finditer(
            r"([A-Z](?:[\-A-Z])?(?:\s+[A-Z](?:[\-A-Z])?)?)\s+"
            r"(EUR|USD|GBP|CHF|JPY|CAD|AUD)\s+"
            r"(Acc|Inc|Accumulation|Income|Distribution|Dist|D)?[^\n]{0,200}?"
            r"(\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b)",
            txt, _re.I
        ):
            code_letter = m.group(1).strip()
            ccy = m.group(2).upper()
            variant = (m.group(3) or "").strip()
            isin_f = m.group(4).upper()
            if isin_f == isin:
                continue
            # Validar: código país debe ser ISO válido
            if isin_f[:2] not in _ISIN_COUNTRIES:
                continue
            # Validar: último carácter debe ser dígito (check digit)
            if not isin_f[-1].isdigit():
                continue
            code_str = f"Class {code_letter} {ccy}" + (f" {variant}" if variant else "")
            if isin_f not in found:
                found[isin_f] = {"isin": isin_f, "code": code_str.strip(), "currency": ccy}
    result = list(found.values())
    _HDR_PDF_CACHE[cache_key] = result
    return result


def _hdr_extract_fund_classes_from_readings(data):
    """Extrae clases del fondo con ISINs desde readings (FT/Morningstar).
    Patrón típico FT: 'Trojan Fund (Ireland) X USD Acc (ISIN IE00BF29R422)'.
    Devuelve [{code, isin, currency}] deduped por ISIN.
    """
    import re as _re
    readings = _hdr_load_side(data.get("isin", ""), "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    fund_base = (data.get("nombre", "") or "").split(" O EUR")[0].split(" Class")[0].strip()
    found = {}
    for r in all_r:
        text = ((r.get("titulo") or "") + " " + (r.get("resumen") or "")).strip()
        # ISIN pattern
        isin_matches = _re.finditer(
            r"(?:ISIN[:\s]*)?([A-Z]{2}[A-Z0-9]{10})", text
        )
        for im in isin_matches:
            isin = im.group(1)
            if not isin or isin == data.get("isin"):
                continue  # Skip principal; ya lo tenemos
            # Context around ISIN: look back 150 chars for "<FUND_NAME> CLASS_CODE"
            start = max(0, im.start() - 150)
            context = text[start:im.start()]
            # Match class code near fund name
            # Pattern: "Fund Name CLASS_CODE CURRENCY Acc/Inc"
            cm = _re.search(
                r"([A-Z][-\w]{0,3})\s+(EUR|USD|GBP|CHF|JPY|CAD|AUD)\s+(Acc|Inc|Accumulation|Income|Distribution|D|Dist)",
                context, _re.I)
            if cm:
                code = f"{cm.group(1).upper()} {cm.group(2).upper()} {cm.group(3).capitalize()}"
                curr = cm.group(2).upper()
            else:
                # fallback: just currency search
                curr_m = _re.search(r"\b(EUR|USD|GBP|CHF|JPY)\b", context[-80:])
                curr = curr_m.group(1) if curr_m else ""
                code = ""
            if isin not in found:
                found[isin] = {"isin": isin, "code": code, "currency": curr}
    return list(found.values())


def _hdr_collect_eur_retail_and_clean(data):
    """Devuelve hasta 2 clases EUR: la retail y la limpia, con sus ISINs.
    Cascada:
    1) `_int_clases` del extractor (si tiene varias EUR con ISIN).
    2) Añade clases con ISIN extraídas de readings (FT/Morningstar).
    3) Clasifica cada una por prefijo (retail / limpia).
    4) Devuelve: {retail: {isin, code}, limpia: {isin, code}} (ambas o una sola).
    El ISIN principal siempre se incluye (por defecto como retail si no está clasificado).
    """
    isin_principal = data.get("isin", "")
    nombre_principal = data.get("nombre", "") or ""
    result = {"retail": None, "limpia": None}

    # Start with principal ISIN as default retail (si no supera clasificación).
    # Prefiere el `code` del _int_clases[0] (más limpio) al nombre completo del fondo.
    _int_clases_principal = next(
        (c for c in (data.get("_int_clases", []) or [])
         if isinstance(c, dict) and ((c.get("currency") or "").upper() == "EUR" or not c.get("currency"))),
        None,
    )
    code_principal = ""
    if _int_clases_principal:
        code_principal = _int_clases_principal.get("code") or _int_clases_principal.get("nombre") or ""

    import re as _re
    # Si el code es igual/parecido al nombre del fondo, NO es un código de clase real
    # (suele ser el caso ES con 1 sola clase). Intentar extraer clase real del nombre.
    def _is_fund_name_not_class_code(code, nombre):
        if not code or not nombre:
            return True
        # Si son iguales (ignorando case), es el nombre
        if code.strip().lower() == nombre.strip().lower():
            return True
        # Si >30 chars sin patrón de clase, es el nombre
        if len(code) > 30 and not _re.search(r"\b(EUR|USD|GBP|CHF|JPY)\s+(Acc|Inc|Accumulation|Income|Distribution|Dist)", code, _re.I):
            return True
        return False

    if _is_fund_name_not_class_code(code_principal, nombre_principal):
        # Extraer clase del nombre: "Trojan Fund (Ireland) O EUR ACC" → "O EUR ACC"
        m = _re.search(r"\b([A-Z](?:-[A-Z])?)\s+(EUR|USD|GBP|CHF|JPY)\s+(Acc|Inc|Accumulation|Income|Dist)",
                       nombre_principal, _re.I)
        code_principal = m.group(0).upper() if m else ""

    # Si seguimos sin code_principal, intentar de FT search (nombre completo del fondo)
    if not code_principal and isin_principal:
        for c in _hdr_ft_search_sibling_classes(nombre_principal, isin_principal):
            if c.get("isin") == isin_principal:
                ft_code = c.get("code") or ""
                full_name = c.get("fund_name") or ""
                if ft_code and "Class" in ft_code:
                    code_principal = ft_code
                elif full_name:
                    m = _re.search(r"\bClass\s+([A-Z][-A-Z0-9]{0,6})\b", full_name, _re.I)
                    if m:
                        code_principal = f"Class {m.group(1)}"
                    else:
                        code_principal = full_name
                break

    code_principal = _hdr_clean_class_code(code_principal)

    tipo_principal = _hdr_classify_class(code_principal or nombre_principal)
    result_key = tipo_principal if tipo_principal in ("retail", "limpia") else "retail"
    result[result_key] = {"isin": isin_principal, "code": code_principal}

    # 1) Del extractor
    for c in (data.get("_int_clases", []) or []):
        if not isinstance(c, dict):
            continue
        curr = (c.get("currency") or "").upper()
        if curr != "EUR":
            continue
        code = c.get("code") or c.get("nombre") or ""
        isin = c.get("isin") or ""
        if not isin or isin == isin_principal:
            continue
        tipo = _hdr_classify_class(code)
        slot = tipo if tipo in ("retail", "limpia") else None
        if slot and not result.get(slot):
            result[slot] = {"isin": isin, "code": code}

    # 2) Del AR (fuente más completa — todas las clases del sub-fondo)
    for c in _hdr_extract_fund_classes_from_ar(isin_principal):
        if c.get("currency", "").upper() != "EUR":
            continue
        code = c.get("code", "")
        isin = c.get("isin", "")
        if not isin or isin == isin_principal:
            continue
        tipo = _hdr_classify_class(code)
        slot = tipo if tipo in ("retail", "limpia") else None
        if slot and not result.get(slot):
            result[slot] = {"isin": isin, "code": code}

    # 3) De readings
    for c in _hdr_extract_fund_classes_from_readings(data):
        if c.get("currency", "").upper() != "EUR":
            continue
        code = c.get("code", "")
        isin = c.get("isin", "")
        if not isin or isin == isin_principal:
            continue
        tipo = _hdr_classify_class(code)
        slot = tipo if tipo in ("retail", "limpia") else None
        if slot and not result.get(slot):
            result[slot] = {"isin": isin, "code": code}

    # 4) De FT search (fuente más completa — lista TODAS las clases del fondo)
    # Prioridad:
    #   A) Mismo código país que el ISIN consultado (evita wrappers de distinta jurisdicción,
    #      p.ej. si consultamos LU... el wrapper FR "Open Access..." NO debe ganar).
    #   B) Código de clase más simple (A sobre AD/AFERG; I sobre ID/WI).
    ft_classes = _hdr_ft_search_sibling_classes(nombre_principal, isin_principal)
    eur_ft = [c for c in ft_classes if c.get("currency") == "EUR" and c.get("isin") != isin_principal]
    principal_country = isin_principal[:2].upper() if isin_principal else ""
    def _code_complexity(c):
        import re as _re
        code = c.get("code", "")
        m = _re.search(r"\bClass\s+([A-Z][-A-Z0-9]{0,6})", code)
        letter = m.group(1) if m else "ZZZ"
        # Priority 0: same country code as principal (0 = highest); distinct = 1
        same_country = 0 if c.get("isin", "")[:2].upper() == principal_country else 1
        return (same_country, len(letter), letter)
    eur_ft.sort(key=_code_complexity)
    for c in eur_ft:
        code = c.get("code", "")
        # Usar full_name si code es solo "Class X CCY" sin más detalle
        full_name = c.get("fund_name") or ""
        # Preferir el que tenga más detalle (Acc/Inc/Hedged)
        import re as _re
        if full_name and _re.search(r"(Acc|Inc|Accumulation|Income|Hedged)", full_name, _re.I) and \
           not _re.search(r"(Acc|Inc|Accumulation|Income|Hedged)", code, _re.I):
            code = full_name
        code = _hdr_clean_class_code(code)
        isin = c.get("isin", "")
        tipo = _hdr_classify_class(code)
        slot = tipo if tipo in ("retail", "limpia") else None
        if slot and not result.get(slot):
            result[slot] = {"isin": isin, "code": code}
    return result


def _hdr_split_lead_and_co(gestores_list, data):
    """Identifica lead vs co-managers.
    Heuristic:
    - manager_profile.equipo[0] o gestor con cargo 'CIO'/'Lead'/'Senior'/'Chief' → lead
    - Resto → co-managers
    Devuelve (lead_name or None, [co_names]).
    """
    if not gestores_list:
        return None, []
    # Intentar usar manager_profile que ya trae cargo
    isin = data.get("isin", "")
    mgr = _hdr_load_side(isin, "manager_profile.json")
    equipo = mgr.get("equipo", []) or []
    if equipo:
        lead = None
        cos = []
        import re as _re
        for i, g in enumerate(equipo):
            if not isinstance(g, dict):
                continue
            nombre = (g.get("nombre") or "").strip()
            cargo = (g.get("cargo") or "").lower()
            if not nombre:
                continue
            is_lead = (i == 0 or
                       bool(_re.search(r"\b(cio|chief|senior\s+(fund\s+)?manager|lead|principal|founder)\b", cargo)))
            if is_lead and lead is None:
                lead = nombre
            else:
                cos.append(nombre)
        return lead, cos
    # Fallback: primer nombre = lead
    names = [n for n in gestores_list if n]
    return (names[0] if names else None), names[1:]


_HDR_FX = {"EUR": 1.0, "GBP": 1.14, "USD": 0.92, "CHF": 1.05, "JPY": 0.006,
           "CAD": 0.68, "AUD": 0.62}


def _hdr_ft_summary_scrape(isin, currency="EUR"):
    """Scrape markets.ft.com/tearsheet/summary para el ISIN específico.
    Devuelve dict con {fund_size_meur, category_ms, ocf_pct, launch_date, price_ccy}
    o {} si falla. Cacheado en _HDR_PDF_CACHE.
    """
    import re as _re
    cache_key = f"ft_summary::{isin}:{currency}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]
    url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:{currency}"
    html = ""
    try:
        import httpx
        with httpx.Client(timeout=8, follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
            resp = c.get(url)
            if resp.status_code == 200:
                html = resp.text
    except Exception:
        pass
    if not html:
        _HDR_PDF_CACHE[cache_key] = {}
        return {}
    result = {}
    # Helper: strip HTML tags para facilitar el matching
    def _strip(s):
        return _re.sub(r"<[^>]+>", " ", s or "")

    # Fund size: estructura FT "Fund size</th><td><div>25.30bn <span...>GBP</span>"
    m = _re.search(
        r"Fund\s+size\s*</th>\s*<td[^>]*>\s*<div[^>]*>\s*([\d.,]+)\s*(bn|mn|M|B)?\s*"
        r"<span[^>]*mod-format__currency[^>]*>\s*(GBP|EUR|USD|CHF|JPY|CAD|AUD)",
        html, _re.I)
    if not m:
        # Fallback sin HTML-tags fijos
        m = _re.search(
            r"Fund\s+size[^A-Za-z]{0,80}?([\d.,]+)\s*(bn|mn|M|B)?\s*[^A-Za-z]{0,30}(GBP|EUR|USD|CHF|JPY|CAD|AUD)",
            _strip(html), _re.I)
    if m:
        try:
            num = float(m.group(1).replace(",", ""))
            unit = (m.group(2) or "mn").lower()
            curr = m.group(3).upper()
            multiplier = 1000 if unit in ("bn", "b") else 1
            num_meur = num * multiplier * _HDR_FX.get(curr, 1.0)
            result["fund_size_meur"] = round(num_meur, 1)
        except Exception:
            pass

    # Morningstar category: después de "Morningstar category" viene en <td>, puede tener HTML
    plain = _strip(html)
    mc = _re.search(
        r"Morningstar\s+category\s+([A-Z][\w\-\s&\(\)]{3,80}?)(?:\s+(?:IMA|Launch|Fund|Price|Investment|Income)\s|$)",
        plain, _re.I)
    if mc:
        result["category_ms"] = mc.group(1).strip().rstrip(" .,")

    # Ongoing charge
    oc = _re.search(r"Ongoing\s+charge\s*</th>\s*<td[^>]*>\s*([\d.,]+)\s*%", html, _re.I)
    if not oc:
        oc = _re.search(r"Ongoing\s+charge[^<]{0,80}?([\d.,]+)\s*%", plain, _re.I)
    if oc:
        try:
            result["ocf_pct"] = float(oc.group(1).replace(",", "."))
        except Exception:
            pass

    # Launch date
    ld = _re.search(r"Launch\s+date[^<]{0,80}?(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})", plain, _re.I)
    if ld:
        result["launch_date"] = ld.group(1)

    # Price currency
    pc = _re.search(r"Price\s+currency\s+([A-Z]{3})", plain, _re.I)
    if pc:
        result["price_ccy"] = pc.group(1)
    _HDR_PDF_CACHE[cache_key] = result
    return result


def _hdr_resolve_aum_meur(data):
    """AUM en M€ del FONDO ENTERO (NUNCA de una sola clase).

    Prioridad ES (Bug 1, 2026-04-27):
      1. kpis.aum_actual_meur (extractor CNMV PDF — suma de clases del semestral, fuente canónica).
      2. Último periodo válido de serie_aum.
      3. Readings de Morningstar/FT con URL del MISMO ISIN.

    Prioridad INT:
      1. kpis.aum_actual_meur (extractor INT v2 — ya filtra sub-fondo correcto tras Fase E).
      2. Último periodo válido de serie_aum.
      3. FT summary scrape SOLO si extractor vacío (riesgo: puede ser de una clase).
      4. Readings.

    NUNCA usamos FT scrape para ES porque el endpoint de FT devuelve
    el AUM de la CLASE consultada, no del fondo agregado, y eso provoca
    que el header difiera del resumen ejecutivo (issue Magallanes 2026-04-27).
    """
    import re as _re
    isin = data.get("isin", "")
    tipo = data.get("tipo", "")
    is_es = tipo == "ES" or isin.upper().startswith("ES")
    k = data.get("kpis", {})
    aum_k = k.get("aum_actual_meur")

    # 1) Extractor (canónico). Confianza: ES siempre, INT siempre tras Fase E.
    if aum_k and aum_k > 1:
        return aum_k

    # 2) Último periodo válido de serie_aum (no "None")
    serie = (data.get("cuantitativo", {}) or {}).get("serie_aum", []) or []
    valid = [s for s in serie if isinstance(s, dict)
             and str(s.get("periodo", "")) not in ("", "None", "none")
             and s.get("valor_meur")]
    if valid:
        return max(valid, key=lambda s: str(s.get("periodo", ""))).get("valor_meur")

    # 3) (solo INT, NUNCA ES) FT summary — riesgo single-class
    if not is_es:
        ft = _hdr_ft_summary_scrape(isin, "EUR")
        if ft.get("fund_size_meur"):
            return ft["fund_size_meur"]

    # 4) Readings con URL del mismo ISIN
    readings = _hdr_load_side(isin, "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    trusted_sources = ("morningstar", "financial times", "ft ")
    for r in all_r:
        url_l = (r.get("url", "") or "").lower()
        src = (r.get("source", "") or "").lower()
        if not any(t in src for t in trusted_sources):
            continue
        if isin.lower() not in url_l:
            continue
        txt = (r.get("resumen", "") or "") + " " + (r.get("titulo", "") or "")
        matches = _re.findall(
            r"([\d.,]+)\s*(mil\s+millones|billones?|billion|bn|mn|millones?|M)?\s*(?:de\s*)?(GBP|EUR|USD|CHF|JPY|£|\$|€)",
            txt, _re.I)
        for num_str, unit, curr in matches:
            try:
                num = float(num_str.replace(",", "").replace(" ", ""))
                unit_l = (unit or "").lower()
                multiplier = 1000 if unit_l in ("mil millones", "billones", "billon", "billones", "billion", "bn") else 1
                if num * multiplier < 10 or num * multiplier > 100000:
                    continue
                curr_up = curr.upper().replace("£", "GBP").replace("$", "USD").replace("€", "EUR")
                num_eur = num * multiplier * _HDR_FX.get(curr_up, 1.0)
                return round(num_eur, 1)
            except Exception:
                continue
    return aum_k  # último recurso


def _hdr_resolve_morningstar_stars(data):
    """Rating Morningstar (1-5 estrellas) del ISIN consultado.
    Orden de prioridad:
    1. `kpis.rating_morningstar` si existe.
    2. **Scrape FT /ratings?s=ISIN:EUR construyendo la URL desde el ISIN** (NO
       usar URLs de readings, que pueden apuntar a clases distintas).
    3. Count "★" en readings del mismo ISIN.
    4. Fallback: scrape URLs de readings sólo si contienen el ISIN.
    """
    import re as _re
    isin = data.get("isin", "")
    cache_key = f"ms_stars::{isin}"
    if cache_key in _HDR_PDF_CACHE:
        return _HDR_PDF_CACHE[cache_key]

    k = data.get("kpis", {})
    r = k.get("rating_morningstar")
    if r:
        try:
            iv = int(r)
            if 1 <= iv <= 5:
                _HDR_PDF_CACHE[cache_key] = iv
                return iv
        except Exception:
            pass

    # 2) Scrape FT ratings con URL construida desde ISIN (más fiable)
    if isin:
        for ccy in ("EUR", "USD", "GBP"):
            direct_url = f"https://markets.ft.com/data/funds/tearsheet/ratings?s={isin}:{ccy}"
            rating = _hdr_scrape_rating_from_url(direct_url)
            if rating:
                _HDR_PDF_CACHE[cache_key] = rating
                return rating

    # 3) readings del mismo ISIN
    readings = _hdr_load_side(isin, "readings_data.json")
    all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
    for item in all_r:
        src = (item.get("source", "") or "").lower()
        url_l = (item.get("url", "") or "").lower()
        # Filtrar: mismo ISIN
        if isin and isin.lower() not in url_l:
            continue
        txt = (item.get("resumen", "") or "") + " " + (item.get("titulo", "") or "")
        star_count = txt.count("★")
        if 1 <= star_count <= 5 and ("morningstar" in src or "morningstar" in txt.lower()):
            _HDR_PDF_CACHE[cache_key] = star_count
            return star_count
        for pat in [
            r"(?i)morningstar\s+rating\s*[:\-]?\s*(\d)\b",
            r"(?i)morningstar\s+(\d)[\s-]*star",
            r"(?i)(\d)[\s-]*star\s+rating",
            r"(?i)calificaci[oó]n\s+morningstar\s*[:\-]?\s*(\d)",
            r"(?i)(\d)\s+estrellas?\s+(?:de\s+)?morningstar",
        ]:
            m = _re.search(pat, txt)
            if m:
                iv = int(m.group(1))
                if 1 <= iv <= 5:
                    _HDR_PDF_CACHE[cache_key] = iv
                    return iv
    _HDR_PDF_CACHE[cache_key] = None
    return None


def _hdr_scrape_rating_from_url(url):
    """Scrape Morningstar rating (1-5) de página FT/Morningstar.

    FT Tearsheet structure (validado 2026-04-22):
    - URL summary `/tearsheet/summary?s=ISIN:CCY` NO contiene rating.
    - URL ratings `/tearsheet/ratings?s=ISIN:CCY` SÍ lo tiene, en:
      <div class="mod-morningstar-rating-app__stars">
        <span>1 filled</span>
        <span>2 filled</span>
        <span data-mod-stars-highlighted="true">3 filled</span>   ← EL RATING
        <span>4 filled</span>
        <span>5 filled</span>
      </div>
    - Contar las `mod-icon--star--filled` dentro del span `highlighted=true`.
    - Si la URL es /summary, se reemplaza por /ratings automáticamente.
    """
    import re as _re
    # Normalizar URL: si es summary → ratings
    url_ratings = _re.sub(r"/tearsheet/summary\?", "/tearsheet/ratings?", url)
    urls_to_try = [url_ratings] if url_ratings != url else [url]
    if url_ratings != url:
        urls_to_try.append(url)  # fallback por si /ratings falla

    html = None
    try:
        import httpx
        with httpx.Client(timeout=8, follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
            for u in urls_to_try:
                resp = c.get(u)
                if resp.status_code == 200:
                    html = resp.text
                    break
    except Exception:
        return None
    if not html:
        return None

    # Patrón FT: contar star--filled dentro del span highlighted
    highlighted = _re.search(
        r'<span[^>]*data-mod-stars-highlighted=[\'"]true[\'"][^>]*>(.*?)</span>',
        html, _re.S,
    )
    if highlighted:
        n = len(_re.findall(r"mod-icon--star--filled", highlighted.group(1)))
        if 1 <= n <= 5:
            return n

    # Patrones adicionales (otras fuentes)
    patterns = [
        r"(\d)[\s\-]?star\s+rating",
        r"aria-label=[\"'](\d)\s+out\s+of\s+5\s+stars[\"']",
        r"morningstar[^<]*rating[^<]*?(\d)\s*/\s*5",
        r"(?:data-rating|rating)=[\"'](\d)[\"']",
        r"rating[^<]{0,40}?(\d)\s+estrellas",
    ]
    for pat in patterns:
        m = _re.search(pat, html, _re.I)
        if m:
            try:
                iv = int(m.group(1))
                if 1 <= iv <= 5:
                    return iv
            except Exception:
                pass
    return None


def _build_publication_block_under_aum(data):
    """Bloque compacto que va DEBAJO del AUM en el header (Fase M_INT G, 2026-05-01).
    3 líneas alineadas a la derecha:
      ÚLT CARTA   2026-06
      ÚLT INFORME 2025-12
      PRÓX        2026-12 (Informe)

    Si no hay publication_calendar, devuelve "" (no se renderiza).
    """
    cal = data.get("publication_calendar") or {}
    if not cal:
        return ""

    from datetime import date
    today = date.today()

    def _fmt_date(iso: str) -> str:
        if not iso:
            return "—"
        try:
            d = date.fromisoformat(iso)
            return d.strftime("%Y-%m")
        except Exception:
            return iso[:7] if iso else "—"

    def _months_ago(iso: str) -> int | None:
        if not iso:
            return None
        try:
            d = date.fromisoformat(iso)
            return (today.year - d.year) * 12 + (today.month - d.month)
        except Exception:
            return None

    # Última carta
    cartas_info = cal.get("quarterly_letters") or {}
    ult_carta = _fmt_date(cartas_info.get("last_known_date", ""))
    prox_carta = _fmt_date(cartas_info.get("next_expected_date", ""))

    # Último informe (annual o semiannual)
    informe_info = (cal.get("semiannual_report") or cal.get("annual_report")
                    or cal.get("quarterly_report") or {})
    ult_informe = _fmt_date(informe_info.get("last_known_date", ""))
    prox_informe = _fmt_date(informe_info.get("next_expected_date", ""))

    # Determinar el próximo más cercano y etiquetarlo
    next_items = []
    for label, iso in [("Carta", cartas_info.get("next_expected_date", "")),
                       ("Informe", informe_info.get("next_expected_date", ""))]:
        if iso:
            try:
                d = date.fromisoformat(iso)
                next_items.append((d, label, iso))
            except Exception:
                continue
    next_items.sort()
    if next_items:
        prox_d, prox_label, prox_iso = next_items[0]
        delta_months = (prox_d.year - today.year) * 12 + (prox_d.month - today.month)
        prox_str = f"{_fmt_date(prox_iso)} ({prox_label})"
        if delta_months <= 1:
            prox_color_cls = "lh-pub-soon"
        else:
            prox_color_cls = "lh-pub-next"
    else:
        prox_str = "—"
        prox_color_cls = "lh-pub-next"

    rows = []
    if cartas_info:
        rows.append(
            f'<div class="lh-pub-row"><span class="lh-pub-lbl">Últ. carta</span>'
            f'<span class="lh-pub-val">{ult_carta}</span></div>'
        )
    if informe_info:
        rows.append(
            f'<div class="lh-pub-row"><span class="lh-pub-lbl">Últ. informe</span>'
            f'<span class="lh-pub-val">{ult_informe}</span></div>'
        )
    if next_items:
        rows.append(
            f'<div class="lh-pub-row"><span class="lh-pub-lbl">Próx.</span>'
            f'<span class="{prox_color_cls}">{prox_str}</span></div>'
        )

    if not rows:
        return ""
    return f'<div class="lh-pubs">{"".join(rows)}</div>'


def _build_recency_badge(data):
    """Bug 4 Fase G + Fix 2 Fase H (2026-04-28): badge con fechas última publicación
    + próxima esperada. Hasta 2 líneas — cartas + informe periódico — para programar
    actualizaciones por fondo.
    """
    cal = data.get("publication_calendar") or {}
    if not cal:
        return ""

    from datetime import date
    today = date.today()

    def _line_for(info: dict, label_prefix: str) -> str:
        if not info:
            return ""
        last_date = info.get("last_known_date", "")
        next_date = info.get("next_expected_date", "")
        if not last_date:
            return ""
        try:
            last = date.fromisoformat(last_date)
            meses = (today.year - last.year) * 12 + (today.month - last.month)
        except Exception:
            meses = 0
        color = "#2d6a4f" if meses <= 4 else ("#b48020" if meses <= 8 else "#8c3214")
        proxima = f" · próxima ~{next_date}" if next_date else ""
        return (
            f'<div style="font-size:10px;color:rgba(255,255,255,0.55);margin-top:2px;">'
            f'<span style="background:{color};color:#fff;padding:1px 6px;border-radius:8px;font-weight:500;">'
            f'{label_prefix}: {last_date} ({meses}m)</span>{proxima}</div>'
        )

    lines = []
    # Línea 1: cartas trimestrales (frecuencia más alta)
    cartas_info = cal.get("quarterly_letters")
    if cartas_info:
        lines.append(_line_for(cartas_info, "Última carta"))

    # Línea 2: informe periódico (annual o semiannual — el de mayor cadencia)
    informe_info = cal.get("semiannual_report") or cal.get("annual_report") or cal.get("quarterly_report")
    # Evitar duplicar si quarterly_letters ya cubre annual (no debería)
    if informe_info and informe_info is not cartas_info:
        # Etiqueta según tipo
        freq = informe_info.get("frequency", "")
        if freq == "annual":
            label = "Último informe anual"
        elif freq == "semiannual":
            label = "Último informe semestral"
        else:
            label = "Último informe"
        lines.append(_line_for(informe_info, label))

    return "".join(l for l in lines if l)


def build_header(data):
    k = data.get("kpis", {})
    srri = _hdr_resolve_srri(data)
    pips_html = ""
    if srri is not None:
        pips = "".join(f'<div class="srri-pip{" on" if i < srri else ""}"></div>' for i in range(7))
        pips_html = f"""
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px;">
        <div class="srri-pips">{pips}</div>
        <span style="font-size:10px;color:rgba(255,255,255,0.50);letter-spacing:0.5px;font-weight:500;">SRRI {srri} / 7</span>
      </div>"""

    nombre = data.get("nombre", "Fondo sin nombre")
    gestora = _hdr_resolve_gestora(data)
    depositario = _hdr_resolve_depositario(data)
    isin_principal = data.get("isin", "")
    fecha_inicio = _hdr_resolve_fecha_inicio(data)
    divisa = _hdr_resolve_divisa(data)
    clasificacion = _hdr_resolve_categoria(data)
    # Morningstar stars: cascada kpis → readings (n estrellas 1-5)
    stars_n = _hdr_resolve_morningstar_stars(data)
    stars = ("★" * stars_n + "☆" * (5 - stars_n)) if stars_n else ""
    # AUM definitivo (Morningstar/FT si trusted, si no extractor)
    aum_meur = _hdr_resolve_aum_meur(data)

    equipo = _hdr_resolve_gestores(data)
    lead, cos = _hdr_split_lead_and_co(equipo, data)
    if lead and cos:
        gestores_str = f"<strong>{lead}</strong> <span style='opacity:.7;'>(lead)</span> · " + " · ".join(
            f"{n} <span style='opacity:.6;font-size:9px;'>(co)</span>" for n in cos[:2]
        )
    elif lead:
        gestores_str = f"<strong>{lead}</strong>"
    else:
        gestores_str = " · ".join(equipo[:3]) if equipo else ""

    # ISINs en EUR: SOLO retail + limpia (máximo 2). Una clase por línea.
    eur_pair = _hdr_collect_eur_retail_and_clean(data)
    isin_rows = []
    if eur_pair.get("retail"):
        r = eur_pair["retail"]
        isin_rows.append(
            f"<div class='lh-meta-line'>ISIN retail EUR: "
            f"<strong>{r['isin']}</strong>"
            + (f" <span style='opacity:.55;font-size:10.5px;'>· {r['code']}</span>" if r.get('code') else "")
            + "</div>"
        )
    if eur_pair.get("limpia"):
        l = eur_pair["limpia"]
        isin_rows.append(
            f"<div class='lh-meta-line'>ISIN limpia EUR: "
            f"<strong>{l['isin']}</strong>"
            + (f" <span style='opacity:.55;font-size:10.5px;'>· {l['code']}</span>" if l.get('code') else "")
            + "</div>"
        )
    if isin_rows:
        isins_line_html = "".join(isin_rows)
    elif isin_principal:
        isins_line_html = f"<div class='lh-meta-line'>ISIN: <strong>{isin_principal}</strong></div>"
    else:
        isins_line_html = ""


    return f"""
<header class="lh">
  <div class="lh-top">
    <!-- ZONA 1: Nombre, gestora, depositario, ISIN(s) -->
    <div class="lh-left">
      <div class="lh-fund">{nombre} {f'<span style="color:rgba(255,255,255,0.45);font-size:14px;margin-left:4px;">{stars}</span>' if stars else ''}</div>
      {f'<div class="lh-meta-line" style="margin-top:3px;">Gestora: <strong>{gestora}</strong></div>' if gestora else ''}
      {f'<div class="lh-meta-line">Depositario: <strong>{depositario}</strong></div>' if depositario else ''}
      {isins_line_html}
    </div>

    <!-- ZONA 2: AUM + Publication calendar + Riesgo UCITS (SRRI solo si es real) -->
    <div style="display:flex;align-items:flex-start;gap:28px;padding:14px 32px;border-left:1px solid rgba(255,255,255,0.08);">
      <div class="lh-aum">
        <div class="lh-aum-v">€{f(aum_meur,0)}M</div>
        <div class="lh-aum-l">AUM</div>
        {_build_publication_block_under_aum(data)}
      </div>{pips_html}
    </div>

    <!-- ZONA 3: Inicio, Categoría, Gestor, Divisa -->
    <div style="display:flex;flex-direction:column;justify-content:center;gap:2px;padding:14px 28px;border-left:1px solid rgba(255,255,255,0.08);">
      {f'<div class="lh-cv"><span class="lh-cl">Inicio</span> <span style="color:rgba(255,255,255,0.80);">{fecha_inicio}</span></div>' if fecha_inicio else ''}
      {f'<div class="lh-cv"><span class="lh-cl">Categoría</span> <span style="color:rgba(255,255,255,0.80);">{clasificacion}</span></div>' if clasificacion else ''}
      {f'<div class="lh-cv"><span class="lh-cl">Gestores</span> <span style="color:rgba(255,255,255,0.80);">{gestores_str}</span></div>' if gestores_str else ''}
      <div class="lh-cv"><span class="lh-cl">Divisa</span> <span style="color:rgba(255,255,255,0.80);">{divisa}</span></div>
    </div>

    <!-- ZONA 4: Botón dark/light -->
    <div style="display:flex;align-items:center;padding:14px 24px;border-left:1px solid rgba(255,255,255,0.08);margin-left:auto;">
      <button class="theme-toggle" onclick="toggleTheme()"><span id="thlbl">Modo oscuro</span></button>
    </div>
  </div>
  <nav class="tabbar">
    <button class="tb on" onclick="goTab(0,this)">Resumen</button>
    <button class="tb" onclick="goTab(1,this)">Historia</button>
    <button class="tb" onclick="goTab(2,this)">Gestores</button>
    <button class="tb" onclick="goTab(3,this)">Evolución</button>
    <button class="tb" onclick="goTab(4,this)">Estrategia</button>
    <button class="tb" onclick="goTab(5,this)">Cartera</button>
    <button class="tb" onclick="goTab(6,this)">Fuentes externas</button>
    <button class="tb" onclick="goTab(7,this)">Documentos</button>
    <button class="tb" onclick="goTab(8,this)" style="margin-left:auto;border:1px solid rgba(255,255,255,0.15);border-radius:4px;">Chat</button>
  </nav>
  <div class="data-banner" style="background:var(--navy-pale);padding:6px 28px;font-size:11px;color:var(--ink-4);display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--rule-light);">
    <span>Datos actualizados a: <strong style="color:var(--ink-3);">{data.get('ultima_actualizacion','Fecha no disponible')[:10]}</strong></span>
    <button onclick="alert('Para actualizar, ejecutar:\\npython -m agents.orchestrator --isin {data.get('isin','')} --auto --force-refresh')" style="background:var(--navy);color:#fff;border:none;padding:4px 14px;font-family:'Source Sans 3';font-size:10px;cursor:pointer;border-radius:3px;letter-spacing:0.3px;">Actualizar an&aacute;lisis</button>
  </div>
</header>"""


# ═══════════════════════════════════════════════════════════════
# TAB 1: RESUMEN
# ═══════════════════════════════════════════════════════════════

def build_tab_resumen(data):
    # Lectura via accessor (Fase C)
    if _ACCESSOR_AVAILABLE:
        s = get_section_resumen(data)
        k = get_kpis(data)
        nombre = get_nombre(data)
    else:
        s = data.get("analyst_synthesis", {}).get("resumen", {})
        k = data.get("kpis", {})
        nombre = data.get("nombre", "")
    cuant = data.get("cuantitativo", {})

    # Nota: la auditoría Opus se imprime en terminal (ver generate()), NO en el dashboard.

    # ── 1. Narrativa (sin headers, max 4 párrafos fluidos) ───────────────
    texto_resumen = s.get("texto", "")
    # Strip any **headers** from the narrative — should be pure prose
    narrative_html = render_narrative_inline(texto_resumen, nombre)

    # ── 2. Filosofía + Criterios (2 columnas) ────────────────────────────
    filosofia = s.get("filosofia_inversion", "")
    criterios = s.get("criterios_inversion", [])

    filosofia_html = ""
    if filosofia:
        filo_formatted = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', filosofia)
        filosofia_html = f'<p class="pr">{filo_formatted}</p>'

    criterios_html = ""
    if criterios:
        for i, c in enumerate(criterios[:3], 1):
            titulo = c.get("titulo", "") if isinstance(c, dict) else str(c)
            desc = c.get("descripcion", "") if isinstance(c, dict) else ""
            desc_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', desc)
            criterios_html += f'''<div style="display:flex;gap:12px;padding:12px 0;border-bottom:1px solid var(--rule-light);">
              <span style="font-family:'Source Code Pro';font-size:12px;color:var(--navy);font-weight:600;min-width:24px;">0{i}</span>
              <div><strong style="color:var(--ink);font-size:13px;">{titulo}:</strong> <span class="pr" style="font-size:12.5px;">{desc_fmt}</span></div>
            </div>'''

    filo_criterios_block = ""
    if filosofia_html or criterios_html:
        filo_criterios_block = f'''<div class="col2 mb20">
    <div>
      <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Filosofía de inversión</div>
      {filosofia_html}
    </div>
    <div>
      <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Criterios de inversión</div>
      {criterios_html}
    </div>
  </div>'''

    # ── 3. Gráficos rent + vol (2 col, mismo ancho) ──────────────────────
    # (Morningstar charts rendered by JS)

    # ── 4. Fortalezas + Riesgos (2 columnas) ─────────────────────────────
    # Bug fix (2026-05-02): convertir **bold** → <strong> también en fortalezas/riesgos
    def _md_bold(t):
        return _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', t or "")
    fort = "".join(f'<div class="prin-i"><span class="prin-n">✓</span><span class="prin-b">{_md_bold(x)}</span></div>' for x in s.get("fortalezas", []))
    risk = "".join(f'<div class="prin-i"><span class="prin-n">⚠</span><span class="prin-b">{_md_bold(x)}</span></div>' for x in s.get("riesgos", []))

    fort_block = f'''<div>
      <div class="sr" style="color:var(--pos);">Fortalezas</div>
      <div class="prin">{fort}</div>
    </div>''' if fort else ""
    risk_block = f'''<div>
      <div class="sr" style="color:var(--neg);">Riesgos</div>
      <div class="prin">{risk}</div>
    </div>''' if risk else ""
    fort_risk_block = f'<div class="col2 mb20">{fort_block}{risk_block}</div>' if (fort or risk) else ""

    # ── 5. Clases disponibles ────────────────────────────────────────────

    # ── 6. Para quién + Compromiso gestor (2 col) ────────────────────────
    para_quien = s.get("para_quien_es", "")
    compromiso = s.get("compromiso_gestor", "")
    # Bug fix (2026-05-02): aplicar conversión markdown **bold** → <strong>
    # también en para_quien_es y compromiso_gestor (antes se insertaba raw).
    para_quien_html = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', para_quien) if para_quien else ""
    compromiso_html = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', compromiso) if compromiso else ""
    para_comp_block = ""
    if para_quien or compromiso:
        pq = f'<div><div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Para quién es adecuado</div><p class="pr">{para_quien_html}</p></div>' if para_quien else ""
        cg = f'<div><div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Compromiso del gestor</div><p class="pr">{compromiso_html}</p></div>' if compromiso else ""
        para_comp_block = f'<div class="col2 mb20">{pq}{cg}</div>'

    # ── 7. Evolución de comisiones ───────────────────────────────────────

    # ═══════════════════════════════════════════════════════════════════════
    # LAYOUT FIJO — orden Avantage: narrativa → filo/criterios → gráficos →
    # fortalezas/riesgos → clases → para_quien/compromiso → comisiones
    # ═══════════════════════════════════════════════════════════════════════
    return f"""
<section class="pane on" id="p0">
  <div class="pane-header"><h1 class="pane-h1">Resumen ejecutivo</h1><span class="pane-dl">Informe analítico</span></div>

  <div class="mb24">
    {narrative_html}
  </div>

  {filo_criterios_block}

  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Rentabilidad y volatilidad anual <span style="font-weight:400;font-size:8px;letter-spacing:0;">(fuente: Morningstar · datos diarios)</span></div>
  <div class="col2 mb20">
    <div class="ch-b"><div class="ch-hm"><canvas id="mst-ret"></canvas></div><div id="mst-ret-note" style="font-size:10px;color:var(--ink-4);font-style:italic;margin-top:4px;"></div></div>
    <div class="ch-b"><div class="ch-hm"><canvas id="mst-vol"></canvas></div></div>
  </div>

  {fort_risk_block}

  <div class="sr">Clases disponibles</div>
  {build_classes_table(data)}

  {para_comp_block}

  {_build_com_chart_or_placeholder(data)}
</section>"""


def _build_com_chart_or_placeholder(data):
    """Gráfico de comisiones ADAPTATIVO (Bug 3, 2026-04-27):
    - Hay datos por clase O ≥3 años de TER global → LINE CHART evolutivo.
      Default = clase con más historia (serie_comisiones_por_clase O serie_ter_por_clase).
    - Solo 1-2 años → BAR APILADO con descomposición TER.
    """
    import json as _json
    cuant = data.get("cuantitativo", {})
    com_series = cuant.get("serie_comisiones_por_clase", []) or []
    ter_clase_series = cuant.get("serie_ter_por_clase", []) or []
    ter_global = cuant.get("serie_ter", []) or []
    com_exito = data.get("comision_exito", {}) or {}
    k = data.get("kpis", {})
    isin = data.get("isin", "")

    # Caso A: line chart evolutivo si hay histórico REAL (valores no-None).
    # Bug 3 (2026-04-27): incluir serie_ter_por_clase como fuente válida
    # cuando serie_comisiones_por_clase está vacía.
    def _serie_has_real_data(series, value_key="ter_pct"):
        for s in series:
            if isinstance(s, dict):
                clases = s.get("clases") or {}
                if any(v is not None for v in clases.values()):
                    return True
                if s.get(value_key) is not None:
                    return True
        return False

    has_per_class_history = (
        _serie_has_real_data(com_series) or _serie_has_real_data(ter_clase_series)
    )
    has_global_history = (
        ter_global and len(ter_global) >= 3 and _serie_has_real_data(ter_global, "ter_pct")
    )
    if has_per_class_history or has_global_history:
        return f'''<div class="sr">Evolución de comisiones <span class="ch-sel"><label>Clase:</label><select id="com-sel" onchange="buildComChart()">{_build_class_selector(data)}</select></span></div>
  <div class="ch-b"><div class="ch-h"><canvas id="c-com"></canvas></div>
    <p style="font-size:10px;color:var(--ink-4);margin-top:6px;font-style:italic;">* Datos excluidos si hay inconsistencia entre TER y comisión de gestión.</p>
  </div>'''

    # Caso B: 1 año → bar apilado con descomposición TER
    eur_pair = _hdr_collect_eur_retail_and_clean(data)
    global_ter = k.get("ter_pct")
    exito_pct = (k.get("comision_exito_pct") or com_exito.get("pct_teorico") or 0)
    tiene_exito = com_exito.get("existe")

    bars = []
    for label, info in eur_pair.items():
        if not info:
            continue
        c_isin = info.get("isin") or ""
        code = _hdr_clean_class_code(info.get("code") or "") or label
        ft_sum = _hdr_ft_summary_scrape(c_isin, "EUR") if c_isin else {}
        ter_v = (k.get("ter_pct") if c_isin == isin else None) \
                or ft_sum.get("ocf_pct") or global_ter
        com_v = k.get("coste_gestion_pct") if c_isin == isin else None
        if ter_v is not None and com_v is not None:
            try:
                if float(ter_v) < float(com_v):
                    com_v = None
            except (ValueError, TypeError):
                pass
        if ter_v is None and com_v is None:
            continue
        ter_f = float(ter_v) if ter_v is not None else None
        com_f = float(com_v) if com_v is not None else None
        exito_f = float(exito_pct) if (tiene_exito and exito_pct) else 0.0
        otros_f = None
        if ter_f is not None and com_f is not None:
            otros_f = max(0.0, ter_f - com_f - exito_f)
        bars.append({
            "label": code, "com_gestion": com_f,
            "com_exito": exito_f if tiene_exito else None,
            "otros": otros_f, "ter_total": ter_f,
        })

    if not bars:
        return ""

    labels = _json.dumps([b["label"] for b in bars], ensure_ascii=False)
    any_breakdown = any(b["otros"] is not None for b in bars)
    if any_breakdown:
        com_g = _json.dumps([b["com_gestion"] for b in bars])
        com_e = _json.dumps([b["com_exito"] for b in bars])
        otros = _json.dumps([b["otros"] for b in bars])
        datasets_js = (
            f"[\n"
            f"      {{label:'Com. Gestión (%)', data:{com_g}, backgroundColor:'rgba(12,35,64,0.85)', stack:'s1'}},\n"
            f"      {{label:'Com. Éxito (%)',   data:{com_e}, backgroundColor:'rgba(180,30,30,0.75)',  stack:'s1'}},\n"
            f"      {{label:'Otros (Admin/Custodia)', data:{otros}, backgroundColor:'rgba(180,128,32,0.75)', stack:'s1'}}\n"
            f"    ]"
        )
        title = "Composición de costes por clase"
        note = "* Bar apilada = TER total. Descomposición: Com.Gestión + Com.Éxito + Otros (admin/custodia/auditoría). Solo 1 año disponible."
        stacked = "true"
    else:
        ter_vals = _json.dumps([b["ter_total"] for b in bars])
        datasets_js = f"[\n      {{label:'TER (%)', data:{ter_vals}, backgroundColor:'rgba(12,35,64,0.85)'}}\n    ]"
        title = "TER por clase (snapshot actual)"
        note = "* Solo TER disponible (gestora no publica descomposición)."
        stacked = "false"

    return f'''<div class="sr">{title}</div>
  <div class="ch-b"><div class="ch-h"><canvas id="c-com-snap"></canvas></div>
    <p style="font-size:10px;color:var(--ink-4);margin-top:6px;font-style:italic;">{note}</p>
  </div>
  <script>
  (function(){{
    if (typeof Chart === 'undefined') return;
    const ctx = document.getElementById('c-com-snap');
    if (!ctx) return;
    new Chart(ctx, {{
      type: 'bar',
      data: {{ labels: {labels}, datasets: {datasets_js} }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ legend: {{ position: 'bottom', labels: {{ font: {{ size: 11 }} }} }} }},
        scales: {{
          x: {{ stacked: {stacked}, grid: {{ display: false }} }},
          y: {{ stacked: {stacked}, beginAtZero: true, grid: {{ display: false }}, ticks: {{ callback: v => v+'%' }} }}
        }}
      }}
    }});
  }})();
  </script>'''


# ═══════════════════════════════════════════════════════════════
# TAB 2: HISTORIA
# ═══════════════════════════════════════════════════════════════

def build_tab_historia(data):
    import re
    # Lectura via accessor (Fase C)
    if _ACCESSOR_AVAILABLE:
        s = get_section_historia(data)
        k = get_kpis(data)
        vl = get_serie_vl_base100(data)
        serie_part = get_serie_participes(data)
    else:
        s = data.get("analyst_synthesis", {}).get("historia", {})
        k = data.get("kpis", {})
        cuant = data.get("cuantitativo", {})
        vl = cuant.get("serie_vl_base100", [])
        serie_part = cuant.get("serie_participes", []) or []
    cuant = data.get("cuantitativo", {})  # algunas líneas siguen usándolo
    hitos = s.get("hitos", [])
    texto = s.get("texto", "")

    # KPIs: años desde inicio y fecha_inicio
    years_since = ""
    if k.get("anio_creacion"):
        years_since = str(datetime.now().year - int(k["anio_creacion"]))
    # Fecha inicio: usar la misma resolución que el header (mes+año cuando se puede)
    fecha_inicio = _hdr_resolve_fecha_inicio(data) or format_date(k.get("fecha_registro", ""))

    # CAGR / peor / mejor año: placeholder "—", se rellenan desde JS con datos
    # daily de Morningstar (igual que la tab Evolución).
    # Este cálculo evita las inconsistencias de serie_vl_base100 del extractor.
    vl_corrupta = data.get("serie_vl_corrupta", False)
    cagr_str = "—"
    best_yr, best_ret, worst_yr, worst_ret = "—", 0, "—", 0

    # Timeline: AGRUPAR hitos por año (regla user feedback 2026-04-22).
    # Cada año tiene un punto grande con sub-hitos separados debajo.
    def _clasify_hito(tipo_hito, titulo_hito, evento):
        tipo_lower = (tipo_hito or "").lower()
        ev_lower = (evento + " " + titulo_hito).lower()
        if tipo_lower == "crisis" or any(w in ev_lower for w in ["crisis", "caída", "pérdida", "negativ"]):
            return "dot-crisis", "tag-crisis", "Crisis"
        if tipo_lower == "comportamiento" or any(w in ev_lower for w in ["rentabilidad", "outperform", "vs benchmark", "retorno"]):
            return "dot-strat", "tag-strat", "Comportamiento vs mercado"
        if tipo_lower == "estrategia" or any(w in ev_lower for w in ["estrateg", "cobertura", "covid", "rotación", "cambio", "decisión"]):
            return "dot-strat", "tag-strat", "Decisión estratégica"
        if tipo_lower == "regulatorio" or any(w in ev_lower for w in ["regulat", "cnmv", "folleto", "registro"]):
            return "dot-reg", "tag-reg", "Regulatorio"
        if tipo_lower == "equipo" or any(w in ev_lower for w in ["equipo", "incorpora", "sale", "cambio gestor"]):
            return "dot-hito", "tag-hito", "Equipo"
        if tipo_lower == "crecimiento" or any(w in ev_lower for w in ["crecimiento", "expansión", "duplica", "cuadruplic", "aum"]):
            return "dot-hito", "tag-hito", "Salto de escala"
        return "dot-hito", "tag-hito", (tipo_hito.capitalize() if tipo_hito else "Hito")

    def _split_title_desc(titulo_hito, evento):
        """Devuelve (title, desc) asegurando SIEMPRE que haya algo que leer.
        - Si hay titulo_hito explícito → usarlo + descripción = evento completo.
        - Si no, split del evento: primera frase como título, resto como desc.
        - Si el evento es corto (<=90 chars) → título = evento, desc = ''.
        """
        if titulo_hito:
            return titulo_hito.strip(), evento.strip()
        evt = (evento or "").strip()
        if not evt:
            return "Hito", ""
        # Si es corto, título = evento entero
        if len(evt) <= 90:
            return evt, ""
        # Split por primera frase (punto/;/— de cierre)
        import re as _re2
        m = _re2.match(r"([^.;—]{20,120}[.;—])\s*(.+)", evt, _re2.S)
        if m:
            return m.group(1).rstrip(".;—").strip(), m.group(2).strip()
        # Fallback: primeras 80 chars + resto
        return evt[:80].rstrip() + "…", evt[80:].lstrip()

    # Agrupar por año
    by_year = {}
    for h in hitos:
        anio = str(h.get("anio", "")).strip() or "s/f"
        by_year.setdefault(anio, []).append(h)

    # Bug fix (2026-05-02): convertir **bold** → <strong> también en timeline (title + desc)
    def _md_bold_tl(t):
        import re as _re_md
        return _re_md.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', t or "")
    tl = ""
    for anio in sorted(by_year.keys(), reverse=False):
        items = by_year[anio]
        if len(items) == 1:
            h = items[0]
            evento = h.get("evento", "")
            titulo_hito = h.get("titulo", "")
            dot_cls, tag_cls, tag_text = _clasify_hito(h.get("tipo", ""), titulo_hito, evento)
            title, desc = _split_title_desc(titulo_hito, evento)
            title = _md_bold_tl(title); desc = _md_bold_tl(desc)
            tl += f"""
    <div class="tl-item">
      <div class="tl-dot {dot_cls}"></div>
      <div class="tl-date">{anio}</div>
      <div class="tl-tag {tag_cls}">{tag_text}</div>
      <div class="tl-title">{title}</div>
      {f'<div class="tl-desc">{desc}</div>' if desc else ''}
    </div>"""
        else:
            sub_html = ""
            for h in items:
                evento = h.get("evento", "")
                titulo_hito = h.get("titulo", "")
                dot_cls, tag_cls, tag_text = _clasify_hito(h.get("tipo", ""), titulo_hito, evento)
                title, desc = _split_title_desc(titulo_hito, evento)
                title = _md_bold_tl(title); desc = _md_bold_tl(desc)
                sub_html += f"""
        <div style="padding:8px 0;border-bottom:1px dashed var(--rule-light);">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;">
            <span class="tl-tag {tag_cls}">{tag_text}</span>
            <strong style="font-size:12.5px;color:var(--ink);">{title}</strong>
          </div>
          {f'<div class="tl-desc" style="margin:2px 0 0 0;">{desc}</div>' if desc else ''}
        </div>"""
            tl += f"""
    <div class="tl-item" style="align-items:flex-start;">
      <div class="tl-dot dot-hito" style="width:14px;height:14px;"></div>
      <div class="tl-date" style="font-weight:600;">{anio}</div>
      <div style="grid-column:3/span 2;">
        {sub_html}
      </div>
    </div>"""

    # ── Plantilla visual fija: narrativa → KPIs calculados → gráficos → cronología
    cronologia_block = f'''
  <div class="sr">Cronología de eventos relevantes</div>
  <div class="timeline">{tl}
  </div>''' if tl else ""

    # M4 Fase M (2026-04-30): bloque "Hechos relevantes detallados" si existen
    # datos enriquecidos (que_cambio/motivo/impacto_inversor) por LLM.
    # Solo renderiza si al menos 1 hecho tiene enrichment.
    hechos = data.get("hechos_relevantes", []) or []
    hechos_block = render_hechos_relevantes(hechos) if hechos else ""

    # Gráficos condicionales: solo mostrar los que tienen datos reales.
    # Partícipes: solo si serie no vacía (el resto de fondos INT no tienen).
    has_participes = any(
        isinstance(p, dict) and p.get("valor") for p in serie_part
    )
    # F4: AUM también necesita guard — antes se renderizaba incondicionalmente
    # y dejaba un canvas vacío para fondos sin serie_aum.
    aum_header_meur = _hdr_resolve_aum_meur(data)
    has_aum = (
        bool(aum_header_meur)
        or any(isinstance(p, dict) and p.get("valor_meur") for p in serie_aum)
    )
    charts_html_parts = []
    if has_aum:
        charts_html_parts.append(
            f'<div class="ch-b"><div class="ch-l">AUM (M€)</div>'
            f'<div class="ch-h"><canvas id="c-aum"></canvas></div></div>'
        )
    if has_participes:
        charts_html_parts.append(
            f'<div class="ch-b"><div class="ch-l">Partícipes</div>'
            f'<div class="ch-h"><canvas id="c-part"></canvas></div></div>'
        )
    # NOTA: gráfico VL Base 100 eliminado de aquí (2026-04-27).
    # Para fondos multi-clase el "VL" del fondo no existe (cada clase tiene NAV
    # propio). El gráfico de valoración Base 100 vive en pestaña Evolución
    # (canvas mst-growth) que es semánticamente más correcto.
    # col2 si hay 2 charts; col1 si solo 1; bloque omitido si ninguno (charts_block = '')
    if len(charts_html_parts) >= 2:
        col_cls = "col2"
    elif len(charts_html_parts) == 1:
        col_cls = "col1"
    else:
        col_cls = None
    charts_block = f'<div class="{col_cls} mb20">{"".join(charts_html_parts)}</div>' if col_cls else ""

    # Historia con sub-pestañas (misma estructura que Estrategia): Resumen
    # general + una pestaña por subsección (idea de Rafa 2026-06-10).
    _hnombre = data.get("nombre", "")
    _h_resumen = s.get("resumen_general") or ""
    _h_tabs = []
    if _h_resumen:
        _h_tabs.append(("Resumen general", render_narrative_inline(_h_resumen, _hnombre)))
    for _h, _block in _narrative_groups(texto):
        _body = render_narrative_inline("\n\n".join(_block), _hnombre) if _block else ""
        if _h is None:
            if not _h_resumen and _body:
                _h_tabs.insert(0, ("Resumen general", _body))
            continue
        _h_tabs.append((_tab_label(_h), _body))
    historia_narr = build_subtabs(_h_tabs, "hist-tabs") if len(_h_tabs) > 1 else render_narrative_inline(texto, _hnombre)

    return f"""
<section class="pane" id="p1">
  <div class="pane-header"><h1 class="pane-h1">Historia del fondo</h1><span class="pane-dl">{fecha_inicio} — presente</span></div>

  <div class="mb24">
    {historia_narr}
  </div>

  <div class="kpi-row">
    <div class="kpi-cell"><div class="kpi-label">Años desde inicio</div><div class="kpi-value">{years_since or '—'}</div><div class="kpi-sub">{fecha_inicio} — presente</div></div>
    <div class="kpi-cell"><div class="kpi-label">CAGR desde inicio</div><div class="kpi-value pos" id="kpi-cagr">—</div><div class="kpi-sub" id="kpi-cagr-sub">Anualizado · Morningstar daily</div></div>
    <div class="kpi-cell"><div class="kpi-label">Peor año</div><div class="kpi-value neg" id="kpi-worst">—</div><div class="kpi-sub" id="kpi-worst-sub">Morningstar daily</div></div>
    <div class="kpi-cell"><div class="kpi-label">Mejor año</div><div class="kpi-value pos" id="kpi-best">—</div><div class="kpi-sub" id="kpi-best-sub">Morningstar daily</div></div>
  </div>

  {charts_block}
  <script>
  // Historia: rellenar KPIs y VL chart desde datos Morningstar daily cuando fetchMST resuelva
  window.__HIST_AUM_HEADER__ = {aum_header_meur if aum_header_meur else 'null'};
  </script>
  {cronologia_block}
  {hechos_block}
</section>"""


def render_hechos_relevantes(hechos: list) -> str:
    """M4 Fase M (2026-04-30): renderiza hechos relevantes con enrichment LLM.

    Cada hecho muestra: año, evento canónico, qué cambió EXACTAMENTE, motivo (si),
    impacto al inversor. Solo renderiza si ≥1 hecho tiene campos enriquecidos.
    """
    if not hechos:
        return ""
    # Verificar si algún hecho tiene enrichment (M4 fields)
    has_enrichment = any(
        (h.get("que_cambio") or h.get("motivo") or h.get("impacto_inversor"))
        for h in hechos if isinstance(h, dict)
    )
    if not has_enrichment:
        # Sin enrichment, no aporta vs cronología → no renderizar
        return ""

    rows = []
    for h in sorted(hechos, key=lambda x: str(x.get("anio", "")), reverse=True):
        if not isinstance(h, dict):
            continue
        anio = h.get("anio", "") or "—"
        evento = h.get("evento", "") or "Hecho relevante"
        que_cambio = h.get("que_cambio", "")
        motivo = h.get("motivo", "")
        impacto = h.get("impacto_inversor", "")

        # Si solo tiene `evento` (sin enrichment), saltar (ya está en timeline cronológico)
        if not (que_cambio or motivo or impacto):
            continue

        motivo_html = (
            f'<div class="hr-field"><span class="hr-label">Motivo:</span> {motivo}</div>'
            if motivo else ''
        )
        impacto_html = (
            f'<div class="hr-field"><span class="hr-label">Impacto:</span> {impacto}</div>'
            if impacto else ''
        )
        que_cambio_html = (
            f'<div class="hr-field"><span class="hr-label">Qué cambió:</span> {que_cambio}</div>'
            if que_cambio else ''
        )

        rows.append(f"""
    <div class="hr-card">
      <div class="hr-head"><span class="hr-year">{anio}</span><span class="hr-evento">{evento}</span></div>
      {que_cambio_html}
      {motivo_html}
      {impacto_html}
    </div>""")

    if not rows:
        return ""

    css = """
<style>
.hr-card{border-left:3px solid var(--navy);padding:12px 16px;margin:10px 0;background:var(--paper-2);}
.hr-head{display:flex;align-items:baseline;gap:14px;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid var(--rule-light);}
.hr-year{font-family:'Source Code Pro',monospace;font-size:13px;color:var(--navy);font-weight:600;min-width:48px;}
.hr-evento{font-size:14px;font-weight:600;color:var(--ink);}
.hr-field{font-size:12.5px;line-height:1.55;color:var(--ink-2);margin:5px 0;}
.hr-label{font-weight:600;color:var(--navy-mid);text-transform:uppercase;font-size:10.5px;letter-spacing:0.5px;margin-right:6px;}
</style>
"""
    return f"""
  {css}
  <div class="sr">Hechos relevantes — análisis detallado</div>
  <div class="hechos-detallados">
    {"".join(rows)}
  </div>
"""


# ═══════════════════════════════════════════════════════════════
# TAB 3: GESTORES
# ═══════════════════════════════════════════════════════════════

def build_tab_gestores(data):
    import re
    s = data.get("analyst_synthesis", {}).get("gestores", {})
    # Usar accessor canónico para perfiles (evita bug histórico de leer del top-level
    # output.gestores.perfiles que contiene info CRUDA por fuente web, no perfiles personas)
    if _ACCESSOR_AVAILABLE:
        perfiles = get_perfiles(data)
    else:
        perfiles = s.get("perfiles", [])
    texto = s.get("texto", "")

    # Uses global render_narrative_inline

    # Avatar colors cycling
    colors = ["linear-gradient(135deg,#1a3a5c,#2c4a6e)", "linear-gradient(135deg,#1e5a8a,#2d8cf0)",
              "linear-gradient(135deg,#2c6e49,#4ecf99)", "#3d5a80", "#5c5850"]

    # ── Validation badge por fuente ──────────────────────────────────────────
    # Muestra trazabilidad del origen del perfil para que el usuario sepa la
    # confianza del dato. Aplicable en todos los layouts (lead/medium/compact).
    BADGE_STYLES = {
        "manual_verificado": ("#2c6e49", "Verificado manual"),
        "manual_verificado_google_morningstar_2026-04-26": ("#2c6e49", "Verificado manual"),
        "google_snippet": ("#2d8cf0", "Snippet Google/Morningstar"),
        "google_snippets": ("#2d8cf0", "Snippet Google/Morningstar"),
        "sibling_auto": ("#7c5cff", "Copiado fondo hermano"),
        "sibling_auto_es": ("#7c5cff", "Copiado fondo hermano"),
        "manager_deep_agent": ("#1e5a8a", "ManagerDeep"),
        "manager_profiler": ("#5c5850", "ManagerProfiler"),
        "analyst_llm": ("#a64949", "Síntesis LLM (verificar)"),
    }
    def _src_badge(perfil: dict) -> str:
        src = (perfil.get("fuente") or "").strip()
        copied_from = perfil.get("copied_from")
        if not src:
            return ""
        bg, label = BADGE_STYLES.get(src, ("#5c5850", src[:30]))
        suffix = f" · {copied_from}" if copied_from else ""
        return (
            f'<span style="display:inline-block;padding:2px 8px;border-radius:8px;'
            f'background:{bg};color:#fff;font-size:10.5px;font-weight:500;'
            f'margin-left:8px;vertical-align:middle;letter-spacing:0.3px;">'
            f'{label}{suffix}</span>'
        )

    mgrs_html = ""
    for i, pr in enumerate(perfiles):
        nombre = pr.get("nombre", "")
        cargo = pr.get("cargo", "")
        initials = "".join(w[0] for w in nombre.split()[:2]) if nombre else "?"
        bg = colors[i % len(colors)]
        trayectoria = pr.get("trayectoria", "")
        filosofia = pr.get("filosofia", "")
        decisiones = pr.get("decisiones_clave", []) or []
        rasgos = pr.get("rasgos_diferenciales", "")

        is_lead = (i <= 1)  # First 2 profiles = lead managers (extensive format)
        cv_bullets = pr.get("cv_bullets", []) or []
        highlights = pr.get("highlights", []) or []

        if is_lead:
            # Lead format: avatar + CV bullets left, narrative + highlights right
            tray_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', trayectoria) if trayectoria else ''
            filo_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', filosofia) if filosofia else ''

            # CV bullets (sidebar)
            cv_html = ""
            for bullet in cv_bullets:
                cv_html += f'<li style="margin-bottom:3px;">{bullet}</li>'
            cv_block = f'<ul class="mgr-cv">{cv_html}</ul>' if cv_html else ''

            # Highlights como bullets diferenciados (label bold + texto), estilo lista limpia
            tipo_labels = {
                "historia": "Historia",
                "filosofia": "Filosofía",
                "decision": "Decisión clave",
                "estrategia": "Estrategia",
                "cita": "Cita",
            }
            hl_html = ""
            for h in highlights:
                if not isinstance(h, dict): continue
                tipo = (h.get("tipo", "") or "").lower()
                txt = h.get("texto", "") or ""
                if not txt: continue
                label = tipo_labels.get(tipo, "Highlight")
                txt_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', txt)
                # Si es cita, italic + comillas
                if tipo == "cita":
                    txt_fmt = f'<em>"{txt_fmt}"</em>'
                hl_html += (
                    f'<li style="margin-bottom:6px;padding-left:4px;">'
                    f'<strong style="color:var(--ink);font-size:12.5px;">{label}:</strong> '
                    f'<span class="pr" style="font-size:12.5px;">{txt_fmt}</span>'
                    f'</li>'
                )
            highlights_block = f'<ul style="margin-top:12px;padding-left:18px;list-style:disc;">{hl_html}</ul>' if hl_html else ''

            # Legacy decisiones_clave + rasgos como expand (compat)
            dec_html = ""
            for d in decisiones:
                d_formatted = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', d)
                dec_html += f'<p class="pr" style="font-size:12px;margin-bottom:6px;padding-left:12px;border-left:2px solid var(--navy-pale);">{d_formatted}</p>'
            extra_html = ""
            if dec_html or rasgos:
                uid = f"mgr-extra-{i}"
                extra_content = ""
                if dec_html:
                    extra_content += f'<div class="sr" style="margin-top:8px;">Decisiones clave (detalle)</div>{dec_html}'
                if rasgos:
                    extra_content += f'<p class="pr" style="font-size:12.5px;margin-top:8px;"><strong>Rasgos diferenciales:</strong> {rasgos}</p>'
                extra_html = f"""
        <button class="exp-btn" onclick="const b=document.getElementById('{uid}');const o=b.classList.toggle('open');this.textContent=(o?'▼':'▶')+' Detalle adicional';" style="margin-top:8px;">▶ Detalle adicional</button>
        <div id="{uid}" class="exp-body">{extra_content}</div>"""

            badge = _src_badge(pr)
            mgrs_html += f"""
    <div class="mgr">
      <div class="mgr-s">
        <div class="mgr-av" style="background:{bg};">{initials}</div>
        <div class="mgr-nm">{nombre}{badge}</div>
        <div class="mgr-rl">{cargo}</div>
        {cv_block}
      </div>
      <div class="mgr-b">
        {f'<p class="pr" style="font-size:13px;">{tray_fmt}</p>' if tray_fmt else ''}
        {f'<p class="pr" style="font-size:13px;font-style:italic;border-left:2px solid var(--navy-pale);padding-left:10px;">{filo_fmt}</p>' if filo_fmt else ''}
        {highlights_block}
        {extra_html}
      </div>
    </div>"""
        elif decisiones or trayectoria:
            # Medium format: has some content
            badge = _src_badge(pr)
            mgrs_html += f"""
    <div class="mgr">
      <div class="mgr-s">
        <div class="mgr-av" style="background:{bg};">{initials}</div>
        <div class="mgr-nm">{nombre}{badge}</div>
        <div class="mgr-rl">{cargo}</div>
      </div>
      <div class="mgr-b">
        <p class="pr" style="font-size:12.5px;">{trayectoria or 'Miembro del equipo.'}</p>
      </div>
    </div>"""
        else:
            # Compact format: minimal info — collect for inline display
            badge = _src_badge(pr)
            mgrs_html += f"""
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-top:1px solid var(--rule);color:var(--ink-3);font-size:12px;">
      <div style="width:32px;height:32px;border-radius:50%;background:{bg};display:flex;align-items:center;justify-content:center;font-family:'EB Garamond';font-size:13px;color:#fff;flex-shrink:0;">{initials}</div>
      <div><strong style="color:var(--ink);font-size:12.5px;">{nombre}</strong> — {cargo}{badge}</div>
    </div>"""

    if not perfiles:
        mgrs_html = '<p class="pr" style="color:var(--ink-4);font-style:italic;">Información de gestores pendiente. Ejecutar manager_deep_agent para obtener perfiles.</p>'

    # Gestores anteriores — cambios de gestor o de gestora/ownership con detalle máximo
    prev_mgrs = s.get("gestores_anteriores", []) or []
    prev_html = ""
    if prev_mgrs and any(isinstance(g, dict) and (g.get("nombre") or g.get("ownership_nuevo")) for g in prev_mgrs):
        rows_prev = ""
        for g in prev_mgrs:
            if not isinstance(g, dict): continue
            nom = g.get("nombre", "") or g.get("ownership_nuevo", "")
            if not nom: continue
            cargo_ant = g.get("cargo", "") or g.get("tipo_cambio", "")
            per = g.get("periodo_en_fondo", "") or g.get("fecha_salida", "")
            motivo = g.get("motivo_salida", "")
            sust = g.get("sustituto", "")
            solap = g.get("periodo_solapamiento", "")
            impacto = g.get("impacto_estrategia", "")
            owner = g.get("ownership_nuevo", "")
            motivo_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', motivo)
            impacto_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', impacto)
            sust_fmt = sust + (f' <span style="color:var(--ink-4);font-size:11px;">(handover {solap})</span>' if solap else '')
            owner_line = f'<div style="font-size:11px;color:var(--ink-3);margin-top:3px;"><strong>Nuevo ownership:</strong> {owner}</div>' if owner else ''
            rows_prev += f'''
      <tr>
        <td style="font-weight:600;color:var(--ink);">{nom}</td>
        <td style="color:var(--ink-3);font-size:12px;">{cargo_ant}</td>
        <td style="font-family:'Source Code Pro',monospace;font-size:11px;color:var(--ink-3);">{per}</td>
        <td style="font-size:12px;">{motivo_fmt}</td>
        <td style="font-size:12px;color:var(--ink-3);">{sust_fmt}</td>
        <td style="font-size:12px;">{impacto_fmt}{owner_line}</td>
      </tr>'''
        prev_html = f'''
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);margin-top:24px;">Equipo anterior y cambios de gestión</div>
  <p class="pr" style="font-size:12px;color:var(--ink-3);margin-bottom:10px;">Cambios recientes en el equipo o en la gestora (ownership/MBO/venta) que explican la configuración actual. La continuidad de filosofía, proceso e incentivos se evalúa en "Impacto estrategia".</p>
  <div class="pt-wrap">
    <table class="pt pt-flex">
      <colgroup>
        <col style="width:18%;">
        <col style="width:12%;">
        <col style="width:11%;">
        <col style="width:21%;">
        <col style="width:13%;">
        <col style="width:25%;">
      </colgroup>
      <thead><tr><th>Gestor / Gestora</th><th>Cargo / Tipo</th><th>Periodo</th><th>Motivo salida / cambio</th><th>Sustituto</th><th>Impacto en estrategia</th></tr></thead>
      <tbody>{rows_prev}</tbody>
    </table>
  </div>'''

    # ── Extract only the overview (before first **Name — Cargo** header)
    # The texto field concatenates overview + detailed profiles; we only want overview here
    overview_paras = []
    if texto:
        for para in texto.split("\n\n"):
            ps = para.strip()
            if not ps:
                continue
            # Stop at first bold header that looks like a name (e.g. **Carlos Santiso — Cogestor**)
            if ps.startswith("**") and ("—" in ps or "–" in ps or "gestor" in ps.lower()):
                break
            overview_paras.append(ps)
    overview_text = "\n\n".join(overview_paras[:4])  # max 4 overview paragraphs
    overview_html = render_narrative_inline(overview_text, data.get("nombre", "")) if overview_text else ""

    return f"""
<section class="pane" id="p2">
  <div class="pane-header"><h1 class="pane-h1">Equipo gestor</h1><span class="pane-dl">Composición actual</span></div>

  {f'<div class="mb24">{overview_html}</div>' if overview_html else ''}

  {mgrs_html}

  {prev_html}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 4: EVOLUCIÓN (vacía)
# ═══════════════════════════════════════════════════════════════

def build_tab_evolucion(data):
    # F4: guard a nivel de bloque. Si el fondo NO tiene NINGUNA señal cuantitativa
    # (ni rating MS, ni series local), los 3 charts MS (Rent/Vol anual, Drawdown)
    # + los rolling se omiten del HTML y se muestra un placeholder.
    cuant = (data or {}).get("cuantitativo") or {}
    kpis = (data or {}).get("kpis") or {}
    has_quant_signals = (
        bool(kpis.get("rating_morningstar"))
        or bool(cuant.get("serie_rentabilidad"))
        or bool(cuant.get("serie_vl_base100"))
        or bool(cuant.get("serie_aum"))
    )
    if not has_quant_signals:
        return """
<section class="pane" id="p3">
  <div class="pane-header"><h1 class="pane-h1">Evolución del fondo</h1><span class="pane-dl">Datos diarios · Morningstar</span></div>
  <div class="mb20"><p class="pr" style="color:var(--ink-4);font-style:italic;">Datos de evolución no disponibles para este fondo (sin series cuantitativas ni rating Morningstar).</p></div>
</section>"""
    return """
<section class="pane" id="p3">
  <div class="pane-header"><h1 class="pane-h1">Evolución del fondo</h1><span class="pane-dl">Datos diarios · Morningstar</span></div>

  <div class="mb20"><p class="pr">Análisis cuantitativo basado en <strong>datos diarios de Morningstar</strong>. Las métricas de volatilidad se calculan desde retornos mensuales (fin de mes) para alinearse con la metodología estándar de Morningstar y Finect. Los rolling son configurables por periodo.</p></div>

  <div id="mst-loading" style="text-align:center;padding:40px 0;color:var(--ink-4);font-size:13px;">Cargando datos de Morningstar...</div>

  <div id="mst-evo-content" style="display:none;">
    <!-- KPIs -->
    <div id="mst-evo-kpis" class="kpi-row mb20"></div>

    <!-- Fila 1: Rentabilidad + Volatilidad anuales -->
    <div class="col2 mb20">
      <div class="ch-b"><div class="ch-l">Rentabilidad anual</div><div class="ch-hm"><canvas id="mst-evo-ret"></canvas></div></div>
      <div class="ch-b"><div class="ch-l">Volatilidad positiva / negativa anual</div><div class="ch-hm"><canvas id="mst-evo-vol"></canvas></div></div>
    </div>

    <!-- Fila 2: Drawdown + Evolución histórica -->
    <div class="col2 mb20">
      <div class="ch-b"><div class="ch-l">Drawdown desde máximos (diario)</div><div class="ch-hm"><canvas id="mst-dd"></canvas></div></div>
      <div class="ch-b"><div class="ch-l">Evolución histórica — Base 100</div><div class="ch-hm"><canvas id="mst-growth"></canvas></div></div>
    </div>

    <!-- Fila 3: Rolling dinámicos -->
    <div class="col2 mb20">
      <div class="ch-b">
        <div class="ch-l" style="display:flex;justify-content:space-between;align-items:center;">
          <span>Rentabilidad rolling (anualizada)</span>
          <select id="mst-roll-ret-sel" onchange="updateRollingRet()" style="font-size:10px;padding:2px 6px;border:1px solid var(--rule);background:var(--paper-2);color:var(--ink);font-family:'Source Sans 3';">
            <option value="12">1 año</option>
            <option value="36" selected>3 años</option>
            <option value="60">5 años</option>
            <option value="120">10 años</option>
          </select>
        </div>
        <div class="ch-hm"><canvas id="mst-roll-ret"></canvas></div>
      </div>
      <div class="ch-b">
        <div class="ch-l" style="display:flex;justify-content:space-between;align-items:center;">
          <span>Volatilidad rolling</span>
          <select id="mst-roll-vol-sel" onchange="updateRollingVol()" style="font-size:10px;padding:2px 6px;border:1px solid var(--rule);background:var(--paper-2);color:var(--ink);font-family:'Source Sans 3';">
            <option value="12" selected>12 meses</option>
            <option value="36">3 años</option>
            <option value="60">5 años</option>
          </select>
        </div>
        <div class="ch-hm"><canvas id="mst-roll-vol"></canvas></div>
      </div>
    </div>
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 5: ESTRATEGIA
# ═══════════════════════════════════════════════════════════════

def render_narrative_accordion(text, fund_name="", open_first=1):
    """Como render_narrative_inline pero cada **Header** standalone es una
    subsección PLEGABLE (<details>): la primera abierta, el resto 'ver más'.
    Hace la sección menos pesada y escaneable."""
    import re as _re
    if not text:
        return render_narrative_inline(text, fund_name)
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]

    def _is_hdr(p):
        return p.startswith("**") and p.endswith("**") and p.count("**") == 2 and "\n" not in p

    groups, cur_h, cur = [], None, []
    for p in paras:
        if _is_hdr(p):
            if cur_h is not None or cur:
                groups.append((cur_h, cur))
            cur_h, cur = p.strip("*").strip(), []
        else:
            cur.append(p)
    if cur_h is not None or cur:
        groups.append((cur_h, cur))

    # Si no hay headers (1 solo grupo sin título), render normal.
    if len(groups) <= 1 and groups and groups[0][0] is None:
        return render_narrative_inline(text, fund_name)

    out, idx = [], 0
    for h, block in groups:
        body = render_narrative_inline("\n\n".join(block), fund_name) if block else ""
        if h is None:
            if body:
                out.append(body)
            continue
        op = " open" if idx < open_first else ""
        idx += 1
        hh = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', h)
        out.append(f'<details class="acc"{op}><summary>{hh}</summary><div class="acc-b">{body}</div></details>')
    return "".join(out)


def _narrative_groups(text):
    """Divide la narrativa en [(header|None, [parrafos])] por **Header** standalone."""
    paras = [p.strip() for p in (text or "").split("\n\n") if p.strip()]
    def _is_hdr(p):
        return p.startswith("**") and p.endswith("**") and p.count("**") == 2 and "\n" not in p
    groups, cur_h, cur = [], None, []
    for p in paras:
        if _is_hdr(p):
            if cur_h is not None or cur:
                groups.append((cur_h, cur))
            cur_h, cur = p.strip("*").strip(), []
        else:
            cur.append(p)
    if cur_h is not None or cur:
        groups.append((cur_h, cur))
    return groups


def _tab_label(h, maxlen=26):
    """Etiqueta corta para la pestaña a partir del header de subsección."""
    import re as _re
    lbl = _re.split(r'[:\-—(]', h.strip("* "))[0].strip()
    return (lbl[:maxlen].rstrip() + "…") if len(lbl) > maxlen else lbl


def build_subtabs(tabs, gid):
    """tabs = [(label, html)] → sub-pestañas dentro de un pane (la 1ª activa)."""
    tabs = [(l, h) for l, h in tabs if h and h.strip()]
    if len(tabs) <= 1:
        return tabs[0][1] if tabs else ""
    nav = "".join(
        f'<button class="subtab-btn{" active" if i==0 else ""}" onclick="subtab(this,\'{gid}\',{i})">{l}</button>'
        for i, (l, _) in enumerate(tabs))
    panels = "".join(
        f'<div class="subtab-panel{" active" if i==0 else ""}">{h}</div>'
        for i, (_, h) in enumerate(tabs))
    js = ("<script>function subtab(btn,g,i){var r=document.getElementById(g);"
          "r.querySelectorAll('.subtab-btn').forEach(function(b,j){b.classList.toggle('active',j===i);});"
          "r.querySelectorAll('.subtab-panel').forEach(function(p,j){p.classList.toggle('active',j===i);});}</script>")
    return f'<div class="subtabs" id="{gid}"><div class="subtab-nav">{nav}</div>{panels}</div>{js}'


def build_tab_estrategia(data):
    import re
    s = get_section_estrategia(data) if _ACCESSOR_AVAILABLE else data.get("analyst_synthesis", {}).get("estrategia", {})
    texto = s.get("texto", "")
    resumen = s.get("estrategia_actual_resumen", "")
    hitos = s.get("hitos_estrategia", []) or []
    quotes = s.get("quotes", [])
    rc = s.get("resumen_consistencia", {}) or {}
    pr = s.get("perfil_riesgo", {}) or {}

    # Ordenar hitos cronológicamente ASCENDENTE (inicio → actual)
    def _hito_year(h):
        if not isinstance(h, dict): return 9999
        per = str(h.get("periodo", "")).strip()
        m = re.search(r"(19|20)\d{2}", per)
        return int(m.group(0)) if m else 9999
    hitos = sorted(hitos, key=_hito_year)

    # Quotes block — pull-quote inline con italic+negrita, sin bordes ni backgrounds.
    # Atribución en la misma línea, tamaño más pequeño y gris.
    # Filtro defensivo: descartar palabras sueltas pero aceptar frases cortas
    # con contenido (≥25 chars, ≥5 palabras con significado).
    def _is_valid_quote(txt: str) -> bool:
        if not txt: return False
        t = txt.strip().strip('"').strip("'").strip('“').strip('”').strip('‘').strip('’')
        if len(t) < 25: return False
        words = [w for w in t.split() if len(w) > 1]
        if len(words) < 5: return False
        return True

    quotes_html = ""
    if quotes:
        valid_quotes = []
        for q in quotes[:6]:
            qtxt = q.get("texto", "") if isinstance(q, dict) else str(q)
            if _is_valid_quote(qtxt):
                valid_quotes.append(q)
            if len(valid_quotes) >= 3:
                break
        for q in valid_quotes:
            qtxt = q.get("texto", "") if isinstance(q, dict) else str(q)
            autor = q.get("autor", "") if isinstance(q, dict) else ""
            ctx = q.get("contexto", "") if isinstance(q, dict) else ""
            attr = f"— {autor}" if autor else ""
            if ctx:
                attr += (", " if attr else "— ") + ctx
            quotes_html += (
                f'<p class="pr" style="text-align:center;padding:10px 40px;margin:18px 0;'
                f'font-style:italic;font-weight:500;font-size:14px;line-height:1.6;color:var(--ink);">'
                f'&ldquo;{qtxt}&rdquo;'
                + (f' <span style="display:block;font-weight:400;font-style:normal;'
                   f'font-size:11px;color:var(--ink-4);margin-top:6px;">{attr}</span>' if attr else '')
                + '</p>'
            )
        # Separar las citas del resto en un bloque propio (mejora visual)
        if quotes_html:
            quotes_html = (
                '<div style="margin:30px 0 10px;padding:20px 0 8px;border-top:1px solid var(--rule);">'
                '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.4px;'
                'color:var(--ink-4);text-align:center;margin-bottom:10px;">En palabras del gestor</div>'
                + quotes_html + '</div>'
            )

    # Matriz estratégica con 4 columnas (periodo/contexto/decisiones/resultado)
    hitos_html = ""
    if hitos:
        for h in hitos:
            periodo = h.get("periodo", "")
            # Support both old format (cambio) and new format (contexto_mercado/decisiones/resultado)
            if h.get("contexto_mercado") or h.get("decisiones"):
                ctx = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', h.get("contexto_mercado") or "")
                dec = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', h.get("decisiones") or "")
                res = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', h.get("resultado") or "")
                hitos_html += f"""<div class="strat-row">
  <div class="strat-yr">{periodo}</div>
  <div class="strat-c">{ctx}</div>
  <div class="strat-c">{dec}</div>
  <div class="strat-c">{res}</div>
</div>"""
            else:
                cambio = h.get("cambio", "")
                cambio_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', cambio)
                hitos_html += f"""<div class="strat-row">
  <div class="strat-yr">{periodo}</div>
  <div class="strat-c" style="grid-column:span 3;">{cambio_fmt}</div>
</div>"""

    # Perfil de riesgo específico — subsección ANTES de la tabla de consistencia.
    # Estética alineada al resto del dashboard: headers `.sr`, sin backgrounds
    # ni borders coloridos. Cada sub-bloque es un párrafo estándar.
    perfil_html = ""
    if pr and (pr.get("tipo_activo_principal") or pr.get("riesgos_especificos")):
        tipo_ap = pr.get("tipo_activo_principal", "") or ""
        riesgos = pr.get("riesgos_especificos", []) or []
        escen = pr.get("escenarios_adversos", "") or ""
        prot = pr.get("protecciones", "") or ""
        liq = pr.get("liquidez_estructura", "") or ""
        riesgos_html = ""
        for r in riesgos:
            if not r: continue
            r_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', r)
            riesgos_html += f'<li style="margin-bottom:4px;">{r_fmt}</li>'
        escen_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', escen)
        prot_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', prot)
        liq_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', liq)

        perfil_rows = []
        if tipo_ap:
            perfil_rows.append(
                f'<p class="pr" style="font-size:12.5px;margin-bottom:8px;">'
                f'<strong>Tipo de activo principal.</strong> {tipo_ap}</p>'
            )
        if riesgos_html:
            perfil_rows.append(
                f'<p class="pr" style="font-size:12.5px;margin-bottom:4px;">'
                f'<strong>Riesgos específicos.</strong></p>'
                f'<ul class="pr" style="font-size:12.5px;margin:0 0 10px 20px;padding-left:0;list-style:disc;">{riesgos_html}</ul>'
            )
        if escen_fmt:
            perfil_rows.append(
                f'<p class="pr" style="font-size:12.5px;margin-bottom:8px;">'
                f'<strong>Escenarios adversos.</strong> {escen_fmt}</p>'
            )
        if prot_fmt:
            perfil_rows.append(
                f'<p class="pr" style="font-size:12.5px;margin-bottom:8px;">'
                f'<strong>Protecciones del gestor.</strong> {prot_fmt}</p>'
            )
        if liq_fmt:
            perfil_rows.append(
                f'<p class="pr" style="font-size:12.5px;margin-bottom:8px;">'
                f'<strong>Liquidez y estructura.</strong> {liq_fmt}</p>'
            )
        # NOTA: `desglose_exposicion` y `desglose_exposicion_resumen` NO se
        # renderizan aquí — se consumen desde la pestaña Cartera (donde el
        # detalle de exposición aporta más contexto junto a las posiciones).
        perfil_html = (
            '\n  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Perfil de riesgo de la estrategia</div>\n'
            '  <div class="mb20">' + "".join(perfil_rows) + '</div>'
        )

    # Strategy summary — header `.sr` estándar sin borde vertical ni background
    resumen_html = ""
    if resumen:
        resumen_html = f"""
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Estrategia actual</div>
  <p class="pr" style="font-size:12.5px;margin-bottom:24px;">{resumen}</p>"""

    # Header row for 4-column matrix
    matrix_header = """<div style="display:grid;grid-template-columns:100px 1fr 1fr 1fr;border-bottom:2px solid var(--ink);margin-bottom:0;">
    <div style="padding:8px 12px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--navy);background:var(--navy-pale);">Periodo</div>
    <div style="padding:8px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-2);">Contexto mercado</div>
    <div style="padding:8px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-2);">Decisiones</div>
    <div style="padding:8px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-2);">Resultado</div>
  </div>""" if any(h.get("contexto_mercado") or h.get("decisiones") for h in hitos) else """<div style="display:grid;grid-template-columns:100px 1fr;border-bottom:2px solid var(--ink);margin-bottom:0;">
    <div style="padding:8px 12px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--navy);background:var(--navy-pale);">Periodo</div>
    <div style="padding:8px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-2);">Descripción</div>
  </div>"""

    # Bloque Resumen consistencia (debajo de la tabla)
    consistencia_html = ""
    if rc and (rc.get("decisiones_vs_estrategia") or rc.get("resultados_vs_objetivo")):
        score = rc.get("score", "")
        dec_est = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', rc.get("decisiones_vs_estrategia", "") or "")
        res_obj = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', rc.get("resultados_vs_objetivo", "") or "")
        just = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', rc.get("justificacion", "") or "")
        # Normalizar score (puede venir "8", "8/10", "8 / 10")
        score_display = ""
        if score:
            m = _re.search(r"(\d{1,2})", str(score))
            if m:
                score_display = f"{m.group(1)}/10"
        # Bloque score: extraído como variable para evitar f-string con backslash escape
        # (PEP 701, requeriría Python >=3.12). Refactor compatible con Python 3.10+.
        score_block = ""
        if score_display:
            _score_span_style = "font-family:'Source Code Pro',monospace;font-size:18px;font-weight:600;color:var(--navy);"
            score_block = (
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">'
                '<span style="font-size:11px;font-weight:600;color:var(--ink-3);text-transform:uppercase;letter-spacing:1px;">Score consistencia</span>'
                f'<span style="{_score_span_style}">{score_display}</span>'
                '</div>'
            )
        consistencia_html = f'''
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);margin-top:24px;">Resumen de consistencia</div>
  <div style="background:var(--paper-2);border-left:3px solid var(--navy);padding:14px 18px;margin-bottom:16px;">
    {score_block}
    {f'<div style="margin-bottom:8px;"><strong style="color:var(--ink);font-size:12px;">Decisiones vs estrategia:</strong> <span class="pr" style="font-size:12.5px;">{dec_est}</span></div>' if dec_est else ''}
    {f'<div style="margin-bottom:8px;"><strong style="color:var(--ink);font-size:12px;">Resultados vs objetivo:</strong> <span class="pr" style="font-size:12.5px;">{res_obj}</span></div>' if res_obj else ''}
    {f'<div><strong style="color:var(--ink);font-size:12px;">Justificación:</strong> <span class="pr" style="font-size:12.5px;font-style:italic;">{just}</span></div>' if just else ''}
  </div>'''

    # Sub-pestañas dentro de Estrategia: "Resumen general" (prosa fluida de todo)
    # + una pestaña por subsección detallada (idea de Rafa 2026-06-10).
    _nombre = data.get("nombre", "")
    _resumen_gral = s.get("resumen_general") or s.get("estrategia_actual_resumen") or ""
    _est_tabs = []
    if _resumen_gral:
        _est_tabs.append(("Resumen general", render_narrative_inline(_resumen_gral, _nombre)))
    for _h, _block in _narrative_groups(texto):
        _body = render_narrative_inline("\n\n".join(_block), _nombre) if _block else ""
        if _h is None:
            if not _resumen_gral and _body:
                _est_tabs.insert(0, ("Resumen general", _body))
            continue
        _est_tabs.append((_tab_label(_h), _body))
    estrategia_narr = build_subtabs(_est_tabs, "est-tabs") if _est_tabs else render_narrative_inline(texto, _nombre)

    # ── Layout fijo: narrativa → quotes → resumen actual → matriz hitos → resumen consistencia
    return f"""
<section class="pane" id="p4">
  <div class="pane-header"><h1 class="pane-h1">Estrategia y coherencia</h1><span class="pane-dl">Evaluación estratégica</span></div>

  <div class="mb24">
    {estrategia_narr}
  </div>

  {quotes_html}

  {resumen_html}

  {perfil_html}

  {f'''<div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Consistencia estratégica (año a año)</div>
  {matrix_header}
  {hitos_html}''' if hitos_html else ''}

  {consistencia_html}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 6: CARTERA
# ═══════════════════════════════════════════════════════════════

def _clean_position_name(name):
    """Normaliza nombres técnicos de posiciones a algo legible a simple vista.
    Ejemplos:
      'US TSY INFL IX N/B 1.75% 24-15/01/2034'  → 'US Treasury TIPS 1.75% 2034'
      'ITALY BTPS 1.5% 23-15/05/2029'           → 'Italy Gov Bond 1.5% 2029'
      'SPAIN I/L BOND 0.7% 18-30/11/2033'       → 'Spain Inflation-Linked 0.7% 2033'
      'JAPAN GOVT 20-YR 1.6% 24-20/03/2044'     → 'Japan Gov 20Y 1.6% 2044'
      'EUROPEAN UNION 3% 24-04/12/2034'         → 'EU Debt 3% 2034'
      'OSTRUM CASH EURIBR-I C EUR'              → 'Ostrum Cash Euribor (fund)'
    """
    import re as _re
    if not name:
        return name
    n = name.strip()
    # Emisor + tipo de bono
    subs = [
        (r'\bUS\s+TSY\s+INFL\s+IX\s+N/?B\b', 'US Treasury TIPS'),
        (r'\bUS\s+TSY\s+NOTE\s+N/?B\b', 'US Treasury Note'),
        (r'\bUS\s+TSY\s+BILL\b', 'US Treasury Bill'),
        (r'\bUS\s+TSY\s+N/?B\b', 'US Treasury'),
        (r'\bUS\s+TREASURY\s+INFLATION[\s\-]?INDEXED\b', 'US Treasury TIPS'),
        (r'\bUNITED\s+STATES\s+TREASURY\b', 'US Treasury'),
        (r'\bITALY\s+BTPS?\b', 'Italy Gov Bond'),
        (r'\bITALY\s+I/L\b', 'Italy Inflation-Linked'),
        (r'\bSPAIN\s+I/L\s+BOND\b', 'Spain Inflation-Linked'),
        (r'\bSPAIN\s+GOVT\b', 'Spain Gov Bond'),
        (r'\bSPAIN\s+BONOS\b', 'Spain Bonos'),
        (r'\bJAPAN\s+GOVT\b', 'Japan Gov'),
        (r'\bAUSTRALI(?:A|AN)\s+GOVT\b', 'Australia Gov'),
        (r'\bEUROPEAN\s+UNION\b', 'EU Debt'),
        (r'\bUK\s+GILT\b', 'UK Gilt'),
        (r'\bUNITED\s+KINGDOM\s+GILT\b', 'UK Gilt'),
        (r'\bFRANCE\s+OAT(?:EI)?\b', 'France OAT'),
        (r'\bGERMAN(?:Y)?\s+BUND\b', 'German Bund'),
        (r'\bGERMAN(?:Y)?\s+SCHATZ\b', 'German Schatz'),
        (r'\bGERMAN(?:Y)?\s+BOBL\b', 'German Bobl'),
        (r'\bCANADIAN?\s+GOVT\b', 'Canada Gov'),
        (r'\bNETHERLANDS\s+GOVT\b', 'Netherlands Gov'),
        (r'\bBELGIUM\s+GOVT\b', 'Belgium Gov'),
    ]
    for pat, rep in subs:
        n = _re.sub(pat, rep, n, flags=_re.I)
    # Convertir "20-YR" → "20Y"
    n = _re.sub(r'\b(\d{1,2})-YR\b', r'\1Y', n, flags=_re.I)
    # Fecha larga "24-15/01/2034" o "15/01/2034" → año 2034
    n = _re.sub(r'\s+\d{1,2}-\d{1,2}/\d{1,2}/(\d{4})\b', r' \1', n)
    n = _re.sub(r'\s+\d{1,2}/\d{1,2}/(\d{4})\b', r' \1', n)
    # Sufijos técnicos
    n = _re.sub(r'\s+(N/?B|IX|C\s+EUR|C\s+USD|ETC|ETF|UCITS)\s*$', '', n, flags=_re.I)
    # ETF/fondos comunes
    n = _re.sub(r'\bISHARES\s+PHYSICAL\s+GOLD\s+ETC\b', 'iShares Physical Gold', n, flags=_re.I)
    n = _re.sub(r'\bINVESCO\s+PHYSICAL\s+GOLD\s+ETC\b', 'Invesco Physical Gold', n, flags=_re.I)
    n = _re.sub(r'\bSPDR\s+GOLD\b', 'SPDR Gold', n, flags=_re.I)
    # Fondos cash
    n = _re.sub(r'\bCASH\s+EURIBR?\w*\b', 'Cash Euribor', n, flags=_re.I)
    # Colapsar espacios
    n = _re.sub(r'\s+', ' ', n).strip()
    # Capitalizar si quedó todo en mayúsculas (>20 chars sin minúsculas)
    if len(n) > 15 and n.isupper():
        # Title case pero conservando acrónimos comunes (US, EU, UK, TIPS, OAT)
        protected = {"US", "EU", "UK", "TIPS", "OAT", "BUND", "BTP", "BTPS", "JGB"}
        parts = n.split(" ")
        n = " ".join(p if p in protected else p.capitalize() for p in parts)
    return n


def _infer_asset_type(pos):
    """Infiere el tipo de activo desde el nombre/sector si el campo 'tipo' está vacío.
    Devuelve tupla (tipo_code, tipo_label_short, tipo_css_class).
    Categorías: RF (bonos), RV (acciones), GOLD (oro/ETC), COMM (commodities),
    CASH (liquidez), ETF (fondos/ETFs), DER (derivados), OTHER.
    """
    import re as _re
    name = (pos.get("nombre") or "").lower()
    tipo = (pos.get("tipo") or pos.get("asset_type") or "").upper()
    sector = (pos.get("sector") or "").lower()

    # Tipo explícito normalizado primero
    if tipo in ("BONO", "BOND", "RF", "FIXED_INCOME", "GOVERNMENT_BOND", "CORPORATE_BOND"):
        return "RF", "Renta fija", "tp-rf"
    if tipo in ("ACCIONES", "EQUITY", "RV", "STOCK"):
        return "RV", "Renta variable", "tp-rv"
    if tipo in ("GOLD",):
        return "GOLD", "Oro", "tp-gold"
    if tipo in ("CASH", "LIQUIDEZ", "MONEY_MARKET"):
        return "CASH", "Liquidez", "tp-cash"
    if tipo in ("ETF", "FUND", "FONDO"):
        return "ETF", "ETF / Fondo", "tp-etf"
    if tipo in ("DERIVATIVE", "DERIVADO"):
        return "DER", "Derivado", "tp-der"

    # Inferencia por nombre
    if _re.search(r"\b(treasury|gilt|bund|btp|oat|jgb|linker|tips|bono|bond|note|debt)\b", name):
        return "RF", "Renta fija", "tp-rf"
    if _re.search(r"\b(gold|oro|xau)\b", name) and _re.search(r"\b(etc|physical|bullion|trust|etf|spdr|ishares|invesco|wisdomtree)\b", name):
        return "GOLD", "Oro", "tp-gold"
    if _re.search(r"\bgold\b", name):
        return "GOLD", "Oro", "tp-gold"
    if _re.search(r"\b(etf|etc|ucits|ishares|spdr|vanguard|invesco|wisdomtree|fund)\b", name):
        return "ETF", "ETF / Fondo", "tp-etf"
    if _re.search(r"\b(commodity|commodities|oil|silver|copper|brent|wti)\b", name):
        return "COMM", "Commodity", "tp-comm"
    if _re.search(r"\b(cash|deposit|money market|fondo monetario|mmf|overnight)\b", name):
        return "CASH", "Liquidez", "tp-cash"
    # Derivados — especificar el TIPO (no solo "Derivado")
    if _re.search(r"\b(future|futuro|fut)\b", name):
        return "DER", "Futuro", "tp-der"
    if _re.search(r"\b(call|put|option|opci[óo]n|warrant|wts|rights?|derechos?)\b", name):
        return "DER", ("Warrant" if _re.search(r"warrant|wts|rights?|derechos?", name) else "Opción"), "tp-der"
    if _re.search(r"\b(swap|cds|irs|trs)\b", name):
        return "DER", "Swap", "tp-der"
    if _re.search(r"\b(forward|fwd)\b", name):
        return "DER", "Forward", "tp-der"
    if _re.search(r"\bcfd\b", name):
        return "DER", "CFD", "tp-der"
    if _re.search(r"\b(hedge|derivative|derivado)\b", name):
        return "DER", "Derivado", "tp-der"
    # Equity por sufijo societario (incl. más formas internacionales)
    if _re.search(r"\b(inc|plc|ag|s\.a\.|sa|nv|spa|oyj|asa|ab|ltd|limited|corp|corporation|co|holdings?|group|se|kgaa|bhd|tbk|pjsc)\b", name):
        return "RV", "Renta variable", "tp-rv"
    # Sector como pista secundaria
    if sector and any(w in sector for w in ("bond", "government", "corporate", "fixed income")):
        return "RF", "Renta fija", "tp-rf"
    if sector and any(w in sector for w in ("equity", "consumer", "technology", "financial", "healthcare", "industrial")):
        return "RV", "Renta variable", "tp-rv"
    # Holding con PAÍS y sin señal de RF/derivado/cash/fondo → es una acción
    # (en un fondo de RV, una posición nominada con país es equity, no "otros").
    if pos.get("pais"):
        return "RV", "Renta variable", "tp-rv"
    return "OTHER", (tipo.capitalize() if tipo else "Otros"), "tp-otro"


def _structure_cartera_narrative(texto, tipos_dominantes):
    """Parsea la narrativa de cartera del analyst y la divide en 3 cards:
    (1) Exposición por tipo activo + racional, (2) Decisiones/cambios recientes,
    (3) Concentración (condicional — solo si el fondo es RV/RF puro).
    Detecta secciones por títulos **MAYÚSCULA** o headers markdown al inicio
    de párrafo. Si no encuentra estructura clara, devuelve el texto tal cual.
    """
    import re as _re
    if not texto:
        return None
    # Patrones típicos (insensible a mayúsculas)
    section_patterns = {
        "exposicion": [
            r"distribuci[óo]n\s+(?:y\s+)?concentraci[óo]n",
            r"distribuci[óo]n\s+(?:y\s+)?exposici[óo]n",
            r"distribuci[óo]n\s+por\s+tipo",
            r"asignaci[óo]n\s+de\s+activos",
            r"exposici[óo]n\s+por",
            r"exposici[óo]n\s+actual\s+y\s+racional",
            r"^exposici[óo]n\s+actual",
            r"^exposici[óo]n\b",
        ],
        "posiciones": [r"posiciones\s+principales", r"principales\s+posiciones", r"top\s+(?:posiciones|holdings)"],
        "sesgos": [r"sesgos?\s+de\s+cartera", r"sesgos?\s+(?:sectorial|geogr[áa]fic)"],
        "cambios": [
            r"cambios?\s+recientes?",
            r"decisiones?\s+recientes?",
            r"decisiones\s*(?:/|y)\s*cambios",
            r"movimientos\s+recientes",
            r"rotaciones?",
        ],
        "calidad": [r"calidad\s+de\s+la\s+cartera", r"perfil\s+de\s+calidad", r"quality\s+bias"],
        "concentracion": [r"^concentraci[óo]n\b", r"\bconcentraci[óo]n\s*$"],
    }
    # Split por párrafos y buscar header en cada uno
    paragraphs = [p.strip() for p in texto.split("\n\n") if p.strip()]
    sections = {"exposicion": [], "posiciones": [], "sesgos": [], "cambios": [], "calidad": [], "concentracion": [], "otros": []}
    current_key = "otros"
    for para in paragraphs:
        # Primera línea = posible header
        first_line = para.split("\n")[0][:120].strip()
        # Strip ** y : de header
        header_txt = _re.sub(r"[*:·—-]", "", first_line).strip().lower()
        # ¿Matchea algún patrón?
        matched = None
        for key, pats in section_patterns.items():
            if any(_re.search(p, header_txt, _re.I) for p in pats):
                matched = key
                break
        # Bug Fase H (2026-04-28): patrones como `principales\s+posiciones`
        # NO están anclados a `^`, así que matchean en MEDIO del párrafo.
        # Solo recortar si first_line es REALMENTE un header (corto, sin punto
        # final, en mayúsculas o con markdown **). Si no, asignar a la sección
        # pero append párrafo COMPLETO.
        is_real_header = (
            len(first_line) < 60
            and not first_line.rstrip().endswith(".")
            and (first_line.isupper() or first_line.startswith("**"))
        )
        if matched:
            current_key = matched
            if is_real_header:
                rest = para[len(first_line):].strip()
                if rest:
                    sections[current_key].append(rest)
                # Si no hay rest, el párrafo era solo header → no append
            else:
                # Match por keyword interna → asignar sin recortar
                sections[current_key].append(para)
        else:
            sections[current_key].append(para)
    return sections


def _build_desglose_exposicion_html(data):
    """Renderiza el desglose granular de exposición (peril / país / sector /
    rating según el tipo de fondo). El dato se genera por el analyst dentro
    de `estrategia.perfil_riesgo.desglose_exposicion` pero se muestra en la
    pestaña de Cartera, que es donde aporta más contexto junto a las
    posiciones. Si no hay datos, devuelve cadena vacía."""
    import re as _re
    estrat = data.get("analyst_synthesis", {}).get("estrategia", {}) or {}
    pr = estrat.get("perfil_riesgo", {}) or {}
    desglose = pr.get("desglose_exposicion", []) or []
    resumen = pr.get("desglose_exposicion_resumen", "") or ""
    if not desglose and not resumen:
        return ""

    rows_de = ""
    for d in desglose:
        if not isinstance(d, dict): continue
        dim = d.get("dimension", "") or ""
        det = d.get("detalle", "") or ""
        peso = d.get("peso_aprox_pct", None)
        com = d.get("comentario", "") or ""
        if not det: continue
        peso_display = (f"{peso:.1f}%" if isinstance(peso, (int, float)) else "—")
        com_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', com)
        rows_de += (
            f'<tr>'
            f'<td>{dim}</td>'
            f'<td style="font-size:12.5px;font-weight:500;color:var(--ink);">{det}</td>'
            f'<td class="num">{peso_display}</td>'
            f'<td style="font-size:12px;color:var(--ink-3);">{com_fmt}</td>'
            f'</tr>'
        )

    resumen_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', resumen)
    header_intro = (
        '<p class="pr" style="font-size:12.5px;margin-bottom:10px;">'
        'Este desglose detalla <strong>a qué riesgos concretos</strong> está '
        'expuesta la cartera más allá de la lista de posiciones. Las dimensiones '
        '(peril, región, sector, rating, país…) se adaptan al tipo de fondo y '
        'permiten entender qué evento adverso específico impactaría el valor '
        'del fondo.</p>'
    )
    resumen_html = (
        f'<p class="pr" style="font-size:12.5px;margin-bottom:14px;">{resumen_fmt}</p>'
        if resumen_fmt else ''
    )
    tabla_html = (
        '<div>'
        '<table class="pt pt-flex">'
        '<colgroup>'
        '<col style="width:18%;">'
        '<col style="width:26%;">'
        '<col style="width:9%;">'
        '<col style="width:47%;">'
        '</colgroup>'
        '<thead><tr>'
        '<th>Dimensión</th>'
        '<th>Detalle</th>'
        '<th style="text-align:right;">Peso</th>'
        '<th>Comentario · evento adverso relevante</th>'
        '</tr></thead>'
        f'<tbody>{rows_de}</tbody>'
        '</table></div>'
    ) if rows_de else ''

    return (
        '\n  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Desglose de exposición al riesgo</div>\n'
        '  <div class="mb24">'
        + header_intro
        + resumen_html
        + tabla_html
        + '</div>'
    )


_SECTOR_COLORS = {
    "Tecnología": "#e7f0fb", "Servicios financieros": "#e9f3ec", "Salud": "#fdeef0",
    "Consumo cíclico": "#fdf3e6", "Consumo defensivo": "#f0ecf8", "Industria": "#eaf0f4",
    "Energía": "#fbeede", "Materiales": "#edf6f8", "Servicios públicos": "#fbe9f1",
    "Inmobiliario": "#f1f6e6", "Comunicación": "#f7f0e6", "Otros": "#f2f1ef",
}


def _sector_cell(sector):
    """Celda de sector con tinte suave por sector (diferenciar a simple vista)."""
    if not sector or sector == "—":
        return '<td style="font-family:\'Source Sans 3\';font-size:11px;color:var(--ink-4);">—</td>'
    bg = _SECTOR_COLORS.get(sector, "#f2f1ef")
    return (f'<td style="font-family:\'Source Sans 3\';font-size:11px;">'
            f'<span style="background:{bg};padding:1px 7px;border-radius:3px;white-space:nowrap;">{sector}</span></td>')


def _canon_pais(pais):
    """Normaliza el país de una posición (USA→Estados Unidos) para la tabla."""
    if not pais:
        return "—"
    try:
        from tools.region_normalizer import canonical_country
        return canonical_country(pais) or "—"
    except Exception:
        return pais or "—"


def build_allocation_evolution_chart(history, subkey, titulo, cid, top_n=5):
    """Gráfico Chart.js de ÁREA APILADA de evolución de pesos por año (geo/sector).
    Muestra las top_n categorías principales + 'Otros'. history = [{periodo,
    <subkey>: {categoria: peso_pct}}]. Devuelve '' si <2 años."""
    import json as _json
    hist = [h for h in (history or []) if h.get("periodo") and isinstance(h.get(subkey), dict)]
    hist.sort(key=lambda h: str(h.get("periodo")))
    if len(hist) < 2:
        return ""
    years = [str(h["periodo"]) for h in hist]
    agg = {}
    for h in hist:
        for k, v in h[subkey].items():
            if v is not None:
                agg[k] = agg.get(k, 0) + (v or 0)
    # 'Otros'/'Liquidez' nunca van al top: son siempre el bucket de resto
    _resto = {"Otros", "Liquidez", "Cash"}
    top = [k for k, _ in sorted(agg.items(), key=lambda x: -x[1]) if k not in _resto][:top_n]
    # paleta con alpha para área
    base = ["#0c2340", "#b48020", "#1b8a3d", "#6b3fa0", "#3d5a80", "#0891b2"]
    def _rgba(hexc, a):
        hexc = hexc.lstrip("#")
        return f"rgba({int(hexc[0:2],16)},{int(hexc[2:4],16)},{int(hexc[4:6],16)},{a})"
    datasets = []
    for i, cat in enumerate(top):
        col = base[i % len(base)]
        datasets.append({"label": cat,
                         "data": [round((h[subkey].get(cat) or 0), 2) for h in hist],
                         "borderColor": col, "backgroundColor": _rgba(col, 0.55),
                         "fill": True, "tension": 0.25, "pointRadius": 2, "borderWidth": 1.5})
    otros = [round(sum((v or 0) for k, v in h[subkey].items() if k not in top), 2) for h in hist]
    if any(o > 0.05 for o in otros):
        datasets.append({"label": "Otros", "data": otros, "borderColor": "#94a3b8",
                         "backgroundColor": _rgba("#94a3b8", 0.45), "fill": True,
                         "tension": 0.25, "pointRadius": 2, "borderWidth": 1.5})
    return f'''<div class="ch-b"><div class="ch-l">{titulo}</div><div class="ch-h"><canvas id="{cid}"></canvas></div>
  <script>(function(){{ if(typeof Chart==='undefined')return; const ctx=document.getElementById('{cid}'); if(!ctx)return;
  new Chart(ctx,{{type:'line',data:{{labels:{_json.dumps(years)},datasets:{_json.dumps(datasets)}}},
  options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
  plugins:{{legend:{{position:'bottom',labels:{{font:{{size:10}},boxWidth:10}}}},tooltip:{{callbacks:{{label:c=>c.dataset.label+': '+c.parsed.y+'%'}}}}}},
  scales:{{x:{{grid:{{display:false}}}},y:{{stacked:true,beginAtZero:true,grid:{{display:false}},ticks:{{callback:v=>v+'%'}}}}}}}}}});}})();</script></div>'''


def build_tab_cartera(data):
    if _ACCESSOR_AVAILABLE:
        s = get_section_cartera(data)
        pos_actual = get_posiciones_actuales(data)
        pos_hist = get_posiciones_historicas(data)
    else:
        s = data.get("analyst_synthesis", {}).get("cartera", {})
        pos_actual = data.get("posiciones", {}).get("actuales", [])
        pos_hist = data.get("posiciones", {}).get("historicas", [])
    # Sectores: rellenar desde el caché global (Opción B) si falta
    try:
        from tools.sector_classifier import apply_sectors
        apply_sectors(pos_actual)
    except Exception:
        pass
    sorted_pos = sorted(pos_actual, key=lambda x: x.get("peso_pct",0) or 0, reverse=True)

    # Inferir tipos de activo que faltan y calcular tipos dominantes
    tipos_weights = {}
    for pos in sorted_pos:
        code, _, _ = _infer_asset_type(pos)
        w = pos.get("peso_pct", 0) or 0
        tipos_weights[code] = tipos_weights.get(code, 0) + w
    tipo_dominante = max(tipos_weights.items(), key=lambda x: x[1])[0] if tipos_weights else "OTHER"
    # ¿Fondo concentra en ETF/fondos/commodities/oro? → concentración menos relevante
    es_fondo_de_instrumentos = tipos_weights.get("ETF", 0) + tipos_weights.get("GOLD", 0) + tipos_weights.get("COMM", 0) > 40

    # Compute historical stats for charts (JS data)
    hist_years = []
    hist_npos = []
    hist_top5 = []
    hist_top10 = []
    hist_top15 = []
    for h in sorted(pos_hist, key=lambda x: x.get("periodo","")):
        todas = h.get("todas", [])
        if not todas:
            continue
        yr = h.get("periodo", "")
        weights = sorted([x.get("peso_pct",0) or 0 for x in todas], reverse=True)
        hist_years.append(yr[-2:] if len(yr) >= 4 else yr)
        hist_npos.append(len(todas))
        hist_top5.append(round(sum(weights[:5]),1))
        hist_top10.append(round(sum(weights[:10]),1))
        hist_top15.append(round(sum(weights[:15]),1))

    # Compute variations vs previous period — ONLY if hay datos previos reales
    # (si no hay histórico de posiciones, NO marcamos nada: ni "NUEVO" ni "—",
    # simplemente se deja el campo en blanco porque no hay dato comparable).
    prev_positions = {}
    has_prev_snapshot = False
    if len(pos_hist) >= 2:
        sorted_hist = sorted(pos_hist, key=lambda x: x.get("periodo",""))
        prev_todas = sorted_hist[-2].get("todas", []) if len(sorted_hist) >= 2 else []
        if prev_todas:
            has_prev_snapshot = True
            for pp in prev_todas:
                name = pp.get("nombre", "")
                if name:
                    prev_positions[name] = pp.get("peso_pct", 0) or 0

    # Historical averages
    avg_npos = round(sum(hist_npos)/len(hist_npos),0) if hist_npos else 0
    avg_top10 = round(sum(hist_top10)/len(hist_top10),1) if hist_top10 else 0

    # Current stats
    cur_weights = sorted([x.get("peso_pct",0) or 0 for x in sorted_pos], reverse=True)
    cur_top10 = round(sum(cur_weights[:10]),1)
    # Liquidez from mix_activos
    mix = data.get("cuantitativo",{}).get("mix_activos_historico",[])
    liq = mix[0].get("liquidez_pct",0) if mix else 0
    # RV exposure
    rv = mix[0].get("rv_pct",0) if mix else 0

    # Table rows
    rows = ""
    cum = 0
    for i, pos in enumerate(sorted_pos):
        w = pos.get("peso_pct",0) or 0
        cum += w
        # Inferir tipo de activo (rellena cuando el extractor no lo pone)
        _, tipo_lbl, tipo_cls = _infer_asset_type(pos)

        # Variation
        name = pos.get("nombre","")
        prev_w = prev_positions.get(name, None)
        if not has_prev_snapshot:
            # No hay datos del año anterior → no podemos saber si es nueva
            delta_html = '<span style="color:var(--ink-5);">—</span>'
        elif prev_w is None:
            delta_html = '<span class="delta-new">NUEVO</span>'
        else:
            delta = w - prev_w
            if abs(delta) < 0.05:
                delta_html = '<span style="color:var(--ink-5);">—</span>'
            else:
                sign = "+" if delta > 0 else ""
                # Color intensity
                intensity = min(abs(delta) / 3, 1)  # normalize to 0-1
                if delta > 0:
                    bg = f"rgba(26,77,46,{0.08 + intensity*0.15})"
                    color = "var(--pos)"
                else:
                    bg = f"rgba(107,26,26,{0.08 + intensity*0.15})"
                    color = "var(--neg)"
                delta_html = f'<span style="background:{bg};color:{color};padding:1px 5px;border-radius:2px;font-size:10px;font-weight:500;">{sign}{f(delta,1)}%</span>'

        bar_w = max(2, int(w * 8))
        cum_bar_w = max(2, min(40, int(cum * 0.4)))

        # Limpiar nombre técnico a algo legible
        name_display = _clean_position_name(name)
        rows += f"""<tr>
  <td title="{name}">{name_display}</td>
  <td style="text-align:center;"><span class="tp-badge {tipo_cls}">{tipo_lbl}</span></td>
  {_sector_cell(pos.get('sector'))}
  <td style="font-family:'Source Sans 3';font-size:11px;">{_canon_pais(pos.get('pais'))}</td>
  <td>{pos.get('divisa','—')}</td>
  <td><div class="wbar"><div class="wfill" style="width:{bar_w}px;background:#0c2340;"></div>{f(w,1)}%</div></td>
  <td style="font-size:10px;color:var(--ink-4);"><div class="wbar"><div class="wfill" style="width:{cum_bar_w}px;background:var(--ink-3);"></div>{f(cum,0)}%</div></td>
  <td>{delta_html}</td>
</tr>"""

    # Avg RV exposure for historical comparison
    mix_all = data.get("cuantitativo", {}).get("mix_activos_historico", [])
    avg_liq = round(sum(m.get("liquidez_pct",0) or 0 for m in mix_all) / max(1,len(mix_all)), 1)

    # ── Narrativa estructurada: 3 cards (exposición / decisiones / concentración)
    texto_cart = s.get('texto', '')

    def _format_paras(paragraphs):
        import re as _re
        html = ""
        for p in paragraphs:
            p_fmt = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', p.strip())
            html += f'<p class="pr" style="font-size:12.5px;margin-bottom:8px;">{p_fmt}</p>'
        return html

    sections = _structure_cartera_narrative(texto_cart, tipos_weights)

    narrativa_html = ""
    if sections and any(sections.get(k) for k in ("exposicion", "posiciones", "sesgos", "cambios", "calidad")):
        # Estilo sobrio ligado al formato global: headers tipo 'sr' (igual que
        # el resto de secciones del dashboard), sin backgrounds ni borders coloridos.
        expo_paras = sections.get("exposicion", []) + sections.get("posiciones", []) + sections.get("sesgos", []) + sections.get("calidad", [])
        if expo_paras:
            narrativa_html += f'''
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Exposición actual y racional</div>
  <div class="mb20">{_format_paras(expo_paras)}</div>'''
        cambios = sections.get("cambios", [])
        if cambios:
            narrativa_html += f'''
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Decisiones / cambios recientes</div>
  <div class="mb20">{_format_paras(cambios)}</div>'''
        conc = sections.get("concentracion", [])
        if conc and not es_fondo_de_instrumentos:
            narrativa_html += f'''
  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Concentración</div>
  <div class="mb20">{_format_paras(conc)}</div>'''
        # Fallback: párrafos que no encajaron en ninguna sección
        otros = sections.get("otros", [])
        if otros and not narrativa_html:
            narrativa_html = f'<div class="mb24">{_format_paras(otros)}</div>'
    elif texto_cart:
        narrativa_html = render_narrative_inline(texto_cart, data.get("nombre",""))
    else:
        narrativa_html = f'<p class="pr">La cartera actual comprende <strong>{len(sorted_pos)} posiciones</strong>.</p>'

    # KPI concentración: etiqueta diferente si el fondo es de instrumentos (ETFs/Oro/Commodities)
    conc_label = "Top 10 concentración"
    conc_sub = f"vs media hist. {f(avg_top10,1)}%" if avg_top10 else "—"
    if es_fondo_de_instrumentos:
        conc_sub = "Menor relevancia · fondo de instrumentos (ETF/Oro/Commodities)"

    # Breakdown por tipo de activo para el KPI.
    # Formato estándar: number grande (peso del tipo dominante) + label del tipo
    # dominante + sub con breakdown del resto. Alineado con otros KPIs de la fila.
    tipos_ordered = sorted(tipos_weights.items(), key=lambda x: -x[1])
    tipos_top4 = [(k, v) for k, v in tipos_ordered if v >= 1.0][:4]
    tipo_labels_short = {
        "RF": "Renta fija", "RV": "Renta variable", "GOLD": "Oro", "CASH": "Liquidez",
        "ETF": "ETF/fondos", "COMM": "Commodities", "DER": "Derivados", "OTHER": "Otros",
    }
    if tipos_top4:
        dom_code, dom_peso = tipos_top4[0]
        dom_label = tipo_labels_short.get(dom_code, dom_code)
        breakdown_value = f"{f(dom_peso, 1)}%"
        # Sub: tipo dominante + otros tipos con %
        others = tipos_top4[1:]
        if others:
            sub_parts = [dom_label] + [
                f"{tipo_labels_short.get(c, c)} {f(w, 1)}%" for c, w in others
            ]
            breakdown_sub = " · ".join(sub_parts)
        else:
            breakdown_sub = dom_label
    else:
        breakdown_value = "—"
        breakdown_sub = "—"

    # Gráficos adaptativos: si hay >1 año histórico → line/bar; si solo 1 año → snapshot
    has_multi_year_hist = len(hist_years) >= 2
    charts_html = ""
    if has_multi_year_hist:
        charts_html = '''<div class="col2 mb20">
    <div class="ch-b"><div class="ch-l">Nº posiciones por año</div><div class="ch-h"><canvas id="c-npos"></canvas></div></div>
    <div class="ch-b"><div class="ch-l">Concentración Top 5 / 10 / 15 (%)</div><div class="ch-h"><canvas id="c-conc"></canvas></div></div>
  </div>'''
    else:
        # Solo 1 año → snapshot bar con top5/10/15 actual + nº posiciones como KPI grande
        cur_top5 = round(sum(cur_weights[:5]), 1)
        cur_top15 = round(sum(cur_weights[:15]), 1)
        # F4: guards — sin mix de tipos válido NO renderizar Composición; sin posiciones NO renderizar Conc.
        has_mix = bool(tipos_weights) and any(v > 0.5 for v in tipos_weights.values())
        has_conc_snapshot = bool(cur_weights)
        mix_card = (
            '<div class="ch-b"><div class="ch-l">Composición por tipo de activo</div>'
            '<div class="ch-h"><canvas id="c-mix"></canvas></div></div>'
            if has_mix else ""
        )
        conc_card = (
            '<div class="ch-b"><div class="ch-l">Concentración Top 5 / 10 / 15 (snapshot actual)</div>'
            '<div class="ch-h"><canvas id="c-conc"></canvas></div></div>'
            if has_conc_snapshot else ""
        )
        cards_n = sum(1 for c in (mix_card, conc_card) if c)
        wrapper_cls = "col2" if cards_n == 2 else ("col1" if cards_n == 1 else "")
        cards_block = f'<div class="{wrapper_cls} mb20">{mix_card}{conc_card}</div>' if wrapper_cls else ""
        charts_html = f'''{cards_block}
  <script>
  (function(){{
    if (typeof Chart === 'undefined') return;
    const ctxMix = document.getElementById('c-mix');
    if (ctxMix) {{
      const tipos = {json.dumps([(k, round(v, 1)) for k, v in sorted(tipos_weights.items(), key=lambda x: -x[1]) if v > 0.5])};
      new Chart(ctxMix, {{
        type: 'doughnut',
        data: {{ labels: tipos.map(x => x[0]), datasets: [{{ data: tipos.map(x => x[1]), backgroundColor: ['#0c2340','#b48020','#1b8a3d','#6b3fa0','#3d5a80','#888'] }}] }},
        options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ position:'right', labels:{{font:{{size:10}}}} }} }} }}
      }});
    }}
    const ctxC = document.getElementById('c-conc');
    if (ctxC) {{
      new Chart(ctxC, {{
        type: 'bar',
        data: {{ labels:['Top 5','Top 10','Top 15'], datasets: [{{ label:'% NAV', data: [{cur_top5}, {cur_top10}, {cur_top15}], backgroundColor:'rgba(12,35,64,0.85)' }}] }},
        options: {{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}}, scales:{{ x:{{grid:{{display:false}}}}, y:{{grid:{{display:false}}, beginAtZero:true, min:0, max:100, ticks:{{stepSize:20, callback:v=>v+'%'}} }} }} }}
      }});
    }}
  }})();
  </script>'''

    desglose_expo_html = _build_desglose_exposicion_html(data)

    # Evolución de pesos por geografía / sector a lo largo de los años (de los AR).
    _geo_evo = build_allocation_evolution_chart(
        data.get("geographic_allocation_history"), "zonas",
        "Evolución por geografía (% sobre patrimonio)", "c-geo-evo")
    _sec_evo = build_allocation_evolution_chart(
        data.get("sector_allocation_history"), "sectores",
        "Evolución por sector (% sobre patrimonio)", "c-sec-evo")
    evo_alloc_html = ""
    if _geo_evo or _sec_evo:
        _cls = "col2" if (_geo_evo and _sec_evo) else "col1"
        evo_alloc_html = f'<div class="{_cls} mb20">{_geo_evo}{_sec_evo}</div>'

    return f"""
<section class="pane" id="p5">
  <div class="pane-header"><h1 class="pane-h1">Cartera actual</h1><span class="pane-dl">Posiciones a cierre</span></div>

  <div class="mb24">
    {narrativa_html}
  </div>

  {evo_alloc_html}

  <div class="kpi-row">
    <div class="kpi-cell"><div class="kpi-label">Posiciones totales</div><div class="kpi-value">{len(sorted_pos)}</div><div class="kpi-sub">vs media hist. {f(avg_npos,0)}</div></div>
    <div class="kpi-cell"><div class="kpi-label">{conc_label}</div><div class="kpi-value">{f(cur_top10,1)}%</div><div class="kpi-sub">{conc_sub}</div></div>
    <div class="kpi-cell"><div class="kpi-label">Liquidez</div><div class="kpi-value">{f(liq,1)}%</div><div class="kpi-sub">vs media hist. {f(avg_liq,1)}%</div></div>
    <div class="kpi-cell"><div class="kpi-label">Desglose por tipo</div><div class="kpi-value">{breakdown_value}</div><div class="kpi-sub">{breakdown_sub}</div></div>
  </div>

  {charts_html}

  <div class="sr">Todas las posiciones ({len(sorted_pos)})</div>
  <div class="pt-wrap">
    <table class="pt">
      <thead><tr><th>Activo</th><th style="text-align:center;">Tipo</th><th>Sector</th><th>País</th><th>Divisa</th><th>Peso %</th><th>Peso acum.</th><th>Var.</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 7: FUENTES EXTERNAS
# ═══════════════════════════════════════════════════════════════

def build_tab_fuentes(data):
    s = get_section_fuentes_externas(data) if _ACCESSOR_AVAILABLE else data.get("analyst_synthesis", {}).get("fuentes_externas", {})
    ops = s.get("opiniones_clave", [])
    # M3 v2 Fase M (2026-04-30): recursos oficiales gestora (cartas, KIID,
    # folleto, presentaciones, videos descubiertos vía Serper en web gestora).
    # Se renderiza en sección PROPIA antes de "Análisis profesionales".
    recursos_oficiales = s.get("recursos_oficiales", []) or []

    # Logo + pro source classification cargados desde data/trusted_sources.json
    # (loader en tools/trusted_sources.py). Para añadir/quitar fuentes editar el JSON.
    try:
        from tools.trusted_sources import get_logo_map, get_pro_source_domains
        logo_map_loaded = get_logo_map()
        pro_domains = get_pro_source_domains()
    except Exception:
        logo_map_loaded, pro_domains = {}, []

    # Iconos especiales no asociados a un dominio (tipo de contenido)
    logo_map = {
        **logo_map_loaded,
        "podcast": ("#8a3a8a", "🎙"),
        "video": ("#c04040", "▶"),
        "vídeo": ("#c04040", "▶"),
        "avantage": ("#0c2340", "AC"),
    }
    if not logo_map_loaded:
        # Fallback hardcoded si el JSON falla
        logo_map.update({
            "substack": ("#ff6719", "SF"),
            "salud financiera": ("#ff6719", "SF"),
            "moclano": ("#ff6719", "MO"),
            "rankia": ("#e85d26", "RK"),
            "finect": ("#1a8c5a", "FN"),
            "astralis": ("#6b3fa0", "AS"),
            "más dividendos": ("#5a6577", "MD"),
            "masdividendos": ("#5a6577", "MD"),
            "youtube": ("#c04040", "▶"),
        })

    def get_logo(fuente):
        fl = (fuente or "").lower()
        for key, (color, initials) in logo_map.items():
            if key in fl:
                return color, initials
        return "#555", fuente[:2].upper() if fuente else "??"

    # Pro sources: dominios de pro_sources del JSON + extra hardcoded
    # (Finanzasmania/UncommonFinance/ZonaValue no tienen entry propia aún).
    pro_sources_extra = ["finanzasmania", "uncommon finance", "zona value"]
    pro_sources = [d.split(".")[0] for d in pro_domains] + [
        "substack",  # cualquier substack (incluye salud financiera, moclano)
    ] + pro_sources_extra
    if not pro_domains:
        # Fallback hardcoded
        pro_sources = [
            "salud financiera", "substack", "moclano", "masdividendos", "más dividendos",
            "rankia", "astralis", "valueschool", "value school",
            "finanzasmania", "uncommon finance", "zona value",
        ]
    pro = []
    otros = []
    for op in ops:
        fuente_l = (op.get("fuente","") or "").lower()
        url_l = (op.get("url","") or "").lower()
        is_pro = any(src in fuente_l or src in url_l for src in pro_sources)
        if is_pro:
            pro.append(op)
        else:
            otros.append(op)

    def render_card(op, expanded=True):
        fuente = op.get('fuente', '')
        color, initials = get_logo(fuente)
        titulo = op.get('titulo', '') or fuente
        opinion = op.get('opinion', '')
        fecha = op.get('fecha', '')
        url = op.get('url', '')
        exp_state = ' open' if expanded else ''
        exp_arrow = '▼' if expanded else '▶'

        link_html = f'<a href="{url}" class="src-lnk" target="_blank">Ver análisis completo →</a>' if url and url != '#' else ''

        return f"""
    <div class="src-card">
      <div class="src-head">
        <div class="src-logo" style="background:{color};">{initials}</div>
        <div class="src-info">
          <div class="src-o">{fuente}</div>
        </div>
        <span class="src-date">{fecha}</span>
      </div>
      <div class="src-t">{titulo}</div>
      <button class="exp-btn" onclick="const b=this.nextElementSibling;const o=b.classList.toggle('open');this.textContent=(o?'▼':'▶')+' Ver puntos clave';">{exp_arrow} Ver puntos clave</button>
      <div class="exp-body{exp_state}">{opinion}</div>
      {link_html}
    </div>"""

    pro_html = "".join(render_card(op, expanded=True) for op in pro)
    otros_html = "".join(render_card(op, expanded=False) for op in otros)

    # M3 v2 Fase M (2026-04-30): render recursos oficiales gestora agrupados
    # por tipo. Mini-cards compactas con icono, título, fecha y link directo.
    recursos_html = render_recursos_oficiales(recursos_oficiales) if recursos_oficiales else ""

    pro_section = (
        f'<div class="sr" style="margin-top:0;color:var(--navy);border-bottom-color:var(--navy);">Análisis profesionales</div>{pro_html}'
        if pro_html else ''
    )
    otros_section = (
        f'<div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Otros recursos externos</div>{otros_html}'
        if otros else ''
    )

    return f"""
<section class="pane" id="p6">
  <div class="pane-header"><h1 class="pane-h1">Fuentes externas</h1><span class="pane-dl">Análisis y recursos de terceros</span></div>
  {recursos_html}
  {pro_section}
  {otros_section}
</section>"""


def render_recursos_oficiales(recursos: list) -> str:
    """M3 v2 Fase M (2026-04-30): renderiza recursos oficiales de la web gestora
    agrupados por tipo. Cada tipo en su propia subsección con icono apropiado.
    """
    if not recursos:
        return ""

    # Agrupar por tipo con orden de prioridad
    TIPO_LABELS = {
        "carta_gestor":   ("📝", "Cartas del gestor"),
        "annual_report":  ("📊", "Informes anuales"),
        "semestral":      ("📈", "Informes semestrales"),
        "presentacion":   ("🎯", "Presentaciones"),
        "comentario":     ("💬", "Comentarios mensuales"),
        "factsheet":      ("📋", "Factsheets / Fichas"),
        "mensual":        ("📅", "Informes mensuales"),
        "video":          ("▶", "Videos / Webinars"),
        "kiid":           ("⚖", "KIID / DFI / Folleto"),
        "folleto":        ("⚖", "Folleto"),
        "sostenibilidad": ("🌱", "Sostenibilidad / ESG"),
        "documento_otro": ("📄", "Otros documentos"),
    }

    grouped = {}
    for r in recursos:
        tipo = r.get("tipo", "documento_otro")
        grouped.setdefault(tipo, []).append(r)

    # Ordenar dentro de cada tipo: por fecha descendente
    for tipo in grouped:
        grouped[tipo].sort(key=lambda r: r.get("fecha", "") or "", reverse=True)

    # Render por orden de prioridad de TIPO_LABELS
    sections = []
    rendered_tipos = set()

    def render_tipo_section(tipo, items, icono, label):
        if not items:
            return ""
        cards = []
        for r in items[:20]:  # max 20 por tipo
            titulo = (r.get("titulo", "") or "Documento").strip() or "Documento"
            url = r.get("url", "#")
            fecha = r.get("fecha", "")
            fecha_html = f'<span class="rec-date">{fecha}</span>' if fecha else ""
            cards.append(f'''
      <a class="rec-card" href="{url}" target="_blank">
        <div class="rec-icon">{icono}</div>
        <div class="rec-meta">
          <div class="rec-title">{titulo[:120]}</div>
          {fecha_html}
        </div>
      </a>''')
        cards_html = "".join(cards)
        more_html = (f'<div class="rec-more">+{len(items)-20} más</div>'
                     if len(items) > 20 else "")
        return f'''
  <div class="rec-group">
    <div class="rec-group-head">{icono} {label} <span class="rec-count">({len(items)})</span></div>
    <div class="rec-grid">{cards_html}</div>
    {more_html}
  </div>'''

    for tipo, (icono, label) in TIPO_LABELS.items():
        if tipo in grouped:
            sections.append(render_tipo_section(tipo, grouped[tipo], icono, label))
            rendered_tipos.add(tipo)

    # Tipos no listados (rare) — al final como "Otros"
    for tipo, items in grouped.items():
        if tipo not in rendered_tipos:
            sections.append(render_tipo_section(tipo, items, "📄", tipo.replace("_", " ").title()))

    sections_html = "".join(sections)

    # CSS inline scoped to this section
    css = """
<style>
.rec-group{margin:18px 0 24px;}
.rec-group-head{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;color:var(--ink-2);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--rule-light);}
.rec-count{font-size:10.5px;color:var(--ink-4);font-weight:400;margin-left:4px;}
.rec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px;}
.rec-card{display:flex;gap:10px;padding:10px 12px;border:1px solid var(--rule-light);background:var(--paper-2);text-decoration:none;color:var(--ink);transition:all 0.15s;}
.rec-card:hover{border-color:var(--navy);background:var(--white);transform:translateY(-1px);}
.rec-icon{font-size:18px;line-height:1;flex-shrink:0;width:24px;text-align:center;}
.rec-meta{flex:1;min-width:0;}
.rec-title{font-size:12px;line-height:1.4;color:var(--ink-2);overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;}
.rec-date{font-size:10px;color:var(--ink-4);margin-top:3px;display:block;font-family:'Source Code Pro',monospace;}
.rec-more{margin-top:6px;font-size:11px;color:var(--ink-4);text-align:right;font-style:italic;}
</style>
"""

    return f'''
  <div class="sr" style="margin-top:0;color:var(--navy);border-bottom-color:var(--navy);">📁 Recursos oficiales del fondo (web gestora)</div>
  {css}
  {sections_html}
'''


# ═══════════════════════════════════════════════════════════════
# TAB 8: DOCUMENTOS
# ═══════════════════════════════════════════════════════════════

def build_tab_documentos(data):
    s = get_documentos(data) if _ACCESSOR_AVAILABLE else data.get("analyst_synthesis", {}).get("documentos", {})
    pdfs = s.get("informes_pdf", [])
    cartas = sorted(s.get("cartas_urls", []), reverse=True)
    xmls = s.get("xmls_cnmv", [])
    ext = sorted(s.get("fuentes_externas_urls", []))
    total = s.get("total_fuentes", 0)

    def url_to_name(url_str):
        """Extract readable name from URL"""
        from urllib.parse import urlparse, unquote
        try:
            parsed = urlparse(url_str)
            domain = parsed.netloc.replace("www.", "").replace("foro.", "")
            path = unquote(parsed.path).strip("/")
            # Use path segments to build name
            parts = [p for p in path.split("/") if p and p not in ("p", "t", "podcast")]
            if parts:
                # Clean: replace hyphens/underscores with spaces, capitalize
                slug = parts[-1]
                # Skip numeric-only slugs, go to previous
                if slug.isdigit() and len(parts) > 1:
                    slug = parts[-2]
                    if slug.isdigit() and len(parts) > 2:
                        slug = parts[-3]
                name = slug.replace("-", " ").replace("_", " ").strip()
                if len(name) > 3:
                    return f"{domain} — {name[:65]}"
            return domain
        except Exception:
            return url_str[:60]

    isin = data.get("isin", "")
    fund_dir = ROOT / "data" / "funds" / isin

    def _resolve_local_pdf(archivo, icon):
        """Construye file:// URI para un fichero local en raw/{subdir}/.
        Devuelve "#" si no existe.
        """
        if not archivo:
            return "#"
        # Mapping icon → subdir bajo raw/ (preferido)
        subdir = {"PDF": "reports", "XML": "xml"}.get(icon, "reports")
        path = fund_dir / "raw" / subdir / archivo
        if path.exists():
            return path.as_uri()  # file:///C:/...
        # Fallback: probar todos los subdirs comunes (INT usa raw/discovery/)
        for sub in ("reports", "xml", "letters", "discovery", "manual"):
            alt = fund_dir / "raw" / sub / archivo
            if alt.exists():
                return alt.as_uri()
        return "#"

    def doc_rows(items, icon, max_n=12):
        html = ""
        for item in items[:max_n]:
            if isinstance(item, dict):
                archivo = item.get("archivo", "")
                name = archivo or str(item)
                url = _resolve_local_pdf(archivo, icon)
            else:
                url = str(item) if str(item).startswith("http") else "#"
                name = url_to_name(str(item)) if url != "#" else str(item)[:60]
            html += f'<div class="doc-r"><span class="doc-ext">{icon}</span><span class="doc-nm">{name}</span><a href="{url}" target="_blank" class="doc-a">{"↗ Abrir" if url != "#" else ""}</a></div>'
        if len(items) > max_n:
            html += f'<div class="doc-r" style="color:var(--ink-4);font-size:11px;">+ {len(items)-max_n} archivos más</div>'
        return html

    return f"""
<section class="pane" id="p7">
  <div class="pane-header"><h1 class="pane-h1">Documentos</h1><span class="pane-dl">{total} fuentes consultadas</span></div>

  <div class="doc-grp">Informes semestrales CNMV ({len(pdfs)})</div>
  {doc_rows(pdfs, 'PDF')}

  <div class="doc-grp">Cartas del gestor ({len(cartas)})</div>
  {doc_rows(cartas, 'PDF', 10)}

  <div class="doc-grp">XMLs CNMV ({len(xmls)})</div>
  {doc_rows(xmls, 'XML', 6)}

  <div class="doc-grp">Fuentes externas ({len(ext)})</div>
  {doc_rows(ext, 'URL', 10)}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 9: CHAT
# ═══════════════════════════════════════════════════════════════

def build_tab_chat(data):
    nombre = data.get("nombre", "Fondo")
    isin = data.get("isin", "")
    return f"""
<section class="pane" id="p8">
  <style>
    .chat-container {{
      max-width: 900px; margin: 0 auto; display: flex; flex-direction: column; height: calc(100vh - 220px); min-height: 500px;
    }}
    .chat-header {{
      padding: 16px 0 12px; border-bottom: 1px solid var(--rule-light);
    }}
    .chat-header h1 {{
      font-family: 'EB Garamond', serif; font-size: 22px; color: var(--ink-1); margin: 0;
    }}
    .chat-header p {{
      font-size: 12px; color: var(--ink-4); margin: 4px 0 0; line-height: 1.4;
    }}
    .chat-status {{
      display: inline-flex; align-items: center; gap: 6px; font-size: 11px; margin-top: 6px;
      padding: 3px 10px; border-radius: 10px; background: var(--navy-pale);
    }}
    .chat-status .dot {{
      width: 7px; height: 7px; border-radius: 50%; background: #ccc;
    }}
    .chat-status .dot.on {{ background: #22c55e; }}
    .chat-messages {{
      flex: 1; overflow-y: auto; padding: 20px 0; display: flex; flex-direction: column; gap: 16px;
    }}
    .chat-msg {{
      max-width: 85%; padding: 12px 16px; border-radius: 10px; font-size: 13.5px; line-height: 1.55;
      font-family: 'Source Sans 3', sans-serif;
    }}
    .chat-msg.user {{
      align-self: flex-end; background: var(--navy); color: #fff; border-bottom-right-radius: 3px;
    }}
    .chat-msg.ai {{
      align-self: flex-start; background: var(--navy-pale); color: var(--ink-1); border-bottom-left-radius: 3px;
      border: 1px solid var(--rule-light);
    }}
    .chat-msg.ai strong {{ color: var(--navy); }}
    .chat-msg.system {{
      align-self: center; background: none; color: var(--ink-4); font-size: 11px; padding: 4px;
    }}
    .chat-input-area {{
      display: flex; gap: 8px; padding: 14px 0; border-top: 1px solid var(--rule-light);
    }}
    .chat-input {{
      flex: 1; padding: 10px 14px; border: 1px solid var(--rule-light); border-radius: 8px;
      font-family: 'Source Sans 3', sans-serif; font-size: 14px; background: var(--bg);
      color: var(--ink-1); resize: none; outline: none; min-height: 42px; max-height: 120px;
    }}
    .chat-input:focus {{ border-color: var(--navy); box-shadow: 0 0 0 2px rgba(15,23,42,0.08); }}
    .chat-send {{
      padding: 10px 20px; background: var(--navy); color: #fff; border: none; border-radius: 8px;
      font-family: 'Source Sans 3', sans-serif; font-size: 13px; font-weight: 600; cursor: pointer;
      letter-spacing: 0.3px; white-space: nowrap;
    }}
    .chat-send:hover {{ opacity: 0.9; }}
    .chat-send:disabled {{ opacity: 0.4; cursor: not-allowed; }}
    .chat-clear {{
      padding: 10px 14px; background: none; color: var(--ink-4); border: 1px solid var(--rule-light);
      border-radius: 8px; font-size: 12px; cursor: pointer; font-family: 'Source Sans 3', sans-serif;
    }}
    .chat-typing {{ display: inline-flex; gap: 4px; padding: 4px 0; }}
    .chat-typing span {{
      width: 6px; height: 6px; border-radius: 50%; background: var(--ink-4); opacity: 0.4;
      animation: blink 1.4s infinite both;
    }}
    .chat-typing span:nth-child(2) {{ animation-delay: 0.2s; }}
    .chat-typing span:nth-child(3) {{ animation-delay: 0.4s; }}
    @keyframes blink {{ 0%,80%,100% {{ opacity: 0.4; }} 40% {{ opacity: 1; }} }}
    .chat-suggestions {{
      display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 0;
    }}
    .chat-sug {{
      padding: 6px 12px; border: 1px solid var(--rule-light); border-radius: 16px;
      font-size: 12px; color: var(--ink-3); cursor: pointer; background: var(--bg);
      font-family: 'Source Sans 3', sans-serif; transition: all 0.15s;
    }}
    .chat-sug:hover {{ background: var(--navy-pale); border-color: var(--navy); color: var(--navy); }}
  </style>

  <div class="chat-container">
    <div class="chat-header">
      <h1>Chat con los documentos del fondo</h1>
      <p>Asistente que responde basandose UNICAMENTE en los documentos analizados de
         {nombre} ({isin}): informes CNMV, cartas del gestor, lecturas externas y perfiles
         de gestores. No consulta fuentes externas ni inventa datos.</p>
      <div class="chat-status">
        <div class="dot" id="chatDot"></div>
        <span id="chatStatusText">Conectando...</span>
      </div>
    </div>

    <div class="chat-messages" id="chatMessages">
      <div class="chat-msg system">Inicia una conversacion o prueba una de las sugerencias.</div>
      <div class="chat-suggestions" id="chatSuggestions">
        <button class="chat-sug" onclick="askSuggestion(this)">Resumen ejecutivo del fondo en 5 puntos</button>
        <button class="chat-sug" onclick="askSuggestion(this)">Que dijo el gestor en su ultima carta?</button>
        <button class="chat-sug" onclick="askSuggestion(this)">Cuales son las 5 mayores posiciones y por que estan?</button>
        <button class="chat-sug" onclick="askSuggestion(this)">Como se comporto el fondo en 2022?</button>
        <button class="chat-sug" onclick="askSuggestion(this)">Que riesgos tiene este fondo?</button>
        <button class="chat-sug" onclick="askSuggestion(this)">Comparame las comisiones entre clases</button>
      </div>
    </div>

    <div class="chat-input-area">
      <textarea class="chat-input" id="chatInput" placeholder="Pregunta sobre el fondo..."
        rows="1" onkeydown="if(event.key==='Enter'&&!event.shiftKey){{event.preventDefault();sendChat();}}"></textarea>
      <button class="chat-send" id="chatSend" onclick="sendChat()">Enviar</button>
      <button class="chat-clear" onclick="clearChat()">Limpiar</button>
    </div>
  </div>

  <script>
  const CHAT_API = 'http://localhost:8899';
  const chatMessages = document.getElementById('chatMessages');
  const chatInput = document.getElementById('chatInput');
  const chatSend = document.getElementById('chatSend');
  const chatDot = document.getElementById('chatDot');
  const chatStatusText = document.getElementById('chatStatusText');
  let chatBusy = false;

  // Auto-resize textarea
  chatInput.addEventListener('input', function() {{
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 120) + 'px';
  }});

  // Check server status
  async function checkServer() {{
    try {{
      const r = await fetch(CHAT_API + '/api/info');
      if (r.ok) {{
        const d = await r.json();
        chatDot.classList.add('on');
        chatStatusText.textContent = 'Conectado — ' + d.documents_loaded.length + ' documentos cargados';
        return true;
      }}
    }} catch(e) {{}}
    chatDot.classList.remove('on');
    chatStatusText.textContent = 'Servidor no disponible. Ejecutar: python chat_server.py {isin}';
    return false;
  }}
  checkServer();
  setInterval(checkServer, 10000);

  function addMessage(text, role) {{
    // Remove suggestions on first message
    const sug = document.getElementById('chatSuggestions');
    if (sug) sug.remove();

    const div = document.createElement('div');
    div.className = 'chat-msg ' + role;
    // Convert markdown bold and newlines
    let html = text.replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>');
    html = html.replace(/\\n/g, '<br>');
    div.innerHTML = html;
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return div;
  }}

  function addTyping() {{
    const div = document.createElement('div');
    div.className = 'chat-msg ai';
    div.id = 'chatTyping';
    div.innerHTML = '<div class="chat-typing"><span></span><span></span><span></span></div>';
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return div;
  }}

  async function sendChat() {{
    if (chatBusy) return;
    const q = chatInput.value.trim();
    if (!q) return;

    const online = await checkServer();
    if (!online) {{
      addMessage('Servidor no disponible. Ejecuta: python chat_server.py {isin}', 'system');
      return;
    }}

    chatBusy = true;
    chatSend.disabled = true;
    chatInput.value = '';
    chatInput.style.height = 'auto';

    addMessage(q, 'user');
    const typing = addTyping();

    try {{
      const resp = await fetch(CHAT_API + '/api/chat', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{question: q}}),
      }});

      // Remove typing indicator and create AI message
      typing.remove();
      const aiDiv = addMessage('', 'ai');

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let fullText = '';

      while (true) {{
        const {{done, value}} = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, {{stream: true}});
        fullText += chunk;
        let html = fullText.replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>');
        html = html.replace(/\\n/g, '<br>');
        aiDiv.innerHTML = html;
        chatMessages.scrollTop = chatMessages.scrollHeight;
      }}
    }} catch(e) {{
      typing.remove();
      addMessage('Error de conexion: ' + e.message, 'system');
    }}

    chatBusy = false;
    chatSend.disabled = false;
    chatInput.focus();
  }}

  function askSuggestion(btn) {{
    chatInput.value = btn.textContent;
    sendChat();
  }}

  async function clearChat() {{
    chatMessages.innerHTML = '<div class="chat-msg system">Conversacion limpiada.</div>';
    try {{ await fetch(CHAT_API + '/api/clear', {{method: 'POST'}}); }} catch(e) {{}}
  }}
  </script>
</section>"""


# ═══════════════════════════════════════════════════════════════
# SCRIPTS
# ═══════════════════════════════════════════════════════════════

def build_scripts(data):
    cuant = data.get("cuantitativo", {})
    pos_hist = data.get("posiciones", {}).get("historicas", [])

    # Extract series
    aum = cuant.get("serie_aum", [])
    part = cuant.get("serie_participes", [])
    vl = cuant.get("serie_vl_base100", [])
    com_a = cuant.get("serie_comisiones_por_clase", [])
    ter = cuant.get("serie_ter", [])

    # Filtrar entradas con periodo inválido ('None'/'none') para evitar
    # que el chart AUM tenga último valor erróneo del extractor
    aum = [s for s in aum if isinstance(s, dict)
           and str(s.get("periodo", "")).strip() not in ("", "None", "none")]
    # Ordenar cronológicamente (serie_aum se construye por upsert, sin orden) —
    # si no, el gráfico AUM sale con los años desordenados y el "último = header"
    # apunta al punto equivocado.
    aum = sorted(aum, key=lambda s: str(s.get("periodo", "")))
    years = [str(s.get("periodo",""))[-2:] for s in aum]
    aum_v = [s.get("valor_meur",0) for s in aum]
    # AUM último valor: forzar consistencia con el header (fuente autoritativa FT/extractor)
    aum_header = _hdr_resolve_aum_meur(data)
    if aum_v and aum_header:
        # Reemplazar el último punto con el valor del header para cuadrar
        try:
            aum_v[-1] = float(aum_header)
        except (ValueError, TypeError):
            pass
    part_v = [s.get("valor",0) for s in part]
    vl_v = [s.get("base100",0) for s in vl]

    # Performance fee flag
    ce = data.get("comision_exito", {})
    has_perf_fee = ce.get("existe", False) or False

    # ── Commission data ──
    # REGLA GENERAL:
    # - Com. gestión: serie_comisiones_por_clase (nombres COMERCIALES: I, R, D, G)
    # - TER: serie_ter_por_clase (nombres INTERNOS CNMV: A, B, C, D, E)
    # - Los nombres NO coinciden entre series. Mapeo por VALOR ASCENDENTE:
    #   se ordena com_gestion y TER por valor, y se emparejan posicionalmente.
    #   TER siempre > com_gestion para la misma clase (diff ~0.05-0.15).
    # - Si un año no tiene dato para una clase → null
    # - Años: unión de todos los años de ambas series

    com_by_year = {}
    for s in com_a:
        com_by_year[str(s.get("periodo", ""))] = s.get("clases", {})

    ter_cls_by_year = {}
    for s in cuant.get("serie_ter_por_clase", []):
        ter_cls_by_year[str(s.get("periodo", ""))] = s.get("clases", {})

    # Fallback: global TER if no per-class TER
    ter_global_by_year = {str(s.get("periodo", "")): s.get("ter_pct") for s in ter}

    # All years from all sources
    all_com_years = sorted(set(
        list(com_by_year.keys()) + list(ter_cls_by_year.keys()) + list(ter_global_by_year.keys())
    ))

    # All classes from comisiones (source of truth for COMMERCIAL names)
    # Bug 3 fix (2026-04-27): si serie_comisiones_por_clase vacía, fallback a
    # serie_ter_por_clase (también tiene clases por año).
    all_classes = set()
    for s in com_a:
        all_classes.update(s.get("clases", {}).keys())
    if not all_classes:
        for s in cuant.get("serie_ter_por_clase", []):
            all_classes.update(s.get("clases", {}).keys())
    all_classes = sorted(all_classes) if all_classes else ["A"]

    # Build mapping: commercial class name → TER internal name (per year)
    # Strategy: sort both by value ascending and pair positionally
    def _map_ter_to_com(com_clases: dict, ter_clases: dict) -> dict:
        """Map TER internal class names to commercial names by ascending value."""
        if not com_clases or not ter_clases:
            return {}
        com_sorted = sorted(com_clases.items(), key=lambda x: x[1])
        ter_sorted = sorted(ter_clases.items(), key=lambda x: x[1])
        mapping = {}
        for i, (com_name, _) in enumerate(com_sorted):
            if i < len(ter_sorted):
                mapping[com_name] = ter_sorted[i][0]
        return mapping

    # Build com_gestion and TER per commercial class, aligned to all_com_years
    # Bug 3 fix (2026-04-27): si com_a vacía, usar nombres de clase directamente
    # de serie_ter_por_clase (sin mapping com→ter).
    com_a_empty = not bool(com_a)
    com_by_class = {}
    ter_by_class = {}
    for cls in all_classes:
        com_vals = []
        ter_vals = []
        for y in all_com_years:
            # Com. gestión: direct lookup
            com_vals.append(com_by_year.get(y, {}).get(cls, None))

            # TER: map commercial name to internal name for this year
            com_y = com_by_year.get(y, {})
            ter_y = ter_cls_by_year.get(y, {})
            if com_a_empty:
                # Sin serie_comisiones, las clases YA son los nombres de TER
                if cls in ter_y:
                    ter_vals.append(ter_y[cls])
                elif ter_global_by_year.get(y):
                    ter_vals.append(ter_global_by_year[y])
                else:
                    ter_vals.append(None)
            else:
                mapping = _map_ter_to_com(com_y, ter_y)
                ter_internal = mapping.get(cls)
                if ter_internal and ter_internal in ter_y:
                    ter_vals.append(ter_y[ter_internal])
                elif len(all_classes) == 1 and ter_global_by_year.get(y):
                    ter_vals.append(ter_global_by_year[y])
                else:
                    ter_vals.append(None)

        com_by_class[cls] = com_vals
        ter_by_class[cls] = ter_vals

    # Default = clase con más historia (más puntos no-None en TER o COM)
    def _class_history_count(cls):
        com_pts = sum(1 for v in com_by_class.get(cls, []) if v is not None)
        ter_pts = sum(1 for v in ter_by_class.get(cls, []) if v is not None)
        return com_pts + ter_pts
    default_cls = max(all_classes, key=_class_history_count) if all_classes else "A"
    # Reordenar com_by_class y ter_by_class para que default_cls sea el PRIMERO
    # (Object.keys(COM_DATA)[0] en JS lo selecciona por defecto si no hay <select>).
    if default_cls in com_by_class:
        com_by_class = {default_cls: com_by_class[default_cls],
                        **{k: v for k, v in com_by_class.items() if k != default_cls}}
        ter_by_class = {default_cls: ter_by_class[default_cls],
                        **{k: v for k, v in ter_by_class.items() if k != default_cls}}
    ter_aligned = ter_by_class.get(default_cls, [None] * len(all_com_years))

    # Comisión de éxito: importes REALES cobrados por año (no el residual TER-com)
    # Estructura en serie_comisiones_por_clase: {'periodo': '2025', 'exito': {'UNICA': 0.62}}
    exito_by_year_real = {}
    for s in com_a:
        y = str(s.get("periodo", ""))
        ex = s.get("exito", {})
        if ex:
            # Tomar cualquier valor (UNICA o primera clase)
            val = next((v for v in ex.values() if v is not None), None)
            if val is not None:
                exito_by_year_real[y] = val
    # Array alineado a com_years con los importes cobrados (o null si no hubo)
    exito_real_aligned = [exito_by_year_real.get(y) for y in all_com_years]

    # TER EFECTIVO (ter_oficial + exito_cobrado) — lo que realmente paga el inversor
    # Fuente: serie_ter[].ter_efectivo_pct calculado por cnmv_agent
    ter_efectivo_by_year = {}
    for s in ter:
        y = str(s.get("periodo", ""))
        tef = s.get("ter_efectivo_pct")
        if tef is not None:
            ter_efectivo_by_year[y] = tef
    # Array alineado: si no hay ter_efectivo, usar ter_pct (mismo que oficial)
    ter_efectivo_aligned = []
    for i, y in enumerate(all_com_years):
        if y in ter_efectivo_by_year:
            ter_efectivo_aligned.append(ter_efectivo_by_year[y])
        elif ter_aligned[i] is not None and exito_real_aligned[i] is not None:
            # Calcular on-the-fly si no viene en cnmv_data
            ter_efectivo_aligned.append(round(ter_aligned[i] + exito_real_aligned[i], 4))
        else:
            ter_efectivo_aligned.append(ter_aligned[i])

    com_years = all_com_years

    # Position history for charts
    hist_sorted = sorted(pos_hist, key=lambda x: x.get("periodo",""))
    ch_yrs = []
    ch_npos = []
    ch_t5 = []
    ch_t10 = []
    ch_t15 = []
    for h in hist_sorted:
        todas = h.get("todas",[])
        if not todas: continue
        ch_yrs.append(str(h.get("periodo",""))[-2:])
        ch_npos.append(len(todas))
        w = sorted([x.get("peso_pct",0) or 0 for x in todas], reverse=True)
        ch_t5.append(round(sum(w[:5]),1))
        ch_t10.append(round(sum(w[:10]),1))
        ch_t15.append(round(sum(w[:15]),1))

    return f"""
<script>
function goTab(i,b){{document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));document.querySelectorAll('.tb').forEach(t=>t.classList.remove('on'));document.getElementById('p'+i).classList.add('on');b.classList.add('on');}}
function toggleTheme(){{const d=document.documentElement;const k=d.getAttribute('data-theme')==='dark';d.setAttribute('data-theme',k?'light':'dark');document.getElementById('thlbl').textContent=k?'Modo oscuro':'Modo claro';buildCharts();}}
const dk=()=>document.documentElement.getAttribute('data-theme')==='dark';
const TC=()=>dk()?'#908c84':'#444';
const GC=()=>dk()?'rgba(255,255,255,0.04)':'rgba(0,0,0,0.05)';
const A1=()=>dk()?'#4a7ea8':'#0c2340';
const A2=()=>dk()?'#4a8a6a':'#1a4d2e';
const A3=()=>dk()?'rgba(120,140,160,0.5)':'rgba(100,120,140,0.4)';
const AR=()=>dk()?'#c04040':'#6b1a1a';
const CI={{}};
// Formato español: . miles, , decimal
function fmtES(v,dec){{
  if(v==null)return'';
  if(dec===undefined)dec=v>=100||v===Math.round(v)?0:1;
  return v.toLocaleString('de-DE',{{minimumFractionDigits:dec,maximumFractionDigits:dec}});
}}
function mk(id,cfg){{if(CI[id])CI[id].destroy();const c=document.getElementById(id);if(!c)return;CI[id]=new Chart(c,cfg);}}
const sc=(mn,mx)=>({{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},y:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v);}}}},... (mn!=null?{{min:mn}}:{{}}),...(mx!=null?{{max:mx}}:{{}})}}}});
const opt=(leg)=>({{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:!!leg,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}}}});
const Y={json.dumps(years)};
// Anti-collision label helper for area/line charts with multiple series
function drawAreaLabels(chart,colors,dec,minGap){{
  const ctx=chart.ctx;
  ctx.font='500 7px Source Code Pro';
  ctx.textAlign='center';
  if(!minGap)minGap=12;
  if(!dec&&dec!==0)dec=1;
  const nds=chart.data.datasets.length;
  const npts=chart.data.datasets[0]?chart.data.datasets[0].data.length:0;
  // For each x-point, collect all labels and resolve collisions
  for(let i=0;i<npts;i++){{
    const labels=[];
    for(let di=0;di<nds;di++){{
      const ds=chart.data.datasets[di];
      const v=ds.data[i];
      const prev=i>0?ds.data[i-1]:null;
      if(v==null||v===0||v===prev)continue;
      const meta=chart.getDatasetMeta(di);
      const pt=meta.data[i];
      if(!pt)continue;
      labels.push({{di:di,v:v,x:pt.x,y:pt.y,color:colors[di]||TC()}});
    }}
    // Sort by y position (top to bottom)
    labels.sort((a,b)=>a.y-b.y);
    // Resolve collisions: push labels apart
    for(let j=1;j<labels.length;j++){{
      const gap=labels[j].y-labels[j-1].y;
      if(Math.abs(gap)<minGap){{
        labels[j-1].y-=(minGap-Math.abs(gap))/2;
        labels[j].y+=(minGap-Math.abs(gap))/2;
      }}
    }}
    // Draw
    labels.forEach(l=>{{
      ctx.fillStyle=l.color;
      const suffix=dec>=0?'%':'';
      const txt=dec>=0?fmtES(l.v,dec)+suffix:fmtES(l.v);
      ctx.fillText(txt,l.x,l.y-7);
    }});
  }}
}}
const valPlugin={{
  id:'valLabels',
  afterDatasetsDraw(chart){{
    const ctx=chart.ctx;
    ctx.font='500 8px Source Code Pro';
    ctx.textAlign='center';
    ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
    chart.data.datasets.forEach((ds,di)=>{{
      chart.getDatasetMeta(di).data.forEach((el,i)=>{{
        const v=ds.data[i];
        if(v!=null){{
          const y=el.y!=null?el.y:(el.y2||el.y);
          ctx.fillText(fmtES(v),el.x,y-6);
        }}
      }});
    }});
  }}
}};
function buildCharts(){{
  mk('c-aum',{{type:'bar',data:{{labels:Y,datasets:[{{data:{json.dumps(aum_v)},backgroundColor:A1()+'99'}}]}},options:{{...opt(),scales:sc(0)}},plugins:[valPlugin]}});
  mk('c-part',{{type:'bar',data:{{labels:Y,datasets:[{{data:{json.dumps(part_v)},backgroundColor:A2()+'99'}}]}},options:{{...opt(),scales:sc(0)}},plugins:[valPlugin]}});
  const vlCorrupta={'true' if data.get('serie_vl_corrupta') else 'false'};
  if(vlCorrupta){{
    const cvl=document.getElementById('c-vl');
    if(cvl){{
      const parent=cvl.parentElement;
      cvl.style.display='none';
      const warn=document.createElement('div');
      warn.style.cssText='display:flex;align-items:center;justify-content:center;height:100%;color:var(--ink-4);font-size:11px;font-style:italic;text-align:center;padding:12px;';
      warn.textContent='Serie VL no fiable (primer valor anómalo). Datos omitidos.';
      parent.appendChild(warn);
    }}
  }}else{{
    const vlMin=Math.max(0,Math.floor(Math.min(...{json.dumps(vl_v)}.filter(v=>v>0))/10)*10-10);
    const vlMax=Math.ceil(Math.max(...{json.dumps(vl_v)})/10)*10+10;
    mk('c-vl',{{type:'line',data:{{labels:Y,datasets:[{{data:{json.dumps(vl_v)},borderColor:A1(),backgroundColor:A1()+'14',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A1()}}]}},options:{{...opt(),scales:sc(vlMin,vlMax)}},plugins:[valPlugin]}});
  }}
  buildComChart();
  // Both charts share same X labels for alignment
  const cartYrs={json.dumps(ch_yrs)};
  const nposMax=Math.max(...{json.dumps(ch_npos)})+10;
  mk('c-npos',{{type:'bar',data:{{labels:cartYrs,datasets:[{{data:{json.dumps(ch_npos)},backgroundColor:A1()+'99'}}]}},options:{{...opt(),scales:sc(0,nposMax)}},plugins:[valPlugin]}});
  const concLblPlugin={{
    id:'concLabels',
    afterDatasetsDraw(chart){{
      drawAreaLabels(chart,[dk()?'#7ba8d0':'#0c2340',dk()?'#7ba8d0':'#1a3a5c',dk()?'#7ba8d0':'#3d5a80'],1,14);
    }}
  }};
  // Eje Y SIEMPRE 0-100% para ver concentración real de un vistazo
  mk('c-conc',{{type:'line',data:{{labels:cartYrs,datasets:[
    {{label:'Top 5',data:{json.dumps(ch_t5)},borderColor:A1(),backgroundColor:A1()+'40',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A1()}},
    {{label:'Top 10',data:{json.dumps(ch_t10)},borderColor:A1()+'99',backgroundColor:A1()+'25',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A1()+'99'}},
    {{label:'Top 15',data:{json.dumps(ch_t15)},borderColor:A1()+'66',backgroundColor:A1()+'15',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A1()+'66'}}
  ]}},options:{{...opt(true),scales:sc(0,100)}},plugins:[concLblPlugin]}});
}}
const COM_DATA={json.dumps(com_by_class)};
const TER_DATA={json.dumps(ter_by_class)};
const TER_EFECTIVO={json.dumps(ter_efectivo_aligned)};
const CY={json.dumps([y[-2:] for y in com_years])};
const HAS_EXITO={json.dumps(has_perf_fee)};
const EXITO_REAL={json.dumps(exito_real_aligned)};
function buildComChart(){{
  const sel=document.getElementById('com-sel');
  const cls=sel?sel.value:Object.keys(COM_DATA)[0]||'A';
  const d=COM_DATA[cls]||CY.map(()=>null);
  const t=TER_DATA[cls]||CY.map(()=>null);
  const tef=TER_EFECTIVO;
  const pctCb=function(v){{return v.toFixed(1)+'%';}};
  // Com. éxito: importes REALES cobrados s/patrimonio
  const exito=HAS_EXITO?EXITO_REAL.map((real,i)=>{{
    if(real!=null)return real;
    const tv=t[i],dv=d[i];
    if(tv==null||dv==null)return null;
    const diff=Math.round((tv-dv)*100)/100;
    return diff>0.05?diff:null;
  }}):t.map(()=>null);
  // Datasets: TER (efectivo = gestión + depositario + éxito + otros), Com. gestión, Com. éxito
  // TER mostrado es el EFECTIVO (lo que paga el inversor), no el TER oficial CNMV que excluye éxito
  const terShown = HAS_EXITO ? tef : t;
  const datasets=[
    {{label:'TER',data:terShown,borderColor:A1(),backgroundColor:A1()+'40',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A1(),order:1}},
    {{label:'Com. gestión',data:d,borderColor:A2(),backgroundColor:A2()+'35',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:A2(),order:2}},
    {{label:'Com. éxito',data:exito,borderColor:dk()?'#c07040':'#8c3214',backgroundColor:dk()?'rgba(192,112,64,0.40)':'rgba(140,50,20,0.30)',borderWidth:1.5,fill:true,tension:0.3,pointRadius:1,pointBackgroundColor:dk()?'#c07040':'#8c3214',order:3}}
  ];
  const allVals=[...terShown,...d,...exito].filter(v=>v!=null);
  const yMax=Math.max(1.2,Math.ceil((Math.max(...allVals)+0.2)*10)/10);
  const comLblPlugin={{
    id:'comAreaLabels',
    afterDatasetsDraw(chart){{
      drawAreaLabels(chart,[dk()?'#7ba8d0':'#0c2340',dk()?'#6aaa88':'#1a4d2e',dk()?'#e0a080':'#8c3214'],2,14);
    }}
  }};
  mk('c-com',{{type:'line',data:{{labels:CY,datasets:datasets}},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{display:true,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}},
      scales:{{
        x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
        y:{{grid:{{display:false}},min:0,max:yMax,ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}}}}
      }}
    }},
    plugins:[comLblPlugin]
  }});
}}
document.addEventListener('DOMContentLoaded',buildCharts);

// ═══════════════════════════════════════════════════════════
// MORNINGSTAR DATA: fetch + calculate + render
// ═══════════════════════════════════════════════════════════
const ISIN='{data.get("isin","ES0112231008")}';
let MST_DATA=null;

function dedupeSort(s){{s.sort((a,b)=>a.date-b.date);const o=[];for(const p of s){{if(!o.length||o[o.length-1].date.getTime()!==p.date.getTime())o.push(p);else o[o.length-1]=p;}}return o;}}
function monthEndF(s){{const m=new Map();for(const p of s){{const k=p.date.getUTCFullYear()+'-'+String(p.date.getUTCMonth()+1).padStart(2,'0');const c=m.get(k);if(!c||p.date>c.date)m.set(k,{{date:p.date,nav:p.nav}});}}return Array.from(m.values()).sort((a,b)=>a.date-b.date);}}
function yearEndF(s){{const m=new Map();for(const p of s){{const y=p.date.getUTCFullYear();const c=m.get(y);if(!c||p.date>c.date)m.set(y,{{year:y,date:p.date,nav:p.nav}});}}return Array.from(m.values()).sort((a,b)=>a.year-b.year);}}
function rets(levels){{const o=[];for(let i=1;i<levels.length;i++){{const a=levels[i-1].nav,b=levels[i].nav;if(a>0&&b>0)o.push({{date:levels[i].date,r:b/a-1}});}}return o;}}
function stdF(arr){{const x=arr.filter(v=>Number.isFinite(v));if(x.length<2)return NaN;const m=x.reduce((a,b)=>a+b,0)/x.length;return Math.sqrt(x.reduce((a,b)=>a+(b-m)*(b-m),0)/(x.length-1));}}

async function fetchMST(){{
  const url='https://tools.morningstar.es/api/rest.svc/timeseries_price/2nhcdckzon?id='+ISIN+'&idtype=Isin&frequency=daily&startDate=1900-01-01&outputType=compactJSON';
  try{{
    const res=await fetch(url,{{credentials:'omit'}});
    if(!res.ok)throw new Error('HTTP '+res.status);
    const arr=await res.json();
    const pts=[];
    for(const it of arr){{
      if(!Array.isArray(it)||it.length<2)continue;
      const ts=Number(it[0]),v=Number(it[1]);
      if(!Number.isFinite(ts)||!Number.isFinite(v)||v<=0)continue;
      const d0=new Date(ts);
      pts.push({{date:new Date(Date.UTC(d0.getUTCFullYear(),d0.getUTCMonth(),d0.getUTCDate())),nav:v}});
    }}
    return dedupeSort(pts);
  }}catch(e){{
    // Fallback proxy
    try{{
      const res2=await fetch('https://api.codetabs.com/v1/proxy?quest='+encodeURIComponent(url));
      const arr2=await res2.json();
      const pts2=[];
      for(const it of arr2){{if(Array.isArray(it)&&it.length>=2){{const ts=Number(it[0]),v=Number(it[1]);if(Number.isFinite(ts)&&Number.isFinite(v)&&v>0)pts2.push({{date:new Date(ts),nav:v}});}}}}
      return dedupeSort(pts2);
    }}catch(e2){{throw e2;}}
  }}
}}

function calcYearlyReturns(ye){{
  const xs=[],ys=[];
  for(let i=1;i<ye.length;i++){{
    const r=ye[i].nav/ye[i-1].nav-1;
    xs.push(ye[i].year);ys.push(r);
  }}
  return {{xs,ys}};
}}

function calcYearlyVol(me){{
  const mr=rets(me).map(x=>({{r:x.r,y:x.date.getUTCFullYear()}}));
  const byPos=new Map(),byNeg=new Map();
  const years=new Set();
  mr.forEach(p=>{{years.add(p.y);if(!byPos.has(p.y)){{byPos.set(p.y,[]);byNeg.set(p.y,[]);}}if(p.r>0)byPos.get(p.y).push(p.r);else if(p.r<0)byNeg.get(p.y).push(p.r);}});
  const xs=Array.from(years).sort(),ysP=[],ysN=[];
  // Detect incomplete last year
  const lastYr=xs[xs.length-1];
  const lastYrPts=mr.filter(p=>p.y===lastYr).length;
  xs.forEach(y=>{{
    const isInc=(y===lastYr&&lastYrPts<11);
    const pos=byPos.get(y)||[];const neg=byNeg.get(y)||[];
    ysP.push(isInc?NaN:(pos.length>=2?stdF(pos)*Math.sqrt(12):NaN));
    ysN.push(isInc?NaN:(neg.length>=2?stdF(neg)*Math.sqrt(12):NaN));
  }});
  return {{xs,ysP,ysN}};
}}

function calcDrawdown(series){{
  let peak=series[0].nav;
  let peakDate=series[0].date;
  const dates=[],dd=[];
  let worstDD=0,worstDate=null,worstPeakDate=null;
  let recoveredDate=null;
  for(const p of series){{
    if(p.nav>peak){{peak=p.nav;peakDate=p.date;}}
    const d=(p.nav-peak)/peak;
    dates.push(p.date);dd.push(d);
    if(d<worstDD){{worstDD=d;worstDate=p.date;worstPeakDate=peakDate;}}
  }}
  // Find recovery: primer punto post-trough donde nav vuelve al peak previo
  if(worstDate && worstPeakDate){{
    let peakVal=0;
    for(const p of series){{if(p.date===worstPeakDate){{peakVal=p.nav;break;}}}}
    for(const p of series){{
      if(p.date>worstDate && p.nav>=peakVal){{recoveredDate=p.date;break;}}
    }}
  }}
  return {{dates,dd,worstDD,worstDate,worstPeakDate,recoveredDate}};
}}

function monthsBetween(d1,d2){{
  if(!d1||!d2)return null;
  const a=new Date(d1),b=new Date(d2);
  return Math.round((b-a)/(1000*60*60*24*30.44));
}}
function fmtDate(d){{
  if(!d)return '—';
  const dt=new Date(d);
  const months=['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
  return months[dt.getMonth()]+' '+dt.getFullYear();
}}

function calcRolling(me,months){{
  const dates=[],vals=[];
  for(let i=months;i<me.length;i++){{
    const r=me[i].nav/me[i-months].nav;
    const ann=Math.pow(r,12/months)-1;
    dates.push(me[i].date);vals.push(ann);
  }}
  return {{dates,vals}};
}}

function calcRollingVol(me,months){{
  const mr=rets(me);
  const dates=[],vals=[];
  for(let i=months-1;i<mr.length;i++){{
    const window=mr.slice(i-months+1,i+1).map(x=>x.r);
    vals.push(stdF(window)*Math.sqrt(12));
    dates.push(mr[i].date);
  }}
  return {{dates,vals}};
}}

// Store processed data globally for rolling updates
let MST_ME=null,MST_SERIES=null;
const pctAxis={{grid:{{display:false}},ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}}}};

function renderMST(series){{
  MST_SERIES=series;
  const me=monthEndF(series);MST_ME=me;
  const ye=yearEndF(series);
  const {{xs:retXsAll,ys:retYsAll}}=calcYearlyReturns(ye);
  const {{xs:volXsAll,ysP:ysPAll,ysN:ysNAll}}=calcYearlyVol(me);
  const incYear=new Date().getUTCFullYear();

  // Limit bar charts to last 10 years + ensure same years for both
  const maxBars=10;
  const allYears=retXsAll.slice();
  const startIdx=Math.max(0,allYears.length-maxBars);
  const retXs=retXsAll.slice(startIdx);
  const retYs=retYsAll.slice(startIdx);
  // Align vol to same years
  const volStart=volXsAll.indexOf(retXs[0]);
  const ysP=ysPAll.slice(volStart>=0?volStart:0);
  const ysN=ysNAll.slice(volStart>=0?volStart:0);
  const retLabels=retXs.map(String);
  const volLabels=retLabels; // SAME years

  // Tooltip interaction for line charts
  const lineOpt=(leg)=>({{responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:!!leg,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}},
      tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return ctx.dataset.label+': '+fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
    scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:12}}}},y:pctAxis}}
  }});

  // ── Shared chart builders ──
  function renderRetChart(id){{
    const colors=retYs.map(v=>v>=0?(dk()?'#4a8a6a':'#1a4d2e'):(dk()?'#c04040':'#6b1a1a'));
    mk(id,{{type:'bar',data:{{labels:retLabels,datasets:[{{data:retYs.map(v=>v*100),backgroundColor:colors}}]}},
      options:{{...opt(),scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
        y:{{grid:{{display:false}},ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}},
          // Extra range so labels don't touch edges
          suggestedMax:Math.ceil(Math.max(...retYs)*100)+10,
          suggestedMin:Math.floor(Math.min(...retYs)*100)-10
        }}
      }}}},
      plugins:[{{id:id+'L',afterDatasetsDraw(chart){{
        const ctx=chart.ctx;ctx.font='500 8px Source Code Pro';ctx.textAlign='center';
        ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
        chart.getDatasetMeta(0).data.forEach((el,i)=>{{
          const v=retYs[i];if(v==null)return;
          ctx.fillText((v>=0?'+':'')+fmtES(v*100,1)+'%',el.x,v>=0?el.y-10:el.y+16);
        }});
      }}}}]
    }});
  }}

  function renderVolChart(id){{
    mk(id,{{type:'bar',data:{{labels:volLabels,datasets:[
      {{label:'Vol. positiva',data:ysP.map(v=>Number.isFinite(v)?v*100:null),backgroundColor:dk()?'#4a8a6a':'#1a4d2e'}},
      {{label:'Vol. negativa',data:ysN.map(v=>Number.isFinite(v)?-v*100:null),backgroundColor:dk()?'#8c3a3a':'#8c3214'}}
    ]}},
      options:{{responsive:true,maintainAspectRatio:false,
        plugins:{{legend:{{display:true,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}},
        scales:{{x:{{grid:{{display:false}},stacked:true,ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
          y:{{grid:{{display:false}},stacked:true,ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}},
            suggestedMax:Math.ceil(Math.max(...ysP.filter(v=>Number.isFinite(v)))*100)+8,
            suggestedMin:-Math.ceil(Math.max(...ysN.filter(v=>Number.isFinite(v)))*100)-8
          }}
        }}
      }},
      plugins:[{{id:id+'L',afterDatasetsDraw(chart){{
        const ctx=chart.ctx;ctx.font='500 7px Source Code Pro';ctx.textAlign='center';
        ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
        chart.data.datasets.forEach((ds,di)=>{{
          chart.getDatasetMeta(di).data.forEach((el,i)=>{{
            const v=ds.data[i];if(v==null||!Number.isFinite(v))return;
            const prev=i>0?ds.data[i-1]:null;if(v===prev)return;
            ctx.fillText(fmtES(Math.abs(v),1)+'%',el.x,di===0?el.y-8:el.y+14);
          }});
        }});
      }}}}]
    }});
  }}

  // ── Resumen tab ──
  renderRetChart('mst-ret');
  renderVolChart('mst-vol');
  if(retXs[retXs.length-1]===incYear){{
    const el=document.getElementById('mst-ret-note');
    if(el)el.textContent='* Último año ('+incYear+') incompleto.';
  }}

  // ── Historia tab: KPIs + VL chart desde datos Morningstar daily ──
  try {{
    // CAGR desde inicio (anualizado)
    const histTotalR = series[series.length-1].nav / series[0].nav;
    const histYears = (series[series.length-1].date - series[0].date) / 31557600000;
    const histCagr = Math.pow(histTotalR, 1/histYears) - 1;
    const kpiCagr = document.getElementById('kpi-cagr');
    if (kpiCagr) kpiCagr.textContent = fmtES(histCagr*100, 1) + '%';
    // Peor/mejor año desde rentabilidades anuales calculadas
    if (retXs.length > 0) {{
      let bestI=0, worstI=0;
      for (let i=0; i<retYs.length; i++) {{
        if (retYs[i] > retYs[bestI]) bestI = i;
        if (retYs[i] < retYs[worstI]) worstI = i;
      }}
      const kpiBest = document.getElementById('kpi-best');
      const kpiBestSub = document.getElementById('kpi-best-sub');
      if (kpiBest) kpiBest.textContent = '+' + fmtES(retYs[bestI]*100, 1) + '%';
      if (kpiBestSub) kpiBestSub.textContent = retXs[bestI];
      const kpiWorst = document.getElementById('kpi-worst');
      const kpiWorstSub = document.getElementById('kpi-worst-sub');
      if (kpiWorst) kpiWorst.textContent = fmtES(retYs[worstI]*100, 1) + '%';
      if (kpiWorstSub) kpiWorstSub.textContent = retXs[worstI];
    }}
    // VL base 100 chart (cvl): reescalado sobre el primer NAV.
    // IMPORTANTE: destruir el chart previo si existe (buildCharts() ya
    // pudo montar uno con datos anuales del extractor).
    const cvlEl = document.getElementById('c-vl');
    if (cvlEl && series.length > 1) {{
      try {{ Chart.getChart(cvlEl)?.destroy(); }} catch(e) {{}}
      const base = series[0].nav;
      const me2 = monthEndF(series);
      const labelsM = me2.map(p => p.date.getUTCFullYear()+'-'+String(p.date.getUTCMonth()+1).padStart(2,'0'));
      const valsM = me2.map(p => (p.nav/base)*100);
      const lastIdx = valsM.length - 1;
      const lastVal = valsM[lastIdx];
      // Anotación del último punto: valor visible en el gráfico
      const lastPointLabel = {{
        id: 'lastPointLabel',
        afterDatasetsDraw(chart) {{
          const {{ ctx, scales: {{ x, y }} }} = chart;
          const meta = chart.getDatasetMeta(0);
          const pt = meta.data[lastIdx];
          if (!pt) return;
          ctx.save();
          ctx.fillStyle = 'rgba(12,35,64,0.95)';
          ctx.strokeStyle = '#fff';
          ctx.lineWidth = 2;
          ctx.beginPath();
          ctx.arc(pt.x, pt.y, 4, 0, Math.PI * 2);
          ctx.fill();
          ctx.stroke();
          // Etiqueta con el valor
          ctx.font = '600 11px "Source Code Pro", monospace';
          ctx.fillStyle = 'rgba(12,35,64,1)';
          ctx.textAlign = 'left';
          ctx.textBaseline = 'middle';
          const txt = fmtES(lastVal, 1);
          const tx = pt.x + 8;
          const ty = pt.y - 2;
          // Fondo blanco de la etiqueta
          const metrics = ctx.measureText(txt);
          const padX = 4, padY = 2;
          ctx.fillStyle = 'rgba(255,255,255,0.92)';
          ctx.fillRect(tx - padX, ty - 8, metrics.width + padX*2, 14);
          ctx.fillStyle = 'rgba(12,35,64,1)';
          ctx.fillText(txt, tx, ty);
          ctx.restore();
        }}
      }};
      new Chart(cvlEl, {{
        type: 'line',
        data: {{ labels: labelsM, datasets: [{{
          label:'VL Base 100', data: valsM,
          borderColor:'rgba(12,35,64,0.85)', backgroundColor:'rgba(12,35,64,0.10)',
          fill:true, tension:0.2,
          pointRadius:0, pointHoverRadius:5, pointHitRadius:8,
          pointHoverBackgroundColor:'rgba(12,35,64,1)',
          pointHoverBorderColor:'#fff', pointHoverBorderWidth:2,
        }}] }},
        options: {{
          responsive:true, maintainAspectRatio:false,
          interaction: {{ mode:'index', intersect:false }},
          plugins:{{
            legend:{{display:false}},
            tooltip:{{
              enabled:true, displayColors:false,
              callbacks:{{
                title: (items) => items[0]?.label || '',
                label: (c) => 'Base 100: '+fmtES(c.parsed.y,1)
              }}
            }}
          }},
          scales:{{
            x:{{ grid:{{display:false}}, ticks:{{ maxTicksLimit:8, font:{{size:9}} }} }},
            y:{{ grid:{{display:false}}, beginAtZero:false, ticks:{{ callback: v => fmtES(v,0) }} }}
          }}
        }},
        plugins: [lastPointLabel]
      }});
    }}
  }} catch(eHist) {{ console.warn('Historia KPIs from MST failed:', eHist); }}

  // ── Evolución tab ──
  const evoEl=document.getElementById('mst-evo-content');
  const loadEl=document.getElementById('mst-loading');
  if(evoEl)evoEl.style.display='block';
  if(loadEl)loadEl.style.display='none';

  // KPIs
  const totalR=series[series.length-1].nav/series[0].nav;
  const nYears=(series[series.length-1].date-series[0].date)/31557600000;
  const cagr=Math.pow(totalR,1/nYears)-1;
  const allMR=rets(me).map(x=>x.r);
  const volAll=stdF(allMR)*Math.sqrt(12);
  const ddCalc=calcDrawdown(series);
  const maxDD=Math.min(...ddCalc.dd);
  const roll3=calcRolling(me,36);
  const avgRoll3=roll3.vals.length?roll3.vals.reduce((a,b)=>a+b,0)/roll3.vals.length:0;
  const rollVol12=calcRollingVol(me,12);
  const avgRollVol=rollVol12.vals.length?rollVol12.vals.reduce((a,b)=>a+b,0)/rollVol12.vals.length:0;

  const kpiEl=document.getElementById('mst-evo-kpis');
  // Drawdown details: fecha del mínimo y duración (peak→trough y trough→recovery)
  const ddDur = ddCalc.worstPeakDate && ddCalc.worstDate ? monthsBetween(ddCalc.worstPeakDate,ddCalc.worstDate) : null;
  const ddRec = ddCalc.worstDate && ddCalc.recoveredDate ? monthsBetween(ddCalc.worstDate,ddCalc.recoveredDate) : null;
  const ddSubParts = [];
  if(ddCalc.worstDate) ddSubParts.push(fmtDate(ddCalc.worstDate));
  if(ddDur) ddSubParts.push('−'+ddDur+'m');
  if(ddRec) ddSubParts.push('rec. '+ddRec+'m');
  else if(ddCalc.worstDate && !ddCalc.recoveredDate) ddSubParts.push('sin recuperar');
  const ddSub = ddSubParts.join(' · ');
  if(kpiEl)kpiEl.innerHTML=`
    <div class="kpi-cell"><div class="kpi-label">CAGR histórico</div><div class="kpi-value pos">`+fmtES(cagr*100,1)+`%</div><div class="kpi-sub">`+fmtES(nYears,1)+` años</div></div>
    <div class="kpi-cell"><div class="kpi-label">Volatilidad media</div><div class="kpi-value">`+fmtES(volAll*100,1)+`%</div><div class="kpi-sub">Anualizada (mensual √12)</div></div>
    <div class="kpi-cell"><div class="kpi-label">Máx. drawdown</div><div class="kpi-value neg">`+fmtES(maxDD*100,1)+`%</div><div class="kpi-sub">`+ddSub+`</div></div>
    <div class="kpi-cell"><div class="kpi-label">Rent. rolling 3A media</div><div class="kpi-value">`+fmtES(avgRoll3*100,1)+`%</div><div class="kpi-sub">Vol. rolling 12M media: `+fmtES(avgRollVol*100,1)+`%</div></div>
  `;

  renderRetChart('mst-evo-ret');
  renderVolChart('mst-evo-vol');

  // Growth base 100 with tooltip
  const step=Math.max(1,Math.floor(series.length/100));
  const gPts=series.filter((_,i)=>i%step===0||i===series.length-1);
  const gVals=gPts.map(p=>p.nav/series[0].nav*100);
  // Find max point for annotation
  const gMax=Math.max(...gVals);const gMaxIdx=gVals.indexOf(gMax);
  const gPointRadii=gVals.map((_,i)=>i===gMaxIdx||i===gVals.length-1?3:0);
  mk('mst-growth',{{type:'line',data:{{labels:gPts.map(p=>p.date.toISOString().slice(0,10)),datasets:[{{data:gVals,borderColor:A1(),backgroundColor:A1()+'18',borderWidth:1.5,fill:true,tension:0.2,pointRadius:gPointRadii,pointBackgroundColor:A1()}}]}},
    options:lineOpt(),
    plugins:[{{id:'gLbl',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#c8c4bc':'#0c2340';
      // Label last point and max
      const meta=chart.getDatasetMeta(0);
      [gMaxIdx,gVals.length-1].forEach(idx=>{{
        if(idx>=0&&idx<meta.data.length){{
          const pt=meta.data[idx];
          ctx.fillText(fmtES(gVals[idx],0),pt.x,pt.y-10);
        }}
      }});
    }}}}]
  }});

  // Drawdown daily with max DD annotation
  const ddStep=Math.max(1,Math.floor(ddCalc.dates.length/100));
  const ddPts=ddCalc.dates.filter((_,i)=>i%ddStep===0||i===ddCalc.dates.length-1);
  const ddV=ddCalc.dd.filter((_,i)=>i%ddStep===0||i===ddCalc.dd.length-1);
  const ddVpct=ddV.map(v=>v*100);
  const ddMin=Math.min(...ddVpct);const ddMinIdx=ddVpct.indexOf(ddMin);
  // Find 2nd worst drawdown (different trough)
  let dd2Idx=-1,dd2Val=0;
  for(let i=0;i<ddVpct.length;i++){{
    if(Math.abs(i-ddMinIdx)>5&&ddVpct[i]<dd2Val){{dd2Val=ddVpct[i];dd2Idx=i;}}
  }}
  const ddRadii=ddVpct.map((_,i)=>(i===ddMinIdx||(dd2Idx>=0&&i===dd2Idx))?3:0);
  mk('mst-dd',{{type:'line',data:{{labels:ddPts.map(d=>d.toISOString().slice(0,10)),datasets:[{{data:ddVpct,borderColor:AR(),backgroundColor:AR()+'20',borderWidth:1.5,fill:true,tension:0.2,pointRadius:ddRadii,pointBackgroundColor:AR()}}]}},
    options:lineOpt(),
    plugins:[{{id:'ddLbl',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#e08080':'#6b1a1a';
      const meta=chart.getDatasetMeta(0);
      [ddMinIdx,dd2Idx].forEach(idx=>{{
        if(idx>=0&&idx<meta.data.length){{
          const pt=meta.data[idx];
          ctx.fillText(fmtES(ddVpct[idx],1)+'%',pt.x,pt.y+14);
        }}
      }});
    }}}}]
  }});

  // Rolling (initial)
  updateRollingRet();
  updateRollingVol();
}}

function updateRollingRet(){{
  if(!MST_ME)return;
  const months=parseInt(document.getElementById('mst-roll-ret-sel').value)||36;
  const r=calcRolling(MST_ME,months);
  const step=Math.max(1,Math.floor(r.dates.length/80));
  const pts=r.dates.filter((_,i)=>i%step===0||i===r.dates.length-1);
  const vals=r.vals.filter((_,i)=>i%step===0||i===r.vals.length-1);
  const vpct=vals.map(v=>v*100);
  const maxV=Math.max(...vpct);const maxI=vpct.indexOf(maxV);
  const radii=vpct.map((_,i)=>i===maxI||i===vpct.length-1?3:0);
  mk('mst-roll-ret',{{type:'line',data:{{labels:pts.map(d=>d.toISOString().slice(0,7)),datasets:[{{data:vpct,borderColor:A2(),backgroundColor:A2()+'18',borderWidth:1.5,fill:true,tension:0.3,pointRadius:radii,pointBackgroundColor:A2()}}]}},
    options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
      plugins:{{legend:{{display:false}},tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
      scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:10}}}},y:pctAxis}}
    }},
    plugins:[{{id:'rrL',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#6aaa88':'#1a4d2e';
      const meta=chart.getDatasetMeta(0);
      [maxI,vpct.length-1].forEach(idx=>{{if(idx>=0&&idx<meta.data.length)ctx.fillText(fmtES(vpct[idx],1)+'%',meta.data[idx].x,meta.data[idx].y-10);}});
    }}}}]
  }});
}}
function updateRollingVol(){{
  if(!MST_ME)return;
  const months=parseInt(document.getElementById('mst-roll-vol-sel').value)||12;
  const r=calcRollingVol(MST_ME,months);
  const step=Math.max(1,Math.floor(r.dates.length/80));
  const pts=r.dates.filter((_,i)=>i%step===0||i===r.dates.length-1);
  const vals=r.vals.filter((_,i)=>i%step===0||i===r.vals.length-1);
  const vpct=vals.map(v=>v*100);
  const maxV=Math.max(...vpct);const maxI=vpct.indexOf(maxV);
  const radii=vpct.map((_,i)=>i===maxI||i===vpct.length-1?3:0);
  mk('mst-roll-vol',{{type:'line',data:{{labels:pts.map(d=>d.toISOString().slice(0,7)),datasets:[{{data:vpct,borderColor:A3(),borderWidth:1.5,fill:false,tension:0.3,pointRadius:radii,pointBackgroundColor:A3()}}]}},
    options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
      plugins:{{legend:{{display:false}},tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
      scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:10}}}},y:pctAxis}}
    }},
    plugins:[{{id:'rvL',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#8095ad':'#3d5a80';
      const meta=chart.getDatasetMeta(0);
      [maxI,vpct.length-1].forEach(idx=>{{if(idx>=0&&idx<meta.data.length)ctx.fillText(fmtES(vpct[idx],1)+'%',meta.data[idx].x,meta.data[idx].y-10);}});
    }}}}]
  }});
}}

// Auto-fetch on page load
document.addEventListener('DOMContentLoaded',async()=>{{
  try{{
    MST_DATA=await fetchMST();
    if(MST_DATA&&MST_DATA.length>30)renderMST(MST_DATA);
    else{{const el=document.getElementById('mst-loading');if(el)el.textContent='Datos insuficientes de Morningstar ('+((MST_DATA||[]).length)+' puntos)';}}
  }}catch(e){{
    const el=document.getElementById('mst-loading');
    if(el)el.textContent='Error cargando datos de Morningstar: '+e.message;
    console.error('MST error:',e);
  }}
}});
</script>"""


# ═══════════════════════════════════════════════════════════════
# GENERATE
# ═══════════════════════════════════════════════════════════════

def build_feedback_widget(data):
    """T3.3 + T3.10 (2026-05-28): widget de feedback humano en el dashboard
    del fondo. Incluye:
      - Botón flotante "📝 Mejorar este análisis"
      - Modal con textarea + URLs + preview editable + 2 acciones (Solo guardar / Guardar+Mejorar)
      - Panel colapsable con histórico de feedbacks (pending/applied/resolved)
    El widget se conecta al server local (API_BASE en localStorage). Si no hay
    server, muestra el botón pero el click avisa que arranque iniciar.bat.
    """
    isin = data.get("isin", "")
    fund_name = (data.get("nombre") or "").replace("'", "&#39;")
    gestora = (data.get("gestora") or "").replace("'", "&#39;")
    return f"""
<!-- T3 feedback widget (2026-05-28) -->
<style>
  .fb-fab {{
    position: fixed; top: 76px; right: 20px; z-index: 1000;
    background: #c5a25a; color: white; border: none;
    padding: 10px 16px; border-radius: 24px;
    font-family: 'Source Sans 3', sans-serif; font-size: 13px;
    font-weight: 600; cursor: pointer;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    transition: all 0.2s;
  }}
  .fb-fab:hover {{ background: #a87b1f; transform: translateY(-1px); }}
  .fb-fab .fb-badge {{
    display: inline-block; background: white; color: #c5a25a;
    border-radius: 10px; padding: 1px 7px; font-size: 11px;
    margin-left: 6px; font-weight: 700;
  }}
  .fb-modal-bg {{
    display: none; position: fixed; inset: 0; z-index: 2000;
    background: rgba(0,0,0,0.5);
    align-items: center; justify-content: center;
  }}
  .fb-modal-bg.show {{ display: flex; }}
  .fb-modal {{
    background: white; max-width: 720px; width: 92%; max-height: 88vh;
    overflow-y: auto; border-radius: 4px; padding: 24px;
    font-family: 'Source Sans 3', sans-serif;
  }}
  .fb-modal h2 {{
    font-family: 'EB Garamond', serif; font-size: 22px;
    margin: 0 0 4px 0; color: #222;
  }}
  .fb-modal .fb-sub {{ font-size: 12px; color: #666; margin-bottom: 16px; }}
  .fb-modal label {{
    display: block; text-transform: uppercase; letter-spacing: 1px;
    font-size: 10px; color: #888; font-weight: 600; margin: 12px 0 6px 0;
  }}
  .fb-modal textarea, .fb-modal input[type="text"] {{
    width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 3px;
    font-family: inherit; font-size: 13px; box-sizing: border-box;
  }}
  .fb-modal textarea {{ min-height: 140px; resize: vertical; }}
  .fb-actions {{ display: flex; gap: 10px; justify-content: flex-end; margin-top: 18px; }}
  .fb-btn {{
    padding: 9px 16px; border: 1px solid #ccc; background: white; color: #333;
    border-radius: 3px; cursor: pointer; font-family: inherit; font-size: 13px;
  }}
  .fb-btn:hover {{ background: #f5f5f5; }}
  .fb-btn-primary {{ background: #c5a25a; color: white; border-color: #c5a25a; }}
  .fb-btn-primary:hover {{ background: #a87b1f; }}
  .fb-btn-secondary {{ background: #2a3d57; color: white; border-color: #2a3d57; }}
  .fb-btn-secondary:hover {{ background: #1a2d47; }}
  .fb-btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
  .fb-preview {{
    background: #fafafa; border: 1px solid #eee; border-radius: 3px;
    padding: 12px; margin-top: 14px; font-size: 12px; color: #444;
  }}
  .fb-preview-item {{
    padding: 8px; border-bottom: 1px dashed #ddd; display: flex; gap: 8px;
    align-items: flex-start;
  }}
  .fb-preview-item:last-child {{ border-bottom: none; }}
  .fb-preview-action {{
    background: #2a3d57; color: white; padding: 2px 8px; border-radius: 3px;
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px;
    flex-shrink: 0;
  }}
  .fb-preview-conf-high {{ color: #1b8c3b; }}
  .fb-preview-conf-medium {{ color: #b8860b; }}
  .fb-preview-conf-low {{ color: #cc0033; }}
  .fb-error {{ color: #cc0033; font-size: 12px; margin-top: 8px; }}
  .fb-info {{ color: #1b8c3b; font-size: 12px; margin-top: 8px; }}
  .fb-history-section {{
    margin: 60px 20px 40px 20px; padding: 20px; background: #fafafa;
    border-radius: 4px; font-family: 'Source Sans 3', sans-serif;
  }}
  .fb-history-section h3 {{
    font-family: 'EB Garamond', serif; font-size: 20px; margin: 0 0 10px 0;
    color: #222; cursor: pointer; user-select: none;
  }}
  .fb-history-section h3::after {{ content: ' ▾'; color: #999; font-size: 14px; }}
  .fb-history-section.collapsed h3::after {{ content: ' ▸'; }}
  .fb-history-section.collapsed .fb-history-body {{ display: none; }}
  .fb-history-item {{
    padding: 12px; border-left: 3px solid #ccc; background: white;
    margin: 10px 0; border-radius: 0 3px 3px 0;
  }}
  .fb-history-item.estado-pending {{ border-left-color: #b8860b; }}
  .fb-history-item.estado-applied {{ border-left-color: #2a3d57; }}
  .fb-history-item.estado-resolved {{ border-left-color: #1b8c3b; }}
  .fb-history-item.estado-partially_resolved {{ border-left-color: #c5a25a; }}
  .fb-history-meta {{ font-size: 11px; color: #888; margin-bottom: 4px; }}
  .fb-history-text {{ font-size: 13px; color: #333; margin: 6px 0; }}
  .fb-history-items {{
    font-size: 11px; color: #666; margin-top: 6px;
    background: #f5f5f5; padding: 8px; border-radius: 3px;
  }}
  .fb-history-delete {{
    float: right; background: none; border: 1px solid #ccc; color: #888;
    padding: 1px 7px; cursor: pointer; font-size: 11px; border-radius: 3px;
  }}
  .fb-history-delete:hover {{ background: #cc0033; color: white; border-color: #cc0033; }}
</style>

<button id="fb-fab" class="fb-fab" type="button" title="Aportar feedback sobre este análisis">📝 Mejorar este análisis<span id="fb-fab-badge" class="fb-badge" style="display:none;">0</span></button>

<div id="fb-modal-bg" class="fb-modal-bg">
  <div class="fb-modal" role="dialog" aria-labelledby="fb-modal-title">
    <h2 id="fb-modal-title">📝 Mejorar análisis: {fund_name}</h2>
    <div class="fb-sub">ISIN: <code>{isin}</code>{f' · {gestora}' if gestora else ''}</div>

    <label for="fb-text">¿Qué hay que mejorar?</label>
    <textarea id="fb-text" placeholder="Describe en texto libre todo lo que ves mal o falta. Ejemplos:&#10;- El nombre del fondo es 'X', no el ISIN.&#10;- Faltan los gestores Y y Z.&#10;- La estrategia menciona value pero el fondo es claramente growth.&#10;- Mira esta entrevista: https://example.com/...&#10;&#10;Puedes mezclar varios temas en un solo feedback."></textarea>

    <label for="fb-urls">URLs/fuentes adicionales (opcional, una por línea)</label>
    <textarea id="fb-urls" style="min-height:60px;" placeholder="https://ejemplo.com/articulo&#10;https://otrafuente.com/pdf"></textarea>

    <div id="fb-error" class="fb-error" style="display:none;"></div>
    <div id="fb-info" class="fb-info" style="display:none;"></div>

    <div id="fb-preview-block" style="display:none;">
      <div style="font-size:11px;color:#666;margin-top:14px;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Items que se van a guardar (puedes editar antes):</div>
      <div id="fb-preview" class="fb-preview"></div>
    </div>

    <div class="fb-actions">
      <button id="fb-btn-cancel" class="fb-btn" type="button">Cancelar</button>
      <button id="fb-btn-preview" class="fb-btn fb-btn-primary" type="button">Analizar texto →</button>
      <button id="fb-btn-save" class="fb-btn fb-btn-primary" type="button" style="display:none;">Solo guardar</button>
      <button id="fb-btn-save-improve" class="fb-btn fb-btn-secondary" type="button" style="display:none;">Guardar + Mejorar ahora</button>
    </div>
  </div>
</div>

<div id="fb-history" class="fb-history-section collapsed" style="display:none;">
  <h3 id="fb-history-title">Histórico de feedback (0)</h3>
  <div class="fb-history-body" id="fb-history-body"></div>
</div>

<script>
(function() {{
  const ISIN = '{isin}';
  const FUND_NAME = '{fund_name}';
  const GESTORA = '{gestora}';
  // API_BASE: localStorage o relativo. Igual que catalog.html.
  let API_BASE = localStorage.getItem('fa_api_base') || '';
  function fbFetch(path, opts) {{ return fetch(API_BASE + path, opts); }}

  const fab = document.getElementById('fb-fab');
  const modalBg = document.getElementById('fb-modal-bg');
  const txt = document.getElementById('fb-text');
  const urlsInput = document.getElementById('fb-urls');
  const errEl = document.getElementById('fb-error');
  const infoEl = document.getElementById('fb-info');
  const prevBlock = document.getElementById('fb-preview-block');
  const prevEl = document.getElementById('fb-preview');
  const btnPreview = document.getElementById('fb-btn-preview');
  const btnSave = document.getElementById('fb-btn-save');
  const btnSaveImprove = document.getElementById('fb-btn-save-improve');
  const btnCancel = document.getElementById('fb-btn-cancel');
  const historySection = document.getElementById('fb-history');
  const historyTitle = document.getElementById('fb-history-title');
  const historyBody = document.getElementById('fb-history-body');
  const fabBadge = document.getElementById('fb-fab-badge');

  let currentItems = null;  // items estructurados tras /parse

  fab.addEventListener('click', () => {{
    if (!API_BASE) {{
      alert('No hay server local conectado. Arranca iniciar.bat y refresca el catalog primero.');
      return;
    }}
    resetModal();
    modalBg.classList.add('show');
    setTimeout(() => txt.focus(), 50);
  }});
  btnCancel.addEventListener('click', () => modalBg.classList.remove('show'));
  modalBg.addEventListener('click', (e) => {{ if (e.target === modalBg) modalBg.classList.remove('show'); }});

  function resetModal() {{
    txt.value = ''; urlsInput.value = '';
    errEl.style.display = 'none'; infoEl.style.display = 'none';
    prevBlock.style.display = 'none';
    btnPreview.style.display = 'inline-block';
    btnSave.style.display = 'none';
    btnSaveImprove.style.display = 'none';
    currentItems = null;
  }}

  function showError(msg) {{ errEl.textContent = msg; errEl.style.display = 'block'; }}
  function showInfo(msg) {{ infoEl.textContent = msg; infoEl.style.display = 'block'; }}

  function parseUrlsLines(s) {{
    return (s || '').split(/[\\n,;]+/).map(t => t.trim()).filter(Boolean);
  }}

  function renderPreview(items) {{
    if (!items || !items.length) {{
      prevEl.innerHTML = '<div style="padding:10px;color:#888;">(El parser no extrajo items. Comprueba el texto o guarda como feedback "genérico".)</div>';
      return;
    }}
    prevEl.innerHTML = items.map((it, idx) => {{
      const target = it.target_path || (it.target_section ? 'sección: ' + it.target_section : '(global)');
      const valStr = it.value === null || it.value === undefined ? '' :
                     (typeof it.value === 'object' ? JSON.stringify(it.value) : String(it.value));
      const urls = (it.source_urls || []).join(', ');
      return `<div class="fb-preview-item" data-idx="${{idx}}">
        <span class="fb-preview-action">${{it.action}}</span>
        <div style="flex:1;">
          <div><strong>${{target}}</strong>${{valStr ? ' → <code>' + valStr.substring(0,80) + '</code>' : ''}}</div>
          <div style="font-size:11px;color:#777;margin-top:3px;">${{it.rationale || ''}}</div>
          ${{urls ? `<div style="font-size:10px;color:#999;margin-top:2px;">${{urls}}</div>` : ''}}
          <div style="font-size:10px;margin-top:2px;" class="fb-preview-conf-${{it.confidence||'medium'}}">Confianza: ${{it.confidence||'medium'}}</div>
        </div>
        <button type="button" class="fb-btn" style="padding:2px 8px;font-size:11px;" data-rm-preview="${{idx}}" title="Quitar este item">✕</button>
      </div>`;
    }}).join('');
    // bind remove
    prevEl.querySelectorAll('button[data-rm-preview]').forEach(b => {{
      b.addEventListener('click', () => {{
        const i = parseInt(b.dataset.rmPreview);
        currentItems.splice(i, 1);
        renderPreview(currentItems);
        if (currentItems.length === 0) {{
          btnSave.style.display = 'none';
          btnSaveImprove.style.display = 'none';
        }}
      }});
    }});
  }}

  btnPreview.addEventListener('click', async () => {{
    errEl.style.display = 'none'; infoEl.style.display = 'none';
    const rawText = txt.value.trim();
    const rawUrls = parseUrlsLines(urlsInput.value);
    if (!rawText && rawUrls.length === 0) {{
      showError('Escribe algo o pega URLs.');
      return;
    }}
    btnPreview.disabled = true;
    btnPreview.textContent = 'Analizando…';
    try {{
      const res = await fbFetch(`/api/feedback/${{encodeURIComponent(ISIN)}}/parse`, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{
          raw_text: rawText, raw_urls: rawUrls,
          fund_name: FUND_NAME, gestora: GESTORA,
        }}),
      }});
      const data = await res.json();
      if (!res.ok) {{ showError(data.error || 'Error en parse'); return; }}
      currentItems = data.structured_items || [];
      renderPreview(currentItems);
      prevBlock.style.display = 'block';
      const method = data.parse_meta?.method || 'unknown';
      const note = method === 'haiku' ? ' (Haiku)' : method === 'fallback' ? ' (parser básico — Haiku no disponible)' : '';
      showInfo(`${{currentItems.length}} item(s) extraído(s)${{note}}. Revisa, elimina los que no quieras y guarda.`);
      btnPreview.style.display = 'none';
      btnSave.style.display = currentItems.length > 0 ? 'inline-block' : 'none';
      btnSaveImprove.style.display = currentItems.length > 0 ? 'inline-block' : 'none';
    }} catch (err) {{
      showError('Error de red: ' + err.message);
    }} finally {{
      btnPreview.disabled = false;
      btnPreview.textContent = 'Analizar texto →';
    }}
  }});

  async function saveAndMaybeImprove(triggerImprove) {{
    if (!currentItems || currentItems.length === 0) {{
      showError('No hay items que guardar.');
      return;
    }}
    btnSave.disabled = true; btnSaveImprove.disabled = true;
    try {{
      const res = await fbFetch(`/api/feedback/${{encodeURIComponent(ISIN)}}`, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{
          raw_text: txt.value.trim(),
          raw_urls: parseUrlsLines(urlsInput.value),
          structured_items: currentItems,
          fund_name: FUND_NAME,
        }}),
      }});
      const data = await res.json();
      if (!res.ok || !data.ok) {{ showError(data.error || 'Error guardando'); return; }}
      showInfo(`✓ Feedback guardado.${{triggerImprove ? ' Lanzando re-run…' : ''}}`);
      if (triggerImprove) {{
        const res2 = await fbFetch('/api/analyze-batch', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            isins: [ISIN], cold_start: false, apply_feedback: true,
          }}),
        }});
        const d2 = await res2.json();
        if (!res2.ok) {{ showError('Encolado falló: ' + (d2.error || res2.status)); return; }}
        showInfo(`✓ Feedback guardado. Re-run encolado. Cierra el modal y verás el progreso en la cola del catalog.`);
      }}
      // Tras 1.5s cerrar
      setTimeout(() => {{ modalBg.classList.remove('show'); loadHistory(); }}, 1500);
    }} catch (err) {{
      showError('Error de red: ' + err.message);
    }} finally {{
      btnSave.disabled = false; btnSaveImprove.disabled = false;
    }}
  }}
  btnSave.addEventListener('click', () => saveAndMaybeImprove(false));
  btnSaveImprove.addEventListener('click', () => saveAndMaybeImprove(true));

  // ───────── Histórico ─────────
  async function loadHistory() {{
    if (!API_BASE) return;
    try {{
      const res = await fbFetch(`/api/feedback/${{encodeURIComponent(ISIN)}}`, {{ cache: 'no-cache' }});
      if (!res.ok) return;
      const data = await res.json();
      const feedbacks = data.feedbacks || [];
      const nPending = feedbacks.filter(f => f.estado === 'pending').length;
      if (nPending > 0) {{
        fabBadge.textContent = nPending;
        fabBadge.style.display = 'inline-block';
      }} else {{
        fabBadge.style.display = 'none';
      }}
      historyTitle.textContent = `Histórico de feedback (${{feedbacks.length}})`;
      if (feedbacks.length === 0) {{ historySection.style.display = 'none'; return; }}
      historySection.style.display = 'block';
      historyBody.innerHTML = feedbacks.slice().reverse().map(fb => {{
        // T3.X (2026-05-28): mostrar resultado per item (applied + reason)
        // + verify (resolved + verify_reason). Si el item no se resolvió,
        // el usuario ve POR QUÉ.
        const itemResults = fb.item_results || [];
        const resolvedSet = new Set(fb.resolved_items || []);
        const items = (fb.structured_items || []).map((it, idx) => {{
          const target = it.target_path || it.target_section || '(global)';
          const result = itemResults[idx] || {{}};
          // Calcular estado visual
          let badge = '<span style="color:#888;">⏳ pendiente</span>';
          let detail = '';
          if (resolvedSet.has(idx) || result.resolved === true) {{
            badge = '<span style="color:#1b8c3b;">✓ resuelto</span>';
            if (result.verify_reason) detail = `<div style="font-size:11px;color:#555;margin-top:3px;">${{result.verify_reason}}</div>`;
          }} else if (result.applied === false) {{
            badge = '<span style="color:#cc0033;">✗ no se aplicó</span>';
            detail = `<div style="font-size:11px;color:#cc0033;margin-top:3px;">${{result.reason || 'sin razón registrada'}}</div>`;
          }} else if (result.applied === true && result.resolved === false) {{
            badge = '<span style="color:#b8860b;">⚠ aplicado pero no resuelto</span>';
            const r1 = result.reason ? `<div style="font-size:11px;color:#555;margin-top:3px;">Aplicado: ${{result.reason}}</div>` : '';
            const r2 = result.verify_reason ? `<div style="font-size:11px;color:#b8860b;margin-top:3px;">Verify: ${{result.verify_reason}}</div>` : '';
            detail = r1 + r2;
          }} else if (result.applied === true) {{
            badge = '<span style="color:#1b8c3b;">✓ aplicado</span>';
            if (result.reason) detail = `<div style="font-size:11px;color:#555;margin-top:3px;">${{result.reason}}</div>`;
          }}
          return `<div style="padding:8px 0;border-bottom:1px dashed #ddd;">
            <div>· <code>${{it.action}}</code> <strong>${{target}}</strong>${{it.value !== null && it.value !== undefined ? ' → ' + JSON.stringify(it.value).substring(0,60) : ''}}</div>
            <div style="margin-top:3px;">${{badge}}</div>
            ${{detail}}
          </div>`;
        }}).join('');
        const ts = new Date(fb.created_at).toLocaleString('es-ES');
        const canDelete = fb.estado === 'pending';
        // M6 (2026-06-06): banner si la skill analyst-cowork no se pronunció
        // sobre el feedback (status none/partial). Distingue «skill no cooperó»
        // de «no se pudo mejorar».
        const diag = fb.skill_diagnostic || {{}};
        let diagBanner = '';
        if (diag.status === 'none' || diag.status === 'partial') {{
          const dc = diag.status === 'none' ? '#cc0033' : '#b8860b';
          diagBanner = `<div style="margin-top:6px;padding:6px 8px;border-left:3px solid ${{dc}};background:#fff8f0;font-size:11px;color:${{dc}};">⚠ ${{diag.message || 'El analyst no se pronunció sobre el feedback.'}}</div>`;
        }}
        return `<div class="fb-history-item estado-${{fb.estado}}">
          ${{canDelete ? `<button class="fb-history-delete" data-del-fb="${{fb.id}}">🗑 borrar</button>` : ''}}
          <div class="fb-history-meta">${{ts}} · estado: <strong>${{fb.estado}}</strong>${{fb.run_id_applied ? ' · run: ' + fb.run_id_applied : ''}}</div>
          <div class="fb-history-text">${{(fb.raw_text || '').substring(0,400)}}</div>
          ${{diagBanner}}
          <div class="fb-history-items">${{items}}</div>
        </div>`;
      }}).join('');
      historyBody.querySelectorAll('button[data-del-fb]').forEach(b => {{
        b.addEventListener('click', async () => {{
          if (!confirm('Borrar este feedback? Solo se borran los pending.')) return;
          const fbId = b.dataset.delFb;
          const r = await fbFetch(`/api/feedback/${{encodeURIComponent(ISIN)}}/${{encodeURIComponent(fbId)}}`, {{ method: 'DELETE' }});
          if (r.ok) loadHistory();
        }});
      }});
    }} catch (e) {{ /* sin server, silencioso */ }}
  }}

  historyTitle.addEventListener('click', () => historySection.classList.toggle('collapsed'));

  // Cargar al inicio si hay API_BASE
  if (API_BASE) loadHistory();
}})();
</script>
"""


def generate():
    data = load_data()
    # Enrichment pass: auto-download KID si falta + log sugerencias de agentes
    try:
        enr = _hdr_enrich_if_missing(data)
        if enr.get("enriched"):
            print("[+] Enriquecimiento aplicado:")
            for e in enr["enriched"]:
                print(f"  + {e}")
        if enr.get("suggestions"):
            print("[!] Sugerencias de enriquecimiento (correr agente):")
            for s in enr["suggestions"]:
                print(f"  → {s}")
    except Exception as _e:
        pass
    html = f"""<!DOCTYPE html>
<html lang="es" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{data.get('nombre', 'Fondo')} — Informe Analítico</title>
<link href="https://fonts.googleapis.com/css2?family=EB+Garamond:ital,wght@0,400;0,500;0,600;1,400&family=Source+Sans+3:wght@300;400;500;600&family=Source+Code+Pro:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
{CSS}
</head>
<body>
{build_header(data)}
<main class="body">
{build_tab_resumen(data)}
{build_tab_historia(data)}
{build_tab_gestores(data)}
{build_tab_evolucion(data)}
{build_tab_estrategia(data)}
{build_tab_cartera(data)}
{build_tab_fuentes(data)}
{build_tab_documentos(data)}
{build_tab_chat(data)}
</main>
{build_feedback_widget(data)}
{build_scripts(data)}
</body>
</html>"""
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Generated: {OUTPUT} ({len(html):,} chars)")

    # Audit Opus — imprimir SIEMPRE en terminal (no en dashboard)
    audit = data.get("opus_audit", {})
    if audit and audit.get("auditado"):
        globl = audit.get("global", {})
        rec = globl.get("recomendacion", "")
        score = globl.get("calidad_score", "?")
        just = globl.get("calidad_justificacion", "")
        print(f"\n[AUDIT OPUS] {rec} · Calidad {score}/10")
        if just:
            print(f"  {just[:400]}")
        sections_audit = audit.get("sections", {})
        probl = {k: v for k, v in sections_audit.items()
                 if v.get("status") in ("REVISAR", "RECHAZADO")}
        for sname, saudit in probl.items():
            issues = saudit.get("issues", [])
            if issues:
                print(f"  · [{sname}]: {'; '.join(issues[:2])[:200]}")

    # Quality report — detect missing data
    synth = data.get("analyst_synthesis", {})
    issues = []
    for section in ["resumen", "historia", "gestores", "evolucion", "estrategia", "cartera", "fuentes_externas"]:
        sec = synth.get(section, {})
        if not sec:
            issues.append(f"CRITICO: Sin {section}. Ejecutar analyst_agent.")
        elif not sec.get("texto"):
            issues.append(f"MEJORA: {section} sin texto narrativo.")
    if not data.get("posiciones", {}).get("actuales"):
        issues.append("CRITICO: Sin posiciones actuales. Verificar cnmv_agent.")
    if len(data.get("cuantitativo", {}).get("serie_aum", [])) < 3:
        issues.append("MEJORA: Serie AUM corta (<3 puntos).")
    # Equipo gestor: admitir cualquiera de las fuentes canonicas
    # (ES usa gestores.equipo, INT usa _int_gestores / cualitativo.gestores /
    # analyst_synthesis.gestores.equipo). Solo flagear si NINGUNA tiene datos.
    has_team = any([
        data.get("gestores", {}).get("equipo"),
        data.get("_int_gestores"),
        data.get("cualitativo", {}).get("gestores"),
        synth.get("gestores", {}).get("equipo"),
        synth.get("gestores", {}).get("perfiles"),
    ])
    if not has_team:
        issues.append("MEJORA: Sin equipo gestor identificado. Ejecutar manager_deep_agent.")

    if issues:
        print(f"\n[!] Informe de calidad ({len(issues)} items):")
        for issue in issues:
            print(f"  · {issue}")
    else:
        print("OK: Todos los datos completos.")


if __name__ == "__main__":
    generate()
