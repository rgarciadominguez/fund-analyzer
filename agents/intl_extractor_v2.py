"""
IntlExtractor v3 — orquestador concept-first.

Pipeline de 2 stages:
  1. ConceptMapper (Gemini Pro, 1 call/doc): mapa semántico de dónde vive
     cada concepto financiero en el documento.
  2. ConceptExtractor (Gemini Flash, N calls/doc): extrae cada concepto
     con la descripción conceptual + páginas pre-filtradas.

El orquestador toma los concepts extraídos de todos los documentos
(annual report, factsheets, cartas, etc.), los funde en el schema universal
de `schemas/fund_output.json`, y guarda `data/funds/{ISIN}/intl_data.json`.

Filosofía: zero conocimiento del dominio fuera de `agents/concepts.TAXONOMY`.
Zero referencias a gestoras o estructuras concretas (grep-check en tests).

API pública:
    class IntlExtractor:
        def __init__(self, isin: str, config: dict | None = None)
        async def run(self) -> dict

    IntlAgent = IntlExtractor  # alias retro-compatibilidad
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console

from agents.concept_extractor import extract_all
from agents.concept_mapper import map_document

console = Console()


# ══════════════════════════════════════════════════════════════════════════
# Fix-5: PDF classifier para modo cowork
# ══════════════════════════════════════════════════════════════════════════
# En modo cowork emitimos UNA task por PDF en raw/discovery/ (no agrupado por
# doc_type del discovery, que clasifica casi todo como annual_report). El
# classifier asigna task_type + schema según el nombre del fichero, descartando
# corporates ESG/stewardship que no aporten datos del sub-fondo target.

FACTSHEET_SCHEMA = {
    "kpis": {
        "aum_actual_meur": "float - AUM TOTAL del sub-fondo target en M€ (NO de la gestora paraguas; OJO 'fund AuM' vs 'firm AuM')",
        "fecha_aum": "string YYYY-MM-DD del corte",
        "ter_pct": "float",
        "num_participes": "int",
        "divisa_base": "string EUR/USD/GBP",
    },
    "posiciones_actuales": "list[{nombre, peso_pct, sector, pais}] top 10",
    "asset_allocation": "{equity_pct, bonds_pct, cash_pct, otros_pct}",
    "geographic_allocation": "list[{region, peso_pct}]",
    "sector_allocation": "list[{sector, peso_pct}]",
    "performance": "{ytd_pct, y1_pct, y3_anual_pct, y5_anual_pct, since_inception_anual_pct}",
    "comentario_gestor": "string si el factsheet incluye block de commentary",
}

COMMENTARY_SCHEMA = {
    "periodo": "string '2024_T3' o 'monthly_2025_03'",
    "tesis_gestora": "string 200-400 palabras",
    "decisiones_tomadas": "string 100-300 palabras con nombres concretos",
    "contexto_mercado": "string 100-200 palabras",
    "citas_textuales": "list[{cita, contexto}] 2-5",
    "outlook": "string",
    "posiciones_destacadas": "list[{nombre, accion, motivo}]",
}

KID_SCHEMA = {
    "kpis": {"divisa_base": "string", "ter_pct": "float"},
    "perfil_riesgo_srri": "int 1-7",
    "objetivo_inversion": "string",
}

PROSPECTUS_SCHEMA = {
    "objetivo_inversion": "string completo",
    "politica_inversion": "string",
    "comisiones": "{gestion_max_pct, deposito_max_pct, exito_pct}",
    "clases": "list[{isin, nombre_clase, divisa, comision_gestion_pct}]",
}

AR_SUBFUND_SCHEMA = {
    "kpis": {"aum_actual_meur": "float", "ter_pct": "float", "num_participes": "int"},
    "posiciones": "list[{nombre, peso_pct, sector, pais}] holdings completos del sub-fondo",
    "cualitativo": {
        "estrategia": "string",
        "decisiones_periodo": "string",
        "contexto_mercado": "string",
        "outlook": "string",
    },
}

_ESG_CORPORATE_KEYWORDS = (
    "stewardship", "esg-report", "esgreport", "engagement", "engagementrep",
    "transition-report", "sfdr-report", "sfdr_report", "global-survey",
    "retirement-index", "appointed",  # press releases / corporate news
    # 2026-06-10: ruido corporativo/temático/marketing que se colaba como
    # generic_pdf en el extractor (consistente con discovery skip_corporate).
    "market-monitor", "market_monitor", "monthly-market", "market-outlook",
    "integrated-annual", "annual-impact", "impact-report", "impact%20report",
    "responsible-investment", "climate-report", "carbon-report", "3d-investing",
    "voting-report", "proxy-voting", "country-esg",
)

_FACTSHEET_KEYWORDS = (
    "fact", "factsheet", "monthly-report", "perfprof", "professional",
    "fact-", "_fact_", "-fact-", "ficha-", "fichainformativa",
)

_COMMENTARY_KEYWORDS = (
    "commentary", "outlook", "monthly-update", "quarterly-update",
    "carta-trimestral", "carta-mensual", "informe-mensual",
    "informe-trimestral", "letter-to-investors", "investor-letter",
)

_KID_KEYWORDS = ("kid", "kiid", "prip", "dfi", "_dic_", "-dic-")
_PROSPECTUS_KEYWORDS = ("prospectus", "folleto", "prospecto")
_AR_KEYWORDS = (
    "annual-report", "annualreport", "semi-annual", "semiannual",
    "informe-anual", "informe-semestral",
)

# Tokens genéricos de nombres de fondo que NO sirven para confirmar identidad
# (aparecen en miles de fondos). El match por nombre exige tokens distintivos.
_GENERIC_NAME_TOKENS = frozenset({
    "fund", "funds", "fondo", "fondos", "class", "clase", "sicav", "ucits",
    "global", "value", "equity", "equities", "bond", "bonds", "capital",
    "invest", "investment", "investments", "investing", "sub", "fund's",
    "growth", "income", "europe", "european", "world", "international",
    "select", "selection", "plus", "trust", "asset", "management",
})


def _classify_pdf_for_task(pdf_path: Path, isin: str, fund_name: str = "") -> tuple[str, dict] | None:
    """Clasifica un PDF de raw/discovery/ por nombre y devuelve (task_type, schema).

    Devuelve None si el PDF debe descartarse (corporate ESG/stewardship que no
    menciona el ISIN target en sus primeras páginas).
    """
    name_lc = pdf_path.name.lower()

    # Skip corporativo/temático/ESG: estos tipos NUNCA son las cuentas/factsheet/
    # carta del fondo, aunque mencionen el ISIN (Robeco etiqueta sus market
    # monitors con el fondo). Skip INCONDICIONAL (2026-06-10, consistente con
    # discovery skip_corporate).
    if any(k in name_lc for k in _ESG_CORPORATE_KEYWORDS):
        return None

    # Factsheet del sub-fondo (clave para AUM + posiciones actuales)
    if any(k in name_lc for k in _FACTSHEET_KEYWORDS):
        return ("factsheet", FACTSHEET_SCHEMA)

    # Manager letter / commentary (cartas distintas a las de letters_collector)
    if any(k in name_lc for k in _COMMENTARY_KEYWORDS):
        return ("manager_letter", COMMENTARY_SCHEMA)

    # KID/KIID
    if any(k in name_lc for k in _KID_KEYWORDS):
        return ("kid", KID_SCHEMA)

    # Prospectus
    if any(k in name_lc for k in _PROSPECTUS_KEYWORDS):
        return ("prospectus", PROSPECTUS_SCHEMA)

    # Annual / semi-annual report — verificar si es del sub-fondo o corporate
    if any(k in name_lc for k in _AR_KEYWORDS):
        try:
            from tools.pdf_extractor import extract_page_range
            # B-AUM (2026-06-04): el ISIN en annual reports suele estar en notas/
            # estadísticas (pág 40+), no en cabecera. Escaneamos 0-12 (antes 0-5)
            # y, si el ISIN no aparece, aceptamos el AR cuando el NOMBRE del fondo
            # coincide en cabecera (caso SICAV mono-fondo, p.ej. Sifter Fund) —
            # antes se descartaba entero y el fondo quedaba sin AUM.
            head_lc = extract_page_range(str(pdf_path), 0, 12)[:16000].lower()
            _is_semi = "semi" in name_lc
            _ttype = "semi_annual_subfund" if _is_semi else "annual_subfund"
            if isin.lower() in head_lc:
                return (_ttype, AR_SUBFUND_SCHEMA)
            tokens = [
                t for t in re.split(r"[^a-z0-9]+", (fund_name or "").lower())
                if len(t) >= 4 and t not in _GENERIC_NAME_TOKENS
            ]
            if tokens and sum(1 for t in tokens if t in head_lc) >= 1:
                # El AR menciona el nombre del fondo target → emitir tarea. El
                # skill extract-pdfs-cowork extrae SOLO el sub-fondo (anti-
                # invención + warnings umbrella en el contexto de la tarea).
                return (_ttype, AR_SUBFUND_SCHEMA)
            # Ni ISIN ni nombre del fondo → AR corporate de otra entidad. Descartar.
            return None
        except Exception:
            return None

    # PDF desconocido — schema genérico (factsheet-like)
    return ("generic_pdf", FACTSHEET_SCHEMA)


# ══════════════════════════════════════════════════════════════════════════
# FX rates — usados solo como fallback cuando el doc no trae su tabla FX
# ══════════════════════════════════════════════════════════════════════════

_FX_TO_EUR_FALLBACK = {
    "EUR": 1.0, "GBP": 1.18, "USD": 0.92, "CHF": 1.04, "JPY": 0.0061,
    "DKK": 0.134, "SEK": 0.087, "NOK": 0.085, "SGD": 0.68, "AUD": 0.61,
    "CAD": 0.68, "HKD": 0.12,
}


def _fx_to_eur(amount: float, from_curr: str, year: str = "", fx_table: dict | None = None) -> float:
    """
    Convierte `amount` (en `from_curr`) a EUR.
    fx_table es un dict {anio: {currency: rate}} opcional con FX históricos.
    """
    from_curr = (from_curr or "EUR").upper()
    if from_curr == "EUR":
        return amount
    if fx_table and year and year in fx_table and from_curr in fx_table[year]:
        # rate = functional_currency → curr. Depende de dirección.
        # Simplificación: usar fallback para no complicar con direcciones.
        pass
    return amount * _FX_TO_EUR_FALLBACK.get(from_curr, 1.0)


# ══════════════════════════════════════════════════════════════════════════
# EMPTY OUTPUT (schema universal)
# ══════════════════════════════════════════════════════════════════════════

def _empty_output(isin: str, nombre: str, gestora: str) -> dict:
    return {
        "isin": isin,
        "nombre": nombre,
        "gestora": gestora,
        "tipo": "INT",
        "ultima_actualizacion": datetime.now().isoformat(timespec="seconds"),
        "kpis": {
            "anio_creacion": None, "clasificacion": "", "benchmark": "",
            "rating_morningstar": None, "aum_actual_meur": None,
            "num_participes": None, "num_activos_cartera": None,
            "concentracion_top10_pct": None, "ter_pct": None, "coste_gestion_pct": None,
        },
        "cualitativo": {
            "estrategia": "", "historia_fondo": "", "gestores": [],
            "tipo_activos": "", "filosofia_inversion": "", "objetivos_reales": "",
            "proceso_seleccion": "",
        },
        "cuantitativo": {
            "serie_aum": [], "serie_participes": [], "serie_ter": [],
            "serie_rentabilidad": [], "mix_activos_historico": [],
            "mix_geografico_historico": [],
        },
        "analisis_consistencia": {"periodos": [], "resumen_global": ""},
        "posiciones": {"actuales": [], "historicas": []},
        "clases": [],
        "economia_fondo": {
            "management_fees_total": [], "net_result": [],
            "expense_ratio_breakdown": None, "viabilidad_nota": "",
        },
        "fuentes": {
            "informes_descargados": [], "cartas_gestores": [],
            "urls_consultadas": [], "xmls_cnmv": [],
        },
    }


# ══════════════════════════════════════════════════════════════════════════
# NORMALIZADORES: concept value → schema output
# ══════════════════════════════════════════════════════════════════════════

def _safe_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        # Quitar comas, espacios, símbolos de divisa, "%"
        s = v.strip().replace(",", "").replace("€", "").replace("$", "").replace("£", "")
        s = s.replace("%", "").strip()
        try:
            return float(s)
        except ValueError:
            # Formatos como "Up to 1.50%": extraer primer número
            m = re.search(r"(-?\d+(?:\.\d+)?)", s)
            if m:
                return float(m.group(1))
            return None
    return None


def _safe_int(v: Any) -> int | None:
    f = _safe_float(v)
    return int(f) if f is not None else None


def _value_of(concept_result: Any) -> Any:
    """Extrae el campo `value` de un resultado del extractor; tolera forma plana."""
    if concept_result is None:
        return None
    if isinstance(concept_result, dict):
        return concept_result.get("value", concept_result)
    return concept_result


# ══════════════════════════════════════════════════════════════════════════
# MERGE concept outputs → fund_output schema
# ══════════════════════════════════════════════════════════════════════════


def _merge_target_fund_identity(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    name = v.get("display_name") or ""
    if name and not out["nombre"]:
        out["nombre"] = name
    # inception → anio_creacion
    inception = v.get("inception_date") or ""
    if inception:
        m = re.search(r"(19|20)\d{2}", str(inception))
        if m and out["kpis"]["anio_creacion"] is None:
            out["kpis"]["anio_creacion"] = int(m.group(0))


def _merge_fund_size_history(out: dict, v: Any, fx_table: dict | None = None) -> None:
    if not isinstance(v, dict):
        return
    snapshots = v.get("snapshots") or []
    if not isinstance(snapshots, list):
        return
    # Agrupar snapshots por fecha: si múltiples por fecha (una por divisa),
    # sumar todas las convertidas a EUR → total del fondo
    by_date: dict[str, float] = {}
    for s in snapshots:
        if not isinstance(s, dict):
            continue
        date = str(s.get("date", ""))[:10]
        val = _safe_float(s.get("value"))
        curr = (s.get("currency") or "EUR").upper()
        if not date or val is None:
            continue
        by_date.setdefault(date, 0.0)
        by_date[date] += _fx_to_eur(val, curr, year=date[:4], fx_table=fx_table)
    # Fase E (2026-04-27): filtrar entries con periodo invalido. Si date no es
    # YYYY-MM-DD valido, year sera "None"/"" y contamina la serie.
    import re as _re_aum2
    for date, total_eur in by_date.items():
        year = date[:4] if date and len(date) >= 4 else ""
        if not _re_aum2.match(r"^\d{4}$", year):
            continue  # skip entries con periodo invalido
        meur = round(total_eur / 1e6, 2) if total_eur >= 1e6 else round(total_eur, 2)
        entry = {"periodo": year, "valor_meur": meur}
        # upsert por periodo
        existing_idx = None
        for i, e in enumerate(out["cuantitativo"]["serie_aum"]):
            if e.get("periodo") == year:
                existing_idx = i
                break
        if existing_idx is None:
            out["cuantitativo"]["serie_aum"].append(entry)
        else:
            # Sumar (divisas distintas de la misma fecha) o reemplazar (mismo año distinta fuente)
            # Simplificación: si suma es > existente, reemplazar
            if meur > out["cuantitativo"]["serie_aum"][existing_idx]["valor_meur"]:
                out["cuantitativo"]["serie_aum"][existing_idx]["valor_meur"] = meur
    # AUM actual = máximo año (filtrar periodos validos antes del max)
    valid_aum = [
        e for e in out["cuantitativo"]["serie_aum"]
        if isinstance(e.get("periodo"), str) and _re_aum2.match(r"^\d{4}$", e["periodo"])
    ]
    if valid_aum:
        latest = max(valid_aum, key=lambda e: int(e["periodo"]))
        out["kpis"]["aum_actual_meur"] = latest.get("valor_meur")


def _merge_share_classes(out: dict, v: Any, fx_table: dict | None = None) -> None:
    if not isinstance(v, dict):
        return
    classes = v.get("classes") or []
    if not isinstance(classes, list):
        return
    # Filtrar a EUR preferente, USD fallback
    eur_classes = [c for c in classes if (c.get("currency") or "").upper() == "EUR"]
    if eur_classes:
        out["clases"] = eur_classes
    else:
        usd = [c for c in classes if (c.get("currency") or "").upper() == "USD"]
        out["clases"] = usd[:10]  # máx 10
    # Inception del fondo = mínima fecha de creación entre TODAS las clases
    # (incluido USD/GBP/CHF — la clase más antigua suele ser Institutional en
    # USD/GBP, mientras la R EUR es retail más reciente). NO se filtra por
    # divisa para esto.
    import re as _re_inc
    all_inceptions = []
    for c in classes:
        if not isinstance(c, dict):
            continue
        for fld in ("inception_date", "launch_date", "fecha_lanzamiento", "first_nav_date"):
            v_inc = c.get(fld)
            if v_inc and isinstance(v_inc, str):
                m = _re_inc.match(r"^(\d{4})-(\d{2})", v_inc.strip())
                if m:
                    all_inceptions.append(f"{m.group(1)}-{m.group(2)}")
    if all_inceptions:
        oldest = min(all_inceptions) + "-01"
        # Solo escribir si no hay valor o el nuevo es anterior
        prev = out["kpis"].get("fecha_lanzamiento") or ""
        if not prev or oldest < str(prev):
            out["kpis"]["fecha_lanzamiento"] = oldest
            yr_oldest = int(oldest[:4])
            cur_yr = out["kpis"].get("anio_creacion")
            if cur_yr is None or yr_oldest < int(cur_yr):
                out["kpis"]["anio_creacion"] = yr_oldest
    # Computar nav_total si falta (shares × pps)
    for c in out["clases"]:
        for snap in c.get("nav_total_snapshots") or []:
            nav = _safe_float(snap.get("nav_total"))
            shares = _safe_float(snap.get("shares_outstanding"))
            pps = _safe_float(snap.get("nav_per_share"))
            if nav is None and shares and pps:
                snap["nav_total"] = round(shares * pps, 2)
    # AUM alternativo: suma de clases (todas las divisas) convertidas a EUR
    # por fecha — se actualiza serie_aum si aporta años nuevos
    if classes:
        by_date: dict[str, float] = {}
        for c in classes:
            curr = (c.get("currency") or "EUR").upper()
            for snap in c.get("nav_total_snapshots") or []:
                date = str(snap.get("date", ""))[:10]
                nav = _safe_float(snap.get("nav_total"))
                if nav is None:
                    shares = _safe_float(snap.get("shares_outstanding"))
                    pps = _safe_float(snap.get("nav_per_share"))
                    if shares and pps:
                        nav = shares * pps
                if not date or nav is None:
                    continue
                by_date.setdefault(date, 0.0)
                by_date[date] += _fx_to_eur(nav, curr, year=date[:4], fx_table=fx_table)
        # Fase E (2026-04-27): filtrar entries con periodo invalido ANTES de
        # añadir a serie_aum. Si date no es YYYY-MM-DD valido, year sera "None"
        # u otra basura → contamina la serie y rompe el max() de abajo.
        import re as _re_aum
        for date, total_eur in by_date.items():
            year = date[:4] if date and len(date) >= 4 else ""
            if not _re_aum.match(r"^\d{4}$", year):
                continue  # skip entries con periodo invalido
            meur = round(total_eur / 1e6, 2)
            existing_idx = None
            for i, e in enumerate(out["cuantitativo"]["serie_aum"]):
                if e.get("periodo") == year:
                    existing_idx = i
                    break
            if existing_idx is None:
                out["cuantitativo"]["serie_aum"].append({"periodo": year, "valor_meur": meur})
            elif abs(meur - out["cuantitativo"]["serie_aum"][existing_idx]["valor_meur"]) > 0.01:
                # Si la suma de clases difiere del `fund_size_history` mergeado antes,
                # preferir la suma de clases (más autoritativo para umbrella SICAVs)
                out["cuantitativo"]["serie_aum"][existing_idx]["valor_meur"] = meur
        # Fase E: filtrar periodos validos antes del max (evita "None" > "2025"
        # en comparacion lexicografica → bug DNCA AUM €41B).
        valid_aum = [
            e for e in out["cuantitativo"]["serie_aum"]
            if isinstance(e.get("periodo"), str) and _re_aum.match(r"^\d{4}$", e["periodo"])
        ]
        if valid_aum:
            latest = max(valid_aum, key=lambda e: int(e["periodo"]))
            out["kpis"]["aum_actual_meur"] = latest.get("valor_meur")


def sanitize_serie_aum(out: dict) -> int:
    """B4 (2026-06-04): elimina entries de serie_aum con periodo no-año.

    Defensa belt-and-suspenders contra el bug umbrella SICAV (DNCA): una entry
    con periodo="None" (string) y valor = suma del SICAV completo (€41B) que se
    colaba por merge incremental de outputs antiguos. Re-deriva
    kpis.aum_actual_meur desde el último año válido. Devuelve nº de entries
    eliminadas. Idempotente y seguro sobre cualquier output/intl_data dict.
    """
    import re as _re_s
    cuant = out.get("cuantitativo")
    if not isinstance(cuant, dict):
        return 0
    serie = cuant.get("serie_aum")
    if not isinstance(serie, list) or not serie:
        return 0
    valid = [
        e for e in serie
        if isinstance(e, dict)
        and isinstance(e.get("periodo"), str)
        and _re_s.match(r"^\d{4}$", e["periodo"])
    ]
    removed = len(serie) - len(valid)
    if removed:
        cuant["serie_aum"] = valid
        kpis = out.get("kpis")
        if isinstance(kpis, dict) and valid:
            latest = max(valid, key=lambda e: int(e["periodo"]))
            kpis["aum_actual_meur"] = latest.get("valor_meur")
    return removed


def _merge_fee_structure(out: dict, v: Any, doc_year: str = "") -> None:
    if not isinstance(v, dict):
        return
    k = out["kpis"]
    mgmt_fee = _safe_float(v.get("management_fee_pct"))
    ter = _safe_float(v.get("ter_pct"))
    if mgmt_fee and k["coste_gestion_pct"] is None:
        k["coste_gestion_pct"] = mgmt_fee
    if ter and k["ter_pct"] is None:
        k["ter_pct"] = ter
    breakdown = {
        "management_services_fee_pct": _safe_float(v.get("management_fee_pct")),
        "management_company_fee_pct": _safe_float(v.get("management_company_fee_pct")),
        "admin_depositary_fees_pct": _safe_float(v.get("admin_depositary_fees_pct")),
        "performance_fee_pct": _safe_float(v.get("performance_fee_pct")),
        "performance_fee_trigger": v.get("performance_fee_trigger"),
    }
    if not out["economia_fondo"]["expense_ratio_breakdown"]:
        out["economia_fondo"]["expense_ratio_breakdown"] = breakdown
    # Serie TER: si tenemos año del doc y ter_pct, añadir entry
    if doc_year and (ter or mgmt_fee):
        entry = {
            "periodo": doc_year,
            "ter_pct": ter,
            "coste_gestion_pct": mgmt_fee,
        }
        if not any(e.get("periodo") == doc_year for e in out["cuantitativo"]["serie_ter"]):
            out["cuantitativo"]["serie_ter"].append(entry)


def _merge_fund_economics(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    year = str(v.get("year", ""))[:4]
    currency = (v.get("currency") or "EUR").upper()
    fees = _safe_float(v.get("management_fees_collected"))
    if year and fees is not None:
        meur = round(_fx_to_eur(fees, currency) / 1e6, 2) if fees >= 1e6 else round(_fx_to_eur(fees, currency), 2)
        entry = {"anio": year, "valor_meur": meur, "currency": currency}
        if not any(e.get("anio") == year for e in out["economia_fondo"]["management_fees_total"]):
            out["economia_fondo"]["management_fees_total"].append(entry)
    net = _safe_float(v.get("net_result_attributable_to_holders"))
    if year and net is not None:
        meur = round(_fx_to_eur(net, currency) / 1e6, 2) if abs(net) >= 1e6 else round(_fx_to_eur(net, currency), 2)
        entry = {"anio": year, "valor_meur": meur, "currency": currency}
        if not any(e.get("anio") == year for e in out["economia_fondo"]["net_result"]):
            out["economia_fondo"]["net_result"].append(entry)


def _merge_fx_rates(out: dict, v: Any) -> dict:
    """Returns fx_table for use by other mergers."""
    fx_table: dict[str, dict[str, float]] = {}
    if not isinstance(v, dict):
        return fx_table
    rates = v.get("rates") or []
    for r in rates if isinstance(rates, list) else []:
        if not isinstance(r, dict):
            continue
        year = str(r.get("date", ""))[:4]
        curr = (r.get("from_currency") or "").upper()
        rate = _safe_float(r.get("to_functional_rate"))
        if year and curr and rate:
            fx_table.setdefault(year, {})[curr] = rate
    return fx_table


def _merge_asset_allocation(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    for s in v.get("snapshots") or []:
        if not isinstance(s, dict):
            continue
        date = str(s.get("date", ""))[:10]
        year = date[:4]
        if not year:
            continue
        entry = {
            "periodo": year,
            "renta_variable_pct": _safe_float(s.get("equity_pct")),
            "renta_fija_pct": _safe_float(s.get("fixed_income_pct")),
            "liquidez_pct": _safe_float(s.get("cash_pct")),
            "otros_pct": _safe_float(s.get("other_pct")),
        }
        if not any(e.get("periodo") == year for e in out["cuantitativo"]["mix_activos_historico"]):
            out["cuantitativo"]["mix_activos_historico"].append(entry)


def _merge_geographic(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    for s in v.get("snapshots") or []:
        if not isinstance(s, dict):
            continue
        year = str(s.get("date", ""))[:4]
        if not year:
            continue
        allocs = s.get("allocations") or []
        zonas = {a.get("zone"): _safe_float(a.get("pct")) for a in allocs if isinstance(a, dict) and a.get("zone")}
        if zonas and not any(e.get("periodo") == year for e in out["cuantitativo"]["mix_geografico_historico"]):
            out["cuantitativo"]["mix_geografico_historico"].append({"periodo": year, "zonas": zonas})


def _merge_portfolio_metrics(out: dict, v: Any) -> None:
    """Rellena KPIs de riesgo/concentración/benchmark del sub-fondo."""
    if not isinstance(v, dict):
        return
    k = out["kpis"]
    if not k.get("benchmark") and v.get("benchmark"):
        k["benchmark"] = str(v["benchmark"])
    top10 = _safe_float(v.get("concentracion_top10_pct"))
    if top10 is not None and k.get("concentracion_top10_pct") is None:
        k["concentracion_top10_pct"] = top10
    nholds = _safe_int(v.get("num_holdings_total"))
    if nholds is not None and k.get("num_activos_cartera") is None:
        k["num_activos_cartera"] = nholds
    if v.get("classification") and not k.get("clasificacion"):
        k["clasificacion"] = str(v["classification"])


def _merge_top_holdings(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    holdings = v.get("holdings") or []
    if not isinstance(holdings, list) or not holdings:
        return
    normalized = []
    for h in holdings:
        if not isinstance(h, dict):
            continue
        normalized.append({
            "nombre": h.get("name", ""),
            "ticker": h.get("ticker", ""),
            "peso_pct": _safe_float(h.get("weight_pct")),
            "asset_type": h.get("asset_type", ""),
            "sector": h.get("sector", ""),
            "pais": h.get("country", ""),
            "racional": h.get("rationale", ""),
        })
    # Mantener la extracción más rica: si tenemos pocas posiciones ahora
    # y la nueva tiene más, reemplazar.
    existing = out["posiciones"].get("actuales", []) or []
    if len(normalized) > len(existing):
        out["posiciones"]["actuales"] = normalized
    as_of = v.get("as_of_date") or ""
    year = str(as_of)[:4]
    if year:
        if not any(e.get("periodo") == year for e in out["posiciones"]["historicas"]):
            # Guardar TODAS las posiciones (no solo top10). Nombre 'todas' refleja
            # esto; conservamos 'top10' como backward-compat.
            out["posiciones"]["historicas"].append({
                "periodo": year,
                "todas": normalized,
                "top10": normalized[:10],
            })


def _merge_performance(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    series = v.get("series") or []
    if not isinstance(series, list):
        return
    existing = {(r.get("periodo"), r.get("clase")) for r in out["cuantitativo"]["serie_rentabilidad"]}
    for row in series:
        if not isinstance(row, dict):
            continue
        entry = {
            "periodo": str(row.get("period", "")),
            "clase": row.get("class_code", ""),
            "rentabilidad_pct": _safe_float(row.get("fund_return_pct")),
            "benchmark_pct": _safe_float(row.get("benchmark_return_pct")),
        }
        key = (entry["periodo"], entry["clase"])
        if entry["periodo"] and key not in existing:
            existing.add(key)
            out["cuantitativo"]["serie_rentabilidad"].append(entry)


def _merge_qualitative(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    c = out["cualitativo"]
    mapping = {
        "strategy": "estrategia",
        "philosophy": "filosofia_inversion",
        "selection_process": "proceso_seleccion",
        "asset_types": "tipo_activos",
        "real_objectives": "objetivos_reales",
        "fund_history": "historia_fondo",
    }
    for src, dst in mapping.items():
        if not c.get(dst) and v.get(src):
            c[dst] = v[src]


def _merge_thesis(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    periods = v.get("periods") or []
    if not isinstance(periods, list):
        return
    existing = {p.get("periodo") for p in out["analisis_consistencia"]["periodos"]}
    for p in periods:
        if not isinstance(p, dict):
            continue
        entry = {
            "periodo": p.get("period", ""),
            "contexto_mercado": p.get("market_context", ""),
            "tesis_gestora": p.get("thesis", ""),
            "decisiones_tomadas": p.get("decisions_taken", ""),
            "resultado_real": p.get("observed_outcome", ""),
            "consistencia_score": None,
            "notas": p.get("notes", ""),
        }
        if entry["periodo"] and entry["periodo"] not in existing:
            existing.add(entry["periodo"])
            out["analisis_consistencia"]["periodos"].append(entry)


def _merge_team(out: dict, v: Any) -> None:
    if not isinstance(v, dict):
        return
    members = v.get("members") or []
    if not isinstance(members, list):
        return
    existing_names = {g.get("nombre") for g in out["cualitativo"]["gestores"]}
    for m in members:
        if not isinstance(m, dict):
            continue
        name = m.get("name", "")
        if not name or name in existing_names:
            continue
        existing_names.add(name)
        out["cualitativo"]["gestores"].append({
            "nombre": name,
            "cargo": m.get("role", ""),
            "background": m.get("background", ""),
            "anio_incorporacion": _safe_int(m.get("since_year")),
        })


# Dispatch table concept_name → merger function
_MERGERS = {
    "target_fund_identity": _merge_target_fund_identity,
    "fund_size_history": _merge_fund_size_history,
    "share_classes_catalog": _merge_share_classes,
    "fee_structure": _merge_fee_structure,
    "fund_economics_yearly": _merge_fund_economics,
    "asset_allocation_history": _merge_asset_allocation,
    "geographic_allocation_history": _merge_geographic,
    "portfolio_metrics": _merge_portfolio_metrics,
    "top_holdings": _merge_top_holdings,
    "performance_history": _merge_performance,
    "manager_qualitative": _merge_qualitative,
    "manager_thesis_and_decisions": _merge_thesis,
    "portfolio_management_team": _merge_team,
}


# ══════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════


class IntlExtractor:
    """Orquestador concept-first multi-gestora."""

    def __init__(self, isin: str, config: dict | None = None):
        self.isin = isin.upper().strip()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.mapper_cache = self.fund_dir / "cache" / "mapper"
        self.extractor_cache = self.fund_dir / "cache" / "extractor_v3"
        self.mapper_cache.mkdir(parents=True, exist_ok=True)
        self.extractor_cache.mkdir(parents=True, exist_ok=True)

        self.fund_name = (
            self.config.get("nombre") or self.config.get("nombre_oficial") or ""
        )
        self.gestora = (
            self.config.get("gestora") or self.config.get("gestora_oficial") or ""
        )

    def _load_identity_from_regulator(self) -> None:
        """Rellena fund_name/gestora desde regulator cache si faltan."""
        if self.fund_name and self.gestora:
            return
        for fname in ("cssf_data.json", "cbi_data.json", "amf_data.json",
                      "bundesanzeiger_data.json"):
            f = self.fund_dir / fname
            if not f.exists():
                continue
            try:
                ident = json.loads(f.read_text(encoding="utf-8")).get("identity", {})
                self.fund_name = self.fund_name or (
                    ident.get("nombre_oficial") or ident.get("sub_fondo") or ""
                )
                self.gestora = self.gestora or ident.get("gestora_oficial", "")
                if self.fund_name and self.gestora:
                    return
            except Exception:
                pass

    def _process_doc(self, doc: dict, out: dict) -> None:
        """
        Procesar un documento:
        - AR / SAR (docs largos, umbrella posible): mapper + extractor dirigido
        - Factsheet / KID / letter / prospectus (docs cortos, ya del sub-fondo):
          extractor directo sobre texto completo, sin mapper (más barato y
          garantiza que TODOS los docs cortos se procesan).
        """
        path = doc.get("local_path", "")
        if not path or not Path(path).exists():
            return
        console.log(f"[bold cyan]{doc.get('doc_type')} @ {doc.get('periodo')}[/bold cyan]  {Path(path).name}")

        from tools.pdf_extractor import get_pdf_metadata
        meta = get_pdf_metadata(path)
        total_pages = meta.get("num_pages", 0)

        # Decidir: mapper (docs largos/umbrella) vs directo (docs cortos)
        needs_mapper = (
            doc.get("doc_type") in ("annual_report", "semi_annual_report")
            or total_pages >= 30
        )

        if needs_mapper:
            try:
                doc_map = map_document(
                    pdf_path=path, isin=self.isin,
                    fund_name=self.fund_name, gestora=self.gestora,
                    cache_dir=self.mapper_cache,
                )
            except Exception as e:
                console.log(f"[red]mapper failed for {path}: {e}")
                return

            if not doc_map.get("target_fund_in_this_doc"):
                console.log(f"[yellow]doc no menciona el sub-fondo objetivo, skip")
                return

            extraction = extract_all(
                pdf_path=path, doc_map=doc_map, isin=self.isin,
                fund_name=self.fund_name, cache_dir=self.extractor_cache,
            )
        else:
            # Doc corto: extractor directo sin mapper. Construir un mapa
            # sintético que apunta a "todas las páginas" para conceptos
            # core/useful del doc_type (skip nice_to_have para ahorrar tokens).
            from agents.concepts import concepts_for_doc_type
            applicable = {
                name: entry for name, entry in
                concepts_for_doc_type(doc.get("doc_type", "")).items()
                if entry.get("priority") != "nice_to_have"
            }
            all_pages = list(range(1, total_pages + 1))
            concept_locations = {
                name: {
                    "pages_1indexed": all_pages,
                    "format_clue": "",
                    "covers_target_only": True,
                    "evidence_quote": "",
                    "confidence": 0.7,
                }
                for name in applicable
            }
            synthetic_map = {
                "target_fund_in_this_doc": True,
                "concept_locations": concept_locations,
                "target_fund_delimiter_signal": {},
                "_meta": {"total_pages": total_pages},
            }
            extraction = extract_all(
                pdf_path=path, doc_map=synthetic_map, isin=self.isin,
                fund_name=self.fund_name, cache_dir=self.extractor_cache,
            )

        console.log(f"[dim]  stats: {extraction['stats']}")

        # FX rates primero (se usa por los demás mergers)
        fx_result = extraction["by_concept"].get("currency_conversion_rates")
        fx_table = _merge_fx_rates(out, _value_of(fx_result))

        # Año del documento, usado como default en mergers que generan series
        doc_year = str(doc.get("periodo", ""))[:4]

        # Merge resto de concepts
        for name, result in extraction["by_concept"].items():
            if name == "currency_conversion_rates":
                continue
            merger = _MERGERS.get(name)
            if not merger:
                continue
            try:
                if name in {"fund_size_history", "share_classes_catalog"}:
                    merger(out, _value_of(result), fx_table)
                elif name == "fee_structure":
                    merger(out, _value_of(result), doc_year)
                else:
                    merger(out, _value_of(result))
            except Exception as e:
                console.log(f"[yellow]merger {name} error: {e}")

        # Fuentes
        if doc.get("doc_type") in ("annual_report", "semi_annual_report", "prospectus", "kid"):
            if path not in out["fuentes"]["informes_descargados"]:
                out["fuentes"]["informes_descargados"].append(path)
        elif doc.get("doc_type") in ("quarterly_letter", "manager_presentation"):
            if path not in out["fuentes"]["cartas_gestores"]:
                out["fuentes"]["cartas_gestores"].append(path)
        url = doc.get("url", "")
        if url and url not in out["fuentes"]["urls_consultadas"]:
            out["fuentes"]["urls_consultadas"].append(url)

    # ─────────────────────────────────────────────────────────────────
    # G11 (2026-05-19, branch v2-cowork): fallback HTML cuando discovery
    # no consigue PDFs descargables del sub-fondo. Gestoras INT como
    # Amiral Gestion publican toda la info del fondo en HTML estático,
    # no en annual reports descargables. Solo se activa si el merge
    # principal NO rellenó kpis críticos.
    # ─────────────────────────────────────────────────────────────────

    def _gather_harvested_urls(self) -> list[str]:
        """Devuelve URLs HTML cacheadas durante discovery/letters_collector.

        Lee `search_cache.json` (urls visitadas por todos los agents
        upstream). Devuelve lista deduplicada de URLs http(s) — el filtro
        de qué vale la pena fetchear vive en `tools.intl_url_filter`.
        """
        urls: list[str] = []
        seen: set[str] = set()
        sc_path = self.fund_dir / "search_cache.json"
        if sc_path.exists():
            try:
                sc = json.loads(sc_path.read_text(encoding="utf-8"))
                for u in (sc.get("urls") or {}).keys():
                    if isinstance(u, str) and u.startswith("http") and u not in seen:
                        seen.add(u)
                        urls.append(u)
            except Exception:
                pass
        # También considerar URLs ya consultadas en intl_data anterior
        out_path = self.fund_dir / "intl_data.json"
        if out_path.exists():
            try:
                prev = json.loads(out_path.read_text(encoding="utf-8"))
                for u in (prev.get("fuentes") or {}).get("urls_consultadas") or []:
                    if isinstance(u, str) and u.startswith("http") and u not in seen:
                        seen.add(u)
                        urls.append(u)
            except Exception:
                pass
        return urls

    # ─────────────────────────────────────────────────────────────────
    # BUG-C (2026-05-20): Aggregator domains that frequently publish AUM
    # for INT funds even when the manager's site only has HTML stubs.
    # Used as fallback URL expansion when rank_fund_page_urls returns <3.
    # ─────────────────────────────────────────────────────────────────
    _AGGREGATOR_DOMAINS = (
        "morningstar.com", "morningstar.es", "morningstar.fr",
        "morningstar.co.uk", "morningstar.de", "morningstar.it",
        "quantalys.com", "quantalys.fr",
        "citywire.com", "citywire.co.uk", "citywire.fr", "citywire.es",
        "citywireselector.com",
        "trustnet.com", "boursorama.com",
        "funds-mutual.com", "fondsfinanz.de",
    )

    def _expand_candidates_from_aggregators(
        self, all_urls: list[str], existing: list[str]
    ) -> list[str]:
        """Add aggregator URLs (Morningstar/Quantalys/Citywire/etc.) that mention
        the target fund. Used by BUG-C when the gestora's site doesn't expose AUM.
        """
        seen = {u.lower() for u in existing}
        fund_tokens = [
            t for t in re.split(r"[^a-zA-Z0-9]+", (self.fund_name or "").lower())
            if len(t) >= 4
        ]
        gestora_tokens = [
            t for t in re.split(r"[^a-zA-Z0-9]+", (self.gestora or "").lower())
            if len(t) >= 4
        ]
        fund_only = [t for t in fund_tokens if t not in gestora_tokens]
        out: list[str] = []
        isin_lc = self.isin.lower()
        for url in all_urls:
            if not isinstance(url, str) or url.lower() in seen:
                continue
            url_lc = url.lower()
            if not any(dom in url_lc for dom in self._AGGREGATOR_DOMAINS):
                continue
            # Must reference the fund either by ISIN or by name token
            if isin_lc in url_lc or any(t in url_lc for t in fund_only):
                out.append(url)
                seen.add(url_lc)
        return out

    def _expand_candidates_from_prospectus(self, existing: list[str]) -> list[str]:
        """Extract HTTP URLs present in prospectus PDFs already downloaded in
        raw/discovery/. BUG-C: gestoras INT a veces dejan en el prospecto el
        link a la página del fondo (factsheet en su web, ficha pública).
        Coste: 1 lectura local por PDF, sin LLM.
        """
        seen = {u.lower() for u in existing}
        new_urls: list[str] = []
        discovery_dir = self.fund_dir / "raw" / "discovery"
        if not discovery_dir.exists():
            return new_urls
        prospectus_pdfs = [
            p for p in discovery_dir.glob("*.pdf")
            if any(k in p.name.lower() for k in ("prospectus", "folleto", "prospecto"))
        ]
        if not prospectus_pdfs:
            return new_urls
        try:
            from tools.pdf_extractor import extract_page_range
        except Exception:
            return new_urls
        url_re = re.compile(r"https?://[A-Za-z0-9._~%:/?#@!$&'()*+,;=\-]+", re.IGNORECASE)
        gestora_token = re.split(r"[^a-zA-Z0-9]+", (self.gestora or "").lower())
        gestora_token = next((t for t in gestora_token if len(t) >= 5), "")
        for pdf in prospectus_pdfs[:2]:  # cap a 2 prospectos
            try:
                text = extract_page_range(str(pdf), 0, 5)
            except Exception:
                continue
            for m in url_re.findall(text or ""):
                url = m.rstrip(".,);:'\"")
                if url.lower() in seen:
                    continue
                # Aceptar URLs de la gestora o de agregadores
                url_lc = url.lower()
                if (
                    (gestora_token and gestora_token in url_lc)
                    or any(dom in url_lc for dom in self._AGGREGATOR_DOMAINS)
                    or self.isin.lower() in url_lc
                ):
                    new_urls.append(url)
                    seen.add(url_lc)
                    if len(new_urls) >= 5:
                        return new_urls
        return new_urls

    async def _fallback_html_extract(self, out: dict) -> bool:
        """Extracción cuanti desde HTML cuando no hay PDFs del sub-fondo.

        Pipeline:
          1. Recoger URLs harvested por discovery (search_cache.json)
          2. Filtrar por `tools.intl_url_filter.rank_fund_page_urls`
          3. Si <3 candidatas (BUG-C 2026-05-20): expandir con URLs
             de prospectus PDF + agregadores (Morningstar/Quantalys/Citywire)
          4. Fetch text via `tools.web_fetcher.fetch_url` (cache 365d)
          5. UNA llamada Gemini Flash con texto concatenado + schema
          6. Merge en `out` con marcador `_html_fallback` para traza
          7. BUG-D: persistir dominios útiles en gestoras_registry.json

        Devuelve True si alguna KPI/posición se rellenó.
        """
        import os

        all_urls = self._gather_harvested_urls()
        if not all_urls:
            console.log("[yellow][HTML-FB] no URLs harvested, skip")
            return False

        from tools.intl_url_filter import rank_fund_page_urls
        candidates = rank_fund_page_urls(
            all_urls,
            isin=self.isin,
            fund_name=self.fund_name,
            gestora=self.gestora,
            max_urls=8,
        )

        # BUG-C: si pocas candidatas, expandir con prospectus + agregadores
        if len(candidates) < 3:
            n_before = len(candidates)
            extra_prospectus = self._expand_candidates_from_prospectus(candidates)
            extra_aggregators = self._expand_candidates_from_aggregators(
                all_urls, candidates
            )
            # Append (sin re-filtrar — ya tienen señal de relevancia por construcción)
            candidates = candidates + extra_prospectus + extra_aggregators
            # Tope total a 10 URLs (Gemini context budget)
            candidates = candidates[:10]
            if len(candidates) > n_before:
                console.log(
                    f"[cyan][HTML-FB] expand BUG-C: +{len(extra_prospectus)} prospectus "
                    f"+{len(extra_aggregators)} agregadores → {len(candidates)} total"
                )

        if not candidates:
            console.log(
                f"[yellow][HTML-FB] 0/{len(all_urls)} URLs pasan el filtro fondo "
                f"({self.fund_name!r}), skip"
            )
            return False

        console.log(
            f"[cyan][HTML-FB] {len(candidates)}/{len(all_urls)} URLs candidatas "
            f"para fallback Gemini Flash"
        )

        from tools.web_fetcher import fetch_url

        pages: list[tuple[str, str]] = []  # (url, text)
        total_chars = 0
        # BUG-C (2026-05-20): bumped 30000 → 50000 para dar más contexto a Gemini
        # cuando candidatas incluyen prospectus + agregadores.
        max_chars_total = 50000
        for url in candidates:
            if total_chars >= max_chars_total:
                break
            try:
                res = await fetch_url(url, max_chars=8000)
            except Exception as e:
                console.log(f"[dim][HTML-FB] fetch fail {url[:60]}: {e}")
                continue
            if not res.ok or not res.text:
                continue
            text = res.text.strip()
            if len(text) < 200:
                continue
            pages.append((url, text[:8000]))
            total_chars += len(text[:8000])

        if not pages:
            console.log("[yellow][HTML-FB] ninguna URL devolvió texto utilizable")
            return False

        # Kill switch (2026-05-28): si GEMINI_DISABLED=1, skip HTML fallback.
        # Es un fallback secundario para INT cuando no se han podido descargar
        # PDFs. Degradación funcional aceptable (el pipeline continúa).
        from tools.gemini_killswitch import is_gemini_disabled
        if is_gemini_disabled():
            console.log("[cyan][HTML-FB] Gemini OFF (killswitch) — skip HTML fallback")
            return False

        gemini_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not gemini_key:
            console.log("[yellow][HTML-FB] sin GOOGLE_API_KEY/GEMINI_API_KEY, skip")
            return False

        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
        except Exception as e:
            console.log(f"[yellow][HTML-FB] Gemini init failed: {e}")
            return False

        joined = "\n\n".join(
            f"### URL {i+1}: {url}\n{text}"
            for i, (url, text) in enumerate(pages)
        )
        prompt = f"""Eres un extractor de datos de fondos de inversión. Te paso el texto plano de varias páginas HTML de la gestora del fondo abajo.

FONDO TARGET:
- ISIN: {self.isin}
- Nombre: {self.fund_name or 'desconocido'}
- Gestora: {self.gestora or 'desconocida'}

ANTI-AGREGACIÓN (REGLA CRÍTICA — BUG-C, revisitando bug DNCA-€41B):
- PROHIBIDO sumar, agregar o reportar AUM de una SICAV, umbrella, gestora o
  familia de fondos. SOLO devuelve aum_actual_meur si la página menciona
  ESPECÍFICAMENTE el sub-fondo {self.isin} ({self.fund_name or 'target'}) y
  el AUM corresponde a ese sub-fondo concreto.
- Si la URL/página agrega varios sub-fondos (ej. "Amiral Gestion: 4 mil M€",
  "DNCA Invest umbrella €41bn") → aum_actual_meur = null.
- Si solo aparece "Fund AuM" de varias clases de acciones del MISMO sub-fondo
  (clase A + clase F + clase H...) y la suma es razonable, OK sumarlas.
  Pero NUNCA mezclar sub-fondos distintos.

DATOS A EXTRAER (devuelve EXACTAMENTE este JSON; null cuando no aparezca en el texto):
{{
  "kpis": {{
    "aum_actual_meur": <float en MILLONES de EUR del SUB-FONDO target, NO de la SICAV/umbrella/gestora>,
    "aum_source_url": <URL de la página donde leíste el AUM (de la lista de abajo) o null>,
    "fecha_aum": <string YYYY-MM-DD o null si no aparece la fecha del corte>,
    "num_participes": <int o null>,
    "ter_pct": <float (ej. 1.85) o null>,
    "divisa_base": <string EUR/USD/GBP/CHF o null>
  }},
  "posiciones_actuales": [
    {{"nombre": "...", "peso_pct": <float>, "sector": "...", "pais": "..."}}
  ],
  "gestores": [
    {{"nombre": "...", "cargo": "..."}}
  ],
  "anti_invencion_note": "<si el texto NO menciona explícitamente el sub-fondo target, deja todo null y explica aquí. También si detectas AUM agregada de SICAV/umbrella>"
}}

REGLAS:
- Si el texto habla de un fondo distinto (otro sub-fondo, otra clase), devuelve null en kpis.
- AUM: convertir M€/M USD a M€ (1 USD = 0.92 EUR de fallback solo si no hay tipo de cambio en el texto).
- posiciones_actuales: top 10 máximo, solo del SUB-FONDO target.
- gestores: solo personas explícitamente atribuidas al sub-fondo target.

TEXTO HTML (varias páginas, separadas por '### URL N:'):
{joined}

Devuelve SOLO el JSON. Nada más."""

        try:
            resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            raw = (resp.text or "").strip()
        except Exception as e:
            console.log(f"[red][HTML-FB] Gemini call failed: {e}")
            return False

        # Strip markdown fences
        if raw.startswith("```"):
            raw = raw.split("```", 2)
            raw = raw[1] if len(raw) > 1 else ""
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
            # cerrar con la otra fence si está al final
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        try:
            data = json.loads(raw)
        except Exception as e:
            console.log(f"[red][HTML-FB] JSON parse failed: {e} | head={raw[:200]!r}")
            return False

        # Anti-invención: si el modelo dice que no aparece el sub-fondo, marcar y salir
        anti = (data.get("anti_invencion_note") or "").strip()
        kpis_in = data.get("kpis") or {}
        all_kpis_null = all(kpis_in.get(k) in (None, "", 0) for k in (
            "aum_actual_meur", "num_participes", "ter_pct"
        ))
        if all_kpis_null and not (data.get("posiciones_actuales") or data.get("gestores")):
            console.log(
                f"[yellow][HTML-FB] Gemini no encontró datos del sub-fondo "
                f"(anti_invencion={anti!r})"
            )
            return False

        changed_fields: list[str] = []
        k = out["kpis"]

        aum = _safe_float(kpis_in.get("aum_actual_meur"))
        if aum is not None and k.get("aum_actual_meur") is None:
            k["aum_actual_meur"] = aum
            changed_fields.append("kpis.aum_actual_meur")
            # Sembrar serie_aum con el año del corte si aparece
            fecha = str(kpis_in.get("fecha_aum") or "")[:4]
            if re.match(r"^\d{4}$", fecha):
                if not any(
                    e.get("periodo") == fecha
                    for e in out["cuantitativo"]["serie_aum"]
                ):
                    out["cuantitativo"]["serie_aum"].append(
                        {"periodo": fecha, "valor_meur": aum}
                    )

        npart = _safe_int(kpis_in.get("num_participes"))
        if npart is not None and k.get("num_participes") is None:
            k["num_participes"] = npart
            changed_fields.append("kpis.num_participes")

        ter = _safe_float(kpis_in.get("ter_pct"))
        if ter is not None and k.get("ter_pct") is None:
            k["ter_pct"] = ter
            changed_fields.append("kpis.ter_pct")

        divisa = (kpis_in.get("divisa_base") or "").upper().strip()
        if divisa and not k.get("clasificacion"):  # divisa_base vive en _empty_output via cnmv pero no en este schema
            pass  # divisa no es un campo top-level del schema INT actual; reservado

        posiciones_in = data.get("posiciones_actuales") or []
        if (
            isinstance(posiciones_in, list)
            and posiciones_in
            and not out["posiciones"]["actuales"]
        ):
            normalized = []
            for p in posiciones_in[:10]:
                if not isinstance(p, dict):
                    continue
                normalized.append({
                    "nombre": p.get("nombre", ""),
                    "ticker": "",
                    "peso_pct": _safe_float(p.get("peso_pct")),
                    "asset_type": "",
                    "sector": p.get("sector", ""),
                    "pais": p.get("pais", ""),
                    "racional": "",
                })
            if normalized:
                out["posiciones"]["actuales"] = normalized
                changed_fields.append(f"posiciones.actuales[{len(normalized)}]")

        gestores_in = data.get("gestores") or []
        if isinstance(gestores_in, list) and gestores_in:
            existing = {g.get("nombre") for g in out["cualitativo"]["gestores"]}
            added = 0
            for g in gestores_in:
                if not isinstance(g, dict):
                    continue
                nombre = (g.get("nombre") or "").strip()
                if not nombre or nombre in existing:
                    continue
                existing.add(nombre)
                out["cualitativo"]["gestores"].append({
                    "nombre": nombre,
                    "cargo": g.get("cargo", ""),
                    "background": "",
                    "anio_incorporacion": None,
                })
                added += 1
            if added:
                changed_fields.append(f"cualitativo.gestores[+{added}]")

        if not changed_fields:
            console.log("[yellow][HTML-FB] Gemini respondió pero no aportó datos nuevos")
            return False

        # BUG-D + N4 (2026-05-20): identificar qué URLs aportaron AUM/posiciones.
        # Si Gemini devolvió aum_source_url específico (pinpoint), el dominio
        # de ESA URL tiene PRIORIDAD en `useful_domains` (aparece primero).
        # Resto de dominios procesados van detrás ordenados alfabéticamente.
        # Esto permite que el registry priorice el dominio realmente útil
        # (típicamente Morningstar/Quantalys/Citywire) sobre la web genérica
        # de la gestora.
        from urllib.parse import urlparse as _urlparse
        aum_source_url = kpis_in.get("aum_source_url") or ""
        aum_domain = ""
        if aum_source_url and any(aum_source_url == u for u, _ in pages):
            useful_urls = [aum_source_url]
            aum_domain = _urlparse(aum_source_url).netloc.lower()
        else:
            useful_urls = [u for u, _ in pages]

        other_domains = sorted({
            _urlparse(u).netloc.lower()
            for u, _ in pages
            if _urlparse(u).netloc and _urlparse(u).netloc.lower() != aum_domain
        })
        useful_domains = ([aum_domain] if aum_domain else []) + other_domains

        out["_html_fallback"] = {
            "extracted_at": datetime.now().isoformat(timespec="seconds"),
            "model": "gemini-2.5-flash",
            "source_urls": [u for u, _ in pages],
            "urls_processed": len(pages),
            "useful_urls": useful_urls,
            "useful_domains": useful_domains,
            "fields_filled": changed_fields,
            "anti_invencion_note": anti or None,
        }
        # Marcar las URLs como consultadas para que el analyst no las pierda
        for url, _ in pages:
            if url not in out["fuentes"]["urls_consultadas"]:
                out["fuentes"]["urls_consultadas"].append(url)
        console.log(
            f"[green][HTML-FB] OK: rellenó {len(changed_fields)} campos "
            f"({', '.join(changed_fields)})"
        )

        # BUG-D (2026-05-20): auto-aprender dominios útiles tras éxito del fallback.
        # Condición: hay urls_processed>0 AND (aum != None OR posiciones añadidas).
        has_aum = out["kpis"].get("aum_actual_meur") is not None
        has_pos = bool(out["posiciones"]["actuales"])
        if useful_domains and self.gestora and (has_aum or has_pos):
            try:
                from agents.discovery_v2 import persist_html_fallback_to_registry
                persist_html_fallback_to_registry(
                    isin=self.isin,
                    gestora=self.gestora,
                    useful_domains=useful_domains,
                )
            except Exception as e:
                console.log(f"[yellow][HTML-FB] G7 persist failed: {e}")

        return True

    def _add_viability_note(self, out: dict) -> None:
        """Nota sobre la viabilidad económica para la gestora."""
        fees = out["economia_fondo"]["management_fees_total"]
        aum = out["kpis"].get("aum_actual_meur")
        if not fees or not aum:
            return
        try:
            latest = max(fees, key=lambda e: e.get("anio", ""))
            fee_meur = latest.get("valor_meur")
            if not fee_meur:
                return
            pct_eff = fee_meur / aum * 100
            note = (
                f"La gestora ingresa {fee_meur:.2f} M EUR ({latest.get('anio')}) "
                f"de este fondo (~{pct_eff:.2f}% efectivo sobre AUM {aum:.1f} M EUR). "
            )
            if fee_meur < 1.0:
                note += "ALERTA: fees bajos, fondo con poca escala para la gestora."
            elif fee_meur > 5.0:
                note += "Volumen solido para la gestora."
            out["economia_fondo"]["viabilidad_nota"] = note
        except Exception:
            pass

    async def run(self) -> dict:
        disc_path = self.fund_dir / "intl_discovery_data.json"
        if not disc_path.exists():
            console.log(f"[red]Falta {disc_path}")
            return _empty_output(self.isin, self.fund_name, self.gestora)

        disc = json.loads(disc_path.read_text(encoding="utf-8"))
        docs_all = disc.get("documents", [])
        docs = [d for d in docs_all if d.get("validated") and d.get("local_path")]

        self._load_identity_from_regulator()

        console.log(
            f"[bold]IntlExtractor v3 {self.isin}[/bold] "
            f"fund={self.fund_name!r} gestora={self.gestora!r} docs={len(docs)}"
        )

        out = _empty_output(self.isin, self.fund_name, self.gestora)

        # Refactor L2 (2026-05-05): in cowork mode, skip the Gemini Pro/Flash
        # 2-stage pipeline (concept_mapper + concept_extractor). Emit one
        # extraction task per validated PDF doc so the extract-pdfs-cowork
        # skill can process them under Claude Max. The minimal intl_data.json
        # written here only carries identity (nombre/gestora/tipo); the skill
        # output gets merged via --consume-extracted later.
        from tools.api_mode import is_cowork_mode
        if is_cowork_mode():
            from tools.pending_manifest import append_extraction_task as _emit
            n_tasks = 0
            n_skipped = 0

            # Fix-5.1: escanear raw/discovery/ directamente (no `docs` del
            # discovery, que clasifica casi todo como annual_report). Usar
            # classifier por nombre de fichero y emitir UNA task por PDF.
            discovery_dir = self.fund_dir / "raw" / "discovery"
            pdfs = sorted(discovery_dir.glob("*.pdf")) if discovery_dir.exists() else []
            for pdf in pdfs:
                classified = _classify_pdf_for_task(pdf, self.isin, self.fund_name)
                if classified is None:
                    n_skipped += 1
                    continue
                task_type, schema = classified
                # A.2 (2026-06-10): verificar que el doc es del fondo target y
                # está sano (no corrupto/truncado ni ajeno) ANTES de emitir tarea.
                # Evita extraer basura (snapshots Wayback a 1MB, docs de otro fondo).
                try:
                    from tools.verify_fund_docs import verify_doc_for_fund
                    _mp = 60 if task_type in ("annual_subfund", "semi_annual_subfund") else 8
                    _ok, _reason = verify_doc_for_fund(
                        pdf, self.isin, self.fund_name, self.gestora, max_pages=_mp)
                    if not _ok:
                        console.log(f"[yellow]A.2 skip {pdf.name[:40]}: {_reason}")
                        n_skipped += 1
                        continue
                except Exception:
                    pass  # ante fallo del verificador, no bloquear
                stem = re.sub(r"[^A-Za-z0-9_-]", "_", pdf.stem)[:50]
                tid = f"intl_{task_type}_{stem}"
                two_stage = task_type in ("annual_subfund", "semi_annual_subfund",
                                            "prospectus")
                # ruta relativa al ROOT del proyecto
                try:
                    rel = pdf.relative_to(self.fund_dir.parent.parent.parent)
                    pdf_path_str = str(rel).replace("\\", "/")
                except Exception:
                    pdf_path_str = str(pdf).replace("\\", "/")
                _emit(
                    self.fund_dir, self.isin,
                    task_id=tid,
                    agent="intl_extractor_v2",
                    pdf_path=pdf_path_str,
                    schema=schema,
                    context=(
                        f"PDF tipo={task_type}. ISIN target: {self.isin} "
                        f"({self.fund_name or 'unknown'} / {self.gestora or 'unknown'}). "
                        f"Extrae SOLO datos del SUB-FONDO target, NUNCA agregues la "
                        f"gestora paraguas/umbrella SICAV. Si el PDF es corporate y no "
                        f"menciona el ISIN, devuelve null en kpis y explica en "
                        f"anti_invencion_note."
                    ),
                    two_stage=two_stage,
                    tipo="INT",
                )
                n_tasks += 1

            # Fix-5.2: task adicional `web_fund_page` si discovery encontró URL
            # canónica del sub-fondo (live data: AUM, NAV, top holdings)
            disc = json.loads(disc_path.read_text(encoding="utf-8"))
            fund_url = disc.get("fund_url") or ""
            if not fund_url:
                for w in (disc.get("websites_confirmados") or []):
                    if self.isin.lower() in str(w).lower():
                        fund_url = w
                        break
            if fund_url:
                _emit(
                    self.fund_dir, self.isin,
                    task_id="intl_web_fund_page",
                    agent="intl_extractor_v2",
                    pdf_path=fund_url,
                    schema=FACTSHEET_SCHEMA,
                    context=(
                        f"NO es PDF. Es URL: {fund_url}. Hacer WebFetch. "
                        f"Extraer datos LIVE del sub-fondo target ({self.isin}): "
                        f"AUM, NAV, top holdings, asset allocation, performance. "
                        f"VERIFICAR que el ISIN aparece en la página antes de extraer. "
                        f"Si no aparece, devuelve null + anti_invencion_note."
                    ),
                    tipo="INT",
                    extra={"is_web_fetch": True, "url": fund_url},
                )
                n_tasks += 1

            console.log(
                f"[cyan]cowork mode: deferred {n_tasks} INT extraction tasks "
                f"({n_skipped} corporate/ESG PDFs skipped) "
                f"to pending_extraction.json (skill: extract-pdfs-cowork)"
            )
            # Write a minimal intl_data.json (identity-only) so downstream
            # agents (orchestrator hint extraction, etc.) don't choke. The
            # extract-pdfs-cowork skill output gets merged via --consume-extracted.
            out["_cowork_pending"] = True
            out["_cowork_n_tasks"] = n_tasks
            out_path = self.fund_dir / "intl_data.json"
            out_path.write_text(
                json.dumps(out, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return out

        # Procesar docs en orden: AR primero (más información), luego el resto
        docs_sorted = sorted(
            docs,
            key=lambda d: 0 if d.get("doc_type") == "annual_report" else (
                1 if d.get("doc_type") == "semi_annual_report" else 2
            ),
        )
        for doc in docs_sorted:
            try:
                self._process_doc(doc, out)
            except Exception as e:
                console.log(f"[red]doc error: {e}")

        # G11 (2026-05-19): fallback HTML cuando el merge principal no rellenó
        # KPIs críticos. Típicamente ocurre cuando la gestora INT no publica
        # annual reports descargables (solo páginas HTML del fondo). Se ejecuta
        # como máximo 1 vez por run, con 1 call Gemini Flash (~$0.02-0.05).
        docs_match_isin = [
            d for d in docs
            if isinstance(d.get("isins_inside"), list)
            and self.isin in d.get("isins_inside", [])
        ]
        needs_fallback = (
            len(docs) == 0
            or not docs_match_isin
            or (
                out["kpis"].get("aum_actual_meur") is None
                and not out["posiciones"]["actuales"]
            )
        )
        if needs_fallback:
            try:
                await self._fallback_html_extract(out)
            except Exception as e:
                console.log(f"[red][HTML-FB] uncaught error: {e}")

        # Limpieza serie_aum: quitar entries sin periodo válido
        out["cuantitativo"]["serie_aum"] = [
            e for e in out["cuantitativo"]["serie_aum"]
            if e.get("periodo") and str(e["periodo"]) not in ("", "None", "null")
        ]

        # Sanity check: drop outliers en serie_aum (contaminación umbrella/
        # strategy AUM desde factsheets). Si un valor es >3× la mediana de
        # los demás del mismo fondo, descartarlo.
        aum_serie = out["cuantitativo"]["serie_aum"]
        if len(aum_serie) >= 2:
            vals = sorted([e["valor_meur"] for e in aum_serie if e.get("valor_meur")])
            mid = vals[len(vals) // 2]
            out["cuantitativo"]["serie_aum"] = [
                e for e in aum_serie
                if e.get("valor_meur") is None or e["valor_meur"] <= mid * 3
            ]
            # Re-calcular AUM actual
            if out["cuantitativo"]["serie_aum"]:
                latest = max(out["cuantitativo"]["serie_aum"],
                             key=lambda e: str(e.get("periodo", "")))
                out["kpis"]["aum_actual_meur"] = latest.get("valor_meur")

        # Ordenar series
        for k in ("serie_aum", "serie_ter", "serie_participes",
                  "serie_rentabilidad", "mix_activos_historico",
                  "mix_geografico_historico"):
            out["cuantitativo"][k].sort(key=lambda r: str(r.get("periodo", "")))
        out["posiciones"]["historicas"].sort(key=lambda r: str(r.get("periodo", "")))

        self._add_viability_note(out)

        # Nota: serie_rentabilidad del extractor es complementaria.
        # La fuente primaria de performance diaria/anual es Morningstar
        # (herramienta externa con NAV diarios desde ISIN).
        if not out["cuantitativo"]["serie_rentabilidad"]:
            out["cuantitativo"]["_rentabilidad_note"] = (
                "Serie rentabilidad no extraida de PDFs. Usar fuente primaria: "
                "Morningstar NAV diarios (herramienta GitHub externa)."
            )

        # ── MERGE INCREMENTAL: nunca sobrescribir con menos datos ──
        # Si intl_data.json ya existe con datos más ricos, preservarlos.
        out_path = self.fund_dir / "intl_data.json"
        if out_path.exists():
            try:
                existing = json.loads(out_path.read_text(encoding="utf-8"))
                out = self._merge_preserve_richer(existing, out)
                console.log("[dim]merge incremental: preservado lo mejor de existente + nuevo")
            except Exception:
                pass

        # B4 (2026-06-04): limpiar serie_aum tras el merge (el merge incremental
        # puede reintroducir entries con periodo="None" de outputs antiguos).
        _n_aum = sanitize_serie_aum(out)
        if _n_aum:
            console.log(f"[yellow]serie_aum: eliminadas {_n_aum} entries con periodo inválido")

        def _safe_default(o):
            """Serializer tolerante. Maneja NaT / None / pandas.Timestamp."""
            try:
                if o is None:
                    return None
                # pandas.Timestamp / datetime / date
                if hasattr(o, "isoformat"):
                    try:
                        return o.isoformat()
                    except Exception:
                        return None
                # pandas.NaT → str → "NaT"
                import math
                if isinstance(o, float) and math.isnan(o):
                    return None
                return str(o)
            except Exception:
                return None

        # Atomic write: write a .tmp + rename (evita que un lector concurrente
        # vea el archivo vacío o parcial durante la escritura).
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=_safe_default),
            encoding="utf-8",
        )
        try:
            tmp_path.replace(out_path)  # rename atómico (Windows OK)
        except Exception:
            # Fallback no atómico
            out_path.write_text(tmp_path.read_text(encoding="utf-8"), encoding="utf-8")
            try: tmp_path.unlink()
            except Exception: pass
        console.log(f"[bold green]Guardado {out_path.name}")
        return out

    @staticmethod
    def _merge_preserve_richer(old: dict, new: dict) -> dict:
        """
        Merge incremental: para cada campo, mantener el más rico.
        - Strings: el más largo
        - Lists: la con más entries
        - Dicts: recursivo
        - Numéricos: el no-null (preferir new si ambos tienen)
        """
        def _richer(o, n):
            if n is None or n == "" or n == [] or n == {}:
                return o  # new vacío → mantener old
            if o is None or o == "" or o == [] or o == {}:
                return n  # old vacío → usar new
            if isinstance(o, str) and isinstance(n, str):
                return n if len(n) >= len(o) else o
            if isinstance(o, list) and isinstance(n, list):
                return n if len(n) >= len(o) else o
            if isinstance(o, dict) and isinstance(n, dict):
                merged = dict(o)
                for k, v in n.items():
                    merged[k] = _richer(o.get(k), v)
                return merged
            return n if n is not None else o

        return _richer(old, new)


# Alias retro-compatibilidad
IntlAgent = IntlExtractor


# ── CLI harness ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--nombre", default="")
    parser.add_argument("--gestora", default="")
    args = parser.parse_args()

    # Lock por fondo: aborta si ya hay otro extractor corriendo para este ISIN.
    # Evita race conditions cuando el bash background lanza pythons duplicados.
    from tools.process_lock import acquire_or_die
    acquire_or_die("extractor", args.isin)

    async def main():
        agent = IntlExtractor(
            args.isin,
            config={"nombre": args.nombre, "gestora": args.gestora},
        )
        result = await agent.run()
        print(json.dumps({
            "isin": result["isin"],
            "nombre": result["nombre"],
            "kpis": result["kpis"],
            "clases_count": len(result["clases"]),
            "serie_aum": result["cuantitativo"]["serie_aum"],
            "mix_activos": len(result["cuantitativo"]["mix_activos_historico"]),
            "consistencia": len(result["analisis_consistencia"]["periodos"]),
            "viabilidad": result["economia_fondo"]["viabilidad_nota"],
            "fuentes_informes": len(result["fuentes"]["informes_descargados"]),
            "fuentes_cartas": len(result["fuentes"]["cartas_gestores"]),
        }, ensure_ascii=False, indent=2, default=lambda o: getattr(o, "isoformat", lambda: str(o))()))

    asyncio.run(main())
