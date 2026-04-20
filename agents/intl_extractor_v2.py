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
    for date, total_eur in by_date.items():
        year = date[:4]
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
    # AUM actual = máximo año
    if out["cuantitativo"]["serie_aum"]:
        latest = max(out["cuantitativo"]["serie_aum"], key=lambda e: str(e.get("periodo", "")))
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
        for date, total_eur in by_date.items():
            year = date[:4]
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
        if out["cuantitativo"]["serie_aum"]:
            latest = max(out["cuantitativo"]["serie_aum"], key=lambda e: str(e.get("periodo", "")))
            out["kpis"]["aum_actual_meur"] = latest.get("valor_meur")


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
            "sector": h.get("sector", ""),
            "pais": h.get("country", ""),
            "racional": h.get("rationale", ""),
        })
    if not out["posiciones"]["actuales"]:
        out["posiciones"]["actuales"] = normalized[:10]
    as_of = v.get("as_of_date") or ""
    year = str(as_of)[:4]
    if year:
        if not any(e.get("periodo") == year for e in out["posiciones"]["historicas"]):
            out["posiciones"]["historicas"].append({"periodo": year, "top10": normalized[:10]})


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

        out_path.write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
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
        }, ensure_ascii=False, indent=2, default=str))

    asyncio.run(main())
