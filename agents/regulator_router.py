"""
Regulator router + gap analysis.

Punto de entrada del pipeline INT.
Rutea cada ISIN al agente de regulador correcto y calcula la lista de
documentos que siguen faltando (gap). El gap es el input del discovery.

Uso:
    from agents.regulator_router import run_regulator, compute_gap

    regulator_out = await run_regulator(isin, config)
    gap = compute_gap(regulator_out, config)
    # discovery(regulator_out, gap, config) ← pendiente
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.regulator_schema import RegulatorOutput, Document, DocType
from agents.bundesanzeiger_agent import BundesanzeigerAgent
from agents.cssf_agent import CSSFAgent
from agents.amf_agent import AMFAgent
from agents.cbi_agent import CBIAgent


# Mapping prefijo ISIN → clase del agente regulador
ROUTING: dict[str, type] = {
    "DE": BundesanzeigerAgent,
    "FR": AMFAgent,
    "LU": CSSFAgent,
    "IE": CBIAgent,
    # ES usa el cnmv_agent existente (ruta separada ya implementada)
    # GB/UK: sin regulador útil → skip directo al discovery
}


async def run_regulator(isin: str, config: dict | None = None) -> dict:
    """
    Ejecuta el agente de regulador correspondiente al prefijo del ISIN.
    Devuelve el dict de RegulatorOutput. Si no hay regulador útil (UK,
    prefijo no cubierto), devuelve un RegulatorOutput vacío para que el
    discovery sepa que tiene que hacer TODO el trabajo.
    """
    prefix = isin[:2].upper()
    agent_cls = ROUTING.get(prefix)

    if agent_cls is None:
        # Sin regulador útil: devolvemos output vacío con nota
        out = RegulatorOutput(isin=isin.upper(), regulator="NONE")
        out.notes = (
            f"Sin agente de regulador para prefijo {prefix}. "
            "Discovery debe hacer identity + todos los documentos."
        )
        return out.to_dict()

    agent = agent_cls(isin, config or {})
    return await agent.run()


# ── Gap analysis ─────────────────────────────────────────────────────────────

def _years_since_inception(inception: str, until_year: int | None = None) -> list[int]:
    """Años fiscales desde inception hasta until_year (incluidos)."""
    if not inception:
        return []
    try:
        start = int(inception[:4])
    except Exception:
        return []
    until = until_year or datetime.now().year
    return list(range(start, until + 1))


def _period_matches_year(periodo: str, year: int) -> bool:
    """Un doc.periodo representa un año dado si contiene ese año en 'YYYY'."""
    if not periodo:
        return False
    return str(year) in periodo


def compute_gap(regulator_out: dict, config: dict | None = None) -> dict:
    """
    Calcula qué documentos faltan por descargar.

    Devuelve un dict con:
      - target_years: años fiscales que deberíamos cubrir
      - missing_annual_reports: lista de años sin AR descargado
      - missing_semi_annual_reports: lista de años sin SAR descargado
      - missing_prospectus: bool (siempre queremos el más reciente)
      - missing_identity_fields: campos vacíos en la identity card
      - extra_doc_types: tipos adicionales que discovery debe buscar
        siempre (cartas trimestrales, factsheets, etc. — ningún
        regulador los publica)
    """
    config = config or {}
    identity = regulator_out.get("identity", {}) or {}
    docs = regulator_out.get("documents", []) or []

    # 1) Años objetivo
    horizonte = (config.get("horizonte_historico") or "1")  # "1"=desde inception, "2"=últimos 10…
    until = datetime.now().year
    if horizonte == "4":  # solo último año
        target_years = [until - 1, until]
    elif horizonte == "3":  # 3 años
        target_years = list(range(until - 3, until + 1))
    elif horizonte == "2":  # 5 años
        target_years = list(range(until - 5, until + 1))
    else:  # desde inception
        target_years = _years_since_inception(
            identity.get("fecha_autorizacion", ""), until,
        ) or list(range(until - 5, until + 1))

    # 2) Qué tenemos descargado por año y tipo
    downloaded = [d for d in docs if d.get("download_ok")]
    ar_years = {y for y in target_years if any(
        d["doc_type"] == "annual_report" and _period_matches_year(d.get("periodo", ""), y)
        for d in downloaded
    )}
    sar_years = {y for y in target_years if any(
        d["doc_type"] == "semi_annual_report" and _period_matches_year(d.get("periodo", ""), y)
        for d in downloaded
    )}

    missing_ar = sorted(set(target_years) - ar_years)
    missing_sar = sorted(set(target_years) - sar_years)

    # 3) Hints útiles del regulador para el discovery
    regulator_hints = []
    for d in docs:
        if not d.get("download_ok") and d.get("doc_type") in ("annual_report", "semi_annual_report"):
            # El regulador tiene el doc pero no descargable (típico AMF)
            hint = {
                "doc_type": d["doc_type"],
                "periodo": d.get("periodo", ""),
                "expected_filename": "",
                "title": d.get("title", ""),
                "source_detail": d.get("source_detail", ""),
            }
            # Parsear docName del notes (lo hace AMF)
            notes = d.get("notes", "") or ""
            if "docName=" in notes:
                hint["expected_filename"] = notes.split("docName=", 1)[1].split(";", 1)[0].strip()
            regulator_hints.append(hint)

    # 4) Identity gaps
    identity_required = ["nombre_oficial", "gestora_oficial", "fecha_autorizacion"]
    identity_missing = [f for f in identity_required if not identity.get(f)]

    # 5) Tipos de documento que el regulador NUNCA da → siempre al discovery
    extras = [
        "quarterly_letter",
        "factsheet",
        "manager_presentation",
        "newsletter",
    ]

    return {
        "target_years": target_years,
        "missing_annual_reports": missing_ar,
        "missing_semi_annual_reports": missing_sar,
        "regulator_hints": regulator_hints,
        "identity_missing": identity_missing,
        "extra_doc_types": extras,
        # Pasamos la identity completa para que discovery tenga todo el contexto
        "identity": identity,
    }


# ── Main para pruebas ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import asyncio
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--name", default="", help="Hint de nombre (CBI lo necesita)")
    parser.add_argument("--horizonte", default="1")
    args = parser.parse_args()

    async def main():
        config = {"horizonte_historico": args.horizonte}
        if args.name:
            config["nombre_fondo_hint"] = args.name
            config["nombre"] = args.name
        regulator_out = await run_regulator(args.isin, config)
        gap = compute_gap(regulator_out, config)
        print(json.dumps({
            "regulator": regulator_out["regulator"],
            "identity": {k: v for k, v in regulator_out["identity"].items() if v},
            "docs_downloaded": sum(1 for d in regulator_out["documents"] if d["download_ok"]),
            "gap": gap,
        }, ensure_ascii=False, indent=2, default=str))

    asyncio.run(main())
