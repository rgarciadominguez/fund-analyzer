"""
AMF Agent — Regulador francés (fondos FR).

Reverse-engineered del SPA GECO (Angular) en geco.amf-france.org.
API base pública (sin auth) en https://geco.amf-france.org/back-office.

Estrategia:
  1. ISIN → parId/cmpId/idInterne vía /funds/shareByCmpCodeParPrincp/{ISIN}
  2. cmpId → prdId + identity del compartiment vía /funds/compartment/{cmpId}
  3. Docs KID del share vía /document/byShare/{idInterne}
  4. Docs a nivel compartment+producto (annual, semi-annual, prospectus...)
     vía /document/byCompartAndProduct?compartCode=&prdCode=

LIMITACIÓN (confirmada empíricamente 2026-04-13):
  /document/download/{docId} devuelve 500 (requiere auth en ROSA).
  → Este agente entrega METADATA únicamente (tipos, fechas, nombres de
    fichero). Los binarios deben resolverse por el discovery usando los
    nombres de fichero devueltos como hints para Google/web gestora.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.regulator_schema import Document, Identity, RegulatorOutput

console = Console()

BASE = "https://geco.amf-france.org/back-office"

# Mapeo docTypeLib de AMF → DocType estándar
DOC_TYPE_MAP = [
    (re.compile(r"rapport\s+annuel.*semestriel", re.I), "semi_annual_report"),
    (re.compile(r"rapport\s+annuel", re.I), "annual_report"),
    (re.compile(r"rapport\s+semestriel", re.I), "semi_annual_report"),
    (re.compile(r"comptes\s+annuels", re.I), "annual_report"),
    (re.compile(r"comptes\s+semestriels", re.I), "semi_annual_report"),
    (re.compile(r"prospectus", re.I), "prospectus"),
    (re.compile(r"règlement|reglement", re.I), "regulation"),
    (re.compile(r"dic\s+priips|kid|kiid", re.I), "kid"),
]


def _classify(text: str) -> str:
    for pat, kind in DOC_TYPE_MAP:
        if pat.search(text or ""):
            return kind
    return "other"


def _extract_period(date_effet: str, doc_name: str) -> str:
    """
    dateEffet viene tipo '2025-03-31T00:00:00' → devolvemos 'YYYY-MM-DD'.
    Si está vacío intentamos parsear del docName (patrón YYYYMMDD).
    """
    if date_effet:
        return date_effet[:10]
    m = re.search(r"\b(20\d{2})(\d{2})(\d{2})\b", doc_name or "")
    if m:
        y, mo, d = m.groups()
        return f"{y}-{mo}-{d}"
    return ""


class AMFAgent:
    """Agente regulador AMF (FR). Metadata-only (no binarios)."""

    def __init__(self, isin: str, config: dict | None = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold blue]AMF (FR)[/bold blue]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        out = RegulatorOutput(isin=self.isin, regulator="AMF")

        if not self.isin.startswith("FR"):
            out.notes = "AMF GECO solo cubre fondos FR-domiciliados"
            self._save(out)
            return out.to_dict()

        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            ) as c:
                # Paso 1: ISIN → share metadata
                share = await self._get_share(c)
                if not share:
                    out.notes = "ISIN no encontrado en AMF GECO"
                    self._save(out)
                    return out.to_dict()

                # Paso 2: compartment detail
                comp = await self._get_compartment(c, share["cmpId"])

                # Identity
                out.identity = Identity(
                    isin=self.isin,
                    nombre_oficial=(comp.get("cmpNom") or "") + (
                        f' - {share.get("parNom", "")}' if share.get("parNom") else ""
                    ),
                    sub_fondo=comp.get("cmpNom", ""),
                    gestora_oficial=comp.get("gestionnaire", ""),
                    pais_domicilio="France",
                    tipo_fondo=comp.get("prdFamlLib", ""),
                    estado="active" if share.get("parStatutCode") == "VIV" else share.get("parStatutLib", "unknown"),
                    moneda=share.get("parRefDevCode", ""),
                    clase=share.get("parNom", ""),
                    fecha_autorizacion=(share.get("parDateCreation") or "")[:10],
                    internal_refs={
                        "amf_parId": share.get("parId", ""),
                        "amf_cmpId": share.get("cmpId", ""),
                        "amf_idInterne_share": share.get("idInterne"),
                        "amf_idInterne_compartment": comp.get("idInterne"),
                        "amf_prdId": comp.get("prdId", ""),
                        "amf_cmpAmfId": comp.get("cmpAmfId", ""),
                    },
                )

                # Paso 3: docs del share (KIDs típicamente)
                share_docs = await self._get_share_docs(c, share["idInterne"])
                # Paso 4: docs del compartment+producto (annual, semi-annual, prospectus...)
                comp_docs = await self._get_compartment_docs(
                    c, share["cmpId"], comp.get("prdId", ""),
                )

                for d in share_docs + comp_docs:
                    doc = Document(
                        doc_type=_classify(d.get("docTypeLib", "")),
                        periodo=_extract_period(d.get("dateEffet", ""), d.get("docName", "")),
                        title=d.get("docTypeLib", ""),
                        url="",  # el endpoint de download devuelve 500, dejamos vacío
                        content_type="pdf",
                        source="regulator",
                        source_detail="AMF GECO",
                        notes=(
                            f'docId={d.get("docId")}; docName={d.get("docName","")}'
                        ),
                    )
                    # download_ok=False deliberadamente (AMF no deja descargar público)
                    out.documents.append(doc)

                out.source_urls.append(f"https://geco.amf-france.org/produit-d-epargne/part/{self.isin}")
                out.notes = (
                    "AMF entrega metadata (tipos, fechas, nombres de fichero) pero "
                    "NO permite descarga pública del binario. "
                    "Pasa los nombres en notes al discovery para resolver el PDF."
                )
        except Exception as exc:
            console.log(f"[red]AMF error: {exc}")
            out.notes = f"error: {exc}"

        self._save(out)
        return out.to_dict()

    async def _get_share(self, c: httpx.AsyncClient) -> dict | None:
        url = f"{BASE}/funds/shareByCmpCodeParPrincp/{self.isin}"
        r = await c.get(url)
        if r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception:
            return None
        # Endpoint devuelve '[]' o '{}' para misses
        return data if isinstance(data, dict) and data.get("parId") else None

    async def _get_compartment(self, c: httpx.AsyncClient, cmp_id: str) -> dict:
        r = await c.get(f"{BASE}/funds/compartment/{cmp_id}")
        if r.status_code == 200 and r.text.strip().startswith("{"):
            return r.json()
        return {}

    async def _get_share_docs(self, c: httpx.AsyncClient, id_interne: int) -> list[dict]:
        r = await c.get(f"{BASE}/document/byShare/{id_interne}")
        if r.status_code == 200 and r.text.strip().startswith("["):
            return r.json()
        return []

    async def _get_compartment_docs(
        self, c: httpx.AsyncClient, cmp_id: str, prd_id: str,
    ) -> list[dict]:
        if not (cmp_id and prd_id):
            return []
        r = await c.get(
            f"{BASE}/document/byCompartAndProduct",
            params={"compartCode": cmp_id, "prdCode": prd_id},
        )
        if r.status_code == 200 and r.text.strip().startswith("["):
            return r.json()
        return []

    def _save(self, out: RegulatorOutput) -> None:
        output_path = self.fund_dir / "amf_data.json"
        output_path.write_text(
            json.dumps(out.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path.name}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="FR0000989626")
    args = parser.parse_args()

    agent = AMFAgent(args.isin)
    result = asyncio.run(agent.run())
    print(json.dumps({
        "isin": result["isin"],
        "identity": {k: v for k, v in result["identity"].items() if v},
        "docs_count": len(result["documents"]),
        "docs": [
            {"type": d["doc_type"], "period": d["periodo"], "title": d["title"], "notes": d["notes"][:80]}
            for d in result["documents"]
        ],
    }, ensure_ascii=False, indent=2))
