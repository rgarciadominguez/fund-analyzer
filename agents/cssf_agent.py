"""
CSSF Agent — Regulador luxemburgués (fondos LU).

CONFIRMADO EMPÍRICAMENTE (2026-04-13): CSSF NO publica annual reports
de UCITS. Solo publica un CSV maestro con identifiers de todos los UCITS
autorizados en Luxemburgo (OPC_COMP_TP_TOUS_OUVERTS.zip).

Estrategia:
  1. Descargar CSV maestro (cacheado local, refresh diario)
  2. Lookup por ISIN → identity card
  3. documents = [] (obligatoriamente; todo lo demás lo busca el discovery)

Output estándar RegulatorOutput.
"""
from __future__ import annotations

import asyncio
import io
import json
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.regulator_schema import Identity, RegulatorOutput

console = Console()

CSSF_CSV_URL = "https://www.cssf.lu/wp-content/uploads/OPC_COMP_TP_TOUS_OUVERTS.zip"
CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"
CACHE_PATH = CACHE_DIR / "cssf_ucits.csv"
CACHE_TTL_HOURS = 24


async def _ensure_cached_csv() -> Path:
    """Descarga el CSV CSSF si no existe o está stale (>24h). Cachea local."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if CACHE_PATH.exists():
        age = datetime.now() - datetime.fromtimestamp(CACHE_PATH.stat().st_mtime)
        if age < timedelta(hours=CACHE_TTL_HOURS):
            return CACHE_PATH

    console.log("[cyan]Descargando CSSF UCITS CSV...")
    async with httpx.AsyncClient(timeout=60, headers={"User-Agent": "Mozilla/5.0"}) as c:
        r = await c.get(CSSF_CSV_URL)
        r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    with z.open(z.namelist()[0]) as f:
        CACHE_PATH.write_bytes(f.read())
    console.log(f"[green]CSSF CSV cacheado: {CACHE_PATH} ({CACHE_PATH.stat().st_size/1024:.0f} KB)")
    return CACHE_PATH


def _lookup(csv_path: Path, isin: str) -> dict | None:
    """Busca el ISIN en el CSV UTF-16 tab-separado de CSSF."""
    text = csv_path.read_text(encoding="utf-16", errors="replace")
    lines = text.split("\n")
    for line in lines[2:]:  # skip header + separator row
        cols = [c.strip() for c in line.split("\t")]
        if len(cols) < 10:
            continue
        if cols[2] == isin:
            return {
                "isin": cols[2],
                "opc_name": cols[3],        # nombre del SICAV/FCP
                "compartment_id": cols[4],  # CCCCCCCC
                "compartment_name": cols[5],  # nombre del sub-fondo
                "authorization_date": cols[6].strip(),
                "currency": cols[7].strip(),
                "share_class_id": cols[8],
                "share_class_name": cols[9].strip() if len(cols) > 9 else "",
            }
    return None


class CSSFAgent:
    """Agente regulador CSSF (LU). Solo identity card."""

    def __init__(self, isin: str, config: dict | None = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold cyan]CSSF (LU)[/bold cyan]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        out = RegulatorOutput(isin=self.isin, regulator="CSSF")

        if not self.isin.startswith("LU"):
            out.notes = "CSSF solo cubre fondos LU-domiciliados"
            self._save(out)
            return out.to_dict()

        try:
            csv_path = await _ensure_cached_csv()
            record = _lookup(csv_path, self.isin)
            if record is None:
                out.notes = "ISIN no encontrado en CSSF UCITS register (no autorizado en LU)"
                out.identity.estado = "unknown"
                self._save(out)
                return out.to_dict()

            _fmt_date = record["authorization_date"]
            if "/" in _fmt_date and len(_fmt_date) == 10:
                d, m, y = _fmt_date.split("/")
                _fmt_date = f"{y}-{m}-{d}"

            out.identity = Identity(
                isin=self.isin,
                nombre_oficial=f'{record["opc_name"]} - {record["compartment_name"]}'.strip(" -"),
                sub_fondo=record["compartment_name"],
                sicav_paraguas=record["opc_name"],
                fecha_autorizacion=_fmt_date,
                pais_domicilio="Luxembourg",
                tipo_fondo="UCITS",
                estado="active",
                moneda=record["currency"],
                clase=record["share_class_name"],
                internal_refs={
                    "cssf_opc_id": "",          # no expuesto en CSV
                    "cssf_compartment_id": record["compartment_id"],
                    "cssf_share_class_id": record["share_class_id"],
                },
            )
            out.notes = (
                "CSSF no publica annual reports. Identity card extraída del "
                "CSV público OPC_COMP_TP_TOUS_OUVERTS. Todos los documentos "
                "financieros deben venir del agente de discovery."
            )
            out.source_urls.append(CSSF_CSV_URL)
        except Exception as exc:
            console.log(f"[red]CSSF error: {exc}")
            out.notes = f"error: {exc}"

        self._save(out)
        return out.to_dict()

    def _save(self, out: RegulatorOutput) -> None:
        output_path = self.fund_dir / "cssf_data.json"
        output_path.write_text(
            json.dumps(out.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path.name}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="LU1694789451")
    args = parser.parse_args()

    agent = CSSFAgent(args.isin)
    result = asyncio.run(agent.run())
    print(json.dumps({
        "isin": result["isin"],
        "identity": {k: v for k, v in result["identity"].items() if v},
    }, ensure_ascii=False, indent=2))
