"""
Bundesanzeiger Agent — Regulador alemán (fondos DE-domiciliados).

En Alemania los fondos están obligados por ley (KAGB) a publicar
Jahresberichte (annual reports) y Halbjahresberichte (semi-annual reports)
en la Federal Gazette (Bundesanzeiger). Es el mejor regulador europeo
para extracción automatizada de reports por ISIN.

Flujo:
  1. Bootstrap sesión (cookies Wicket) en /pub/en/start
  2. POST search con fulltext=ISIN
  3. Parseo del result table: lista de publicaciones con tipo, fecha, ISIN
  4. Para cada Jahresbericht / Halbjahresbericht → click detail → guarda HTML
     completo (Bundesanzeiger renderiza el annual report íntegro en HTML)

Confirmado empíricamente (2026-04-13):
  DE0008476524 (DWS Vermögensbildungsfonds I) → 177 publicaciones históricas.
  Detail de un Jahresbericht = 127KB de contenido HTML extraíble.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.regulator_schema import Document, Identity, RegulatorOutput

console = Console()

BASE = "https://www.bundesanzeiger.de"
START_URL = f"{BASE}/pub/en/start?0"

# Mapeo tipo de publicación Bundesanzeiger → DocType estándar.
# ORDEN IMPORTA: halbjahres (semi) antes de jahres (annual), porque
# "Halbjahresbericht" contiene "jahresbericht" como substring.
DOC_TYPE_MAP = [
    (re.compile(r"halbjahresbericht", re.I), "semi_annual_report"),
    (re.compile(r"quartalsbericht", re.I), "quarterly_report"),
    (re.compile(r"monatsbericht", re.I), "monthly_report"),
    (re.compile(r"jahresbericht", re.I), "annual_report"),
    (re.compile(r"prospekt", re.I), "prospectus"),
    (re.compile(r"anlagebedingungen", re.I), "regulation"),
    (re.compile(r"wesentliche\s+anlegerinformationen|kid|kiid", re.I), "kid"),
]


def _classify(text: str) -> str:
    for pat, kind in DOC_TYPE_MAP:
        if pat.search(text):
            return kind
    return "other"


def _extract_period(text: str) -> str:
    """
    Extrae el período fiscal del título de la publicación.
    Ej: 'Jahresbericht 01.10.2024 bis 30.09.2025' → '2024-10-01..2025-09-30'
        'Halbjahresbericht 01.10.2024 bis 31.03.2025' → '2024-10-01..2025-03-31'
    """
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})\s*bis\s*(\d{2})\.(\d{2})\.(\d{4})", text)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{y1}-{m1}-{d1}..{y2}-{m2}-{d2}"
    m = re.search(r"\b(\d{4})\b", text)
    if m:
        return m.group(1)
    return ""


class BundesanzeigerAgent:
    """Agente del regulador alemán. Output: RegulatorOutput con HTML completo."""

    def __init__(self, isin: str, config: dict | None = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.raw_dir = self.fund_dir / "raw" / "bundesanzeiger"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold magenta]Bundesanzeiger[/bold magenta]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        out = RegulatorOutput(isin=self.isin, regulator="BUNDESANZEIGER")

        if not self.isin.startswith("DE"):
            out.notes = "Bundesanzeiger solo cubre fondos DE-domiciliados"
            self._save(out)
            return out.to_dict()

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=60,
                headers={"User-Agent": "Mozilla/5.0"},
            ) as c:
                entries = await self._search(c)
                console.log(f"[cyan]{len(entries)} publicaciones encontradas")

                # Identity desde la primera fila (nombre de la gestora y del fondo)
                if entries:
                    first = entries[0]
                    out.identity = Identity(
                        isin=self.isin,
                        nombre_oficial=first.get("fund_name", ""),
                        gestora_oficial=first.get("issuer", ""),
                        pais_domicilio="Germany",
                        tipo_fondo="UCITS",
                        estado="active",
                    )

                # Filtrar: annual + semi-annual (+ prospectus como bonus)
                keep_types = {"annual_report", "semi_annual_report", "prospectus", "regulation"}
                for e in entries:
                    dt = _classify(e["title"])
                    if dt not in keep_types:
                        continue

                    doc = Document(
                        doc_type=dt,
                        periodo=_extract_period(e["title"]),
                        title=e["title"],
                        url=e["href"],
                        content_type="html",
                        source="regulator",
                        source_detail="Bundesanzeiger",
                    )

                    # Solo descargamos annual y semi-annual por defecto
                    if dt in ("annual_report", "semi_annual_report"):
                        saved = await self._fetch_detail(c, e["href"], e["title"])
                        if saved:
                            doc.downloaded_path = str(saved.relative_to(self.fund_dir))
                            doc.size_bytes = saved.stat().st_size
                            doc.download_ok = True
                            doc.validated = self._validate(saved)

                    out.documents.append(doc)

                out.source_urls.append(START_URL)

        except Exception as exc:
            console.log(f"[red]Bundesanzeiger error: {exc}")
            out.notes = f"error: {exc}"

        self._save(out)
        return out.to_dict()

    async def _search(self, c: httpx.AsyncClient) -> list[dict]:
        """Bootstrap + POST search + parseo del result table."""
        r = await c.get(START_URL)
        soup = BeautifulSoup(r.text, "html.parser")
        form = next(
            (f for f in soup.find_all("form")
             if "start" in (f.get("action", "") or "").lower()
             and "login" not in (f.get("action", "") or "").lower()),
            None,
        )
        if not form:
            raise RuntimeError("No encuentro el formulario de búsqueda en Bundesanzeiger")

        data = {
            f.get("name"): (f.get("value") or "")
            for f in form.find_all(["input", "select"])
            if f.get("name")
        }
        data["fulltext"] = self.isin

        r = await c.get(f"{BASE}/pub/en/start", params=data)
        soup = BeautifulSoup(r.text, "html.parser")
        entries = self._parse_results(soup, r.text)
        return entries

    def _parse_results(self, soup: BeautifulSoup, raw_html: str) -> list[dict]:
        """
        Bundesanzeiger no usa <table>: renderiza el listado con divs.
        Cada fila tiene un <a> con el título (Jahresbericht..., Halbjahresbericht...),
        y cerca aparece la fecha de validez (dd/mm/yyyy) y el issuer (DWS Investment GmbH, ...).
        """
        entries = []
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if not text:
                continue
            if any(k in text.lower() for k in (
                "jahresbericht", "halbjahresbericht", "prospekt",
                "anlagebedingungen", "quartalsbericht",
            )):
                href = a["href"]
                if not href.startswith("http"):
                    href = BASE + href

                # Busca la fila completa del resultado (padre común)
                row_text = ""
                issuer = ""
                fund_name = ""
                node = a
                for _ in range(6):
                    node = node.parent if node and node.parent else None
                    if node is None:
                        break
                    row_text = node.get_text(" | ", strip=True)
                    if self.isin in row_text:
                        break
                # issuer suele ser la primera línea de la fila
                parts = [p.strip() for p in row_text.split("|") if p.strip()]
                for p in parts:
                    if "GmbH" in p or "KAG" in p or "KVG" in p or "Management" in p:
                        issuer = p
                        break
                # fund_name: suele aparecer en la misma fila junto al ISIN
                # (muchas filas tienen el nombre del fondo justo antes del ISIN)
                if self.isin in row_text:
                    idx = row_text.find(self.isin)
                    before = row_text[:idx].rsplit("|", 1)[-1].strip()
                    if before:
                        fund_name = before

                entries.append({
                    "title": text,
                    "href": href,
                    "issuer": issuer,
                    "fund_name": fund_name,
                })
        return entries

    async def _fetch_detail(
        self, c: httpx.AsyncClient, url: str, title: str,
    ) -> Path | None:
        """Descarga la página de detalle con el annual report íntegro en HTML."""
        try:
            r = await c.get(url)
            soup = BeautifulSoup(r.text, "html.parser")
            # Extraer SOLO el contenedor de la publicación
            container = soup.select_one(".publication_container") or soup.select_one("main")
            content_html = str(container) if container else r.text

            # Nombre de fichero seguro
            safe_title = re.sub(r"[^\w\-_.]", "_", title)[:80]
            filename = f"{safe_title}.html"
            out_path = self.raw_dir / filename
            out_path.write_text(content_html, encoding="utf-8")
            return out_path
        except Exception as exc:
            console.log(f"[yellow]Error detail {title[:40]}: {exc}")
            return None

    def _validate(self, path: Path) -> bool:
        """Verifica que el HTML descargado contiene el ISIN del fondo objetivo."""
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
            return self.isin in content
        except Exception:
            return False

    def _save(self, out: RegulatorOutput) -> None:
        output_path = self.fund_dir / "bundesanzeiger_data.json"
        output_path.write_text(
            json.dumps(out.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path.name}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="DE0008476524")
    args = parser.parse_args()

    agent = BundesanzeigerAgent(args.isin)
    result = asyncio.run(agent.run())
    print(json.dumps({
        "isin": result["isin"],
        "regulator": result["regulator"],
        "docs": len(result["documents"]),
        "downloaded": sum(1 for d in result["documents"] if d["download_ok"]),
    }, indent=2))
