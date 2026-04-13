"""
CBI Agent — Central Bank of Ireland (fondos IE).

CONFIRMADO EMPÍRICAMENTE (2026-04-13): CBI NO publica annual reports
de UCITS. El registro público solo contiene identity (sin siquiera ISIN
— usan su propia numeración C-xxxxxx).

Estrategia:
  1. Si el caller ya tiene un nombre del fondo (hint), usarlo.
     Si no, intentar deducir del ISIN vía otro portal (fundinfo, Google…)
     → en este agente hacemos search con hint y aceptamos lo que encuentre.
  2. POST FundSearchPage.aspx con nombre del fondo → lista de matches
  3. Por cada match, GET FundRegisterDataPage?fundReferenceNumber=C..
     → identity (Management Company, Depositary, umbrella, sub-funds)
  4. documents = [] (CBI no los publica)

Output RegulatorOutput. El discovery se encarga de mapear ISIN↔C-ref si
hace falta vía fuentes externas.
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

from agents.regulator_schema import Identity, RegulatorOutput

console = Console()

SEARCH_URL = "https://registers.centralbank.ie/FundSearchPage.aspx"
DETAIL_URL = "https://registers.centralbank.ie/FundRegisterDataPage.aspx"


class CBIAgent:
    """Agente regulador CBI (IE). Solo identity (sin ISIN en registro CBI)."""

    def __init__(self, isin: str, config: dict | None = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        # Hint de nombre desde config — crítico porque CBI no indexa por ISIN
        self.fund_name_hint = (config or {}).get("nombre_fondo_hint", "") or (config or {}).get("nombre", "")
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold yellow]CBI (IE)[/bold yellow]\nISIN: [green]{self.isin}[/green]  "
            f"Hint: [dim]{self.fund_name_hint or '(ninguno)'}[/dim]",
            expand=False,
        ))

        out = RegulatorOutput(isin=self.isin, regulator="CBI")

        if not self.isin.startswith("IE"):
            out.notes = "CBI solo cubre fondos IE-domiciliados"
            self._save(out)
            return out.to_dict()

        if not self.fund_name_hint:
            out.notes = (
                "Sin nombre_fondo_hint. CBI no indexa por ISIN, "
                "el discovery tiene que proveer un hint externo."
            )
            self._save(out)
            return out.to_dict()

        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            ) as c:
                matches = await self._search(c, self.fund_name_hint)
                console.log(f"[cyan]{len(matches)} matches para '{self.fund_name_hint}'")

                if not matches:
                    out.notes = f"Sin matches en CBI para '{self.fund_name_hint}'"
                    self._save(out)
                    return out.to_dict()

                # Elegir el match que más se parezca (fuzzy simple)
                best = self._pick_best(matches, self.fund_name_hint)
                console.log(f"[cyan]Best match: {best['name']} ({best['ref']})")

                # GET detail
                detail = await self._fetch_detail(c, best["ref"])
                out.identity = Identity(
                    isin=self.isin,
                    nombre_oficial=detail.get("name", best["name"]),
                    sub_fondo=detail.get("name", best["name"]),
                    sicav_paraguas=detail.get("umbrella_name", ""),
                    gestora_oficial=detail.get("management_company", best.get("mgmt", "")),
                    depositario=detail.get("depositary", best.get("depositary", "")),
                    fecha_autorizacion=detail.get("date_approval", ""),
                    pais_domicilio="Ireland",
                    tipo_fondo=detail.get("status", "UCITS"),
                    estado="active",
                    internal_refs={
                        "cbi_ref": best["ref"],
                        "cbi_umbrella_ref": detail.get("umbrella_ref", ""),
                        "cbi_mgmt_ref": detail.get("management_company_ref", ""),
                        "cbi_depositary_ref": detail.get("depositary_ref", ""),
                    },
                )
                out.notes = (
                    "CBI no publica annual reports. Identity extraída del registro "
                    "público. El discovery debe obtener todos los documentos financieros."
                )
                out.source_urls.append(f"{DETAIL_URL}?fundReferenceNumber={best['ref']}&register=28")
        except Exception as exc:
            console.log(f"[red]CBI error: {exc}")
            out.notes = f"error: {exc}"

        self._save(out)
        return out.to_dict()

    async def _search(self, c: httpx.AsyncClient, name: str) -> list[dict]:
        r = await c.get(SEARCH_URL)
        soup = BeautifulSoup(r.text, "html.parser")
        vs = soup.find("input", {"name": "__VIEWSTATE"})["value"]
        vsg = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})["value"]
        evv = soup.find("input", {"name": "__EVENTVALIDATION"})["value"]
        post = {
            "__VIEWSTATE": vs,
            "__VIEWSTATEGENERATOR": vsg,
            "__EVENTVALIDATION": evv,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$cphRegistersMasterPage$txtFundNameSearch": name,
            "ctl00$cphRegistersMasterPage$ddlFunds": "All",
            "ctl00$cphRegistersMasterPage$btnFundNameSearch.x": "10",
            "ctl00$cphRegistersMasterPage$btnFundNameSearch.y": "10",
        }
        r = await c.post(SEARCH_URL, data=post)
        soup = BeautifulSoup(r.text, "html.parser")

        # Parse resultados
        results = []
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            link = cells[0].find("a", href=True)
            if not link or "FundRegisterDataPage" not in link.get("href", ""):
                continue
            m = re.search(r"fundReferenceNumber=([A-Z0-9]+)", link["href"])
            if not m:
                continue
            results.append({
                "ref": m.group(1),
                "name": link.get_text(strip=True),
                "aifm": cells[1].get_text(strip=True),
                "mgmt": cells[2].get_text(strip=True),
                "depositary": cells[3].get_text(strip=True),
            })
        return results

    def _pick_best(self, matches: list[dict], hint: str) -> dict:
        """Scoring simple: match exacto > match parcial del hint."""
        hint_low = hint.lower().strip()
        scored = []
        for m in matches:
            name_low = m["name"].lower()
            score = 0
            if hint_low == name_low:
                score = 100
            elif hint_low in name_low:
                score = 50 + len(hint_low)
            elif any(w in name_low for w in hint_low.split() if len(w) > 3):
                score = 20
            # Preferir sub-funds (tienen nombre largo) sobre umbrellas genéricos
            if len(m["name"]) > 30:
                score += 5
            scored.append((score, m))
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    async def _fetch_detail(self, c: httpx.AsyncClient, ref: str) -> dict:
        r = await c.get(f"{DETAIL_URL}?fundReferenceNumber={ref}&register=28")
        soup = BeautifulSoup(r.text, "html.parser")
        plain = soup.get_text(separator=" | ", strip=True)
        plain = re.sub(r"(\|\s*)+", " | ", plain)

        def _grab(pattern: str, default: str = "") -> str:
            m = re.search(pattern, plain)
            return m.group(1).strip() if m else default

        out = {
            "name": _grab(r"Name:\s*\|\s*([^|]+?)\s*\|"),
            "status": _grab(r"Status:\s*\|\s*([^|]+?)\s*\|"),
            "date_approval": _grab(r"Date of (?:Approval|Authorisation)\s*\|\s*([^|]+?)\s*\|"),
        }

        # Buscar referencias a Management Company, Depositary, Umbrella
        # Formato: "Management Company:| Reference Number | Name | C149382 | Fundrock..."
        for label, fld_prefix in [
            ("Management Company", "management_company"),
            ("Depositary", "depositary"),
            ("Umbrella Fund", "umbrella"),
        ]:
            m = re.search(
                rf"{label}:\s*\|\s*Reference\s*Number\s*\|\s*Name\s*\|\s*(C\d+)\s*\|\s*([^|]+?)\s*\|",
                plain, re.I,
            )
            if m:
                out[f"{fld_prefix}_ref"] = m.group(1)
                out[f"{fld_prefix}_name"] = m.group(2).strip()

        return out

    def _save(self, out: RegulatorOutput) -> None:
        output_path = self.fund_dir / "cbi_data.json"
        output_path.write_text(
            json.dumps(out.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {output_path.name}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", default="IE00B3Q8M574")
    parser.add_argument("--name", default="GAM Swiss Re Cat Bond")
    args = parser.parse_args()

    agent = CBIAgent(args.isin, {"nombre_fondo_hint": args.name})
    result = asyncio.run(agent.run())
    print(json.dumps({
        "isin": result["isin"],
        "identity": {k: v for k, v in result["identity"].items() if v},
    }, ensure_ascii=False, indent=2))
