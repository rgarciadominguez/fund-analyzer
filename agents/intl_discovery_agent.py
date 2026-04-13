"""
INT Discovery Agent — orquestador de los tracks de búsqueda para fondos
internacionales (LU, IE, FR, DE, UK).

NOTA: distinto de discovery_agent.py (ese cubre el flujo ES de manager_deep
+ letters + readings). Este es específico para INT y consume el output de
regulator_router.

Input:
  - isin, identity (del regulator), gap (del regulator_router.compute_gap)
  - web_search_fn: callable async (query: str) -> list[{title, url}]
    — inyectado por el orchestrator. Si es None usa un stub offline.

Output JSON: data/funds/{ISIN}/intl_discovery_data.json
  {
    "isin", "ultima_actualizacion",
    "budget_spent": {...},
    "documents": [ {doc_type, periodo, url, local_path, source, ...}, ... ],
    "still_missing": [ {doc_type, periodo}, ... ],
    "notes": ""
  }

Los dos tracks (reports + commercial) corren en paralelo con el mismo
SharedState. La cascada por track: KB → crawl de gestora → Google.
"""
from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.discovery import kb as kb_mod
from agents.discovery.gestora_crawler import crawl_gestora
from agents.discovery.state import SharedState
from agents.discovery.tracks import run_commercial_track, run_reports_track

console = Console()


async def _default_web_search(query: str) -> list[dict]:
    """
    Stub offline por defecto. El orchestrator real debe pasar una implementación
    (Google Search via tools.google_search o WebSearch API de Claude).
    """
    return []


class IntlDiscoveryAgent:
    def __init__(
        self,
        isin: str,
        identity: dict,
        gap: dict,
        web_search_fn=None,
        config: dict | None = None,
    ):
        self.isin = isin.strip().upper()
        self.identity = identity or {"isin": self.isin}
        self.gap = gap or {}
        self.web_search_fn = web_search_fn or _default_web_search
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold yellow]IntlDiscoveryAgent[/bold yellow]\n"
            f"ISIN: [green]{self.isin}[/green]  "
            f"Gestora: [cyan]{self.identity.get('gestora_oficial','-')}[/cyan]",
            expand=False,
        ))

        kb_data = kb_mod.load_kb(self.fund_dir, self.isin)
        state = SharedState(
            isin=self.isin,
            identity=self.identity,
            gap=self.gap,
            fund_dir=self.fund_dir,
            kb=kb_data,
        )

        targets = state.missing_doc_targets()
        console.log(f"[yellow]Targets iniciales: {len(targets)}")
        for t in targets[:10]:
            console.log(f"  - {t[0]}@{t[1] or 'latest'}")

        if state.is_fully_covered():
            console.log("[green]Nada que buscar, todo ya cubierto por el regulator.")
            self._save(state)
            return self._to_output(state)

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=60,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as c:
            # 1) Crawl de gestora UNA vez, resultado compartido por ambos tracks
            gestora = self.identity.get("gestora_oficial", "")
            candidates: list[dict] = []
            if gestora:
                console.log(f"[blue]Crawl gestora: {gestora}")
                candidates = await crawl_gestora(state, c, gestora)
                console.log(f"[blue]  {len(candidates)} candidatos del crawl")

            # 2) Dos tracks en paralelo con el mismo state
            await asyncio.gather(
                run_reports_track(state, c, candidates, self.web_search_fn),
                run_commercial_track(state, c, candidates, self.web_search_fn),
            )

        # 3) Persistir KB actualizado + guardar output
        kb_mod.save_kb(self.fund_dir, state.kb)
        self._save(state)
        return self._to_output(state)

    def _to_output(self, state: SharedState) -> dict:
        docs = []
        for d in state.downloaded_docs:
            dd = asdict(d)
            dd["contains"] = sorted(list(d.contains))
            docs.append(dd)
        still_missing = [
            {"doc_type": dt, "periodo": p}
            for dt, p in state.missing_doc_targets()
        ]
        return {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(timespec="seconds"),
            "budget_spent": {
                "http_used": 60 - state.budget.http_remaining,
                "google_used": 10 - state.budget.google_remaining,
                "llm_used": 4 - state.budget.llm_remaining,
            },
            "documents": docs,
            "still_missing": still_missing,
            "fetched_urls_count": len(state.fetched_urls),
            "notes": "",
        }

    def _save(self, state: SharedState) -> None:
        output = self._to_output(state)
        out_path = self.fund_dir / "intl_discovery_data.json"
        out_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.log(f"[bold green]Guardado: {out_path.name}  "
                    f"({len(output['documents'])} docs, "
                    f"{len(output['still_missing'])} aún faltan)")


# ── CLI / test harness ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    from agents.regulator_router import compute_gap, run_regulator

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--name", default="")
    parser.add_argument("--horizonte", default="3")
    args = parser.parse_args()

    async def main():
        config = {"horizonte_historico": args.horizonte}
        if args.name:
            config["nombre_fondo_hint"] = args.name
            config["nombre"] = args.name
        regulator_out = await run_regulator(args.isin, config)
        gap = compute_gap(regulator_out, config)
        agent = IntlDiscoveryAgent(
            args.isin,
            identity=regulator_out["identity"],
            gap=gap,
            web_search_fn=None,  # offline stub; activa via orchestrator real
            config=config,
        )
        result = await agent.run()
        print(json.dumps({
            "isin": result["isin"],
            "budget": result["budget_spent"],
            "docs_count": len(result["documents"]),
            "docs": [
                {"doc_type": d["doc_type"], "periodo": d["periodo"],
                 "source": d["source"], "url": d["url"][-80:]}
                for d in result["documents"]
            ],
            "still_missing_count": len(result["still_missing"]),
            "still_missing": result["still_missing"][:10],
        }, ensure_ascii=False, indent=2, default=str))

    asyncio.run(main())
