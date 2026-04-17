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

from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.discovery.state import SharedState

console = Console()


async def _default_web_search(query: str) -> list[dict]:
    """
    Stub offline por defecto. Se reemplaza al instanciar con Serper si hay key.
    """
    return []


def _serper_or_stub(isin: str):
    """Devuelve el web_search_fn de Serper si SERPER_API_KEY está, sino el stub."""
    import os
    if not os.getenv("SERPER_API_KEY"):
        try:
            from dotenv import load_dotenv
            load_dotenv(Path(__file__).parent.parent / ".env")
        except Exception:
            pass
    if os.getenv("SERPER_API_KEY"):
        from agents.discovery.serper_adapter import make_web_search_fn
        return make_web_search_fn(isin)
    return _default_web_search


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
        # Si el caller no inyecta función, usar Serper si hay API key, sino stub
        self.web_search_fn = web_search_fn or _serper_or_stub(self.isin)
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> dict:
        console.print(Panel(
            f"[bold yellow]IntlDiscoveryAgent (v2)[/bold yellow]\n"
            f"ISIN: [green]{self.isin}[/green]  "
            f"Gestora: [cyan]{self.identity.get('gestora_oficial','-')}[/cyan]",
            expand=False,
        ))

        from agents.discovery_v2 import DiscoveryV2
        pipeline = DiscoveryV2(
            isin=self.isin,
            identity=self.identity,
            gap=self.gap,
            fund_dir=self.fund_dir,
            web_search_fn=self.web_search_fn,
            config=self.config,
        )
        state = await pipeline.run()
        self._save(state)
        return self._to_output(state)


    def _to_output(self, state: SharedState) -> dict:
        docs = []
        for d in state.downloaded_docs:
            dd = asdict(d)
            dd["contains"] = sorted(list(d.contains))
            dd["isins_inside"] = sorted(list(d.isins_inside))
            docs.append(dd)
        still_missing = [
            {"doc_type": dt, "periodo": p}
            for dt, p in state.missing_doc_targets()
        ]
        return {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(timespec="seconds"),
            "budget_spent": {
                "http_used": 80 - state.budget.http_remaining,
                "download_used": 120 - state.budget.download_remaining,
                "google_used": 15 - state.budget.google_remaining,
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
