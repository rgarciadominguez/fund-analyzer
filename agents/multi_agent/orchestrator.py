"""
Orchestrator — the user-facing agent of the Fund Analyzer multi-agent system.

Architecture:
  [User] ↔ [Orchestrator] ──→ code_agent
                           ──→ review_agent
                           ──→ analysis_agent
                           ──→ run_pipeline
                           ──→ test_agent
                           ──→ git_agent
                           ──→ call_improvements_agent (optional coordinator)

The Orchestrator:
  - Is the ONLY agent that talks to the user.
  - Has DIRECT access to ALL subagents — calls them directly.
  - Can optionally delegate complex multi-step improvements to
    the ImprovementsAgent, which coordinates code→test→review loops.
  - Evaluates and presents results back to the user.

Usage:
  python -m agents.multi_agent.orchestrator
  python -m agents.multi_agent.orchestrator --isin ES0112231008
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from anthropic import Anthropic
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt

from .improvements_agent import ImprovementsAgent
from .shared_tools import IMPROVEMENTS_AGENT_TOOL, MODEL, SUB_AGENT_TOOLS
from .sub_agents import execute_sub_agent_tools

PROJECT_ROOT = Path(__file__).parent.parent.parent
console = Console()

MAX_ITERATIONS = 40   # safety cap per user turn


# ── System prompt ─────────────────────────────────────────────────────────────

def _build_system_prompt(isin: str | None) -> str:
    isin_line = f"\nISIN activo en esta sesión: **{isin}**" if isin else ""

    return f"""Eres el Orquestador del sistema Fund Analyzer.{isin_line}

## Tu rol
- Eres el ÚNICO agente que interactúa con el usuario.
- Tienes dos flujos paralelos para gestionar el trabajo.
- Presentas resultados de forma clara, concisa y útil.
- Hablas siempre en español.

## Dos flujos paralelos

### Flujo 1 — Directo (tú → agente)
Tienes acceso directo a TODOS los agentes para tareas puntuales:

| Agente | Cuándo usarlo directamente |
|--------|---------------------------|
| **analysis_agent** | Diagnosticar output.json: campos null, errores, calidad |
| **code_agent** | Cambio puntual y aislado en un archivo |
| **test_agent** | Verificar que código compila |
| **review_agent** | Validar un resultado específico |
| **run_pipeline** | Ejecutar el pipeline de análisis de un fondo |
| **git_agent** | Commit y push |

### Flujo 2 — Mejoras (tú → improvements_agent → agentes)
Para mejoras, correcciones o refactors que requieran coordinación:

**call_improvements_agent** recibe las mejoras y él se encarga de:
- Distribuir cada mejora al subagente correcto
- Coordinar ciclos code_agent → test_agent → review_agent
- Reintentar si review_agent rechaza (máx. 2 veces)
- Hacer git commit/push al final

Usa Flujo 2 cuando:
- El usuario pide arreglar, mejorar o refinar algo existente
- Hay múltiples cambios coordinados
- Se necesitan ciclos iterativos de code→test→review

## Contexto del proyecto
Fund Analyzer: sistema multi-agente para analizar fondos de inversión españoles e internacionales.
Stack: Python 3.11, Anthropic API, Streamlit, Plotly, CNMV XMLs/PDFs.

Archivos clave:
- agents/cnmv_agent.py         → extracción datos CNMV (XMLs + PDFs)
- agents/analyst_agent.py      → síntesis cualitativa con Claude
- agents/letters_agent.py      → cartas trimestrales gestores
- agents/readings_agent.py     → búsqueda web (lecturas, análisis externos)
- dashboard/app.py             → Streamlit dashboard (8 pestañas)
- dashboard/ui_components.py   → componentes UI dark/light
- data/funds/{{ISIN}}/output.json → resultado final del análisis

Fondo principal de prueba: ES0112231008 (Avantage Fund FI / Renta 4)

## Reglas
1. Tras code_agent directo, siempre llama a test_agent.
2. Tras cambios exitosos, usa git_agent para commit.
3. Informa al usuario qué se hizo y si necesita re-ejecutar el pipeline.
"""


# ── call_improvements_agent handler ──────────────────────────────────────────

_improvements_agent = ImprovementsAgent()


def _handle_improvements_agent(tool_input: dict[str, Any]) -> str:
    """Called when the Orchestrator uses the call_improvements_agent tool."""
    prioridad = tool_input.get("prioridad", "media")
    n_mejoras = len(tool_input.get("mejoras", []))
    console.print(
        Panel(
            f"[cyan]Agente de Mejoras activado[/cyan]\n"
            f"Prioridad: [bold]{prioridad}[/bold] · {n_mejoras} mejora(s)",
            border_style="cyan",
        )
    )
    result = _improvements_agent.run(tool_input)
    console.print("[dim]← Agente de Mejoras completado[/dim]")
    return result


# ── Orchestrator agent loop ───────────────────────────────────────────────────

class OrchestratorAgent:
    """
    Maintains conversation history and runs the tool-use loop per user turn.
    """

    def __init__(self, isin: str | None = None) -> None:
        self._client    = Anthropic()
        self._isin      = isin
        self._system    = _build_system_prompt(isin)
        self._messages: list[dict] = []
        # Orchestrator has access to ALL subagents + improvements agent (two parallel flows)
        self._tools = SUB_AGENT_TOOLS + [IMPROVEMENTS_AGENT_TOOL]
        # Extra handler to inject improvements agent without circular import
        self._extra_handlers = {"call_improvements_agent": _handle_improvements_agent}

    def _run_turn(self, user_message: str) -> str:
        """Processes one user message and returns the assistant's final response."""
        self._messages.append({"role": "user", "content": user_message})

        iteration = 0
        while iteration < MAX_ITERATIONS:
            iteration += 1

            response = self._client.messages.create(
                model=MODEL,
                max_tokens=8096,
                system=self._system,
                tools=self._tools,
                messages=self._messages,
            )

            self._messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                # Done — return text
                for block in response.content:
                    if hasattr(block, "text"):
                        return block.text
                return "_(el orquestador completó sin devolver texto)_"

            # Execute tools (subagents + improvements agent)
            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            # Log which tools are being called
            for b in tool_use_blocks:
                console.print(f"  [dim]→ {b.name}[/dim]")

            tool_results = execute_sub_agent_tools(
                tool_use_blocks,
                extra_handlers=self._extra_handlers,
            )
            self._messages.append({"role": "user", "content": tool_results})

        return (
            f"ERROR: El orquestador alcanzó el límite de {MAX_ITERATIONS} iteraciones. "
            "Intenta reformular tu petición."
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

WELCOME = """\
╔══════════════════════════════════════════════════════╗
║  Fund Analyzer — Sistema Multi-Agente                ║
║  Escribe tu feedback o petición. 'salir' para salir. ║
╚══════════════════════════════════════════════════════╝"""


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fund Analyzer — Orquestador multi-agente")
    parser.add_argument("--isin", help="ISIN del fondo activo en esta sesión")
    args = parser.parse_args()

    console.print(WELCOME, style="bold cyan")
    if args.isin:
        console.print(f"ISIN activo: [bold]{args.isin}[/bold]\n")

    agent = OrchestratorAgent(isin=args.isin)

    while True:
        try:
            user_input = Prompt.ask("\n[bold green]Tú[/bold green]").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nSaliendo...")
            sys.exit(0)

        if not user_input:
            continue
        if user_input.lower() in ("salir", "exit", "quit"):
            console.print("Hasta luego.")
            sys.exit(0)

        console.print("[dim]Procesando...[/dim]")
        result = agent._run_turn(user_input)
        console.print("\n[bold blue]Sistema[/bold blue]")
        console.print(Markdown(result))


if __name__ == "__main__":
    main()
