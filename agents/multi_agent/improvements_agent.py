"""
Improvements Agent — coordinates code/data improvements for Fund Analyzer.

Receives a structured improvement request from the Orchestrator,
calls the necessary subagents in the right order, verifies results,
retries if needed (max 2 retries per improvement), and assembles
a final report for the Orchestrator.

Communication flow:
  Orchestrator → ImprovementsAgent.run(request) → subagents → return report
"""

from __future__ import annotations

import json
from typing import Any

from anthropic import Anthropic

from .shared_tools import MODEL, SUB_AGENT_TOOLS
from .sub_agents import execute_sub_agent_tools

MAX_ITERATIONS = 30   # safety cap on the tool-use loop


_SYSTEM_PROMPT = """Eres el Agente de Mejoras del sistema Fund Analyzer.

Recibes del Orquestador un bloque JSON con las mejoras a aplicar y debes:

1. ORGANIZAR: dividir las mejoras en subtareas y asignarlas al subagente correcto.
2. COORDINAR: llamar a los subagentes en el orden necesario.
   - Para mejoras de código: code_agent → test_agent → review_agent
   - Si review_agent rechaza: vuelve a code_agent con el feedback (máx. 2 reintentos)
3. REVISAR: no devuelvas el resultado hasta verificar que todos los criterios se cumplen.
4. ENSAMBLAR: devuelve al Orquestador un informe estructurado con lo hecho.

Herramientas disponibles:
- code_agent: aplica cambios en archivos Python/CSS/JSON.
- test_agent: verifica que el código compila (SIEMPRE llamar tras code_agent).
- review_agent: valida que un resultado cumple los criterios.
- analysis_agent: diagnostica la calidad del output.json de un fondo.
- run_pipeline: re-ejecuta el pipeline de análisis de un fondo.
- git_agent: git add + commit + push (llama al final si hay cambios de código).

Regla crítica: NUNCA devuelvas el texto final sin haber:
a) Llamado a todos los subagentes necesarios.
b) Verificado con test_agent que el código compila.
c) Verificado con review_agent que el resultado es correcto.
d) Reintentado con code_agent si review_agent rechazó (máx. 2 veces por mejora).
e) Hecho git commit+push si hubo cambios de código.

En tu respuesta final (cuando ya no llames más herramientas), incluye:
- Qué cambios se aplicaron (por archivo)
- Qué errores hubo (si alguno)
- Si el resultado está listo para el usuario
"""


class ImprovementsAgent:
    """
    Stateless agent that runs an agentic loop to apply improvements.
    Instantiate once and call run() per improvement batch.
    """

    def __init__(self) -> None:
        self._client = Anthropic()

    def run(self, improvement_request: dict[str, Any]) -> str:
        """
        Execute the improvements loop.

        improvement_request: the structured dict sent by the Orchestrator
          (fields: contexto, feedback_original, mejoras, criterios_de_exito, prioridad)

        Returns: a human-readable report for the Orchestrator to present to the user.
        """
        messages: list[dict] = [
            {
                "role": "user",
                "content": (
                    "Aquí están las mejoras a aplicar:\n\n"
                    + json.dumps(improvement_request, ensure_ascii=False, indent=2)
                ),
            }
        ]

        iteration = 0
        while iteration < MAX_ITERATIONS:
            iteration += 1

            response = self._client.messages.create(
                model=MODEL,
                max_tokens=8096,
                system=_SYSTEM_PROMPT,
                tools=SUB_AGENT_TOOLS,
                messages=messages,
            )

            # Append assistant turn
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                # Agent is done — extract final text
                for block in response.content:
                    if hasattr(block, "text"):
                        return block.text
                return "Mejoras aplicadas (el agente no devolvió texto final)."

            # Extract tool_use blocks and execute them
            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            tool_results = execute_sub_agent_tools(tool_use_blocks)

            # Append tool results as user turn
            messages.append({"role": "user", "content": tool_results})

        return (
            f"ERROR: Agente de Mejoras alcanzó el límite de {MAX_ITERATIONS} iteraciones. "
            "Es posible que algunos cambios estén parcialmente aplicados. "
            "Revisa los archivos modificados manualmente."
        )
