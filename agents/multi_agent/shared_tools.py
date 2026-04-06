"""
Shared constants for the multi-agent system.

Both the Orchestrator and the Improvements Agent import from here:
  - MODEL: the Anthropic model to use for all agents
  - SUB_AGENT_TOOLS: tool schemas available to all agents
  - IMPROVEMENTS_AGENT_TOOL: extra tool only for the Orchestrator
"""

MODEL = "claude-sonnet-4-6"

# ── Subagent tool definitions — used by Improvements Agent ────────────────────
SUB_AGENT_TOOLS: list[dict] = [
    {
        "name": "code_agent",
        "description": (
            "Lee archivos de código del proyecto y aplica cambios puntuales. "
            "Úsalo para implementar mejoras concretas en Python, CSS o JSON. "
            "El agente lee los archivos indicados, genera los diffs necesarios y los aplica."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": (
                        "Descripción detallada de qué cambiar y cómo debe quedar. "
                        "Incluye el comportamiento actual (incorrecto) y el esperado (correcto)."
                    ),
                },
                "files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Rutas de archivos relevantes, relativas a la raíz del proyecto.",
                },
                "context": {
                    "type": "string",
                    "description": "Contexto adicional: por qué se hace el cambio, criterios de éxito.",
                },
            },
            "required": ["task", "files"],
        },
    },
    {
        "name": "review_agent",
        "description": (
            "Revisa un resultado (código, JSON, texto) y valida si cumple los criterios indicados. "
            "Devuelve aprobado/rechazado con puntuación y feedback detallado."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "what_to_review": {
                    "type": "string",
                    "description": "Qué se está revisando: código modificado, extracción de datos, output JSON, etc.",
                },
                "criteria": {
                    "type": "string",
                    "description": "Criterios de éxito que debe cumplir el resultado.",
                },
                "content": {
                    "type": "string",
                    "description": "El contenido a revisar (código, JSON, resumen de cambios).",
                },
            },
            "required": ["what_to_review", "criteria", "content"],
        },
    },
    {
        "name": "analysis_agent",
        "description": (
            "Analiza el output.json de un fondo y devuelve un diagnóstico: "
            "campos vacíos, errores de extracción, calidad de datos, inconsistencias."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "isin": {
                    "type": "string",
                    "description": "ISIN del fondo a analizar (ej: ES0112231008).",
                },
                "focus": {
                    "type": "string",
                    "description": (
                        "Aspecto específico: 'aum', 'gestores', 'posiciones', "
                        "'consistencia', 'cualitativo', etc. "
                        "Omitir para análisis completo."
                    ),
                },
            },
            "required": ["isin"],
        },
    },
    {
        "name": "run_pipeline",
        "description": (
            "Ejecuta el pipeline de análisis completo para un fondo. "
            "Usa esto cuando hay que re-analizar tras cambios en la extracción."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "isin": {
                    "type": "string",
                    "description": "ISIN del fondo.",
                },
                "clear_cache": {
                    "type": "boolean",
                    "description": (
                        "Si true, borra pdf_cache.json antes de ejecutar. "
                        "Necesario cuando se cambian los parsers de PDFs."
                    ),
                },
            },
            "required": ["isin"],
        },
    },
    {
        "name": "test_agent",
        "description": (
            "Verifica que archivos Python compilan sin errores de sintaxis. "
            "Llama siempre tras code_agent para validar los cambios."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Archivos Python a compilar (rutas relativas al proyecto).",
                },
            },
            "required": ["files"],
        },
    },
    {
        "name": "git_agent",
        "description": (
            "Realiza operaciones git: add, commit y push. "
            "Úsalo al final de un ciclo de mejoras para publicar los cambios en Streamlit Cloud."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "Mensaje del commit (conciso, describe los cambios).",
                },
                "files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Archivos a añadir al commit. Si vacío, usa 'git add -A'.",
                },
            },
            "required": ["message"],
        },
    },
]

# ── Tools available to the Orchestrator ───────────────────────────────────────

# Orchestrator can call these directly (no need to go through improvements_agent)
ORCHESTRATOR_DIRECT_TOOLS: list[dict] = [
    {
        "name": "analysis_agent",
        "description": (
            "Analiza el output.json de un fondo y devuelve un diagnóstico: "
            "campos vacíos, errores de extracción, calidad de datos, inconsistencias."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "isin": {
                    "type": "string",
                    "description": "ISIN del fondo a analizar (ej: ES0112231008).",
                },
                "focus": {
                    "type": "string",
                    "description": (
                        "Aspecto específico: 'aum', 'gestores', 'posiciones', "
                        "'consistencia', 'cualitativo', etc. "
                        "Omitir para análisis completo."
                    ),
                },
            },
            "required": ["isin"],
        },
    },
    {
        "name": "run_pipeline",
        "description": (
            "Ejecuta el pipeline de análisis completo para un fondo. "
            "Usa esto cuando hay que re-analizar tras cambios en la extracción."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "isin": {
                    "type": "string",
                    "description": "ISIN del fondo.",
                },
                "clear_cache": {
                    "type": "boolean",
                    "description": (
                        "Si true, borra pdf_cache.json antes de ejecutar. "
                        "Necesario cuando se cambian los parsers de PDFs."
                    ),
                },
            },
            "required": ["isin"],
        },
    },
]

# Improvements agent tool — Orchestrator delegates ALL code changes through this
IMPROVEMENTS_AGENT_TOOL: dict = {
    "name": "call_improvements_agent",
    "description": (
        "Delega un bloque de mejoras al Agente de Mejoras. "
        "Úsalo cuando el usuario pide cambiar, arreglar o refinar algo existente. "
        "El Agente de Mejoras coordinará los subagentes necesarios, verificará los resultados "
        "y devolverá un informe completo. "
        "NO uses esto para tareas nuevas simples — llama directamente a los subagentes en esos casos."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "contexto": {
                "type": "string",
                "description": "Qué existe actualmente y sobre qué se van a aplicar las mejoras.",
            },
            "feedback_original": {
                "type": "string",
                "description": "Exactamente lo que dijo el usuario.",
            },
            "mejoras": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id":              {"type": "integer"},
                        "que":             {"type": "string", "description": "Qué hay que cambiar."},
                        "por_que":         {"type": "string", "description": "Por qué el estado actual no es suficiente."},
                        "como":            {"type": "string", "description": "Cómo debería quedar."},
                        "agente_sugerido": {"type": "string", "description": "Subagente más adecuado."},
                    },
                    "required": ["id", "que", "por_que", "como", "agente_sugerido"],
                },
                "description": "Lista de mejoras concretas y accionables.",
            },
            "criterios_de_exito": {
                "type": "string",
                "description": "Cómo sabe el Agente de Mejoras que el trabajo está bien hecho.",
            },
            "prioridad": {
                "type": "string",
                "enum": ["alta", "media", "baja"],
            },
        },
        "required": ["contexto", "feedback_original", "mejoras", "criterios_de_exito", "prioridad"],
    },
}
