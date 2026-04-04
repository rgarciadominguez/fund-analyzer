"""
Extractor estructurado usando Claude API.

Solo para:
  - Datos cualitativos (estrategia, gestores, filosofía)
  - Parsing cuando regex/pdfplumber falla

NUNCA usar para datos que se puedan extraer con regex o parsing directo.
Modelo: claude-sonnet-4-5 (optimizado para extracción estructurada)
"""
import json
import os
import re

import anthropic
from rich.console import Console

console = Console()

MODEL = "claude-sonnet-4-5"
MAX_TOKENS = 4096


def _get_client() -> anthropic.Anthropic:
    """Instancia el cliente Anthropic leyendo ANTHROPIC_API_KEY del entorno."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY no encontrada. "
            "Añádela al fichero .env en la raíz del proyecto."
        )
    return anthropic.Anthropic(api_key=api_key)


def _parse_json_response(text: str) -> dict | list:
    """
    Extrae JSON de la respuesta de Claude.
    Acepta respuestas con markdown (```json ... ```) o JSON limpio.
    """
    # Intentar parsear directamente
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass

    # Buscar bloque ```json ... ```
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Buscar el primer { ... } o [ ... ] en el texto
    for pattern in (r"\{[\s\S]+\}", r"\[[\s\S]+\]"):
        match = re.search(pattern, text)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                continue

    raise ValueError(f"No se pudo extraer JSON válido de la respuesta:\n{text[:500]}")


def extract_structured_data(text: str, schema: dict, context: str = "") -> dict:
    """
    Llama a Claude para extraer datos estructurados según el schema dado.

    Args:
        text:    Texto del que extraer datos (fragmento de PDF, HTML, etc.)
        schema:  Dict con los campos esperados y sus tipos/descripciones.
        context: Contexto adicional (ej. "Informe semestral CNMV, fondo ES").

    Returns:
        Dict con los datos extraídos según el schema.
    """
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
    context_block = f"\nContexto adicional: {context}\n" if context else ""

    prompt = f"""Eres un experto en análisis de fondos de inversión.
Extrae los datos del siguiente texto y devuelve ÚNICAMENTE un JSON válido sin markdown.
El JSON debe seguir exactamente este schema (usa null para campos no encontrados):{context_block}

SCHEMA:
{schema_str}

TEXTO A ANALIZAR:
{text}

IMPORTANTE:
- Responde SOLO con el JSON, sin explicaciones ni markdown
- Usa null para campos no encontrados o no aplicables
- Mantén los nombres de campo exactamente como en el schema
- Los porcentajes como números (ej. 3.83, no "3.83%")
- Las fechas en formato ISO 8601 cuando sea posible"""

    client = _get_client()
    console.log(f"[blue]Claude extractor: {len(text)} chars -> schema con {len(schema)} campos")

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text
    result = _parse_json_response(response_text)

    console.log(
        f"[green]Extracción completada "
        f"(tokens: {message.usage.input_tokens} in / {message.usage.output_tokens} out)"
    )
    return result


def extract_performance_table(text: str) -> list:
    """
    Extrae tabla de rentabilidad por clase de acción del Directors' Report.

    Patrón DNCA: columnas con clase, rentabilidad_pct, benchmark_pct, año.

    Returns:
        [{"clase": str, "rentabilidad_pct": float, "benchmark_pct": float, "anio": int}]
    """
    schema = {
        "performances": [
            {
                "clase": "nombre de la clase (ej. 'A EUR', 'I EUR')",
                "rentabilidad_pct": "rentabilidad de la clase en el periodo (número)",
                "benchmark_pct": "rentabilidad del benchmark en el mismo periodo (número)",
                "anio": "año del dato (entero)",
            }
        ]
    }

    result = extract_structured_data(
        text, schema, context="Tabla de rentabilidad de Directors' Report, annual report SICAV"
    )

    return result.get("performances", []) if isinstance(result, dict) else []


def extract_top_holdings(text: str) -> list:
    """
    Extrae las posiciones del top 10 holdings con sus pesos.

    Returns:
        [{"nombre": str, "ticker": str, "peso_pct": float, "sector": str, "pais": str}]
    """
    schema = {
        "holdings": [
            {
                "nombre": "nombre de la posición",
                "ticker": "ticker o ISIN si aparece (puede ser null)",
                "peso_pct": "peso en % sobre el NAV (número)",
                "sector": "sector de la posición (puede ser null)",
                "pais": "país del emisor (puede ser null)",
            }
        ]
    }

    result = extract_structured_data(
        text, schema, context="Sección Top 10 Holdings o Securities Portfolio, annual report"
    )

    return result.get("holdings", []) if isinstance(result, dict) else []


def extract_portfolio_breakdown(text: str) -> dict:
    """
    Extrae breakdown de cartera por país y sector.

    Returns:
        {
          "por_pais":   [{"pais": str, "pct": float}],
          "por_sector": [{"sector": str, "pct": float}],
        }
    """
    schema = {
        "por_pais": [
            {"pais": "nombre del país", "pct": "porcentaje (número)"}
        ],
        "por_sector": [
            {"sector": "nombre del sector", "pct": "porcentaje (número)"}
        ],
    }

    result = extract_structured_data(
        text,
        schema,
        context="Sección Portfolio Breakdown por país y sector, annual report SICAV",
    )

    if isinstance(result, dict):
        return {
            "por_pais": result.get("por_pais", []),
            "por_sector": result.get("por_sector", []),
        }
    return {"por_pais": [], "por_sector": []}
