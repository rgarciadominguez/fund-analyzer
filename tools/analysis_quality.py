"""analysis_quality.py — Validación de calidad del bloque analyst_synthesis.

Detecta análisis rotos antes de publicarlos en el catálogo (Supabase):

- analyst_synthesis vacío o ausente (el analyst nunca corrió / no guardó)
- (casi) todas las secciones vacías (stubs)
- contenido FABRICADO / "ilustrativo" (alucinación del LLM cuando no había
  datos reales en las fuentes)

Es el chokepoint que impide marcar `has_qualitative_analysis = true` para
fondos con análisis roto. Lo usan:
  - tools/sync_to_supabase.py (gate antes de subir)
  - tools/recheck_published_quality.py (limpieza de filas ya publicadas)
  - tests/test_analysis_quality.py

Diseño conservador: ante la duda NO bloquea (evita despublicar análisis buenos).
Las señales de bloqueo se calibraron contra el set real de fondos del repo
(ES0112612001 alucinado vs el resto sano) para 0 falsos positivos.
"""

from __future__ import annotations

import re

# Las 6 secciones narrativas centrales + las 2 estructuradas.
CORE_SECTIONS = ["resumen", "historia", "gestores", "evolucion", "estrategia", "cartera"]
ALL_SECTIONS = CORE_SECTIONS + ["fuentes_externas", "documentos"]

# Longitud mínima de `texto` para considerar que una sección tiene contenido real.
MIN_SECTION_CHARS = 100

# Marcadores de alucinación: el LLM admite que inventó cifras por falta de datos.
# Patrón COMBINADO y acotado a `texto` de cada sección (NO al output.json completo,
# que incluye cartas/lecturas crudas donde estas palabras aparecen legítimamente).
_HALLUCINATION_RE = re.compile(
    r"(ilustrativ|hipot[eé]tic|a modo de ejemplo|son representativ)"
    r".{0,200}"
    r"(no han sido proporcionad|no se han proporcionad|no.{0,15}disponib|"
    r"en un informe real|datos.{0,20}fondo)"
    r"|"
    r"(no han sido proporcionad|no se han proporcionad)"
    r".{0,200}"
    r"(ilustrativ|representativ|a modo de ejemplo)",
    re.IGNORECASE | re.DOTALL,
)


def _section_text(analyst: dict, name: str) -> str:
    node = analyst.get(name) if isinstance(analyst, dict) else None
    if isinstance(node, dict):
        t = node.get("texto", "") or ""
        return t if isinstance(t, str) else ""
    if isinstance(node, str):
        return node
    return ""


def _gestores_has_content(analyst: dict) -> bool:
    """gestores cuenta como contenido si tiene texto o perfiles."""
    node = analyst.get("gestores") if isinstance(analyst, dict) else None
    if not isinstance(node, dict):
        return False
    if len((node.get("texto") or "")) >= MIN_SECTION_CHARS:
        return True
    perfiles = node.get("perfiles")
    return isinstance(perfiles, list) and len(perfiles) > 0


def assess_analysis_quality(output_data: dict) -> dict:
    """Evalúa la calidad del análisis cualitativo de un output.json.

    Devuelve dict:
      - ok (bool):                False si hay blockers
      - blockers (list[str]):     fallos que IMPIDEN publicar como analizado
      - warnings (list[str]):     degradaciones que NO bloquean pero conviene avisar
      - sections_with_content (int)
      - hallucinated_sections (list[str])
    """
    blockers: list[str] = []
    warnings: list[str] = []
    hallucinated: list[str] = []

    analyst = output_data.get("analyst_synthesis") if isinstance(output_data, dict) else None

    if not isinstance(analyst, dict) or not analyst:
        return {
            "ok": False,
            "blockers": ["analyst_synthesis vacío o ausente"],
            "warnings": [],
            "sections_with_content": 0,
            "hallucinated_sections": [],
        }

    # Contar secciones con contenido real
    n_content = 0
    for s in CORE_SECTIONS:
        if s == "gestores":
            if _gestores_has_content(analyst):
                n_content += 1
            continue
        if len(_section_text(analyst, s)) >= MIN_SECTION_CHARS:
            n_content += 1

    # Detección de alucinación (acotada a texto de secciones)
    for s in ALL_SECTIONS:
        t = _section_text(analyst, s)
        if t and _HALLUCINATION_RE.search(t):
            hallucinated.append(s)

    if hallucinated:
        blockers.append(
            "contenido fabricado/ilustrativo detectado en: " + ", ".join(hallucinated)
        )

    # < 2 secciones con contenido → análisis esencialmente vacío
    if n_content < 2:
        blockers.append(f"solo {n_content} sección(es) con contenido real")
    elif n_content < 4:
        warnings.append(f"análisis parcial: {n_content}/6 secciones con contenido")

    # gestores vacío (caso COBAS RENTA): no bloquea, pero se avisa
    if not _gestores_has_content(analyst):
        warnings.append("sección gestores vacía (sin perfiles ni texto)")

    return {
        "ok": len(blockers) == 0,
        "blockers": blockers,
        "warnings": warnings,
        "sections_with_content": n_content,
        "hallucinated_sections": hallucinated,
    }
