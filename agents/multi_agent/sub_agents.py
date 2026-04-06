"""
Subagent implementations for the Fund Analyzer multi-agent system.

Each function implements a specific capability:
  - code_agent      → reads files, applies Claude-generated code edits
  - review_agent    → validates results against criteria
  - analysis_agent  → diagnoses fund output.json quality
  - run_pipeline    → executes the existing CNMV/analyst pipeline
  - test_agent      → py_compile check
  - git_agent       → git add / commit / push

dispatch_sub_agent() routes tool_use calls to the right function.
execute_sub_agent_tools() processes a full list of tool_use blocks.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from anthropic import Anthropic

from .shared_tools import MODEL, SUB_AGENT_TOOLS

PROJECT_ROOT = Path(__file__).parent.parent.parent
_client: Anthropic | None = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic()
    return _client


# ── Helpers ───────────────────────────────────────────────────────────────────

MAX_FILE_CHARS = 9000   # chars per file sent to code/review agents
MAX_FILES      = 4      # max files per code_agent call


def _read_file(rel_path: str) -> str:
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        return f"[ARCHIVO NO EXISTE: {rel_path}]"
    content = path.read_text(encoding="utf-8", errors="replace")
    if len(content) > MAX_FILE_CHARS:
        half = MAX_FILE_CHARS // 2
        return (
            content[:half]
            + f"\n\n... [TRUNCADO — {len(content)} chars totales, mostrando primeros y últimos {half//2}] ...\n\n"
            + content[-half // 2 :]
        )
    return content


def _strip_markdown_json(text: str) -> str:
    """Remove ```json ... ``` wrappers if present."""
    t = text.strip()
    if t.startswith("```"):
        lines = t.split("\n")
        # Drop first line (```json or ```) and last line (```)
        inner = lines[1:] if lines[-1].strip() == "```" else lines[1:]
        if inner and inner[-1].strip() == "```":
            inner = inner[:-1]
        t = "\n".join(inner)
    return t.strip()


# ── Subagent: code_agent ──────────────────────────────────────────────────────

def code_agent(task: str, files: list[str], context: str = "") -> str:
    """
    Reads the specified files, sends them to Claude, gets back a JSON diff,
    and applies the edits to disk. Returns a human-readable summary.
    """
    files = files[:MAX_FILES]

    file_section = "\n\n".join(
        f"### {f}\n```\n{_read_file(f)}\n```"
        for f in files
    )

    system = (
        "Eres un agente de código experto en Python, Streamlit y Plotly. "
        "Dado el contenido de archivos y una tarea, produces las modificaciones necesarias.\n\n"
        "Responde ÚNICAMENTE con JSON válido (sin markdown, sin texto extra):\n"
        "{\n"
        '  "cambios": [\n'
        '    {\n'
        '      "archivo": "ruta/relativa.py",\n'
        '      "tipo": "edit",\n'
        '      "old_string": "texto exacto tal como aparece en el archivo (mínimo 3 líneas de contexto)",\n'
        '      "new_string": "texto de reemplazo"\n'
        '    }\n'
        "  ],\n"
        '  "resumen": "descripción breve de los cambios realizados"\n'
        "}\n\n"
        "Reglas:\n"
        "- Usa tipo 'edit' (reemplazos puntuales). Nunca reescribas el archivo completo.\n"
        "- old_string debe ser EXACTAMENTE como aparece en el archivo (preserva espacios, tabs, saltos de línea).\n"
        "- Incluye suficiente contexto en old_string para que sea único en el archivo.\n"
        "- Máximo 6 cambios por respuesta. Prioriza los más impactantes.\n"
        "- Si no hay nada que cambiar, devuelve {'cambios': [], 'resumen': 'Sin cambios necesarios'}."
    )

    response = _get_client().messages.create(
        model=MODEL,
        max_tokens=8096,
        system=system,
        messages=[{
            "role": "user",
            "content": (
                f"TAREA:\n{task}\n\n"
                f"CONTEXTO ADICIONAL:\n{context or 'Ninguno'}\n\n"
                f"ARCHIVOS:\n{file_section}"
            ),
        }],
    )

    raw = _strip_markdown_json(response.content[0].text)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        return f"ERROR: code_agent devolvió JSON inválido ({e}).\nRespuesta:\n{raw[:600]}"

    applied: list[str] = []
    errors: list[str] = []

    for cambio in result.get("cambios", []):
        rel = cambio.get("archivo", "")
        path = PROJECT_ROOT / rel
        tipo = cambio.get("tipo", "edit")

        if not path.exists():
            errors.append(f"Archivo no encontrado: {rel}")
            continue

        if tipo == "edit":
            old = cambio.get("old_string", "")
            new = cambio.get("new_string", "")
            if not old:
                errors.append(f"old_string vacío en {rel}")
                continue
            content = path.read_text(encoding="utf-8", errors="replace")
            if old not in content:
                errors.append(
                    f"old_string no encontrado en {rel}:\n  «{old[:80]}»"
                )
                continue
            path.write_text(content.replace(old, new, 1), encoding="utf-8")
            applied.append(f"✅ {rel}: edit aplicado")
        else:
            errors.append(f"Tipo desconocido '{tipo}' en {rel}")

    parts: list[str] = []
    if applied:
        parts.append("CAMBIOS APLICADOS:\n" + "\n".join(applied))
    if errors:
        parts.append("ERRORES:\n" + "\n".join(errors))
    parts.append(f"RESUMEN: {result.get('resumen', '—')}")

    return "\n\n".join(parts)


# ── Subagent: review_agent ────────────────────────────────────────────────────

def review_agent(what_to_review: str, criteria: str, content: str) -> str:
    """
    Reviews `content` against `criteria`. Returns JSON with approved/rejected + feedback.
    """
    system = (
        "Eres un agente de revisión de calidad. "
        "Evalúas si un resultado cumple los criterios especificados.\n"
        "Responde ÚNICAMENTE con JSON:\n"
        "{\n"
        '  "aprobado": true/false,\n'
        '  "puntuacion": 0-10,\n'
        '  "feedback": "explicación detallada de por qué aprueba o rechaza",\n'
        '  "puntos_faltantes": ["criterio 1 no cumplido", ...],\n'
        '  "sugerencias": "qué cambiar para que sea aprobado (si rechaza)"\n'
        "}"
    )
    response = _get_client().messages.create(
        model=MODEL,
        max_tokens=2048,
        system=system,
        messages=[{
            "role": "user",
            "content": (
                f"QUÉ SE REVISA:\n{what_to_review}\n\n"
                f"CRITERIOS DE ÉXITO:\n{criteria}\n\n"
                f"CONTENIDO A REVISAR:\n{content[:5000]}"
            ),
        }],
    )
    return response.content[0].text


# ── Subagent: analysis_agent ──────────────────────────────────────────────────

def analysis_agent(isin: str, focus: str = "") -> str:
    """
    Reads output.json for a fund and returns a structured quality diagnosis.
    """
    output_path = PROJECT_ROOT / "data" / "funds" / isin / "output.json"
    if not output_path.exists():
        return (
            f"ERROR: No existe output.json para {isin}. "
            "Ejecuta el pipeline primero con run_pipeline."
        )

    try:
        data = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return f"ERROR leyendo output.json: {exc}"

    kpis   = data.get("kpis", {})
    cual   = data.get("cualitativo", {})
    cuant  = data.get("cuantitativo", {})
    pos    = data.get("posiciones", {})
    consist = data.get("analisis_consistencia", {})

    # Collect null/empty fields
    null_fields: list[str] = []

    def _scan(d: dict, prefix: str = "") -> None:
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if v is None:
                null_fields.append(key)
            elif isinstance(v, list) and len(v) == 0:
                null_fields.append(f"{key} (lista vacía)")
            elif isinstance(v, dict):
                _scan(v, key)

    _scan(kpis, "kpis")
    _scan(cual, "cualitativo")

    diagnosis = {
        "isin":                    isin,
        "nombre":                  data.get("nombre", "?"),
        "gestora":                 data.get("gestora", "?"),
        "aum_meur":                kpis.get("aum_actual_meur"),
        "num_participes":          kpis.get("num_participes"),
        "ter_pct":                 kpis.get("ter_pct"),
        "gestores":                [g.get("nombre") for g in cual.get("gestores", [])],
        "estrategia_presente":     bool(cual.get("estrategia")),
        "filosofia_presente":      bool(cual.get("filosofia_inversion")),
        "historia_fondo_presente": bool(cual.get("historia_fondo")),
        "hechos_relevantes":       len(cual.get("hechos_relevantes", [])),
        "serie_aum_puntos":        len(cuant.get("serie_aum", [])),
        "serie_ter_puntos":        len(cuant.get("serie_ter", [])),
        "mix_activos_anos":        len(cuant.get("mix_activos_historico", [])),
        "posiciones_actuales":     len(pos.get("actuales", [])),
        "posiciones_historicas":   len(pos.get("historicas", [])),
        "periodos_consistencia":   len(consist.get("periodos", [])),
        "resumen_global_presente": bool(consist.get("resumen_global")),
        "campos_null":             null_fields[:25],
    }

    header = f"DIAGNÓSTICO FONDO {isin}" + (f" (foco: {focus})" if focus else "")
    return f"{header}\n{json.dumps(diagnosis, ensure_ascii=False, indent=2)}"


# ── Subagent: run_pipeline ────────────────────────────────────────────────────

def run_pipeline(isin: str, clear_cache: bool = False) -> str:
    """
    Runs the existing fund pipeline (orchestrator.py) as a subprocess.
    """
    if clear_cache:
        cache = PROJECT_ROOT / "data" / "funds" / isin / "pdf_cache.json"
        if cache.exists():
            cache.unlink()
            print(f"  [run_pipeline] Cache borrado: {cache.name}")

    cmd = [sys.executable, "-m", "agents.orchestrator", "--isin", isin, "--auto"]
    print(f"  [run_pipeline] Ejecutando pipeline para {isin}...")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=600,
            encoding="utf-8",
            errors="replace",
        )
        tail_out = proc.stdout[-3000:] if len(proc.stdout) > 3000 else proc.stdout
        tail_err = proc.stderr[-800:]  if proc.stderr               else ""
        status   = "COMPLETADO" if proc.returncode == 0 else f"ERROR (código {proc.returncode})"
        parts = [f"Pipeline {status}", f"Output:\n{tail_out}"]
        if tail_err:
            parts.append(f"Stderr:\n{tail_err}")
        return "\n\n".join(parts)
    except subprocess.TimeoutExpired:
        return "ERROR: Pipeline timeout (>10 min)"
    except Exception as exc:
        return f"ERROR ejecutando pipeline: {exc}"


# ── Subagent: test_agent ──────────────────────────────────────────────────────

def test_agent(files: list[str]) -> str:
    """
    Runs py_compile on specified Python files.
    Returns per-file OK / ERROR and overall status.
    """
    results: list[str] = []
    all_ok = True

    for f in files:
        path = PROJECT_ROOT / f
        if not path.exists():
            results.append(f"❌ {f}: no encontrado")
            all_ok = False
            continue
        proc = subprocess.run(
            [sys.executable, "-m", "py_compile", str(path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if proc.returncode == 0:
            results.append(f"✅ {f}: OK")
        else:
            results.append(f"❌ {f}: {proc.stderr.strip()}")
            all_ok = False

    overall = "TODOS OK" if all_ok else "HAY ERRORES DE COMPILACIÓN"
    return f"{overall}\n" + "\n".join(results)


# ── Subagent: git_agent ───────────────────────────────────────────────────────

def git_agent(message: str, files: list[str] | None = None) -> str:
    """
    git add → git commit → git push.
    If files is empty/None, uses `git add -A`.
    """
    try:
        # Stage
        if files:
            stage_cmd = ["git", "add"] + [str(PROJECT_ROOT / f) for f in files]
        else:
            stage_cmd = ["git", "add", "-A"]

        subprocess.run(stage_cmd, cwd=str(PROJECT_ROOT), check=True,
                       capture_output=True, text=True)

        # Commit
        full_msg = f"{message}\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
        commit = subprocess.run(
            ["git", "commit", "-m", full_msg],
            cwd=str(PROJECT_ROOT),
            capture_output=True, text=True,
        )
        if commit.returncode != 0:
            if "nothing to commit" in commit.stdout + commit.stderr:
                return "Sin cambios para commitear."
            return f"ERROR en commit: {commit.stderr.strip()}"

        # Push
        push = subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=str(PROJECT_ROOT),
            capture_output=True, text=True,
        )
        if push.returncode != 0:
            return f"Commit OK pero error en push: {push.stderr.strip()}"

        return f"✅ Commit y push completados.\n{commit.stdout.strip()}\n{push.stdout.strip()}"
    except Exception as exc:
        return f"ERROR en git_agent: {exc}"


# ── Dispatcher ────────────────────────────────────────────────────────────────

def dispatch_sub_agent(tool_name: str, tool_input: dict[str, Any]) -> str:
    """Routes a tool_use call to the appropriate subagent function."""
    if tool_name == "code_agent":
        return code_agent(
            task=tool_input["task"],
            files=tool_input.get("files", []),
            context=tool_input.get("context", ""),
        )
    if tool_name == "review_agent":
        return review_agent(
            what_to_review=tool_input["what_to_review"],
            criteria=tool_input["criteria"],
            content=tool_input.get("content", ""),
        )
    if tool_name == "analysis_agent":
        return analysis_agent(
            isin=tool_input["isin"],
            focus=tool_input.get("focus", ""),
        )
    if tool_name == "run_pipeline":
        return run_pipeline(
            isin=tool_input["isin"],
            clear_cache=tool_input.get("clear_cache", False),
        )
    if tool_name == "test_agent":
        return test_agent(files=tool_input["files"])
    if tool_name == "git_agent":
        return git_agent(
            message=tool_input["message"],
            files=tool_input.get("files"),
        )
    return f"ERROR: subagente desconocido '{tool_name}'"


# ── Tool executor (reusable) ──────────────────────────────────────────────────

def execute_sub_agent_tools(
    tool_use_blocks: list,
    extra_handlers: dict[str, Any] | None = None,
) -> list[dict]:
    """
    Processes all tool_use blocks from a Claude response.

    extra_handlers: {tool_name: callable(input) -> str}
        Used by the Orchestrator to inject the call_improvements_agent handler
        without coupling this module to improvements_agent.py.

    Returns: list of tool_result dicts ready to append to the messages list.
    """
    results: list[dict] = []

    for block in tool_use_blocks:
        if not hasattr(block, "type") or block.type != "tool_use":
            continue

        tool_name = block.name
        tool_input = block.input
        tool_use_id = block.id

        try:
            if extra_handlers and tool_name in extra_handlers:
                output = extra_handlers[tool_name](tool_input)
            else:
                output = dispatch_sub_agent(tool_name, tool_input)
        except Exception as exc:
            output = f"ERROR inesperado en {tool_name}: {exc}"

        results.append({
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": str(output),
        })

    return results
