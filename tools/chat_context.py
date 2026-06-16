"""Contexto del chat por fondo (carga de documentos + system prompt).

Extraído de chat_server.py (2026-06-16) para que lo use también web_server.py
SIN arrastrar FastAPI/uvicorn. Así el chat puede servirse desde el MISMO backend
que el feedback (sin arrancar chat_server.py aparte) — ver /api/chat/<isin>.
"""
from __future__ import annotations

import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def load_fund_context(isin: str, root: Path | None = None) -> dict:
    """Carga TODOS los documentos crudos de un fondo en un dict de contexto."""
    root = root or _ROOT
    fund_dir = root / "data" / "funds" / isin
    if not fund_dir.exists():
        return {"error": f"Fund directory not found: {fund_dir}"}
    ctx = {"isin": isin}
    files = {
        "output": "output.json",
        "cnmv_data": "cnmv_data.json",
        "letters_data": "letters_data.json",
        "readings_data": "readings_data.json",
        "manager_profile": "manager_profile.json",
        "pdf_cache": "pdf_cache.json",
    }
    for key, fname in files.items():
        fpath = fund_dir / fname
        if fpath.exists():
            try:
                ctx[key] = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                ctx[key] = None
    return ctx


def build_system_prompt(ctx: dict) -> str:
    """System prompt con TODOS los documentos del fondo como contexto (RAG)."""
    output = ctx.get("output", {}) or {}
    nombre = output.get("nombre", ctx.get("isin", ""))
    gestora = output.get("gestora", "")
    sections = []

    kpis = output.get("kpis", {})
    if kpis:
        sections.append(f"=== KPIs DEL FONDO ===\n{json.dumps(kpis, ensure_ascii=False, indent=2)}")

    cuant = output.get("cuantitativo", {})
    if cuant:
        cuant_text = ""
        for k, v in cuant.items():
            if isinstance(v, list) and v:
                cuant_text += f"\n{k}: {json.dumps(v, ensure_ascii=False)}\n"
        if cuant_text:
            sections.append(f"=== DATOS CUANTITATIVOS COMPLETOS ===\n{cuant_text}")

    posiciones = output.get("posiciones", {})
    actuales = posiciones.get("actuales", [])
    if actuales:
        pos_text = "\n".join(
            f"- {p.get('nombre','')}: {p.get('peso_pct','')}% | {p.get('sector','')} | {p.get('pais','')}"
            for p in actuales)
        sections.append(f"=== POSICIONES ACTUALES ({len(actuales)}) ===\n{pos_text}")
    historicas = posiciones.get("historicas", [])
    if historicas:
        hist_text = ""
        for h in historicas:
            top = h.get("top10", [])
            if top:
                hist_text += f"\n--- {h.get('periodo','')} ---\n" + "\n".join(
                    f"  {p.get('nombre','')}: {p.get('peso_pct','')}%" for p in top[:10])
        if hist_text:
            sections.append(f"=== POSICIONES HISTÓRICAS ===\n{hist_text}")

    synth = output.get("analyst_synthesis", {})
    synth_text = ""
    for sec_name in ["resumen", "historia", "gestores", "evolucion", "estrategia",
                     "cartera", "fuentes_externas"]:
        sec = synth.get(sec_name, {})
        if isinstance(sec, dict):
            if sec.get("texto"):
                synth_text += f"\n--- ANÁLISIS: {sec_name.upper()} ---\n{sec['texto']}\n"
            for extra_key in ["hitos", "hitos_estrategia", "quotes", "fortalezas", "riesgos",
                              "filosofia_inversion", "criterios_inversion", "para_quien_es",
                              "signal", "signal_rationale", "perfiles"]:
                val = sec.get(extra_key)
                if val:
                    synth_text += f"\n{sec_name}.{extra_key}: {json.dumps(val, ensure_ascii=False)}\n"
    if synth_text:
        sections.append(f"=== ANÁLISIS COMPLETO DEL FONDO ===\n{synth_text}")

    letters = ctx.get("letters_data", {}) or {}
    cartas_text = ""
    for carta in (letters.get("cartas", []) or []):
        texto = carta.get("texto_completo", "") or carta.get("texto", "") or carta.get("contenido", "")
        if texto and len(texto) > 100:
            periodo = carta.get("periodo", "") or carta.get("fecha_inferida", "")
            cartas_text += f"\n--- CARTA: {carta.get('titulo','')} ({periodo}) ---\n{texto}\n"
    if cartas_text:
        sections.append(f"=== CARTAS DEL GESTOR ===\n{cartas_text}")

    pdf_cache = ctx.get("pdf_cache", {}) or {}
    if pdf_cache:
        cnmv_pdfs_text = ""
        for fname in sorted(pdf_cache.keys()):
            entry = pdf_cache[fname]
            if isinstance(entry, str) and entry:
                cnmv_pdfs_text += f"\n--- INFORME: {fname} ---\n{entry}\n"
            elif isinstance(entry, dict):
                parts = []
                for k, v in entry.items():
                    if isinstance(v, str) and v:
                        parts.append(f"[{k}]\n{v}")
                    elif isinstance(v, list):
                        for item in v:
                            parts.append(item if isinstance(item, str) else json.dumps(item, ensure_ascii=False))
                if parts:
                    cnmv_pdfs_text += f"\n--- INFORME: {fname} ---\n" + "\n".join(parts) + "\n"
        if cnmv_pdfs_text:
            sections.append(f"=== INFORMES CNMV SEMESTRALES ===\n{cnmv_pdfs_text}")

    cnmv = ctx.get("cnmv_data", {}) or {}
    cual = cnmv.get("cualitativo", {}) or {}
    cnmv_cual_text = ""
    for key in ["seccion_9_texto_completo", "seccion_10_perspectivas_texto"]:
        if cual.get(key):
            cnmv_cual_text += f"\n--- CNMV {key} ---\n{cual[key]}\n"
    if cnmv_cual_text:
        sections.append(f"=== CNMV CUALITATIVO ===\n{cnmv_cual_text}")

    manager = ctx.get("manager_profile", {}) or {}
    manager_text = ""
    profiles = (manager.get("profiles", []) or manager.get("perfiles", [])
                or manager.get("equipo", []) or [])
    for profile in profiles:
        if isinstance(profile, dict):
            manager_text += f"\n--- GESTOR: {profile.get('nombre', '')} ---\n{json.dumps(profile, ensure_ascii=False)}\n"
    for src in (manager.get("fuentes_web", []) or []):
        if isinstance(src, dict):
            texto = src.get("texto", "") or src.get("content", "")
            if texto and len(texto) > 100:
                manager_text += f"\n--- WEB: {src.get('titulo', src.get('url',''))} ---\n{texto}\n"
    if manager_text:
        sections.append(f"=== PERFILES DE GESTORES ===\n{manager_text}")

    readings = ctx.get("readings_data", {}) or {}
    readings_text = ""
    for key in ["analisis_completos", "analisis_escritos", "analisis", "otros_readings", "lecturas"]:
        for item in (readings.get(key, []) or []):
            if isinstance(item, dict):
                texto = item.get("texto_completo", "") or item.get("texto", "") or item.get("resumen", "")
                if texto and len(texto) > 80:
                    readings_text += f"\n--- {item.get('fuente','')} | {item.get('titulo','')} ---\n{texto}\n"
    if readings_text:
        sections.append(f"=== FUENTES EXTERNAS ===\n{readings_text}")

    context_body = "\n\n".join(sections)
    return f"""Eres un asistente RAG sobre el fondo {nombre} ({ctx.get('isin','')}) gestionado por {gestora}.

ROL Y LÍMITES:
- SOLO resumes y extraes información de los DOCUMENTOS DEL FONDO cargados aquí.
- NO consultas la web ni usas conocimiento de mercado para inventar datos.
- Si la pregunta requiere algo que NO está en los documentos, responde:
  "No tengo ese dato en los documentos del fondo."
- NUNCA inventes cifras, fechas, nombres de gestores o decisiones que no aparezcan en el contexto.

CITA FUENTES cuando puedas ("Según la carta de enero 2024...", "En el informe CNMV 2023 H2...").

ESTILO: español, conciso pero completo, cifras concretas con unidad y periodo. Si hay
contradicción entre fuentes, señálala.

CONTEXTO DISPONIBLE ({len(sections)} bloques) — estos son TUS ÚNICOS DATOS:

{context_body}
"""


__all__ = ["load_fund_context", "build_system_prompt"]
