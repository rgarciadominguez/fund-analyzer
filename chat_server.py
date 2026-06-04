"""
Chat Server — FastAPI backend for fund Q&A.

Loads ALL raw documents for a fund and uses Gemini as context-aware assistant.
Supports streaming responses.

Usage:
    python chat_server.py                    # default: ES0112231008
    python chat_server.py ES0156572002       # specific ISIN
    python chat_server.py --port 8080        # custom port
"""
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

load_dotenv(Path(__file__).parent / ".env")

app = FastAPI(title="Fund Chat API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ROOT = Path(__file__).parent
FUND_CONTEXT: dict = {}
FUND_ISIN: str = ""
FUND_NAME: str = ""
CONVERSATION_HISTORY: list = []
MAX_HISTORY = 20


# ═══════════════════════════════════════════════════════════════
# Load fund documents
# ═══════════════════════════════════════════════════════════════

def load_fund_context(isin: str) -> dict:
    """Load ALL raw documents for a fund into a single context dict."""
    fund_dir = ROOT / "data" / "funds" / isin
    if not fund_dir.exists():
        return {"error": f"Fund directory not found: {fund_dir}"}

    ctx = {"isin": isin}

    # Load each JSON file (search_cache excluded — no semantic value)
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
                data = json.loads(fpath.read_text(encoding="utf-8"))
                ctx[key] = data
            except Exception:
                ctx[key] = None

    return ctx


def build_system_prompt(ctx: dict) -> str:
    """Build system prompt with ALL fund documents as context."""
    output = ctx.get("output", {}) or {}
    nombre = output.get("nombre", ctx.get("isin", ""))
    gestora = output.get("gestora", "")

    sections = []

    # ── 1. KPIs ──
    kpis = output.get("kpis", {})
    if kpis:
        sections.append(f"=== KPIs DEL FONDO ===\n{json.dumps(kpis, ensure_ascii=False, indent=2)}")

    # ── 2. Cuantitativo completo ──
    cuant = output.get("cuantitativo", {})
    if cuant:
        cuant_text = ""
        for k, v in cuant.items():
            if isinstance(v, list) and v:
                cuant_text += f"\n{k}: {json.dumps(v, ensure_ascii=False)}\n"
        if cuant_text:
            sections.append(f"=== DATOS CUANTITATIVOS COMPLETOS ===\n{cuant_text}")

    # ── 3. Posiciones actuales e históricas ──
    posiciones = output.get("posiciones", {})
    actuales = posiciones.get("actuales", [])
    if actuales:
        pos_text = "\n".join(
            f"- {p.get('nombre','')}: {p.get('peso_pct','')}% | {p.get('sector','')} | {p.get('pais','')}"
            for p in actuales
        )
        sections.append(f"=== POSICIONES ACTUALES ({len(actuales)}) ===\n{pos_text}")

    historicas = posiciones.get("historicas", [])
    if historicas:
        hist_text = ""
        for h in historicas:
            periodo = h.get("periodo", "")
            top = h.get("top10", [])
            if top:
                hist_text += f"\n--- {periodo} ---\n"
                hist_text += "\n".join(f"  {p.get('nombre','')}: {p.get('peso_pct','')}%" for p in top[:10])
        if hist_text:
            sections.append(f"=== POSICIONES HISTÓRICAS ===\n{hist_text}")

    # ── 4. Analyst synthesis (all sections) ──
    synth = output.get("analyst_synthesis", {})
    synth_text = ""
    for sec_name in ["resumen", "historia", "gestores", "evolucion", "estrategia", "cartera", "fuentes_externas"]:
        sec = synth.get(sec_name, {})
        if isinstance(sec, dict):
            texto = sec.get("texto", "")
            if texto:
                synth_text += f"\n--- ANÁLISIS: {sec_name.upper()} ---\n{texto}\n"
            # Also include structured data (hitos, quotes, fortalezas, etc.)
            for extra_key in ["hitos", "hitos_estrategia", "quotes", "fortalezas", "riesgos",
                              "filosofia_inversion", "criterios_inversion", "para_quien_es",
                              "signal", "signal_rationale", "perfiles"]:
                val = sec.get(extra_key)
                if val:
                    synth_text += f"\n{sec_name}.{extra_key}: {json.dumps(val, ensure_ascii=False)}\n"
    if synth_text:
        sections.append(f"=== ANÁLISIS COMPLETO DEL FONDO ===\n{synth_text}")

    # ── 5. Cartas del gestor (texto completo de cada una) ──
    letters = ctx.get("letters_data", {}) or {}
    cartas = letters.get("cartas", []) or []
    cartas_text = ""
    for carta in cartas:
        texto = carta.get("texto_completo", "") or carta.get("texto", "") or carta.get("contenido", "")
        if texto and len(texto) > 100:
            periodo = carta.get("periodo", "") or carta.get("fecha_inferida", "")
            titulo = carta.get("titulo", "")
            cartas_text += f"\n--- CARTA: {titulo} ({periodo}) ---\n{texto}\n"
    if cartas_text:
        sections.append(f"=== CARTAS DEL GESTOR ({len([c for c in cartas if len(c.get('texto_completo','') or '')>100])}) ===\n{cartas_text}")

    # ── 6. Informes CNMV semestrales (texto extraído de PDFs) ──
    pdf_cache = ctx.get("pdf_cache", {}) or {}
    if pdf_cache:
        cnmv_pdfs_text = ""
        for fname in sorted(pdf_cache.keys()):
            entry = pdf_cache[fname]
            # Entry can be a string (raw text) or a dict with sections
            if isinstance(entry, str) and entry:
                cnmv_pdfs_text += f"\n--- INFORME: {fname} ---\n{entry}\n"
            elif isinstance(entry, dict):
                # Extract all text from dict values
                text_parts = []
                for k, v in entry.items():
                    if isinstance(v, str) and v:
                        text_parts.append(f"[{k}]\n{v}")
                    elif isinstance(v, list):
                        for item in v:
                            if isinstance(item, str):
                                text_parts.append(item)
                            elif isinstance(item, dict):
                                text_parts.append(json.dumps(item, ensure_ascii=False))
                if text_parts:
                    cnmv_pdfs_text += f"\n--- INFORME: {fname} ---\n" + "\n".join(text_parts) + "\n"
        if cnmv_pdfs_text:
            sections.append(f"=== INFORMES CNMV SEMESTRALES ({len(pdf_cache)} informes) ===\n{cnmv_pdfs_text}")

    # ── 7. CNMV cualitativo (sección 9 y 10) ──
    cnmv = ctx.get("cnmv_data", {}) or {}
    cual = cnmv.get("cualitativo", {}) or {}
    cnmv_cual_text = ""
    for key in ["seccion_9_texto_completo", "seccion_10_perspectivas_texto"]:
        val = cual.get(key, "")
        if val:
            cnmv_cual_text += f"\n--- CNMV {key} ---\n{val}\n"
    if cnmv_cual_text:
        sections.append(f"=== CNMV CUALITATIVO ===\n{cnmv_cual_text}")

    # ── 8. Perfiles de gestores ──
    manager = ctx.get("manager_profile", {}) or {}
    manager_text = ""
    # Try multiple profile locations
    profiles = (manager.get("profiles", []) or manager.get("perfiles", [])
                or manager.get("equipo_detalle_web", []) or [])
    for profile in profiles:
        if isinstance(profile, dict):
            manager_text += f"\n--- GESTOR: {profile.get('nombre', '')} ---\n"
            manager_text += json.dumps(profile, ensure_ascii=False, indent=2) + "\n"
    # Web sources about managers
    web_sources = manager.get("fuentes_web", []) or manager.get("sources", []) or []
    for src in web_sources:
        if isinstance(src, dict):
            texto = src.get("texto", "") or src.get("content", "")
            if texto and len(texto) > 100:
                manager_text += f"\n--- WEB: {src.get('titulo', src.get('url', ''))} ---\n{texto}\n"
    # Info from letters about managers
    info_cartas = manager.get("informacion_cartas", []) or []
    for item in info_cartas:
        if isinstance(item, dict):
            manager_text += f"\n--- CARTA GESTOR: {item.get('periodo', '')} ---\n{json.dumps(item, ensure_ascii=False)}\n"
    # CNMV info
    info_cnmv = manager.get("informacion_cnmv", []) or []
    for item in info_cnmv:
        if isinstance(item, dict):
            manager_text += f"\n--- CNMV GESTOR: {item.get('periodo', '')} ---\n{json.dumps(item, ensure_ascii=False)}\n"
    # Gemini raw analysis
    gemini_raw = manager.get("gemini_raw", "")
    if gemini_raw:
        manager_text += f"\n--- ANÁLISIS GEMINI GESTORES ---\n{gemini_raw}\n"
    if manager_text:
        sections.append(f"=== PERFILES DE GESTORES ===\n{manager_text}")

    # ── 9. Fuentes externas / readings ──
    readings = ctx.get("readings_data", {}) or {}
    readings_text = ""
    for key in ["analisis_escritos", "analisis", "multimedia", "lecturas"]:
        for item in (readings.get(key, []) or []):
            if isinstance(item, dict):
                texto = (item.get("texto_completo", "") or item.get("texto", "")
                         or item.get("contenido", "") or "")
                if texto and len(texto) > 100:
                    readings_text += f"\n--- {item.get('fuente', '')} | {item.get('titulo', '')} ({item.get('fecha', '')}) ---\n{texto}\n"
    if readings_text:
        sections.append(f"=== FUENTES EXTERNAS ===\n{readings_text}")

    # ── 10. Comisiones detalladas ──
    comision_exito = output.get("comision_exito", {})
    if comision_exito:
        sections.append(f"=== COMISIÓN DE ÉXITO ===\n{json.dumps(comision_exito, ensure_ascii=False, indent=2)}")

    # Build final prompt
    context_body = "\n\n".join(sections)

    system = f"""Eres un asistente RAG (retrieval-augmented) sobre el fondo {nombre} ({ctx['isin']}) gestionado por {gestora}.

ROL Y LÍMITES — leer con atención (Bug 6, 2026-04-27):
- Eres un asistente que SOLO resume y extrae información de los DOCUMENTOS DEL FONDO cargados aquí.
- NO eres un asesor financiero general, NO consultas la web, NO usas tu conocimiento de mercado para inventar datos.
- Si la pregunta requiere información que NO está en los documentos, responde literalmente:
  "No tengo ese dato en los documentos del fondo."
- NUNCA inventes cifras, fechas, nombres de gestores o decisiones que no aparezcan en el contexto.
- Si te preguntan sobre mercados generales, otros fondos, o noticias actuales, responde:
  "Mi alcance está limitado a los documentos de este fondo. No puedo responder eso."

CITAR FUENTES (obligatorio cuando sea posible):
- "Según la carta semestral de enero 2024..."
- "En el informe CNMV 2023 H2..."
- "El perfil del gestor menciona..."
- "Reading externo de Morningstar (URL en fuentes_externas) indica..."

ESTILO:
- Responde en español.
- Conciso pero completo: si la pregunta requiere detalle, dalo. Si es factual, una frase.
- Usa cifras concretas siempre que las tengas (con unidad y periodo).
- Si hay contradicción entre fuentes, señálalo: "El informe CNMV dice X mientras la carta del gestor dice Y."

CONTEXTO DISPONIBLE: {len(sections)} bloques cargados (output.json + cnmv_data + letters + readings + manager_profile + pdf_cache). Estos son TUS ÚNICOS DATOS — no hay nada más.

{context_body}
"""
    return system


# ═══════════════════════════════════════════════════════════════
# Gemini streaming with CONTEXT CACHING (90% cost savings)
# ═══════════════════════════════════════════════════════════════

CACHED_CONTEXT = None  # Stores the cache reference

def get_gemini_client():
    # Kill switch (2026-05-28): si GEMINI_DISABLED=1, abortar el chat.
    # El chat conversacional usa context caching de Gemini que no tiene
    # equivalente directo en Anthropic — desactivado hasta migración futura.
    from tools.gemini_killswitch import is_gemini_disabled
    if is_gemini_disabled():
        raise RuntimeError(
            "Chat conversacional desactivado: Gemini está OFF (GEMINI_DISABLED=1). "
            "Para reactivar, edita .env. El resto del pipeline usa Anthropic."
        )
    from google import genai
    return genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))


def create_or_get_cache():
    """Create a cached context for the fund (one-time) or return existing.
    Gemini caches cost 1/4 of normal input tokens and last 1h by default.
    Reusing the cache across questions saves ~75-90% of input cost."""
    global CACHED_CONTEXT
    if CACHED_CONTEXT is not None:
        return CACHED_CONTEXT

    from google.genai import types

    client = get_gemini_client()
    system = build_system_prompt(FUND_CONTEXT)

    try:
        # Context caching requires explicit model version + >=1024 tokens
        cache = client.caches.create(
            model="gemini-2.5-flash",
            config=types.CreateCachedContentConfig(
                system_instruction=system,
                ttl="3600s",  # 1 hour
                display_name=f"fund_{FUND_ISIN}",
            ),
        )
        CACHED_CONTEXT = cache.name
        print(f"[CACHE] Created cache: {cache.name}")
        return CACHED_CONTEXT
    except Exception as exc:
        print(f"[CACHE] Failed to create cache ({exc}), using inline context")
        return None


async def stream_gemini_response(question: str):
    """Stream response from Gemini with cached fund context."""
    from google.genai import types

    client = get_gemini_client()

    # Build messages with conversation history
    contents = []
    for msg in CONVERSATION_HISTORY[-MAX_HISTORY:]:
        contents.append(types.Content(
            role=msg["role"],
            parts=[types.Part(text=msg["text"])]
        ))
    contents.append(types.Content(
        role="user",
        parts=[types.Part(text=question)]
    ))

    # Try to use cached context (90% cost savings)
    cache_name = create_or_get_cache()

    try:
        if cache_name:
            # Use cached context
            response = client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    cached_content=cache_name,
                    temperature=0.3,
                    max_output_tokens=4000,
                ),
            )
        else:
            # Fallback: inline context
            system = build_system_prompt(FUND_CONTEXT)
            response = client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system,
                    temperature=0.3,
                    max_output_tokens=4000,
                ),
            )

        full_response = ""
        for chunk in response:
            if chunk.text:
                full_response += chunk.text
                yield chunk.text

        # Save to history
        CONVERSATION_HISTORY.append({"role": "user", "text": question})
        CONVERSATION_HISTORY.append({"role": "model", "text": full_response})

    except Exception as exc:
        # If cache expired or failed, invalidate and retry inline
        global CACHED_CONTEXT
        CACHED_CONTEXT = None
        yield f"\n\nError: {exc}"


# ═══════════════════════════════════════════════════════════════
# API endpoints
# ═══════════════════════════════════════════════════════════════

@app.get("/api/info")
async def info():
    return {
        "isin": FUND_ISIN,
        "nombre": FUND_NAME,
        "documents_loaded": list(k for k, v in FUND_CONTEXT.items() if v and k != "isin"),
        "history_length": len(CONVERSATION_HISTORY),
        "cache_active": CACHED_CONTEXT is not None,
    }


@app.post("/api/cache/refresh")
async def refresh_cache():
    global CACHED_CONTEXT
    CACHED_CONTEXT = None
    return {"ok": True, "message": "Cache invalidated, will be recreated on next query"}


@app.post("/api/chat")
async def chat(request: Request):
    body = await request.json()
    question = body.get("question", "").strip()
    if not question:
        return {"error": "No question provided"}

    return StreamingResponse(
        stream_gemini_response(question),
        media_type="text/plain",
    )


@app.post("/api/clear")
async def clear_history():
    CONVERSATION_HISTORY.clear()
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("isin", nargs="?", default="ES0112231008")
    parser.add_argument("--port", type=int, default=8899)
    args = parser.parse_args()

    FUND_ISIN = args.isin.strip().upper()
    print(f"Loading fund context for {FUND_ISIN}...")
    FUND_CONTEXT = load_fund_context(FUND_ISIN)

    output = FUND_CONTEXT.get("output", {}) or {}
    FUND_NAME = output.get("nombre", FUND_ISIN)

    # Calculate context size
    ctx_size = len(json.dumps(FUND_CONTEXT, ensure_ascii=False))
    docs = [k for k, v in FUND_CONTEXT.items() if v and k != "isin"]
    print(f"Loaded: {FUND_NAME}")
    print(f"Documents: {', '.join(docs)}")
    print(f"Context size: {ctx_size:,} chars ({ctx_size//1000}K)")
    print(f"\nChat server ready at http://localhost:{args.port}")
    print(f"Dashboard chat tab will connect to this server.\n")

    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")
