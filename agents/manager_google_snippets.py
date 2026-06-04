"""
Manager via Google Snippets — extrae nombres de gestores ES de snippets de
Google search en sitios indexados (Morningstar, FT, Finect, web gestora).

Cuando Cloudfront bloquea el scraping directo de Morningstar, los snippets
de Google contienen igualmente la info ("Equipo de gestión: X. 23 abr 2021").

Pipeline:
1. Search por ISIN + queries variadas en Serper
2. Filtrar snippets de fuentes confiables
3. Extraer nombres con regex Person-Name + filtros gestora
4. Validar con Gemini Flash que son personas reales (no inventos)
5. Guardar en manager_profile.json (compatible con resto de pipeline)

Sin LLM si solo se necesita el nombre. Con Gemini cuando hay ambigüedad.
"""
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent

# Cargada desde data/trusted_sources.json (loader en tools/trusted_sources.py).
# Para añadir/quitar dominios, editar el JSON.
sys.path.insert(0, str(ROOT))
from tools.trusted_sources import get_trusted_domains  # noqa: E402

TRUSTED_DOMAINS = get_trusted_domains() or [
    # Fallback hardcoded — solo se usa si el JSON falta o falla.
    "morningstar.com", "morningstar.es",
    "ft.com",
    "finect.com", "preahorro.com",
    "rankia.com",
    "moclano.substack.com", "saludfinanciera.substack.com",
    "r4.com", "renta4.com",
    "dunascapital.com", "cartesio.com", "cartesioinversiones.com",
    "singularam.com", "sigmainversiones.com",
    "myinvestor.es", "avantagecapital.com",
    "numantiapatrimonio.com",
    "moiglobal.com", "valueinversion.com",
    "citywire.com", "citywire.es",
]

# Patrones que sugieren mención de gestor en snippet
GESTOR_HINTS = [
    "Equipo de gestión", "Equipo gestor", "Gestor", "gestionado por",
    "Manager & start date", "Manager:", "asesor", "asesorado por",
]

# Stop-words para descartar nombres falsos
STOP_NAMES = {"Renta", "Singular", "Gamma", "Sigma", "Cartesio", "Dunas",
              "Equipo", "Gestor", "Manager", "Asesor", "Cartera"}


def _log(isin, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [MGR_SNIP] [{isin}] {msg}")


def search_serper(query: str, num: int = 10) -> list:
    key = os.getenv("SERPER_API_KEY")
    if not key:
        return []
    # Guardrail de coste Serper (2026-06-03): respeta el tope mensual / killswitch.
    from tools.google_search import serper_allowed_and_reserve
    if not serper_allowed_and_reserve():
        return []
    try:
        r = httpx.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            json={"q": query, "gl": "es", "hl": "es", "num": num},
            timeout=15,
        )
        return r.json().get("organic") or []
    except Exception:
        return []


def extract_names_from_text(text: str) -> list[str]:
    """Extract Person Names (2-5 capitalized words, allow tildes/ñ) from text.
    Filtra stop words y patrones de fund/gestora."""
    # Pattern: 2-5 capitalized tokens, allow accented chars + 'ñ'
    pattern = re.compile(
        r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+(?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+|de|del|la|el|y|San)){1,4})"
    )
    candidates = []
    for m in pattern.finditer(text):
        name = m.group(1).strip()
        tokens = name.split()
        # Filter: at least 2 tokens, first not stop word, not include stop names alone
        if len(tokens) < 2:
            continue
        if tokens[0] in STOP_NAMES:
            continue
        # Drop names that are clearly company/fund descriptors
        if any(t in {"Asset", "Capital", "Bank", "Banco", "Inversiones", "Gestora",
                     "Fondos", "Fund", "Fondo", "Partners", "Management",
                     "Patrimonio", "Global", "Multigestión"} for t in tokens):
            continue
        # Avoid 2-token names that are common stop word + word
        if name.lower() in {"renta variable", "renta fija", "asset management"}:
            continue
        candidates.append(name)
    return candidates


def _validate_names_with_gemini(candidates: list[str], fund_name: str, gestora: str) -> list[str]:
    """Filtra candidatos para quedarse solo con NOMBRES DE PERSONAS REALES.
    Elimina ruido tipo 'Ver Morningstar Medalist Rating', 'Citywire Rating', etc.
    Si Gemini no disponible, devuelve heurística simple."""
    if not candidates:
        return []
    # Heurística rápida primero: descartar candidatos que contienen palabras-marca
    BRAND_BAD = ["morningstar", "citywire", "rating", "medalist", "rankia",
                 "asesor", "analyst", "ver", "leer", "next", "previous"]
    pre = [c for c in candidates
           if not any(b in c.lower() for b in BRAND_BAD)]
    if not pre:
        return []

    # Kill switch (2026-05-28): si GEMINI_DISABLED=1, devolver pre-filtrados
    # heurísticos sin validación LLM (degradación funcional aceptable).
    from tools.gemini_killswitch import is_gemini_disabled
    if is_gemini_disabled():
        return pre[:5]

    # Validar con Gemini Flash (cheap)
    key = os.environ.get("GOOGLE_API_KEY")
    if not key:
        return pre[:5]  # sin LLM, devolver pre-filtrados
    try:
        from google import genai
        client = genai.Client(api_key=key)
        prompt = (
            f"Tarea: filtra una lista de candidatos para quedarte SOLO con nombres "
            f"de personas reales que sean gestores del fondo {fund_name} (gestora {gestora}).\n\n"
            "REGLAS:\n"
            "- Conserva nombres de personas con nombre+apellido(s) reales (Ivan Garcia, Maria Lopez, etc).\n"
            "- DESCARTA frases que no son nombres ('Ver Morningstar Medalist', 'Cita el Trabajo', 'Anos de Experiencia', etc).\n"
            "- DESCARTA nombres de empresas o entidades (Singular Asset Management, Renta 4 Gestora).\n"
            "- DESCARTA palabras genéricas (Equipo, Director, Gestor solos sin nombre).\n"
            "- NO inventes — si no estás seguro, descarta.\n\n"
            f"Candidatos:\n" + "\n".join(f"- {c}" for c in candidates) +
            "\n\nResponde SOLO con un JSON array de los nombres validados, sin texto adicional."
        )
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        text = resp.text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        validated = json.loads(text)
        if isinstance(validated, list):
            return [str(n).strip() for n in validated if str(n).strip()]
    except Exception:
        pass
    return pre[:5]


def find_managers(isin: str, fund_name: str = "", gestora: str = "") -> dict:
    """Devuelve dict con managers extraídos de snippets Google + validación Gemini."""
    queries = [
        f"site:morningstar.com {isin} gestor",
        f"{isin} morningstar gestor manager",
        f'"{fund_name}" gestor responsable' if fund_name else None,
        f"{isin} site:r4.com OR site:renta4.com OR site:cartesio.com gestor",
        f"{isin} fund manager site:ft.com",
        f'{isin} "Equipo de gestión"',
    ]
    queries = [q for q in queries if q]

    counter: dict = {}
    snippets_used = []
    sources_used = []

    for q in queries:
        results = search_serper(q, num=8)
        for it in results:
            link = it.get("link", "").lower()
            snippet = it.get("snippet", "")
            if not snippet:
                continue
            if not any(d in link for d in TRUSTED_DOMAINS):
                continue
            if not any(h.lower() in snippet.lower() for h in GESTOR_HINTS):
                continue

            names = extract_names_from_text(snippet)
            if names:
                snippets_used.append({"q": q, "link": link[:120], "snippet": snippet[:300]})
                sources_used.append(link)
            for n in names:
                counter[n] = counter.get(n, 0) + 1

    ranked = sorted(counter.items(), key=lambda x: -x[1])
    raw_candidates = [n for n, _ in ranked[:15]]

    # Validar con Gemini para eliminar basura tipo "Ver Morningstar Medalist"
    confirmed = _validate_names_with_gemini(raw_candidates, fund_name, gestora)

    return {
        "isin": isin,
        "fund_name": fund_name,
        "gestora": gestora,
        "managers": confirmed[:5],
        "all_candidates": [{"nombre": n, "score": c} for n, c in ranked[:20]],
        "validated_by": "gemini-2.5-flash" if os.environ.get("GOOGLE_API_KEY") else "heuristica",
        "snippets_used": snippets_used,
        "n_sources": len(set(sources_used)),
        "generado": datetime.now().isoformat(),
        "fuente": "google_snippets",
    }


def save_to_manager_profile(isin: str, result: dict):
    """Guarda en manager_profile.json (compatible con resto de pipeline)."""
    fund_dir = ROOT / "data" / "funds" / isin
    fund_dir.mkdir(parents=True, exist_ok=True)
    prof_path = fund_dir / "manager_profile.json"

    # Merge con existente si hay
    existing = {}
    if prof_path.exists():
        try:
            existing = json.loads(prof_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    if not result["managers"]:
        _log(isin, "Sin managers encontrados via snippets")
        return False

    existing["equipo_gestor"] = result["managers"]
    existing["equipo_detalle_web"] = [{"nombre": n, "fuente": "google_snippet"} for n in result["managers"]]
    existing["snippets_evidencia"] = result["snippets_used"]
    existing["isin"] = isin
    existing["fondo"] = result.get("fund_name", "")
    existing["fuente_managers"] = "google_snippets"
    existing["generado_snippets_at"] = result["generado"]

    prof_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(isin, f"Guardado {len(result['managers'])} managers: {result['managers']}")
    return True


def sync_to_output(isin: str, managers: list[str], gestora: str = ""):
    """Propaga los managers encontrados al output.json en analyst_synthesis.gestores.perfiles
    como perfiles minimales (nombre + cargo placeholder). El analyst_agent puede luego
    enriquecerlos en futuros runs."""
    out_p = ROOT / "data" / "funds" / isin / "output.json"
    if not out_p.exists() or not managers:
        return False
    d = json.loads(out_p.read_text(encoding="utf-8"))
    asy = d.setdefault("analyst_synthesis", {})
    g = asy.setdefault("gestores", {})
    existing_perfiles = g.get("perfiles") or []
    existing_names = {p.get("nombre", "").lower() for p in existing_perfiles}

    n_added = 0
    for m in managers:
        if m.lower() not in existing_names:
            existing_perfiles.append({
                "nombre": m,
                "cargo": "Gestor",
                "trayectoria": f"Gestor identificado vía Google snippets (Morningstar/FT/web gestora). Pendiente enriquecer perfil con búsquedas dirigidas o web equipo de la gestora.",
                "cv_bullets": [],
                "filosofia": "",
                "decisiones_clave": [],
                "rasgos_diferenciales": "",
                "fuente": "google_snippets",
            })
            existing_names.add(m.lower())
            n_added += 1
    g["perfiles"] = existing_perfiles
    out_p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(isin, f"output.json: añadidos {n_added} perfiles a analyst_synthesis.gestores.perfiles")
    return n_added > 0


if __name__ == "__main__":
    # Load env
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if len(sys.argv) < 2:
        print("Uso: python -m agents.manager_google_snippets <ISIN> [<ISIN>...]")
        sys.exit(1)

    for isin in sys.argv[1:]:
        isin = isin.strip().upper()
        # Read fund_name + gestora from output.json
        out_p = ROOT / "data" / "funds" / isin / "output.json"
        fname = ""
        gest = ""
        if out_p.exists():
            d = json.loads(out_p.read_text(encoding="utf-8"))
            fname = d.get("nombre", "")
            gest = d.get("gestora", "")

        _log(isin, f"Buscando managers para {fname} (gestora: {gest})")
        result = find_managers(isin, fname, gest)
        _log(isin, f"Candidatos: {[c['nombre'] + ':' + str(c['score']) for c in result['all_candidates'][:5]]}")
        _log(isin, f"Confirmados: {result['managers']}")

        if save_to_manager_profile(isin, result):
            sync_to_output(isin, result["managers"], gest)
        print()
