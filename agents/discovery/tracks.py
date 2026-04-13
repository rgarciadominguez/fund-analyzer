"""
Dos tracks que corren en paralelo y comparten state.

Track REPORTS   → annual_report, semi_annual_report, prospectus, kid, factsheet
Track COMMERCIAL → quarterly_letter, manager_presentation

Ambos usan la misma cascada:
  1. KB hit → valida → done
  2. Candidatos del gestora_crawl (lista compartida) → filtrar por tipo
  3. Google queries específicas del tipo
  4. Early exit si el gap de su pool está cubierto
"""
from __future__ import annotations

import httpx
from rich.console import Console

from agents.discovery import kb
from agents.discovery.downloader import download_and_register
from agents.discovery.google_finder import build_queries, search_google
from agents.discovery.state import SharedState

console = Console()


# Pools de doc_types por track
REPORTS_TYPES = {
    "annual_report", "semi_annual_report",
    "prospectus", "kid", "factsheet",
}
COMMERCIAL_TYPES = {
    "quarterly_letter", "manager_presentation",
}


async def _try_kb_hits(
    state: SharedState, c: httpx.AsyncClient, types: set[str],
) -> None:
    """Intenta URLs del KB para los targets del track."""
    for dt, periodo in state.missing_doc_targets():
        if dt not in types:
            continue
        url = kb.lookup(state.kb, dt, periodo)
        if not url or state.already_downloaded(url):
            continue
        doc = await download_and_register(
            state, c, url, dt, periodo,
            source="knowledge_base",
            source_detail="kb-hit",
        )
        if doc:
            console.log(f"[dim]kb-hit {dt}@{periodo}[/dim] -> {url[-80:]}")


async def _try_candidates(
    state: SharedState, c: httpx.AsyncClient, types: set[str],
    candidates: list[dict],
) -> None:
    """Para cada candidato del crawl compatible con el track, descargar+validar."""
    for cand in candidates:
        if cand["doc_type"] not in types:
            continue
        # Hints básicos del periodo: extraer año del nombre del link si hay
        import re as _re
        m = _re.search(r"\b(20\d{2})\b", cand.get("url", "") + cand.get("text", ""))
        periodo = m.group(1) if m else ""
        # ¿Ya cubrimos (doc_type, periodo)?
        if state.coverage(cand["doc_type"], periodo):
            continue
        if state.already_downloaded(cand["url"]):
            continue
        doc = await download_and_register(
            state, c, cand["url"], cand["doc_type"], periodo,
            source="gestora_web",
            source_detail=cand.get("page_found_at", ""),
        )
        if doc:
            # Guardar en KB
            kb.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
            console.log(f"[green]gestora {doc.doc_type}@{doc.periodo}[/green] "
                        f"<- {cand.get('page_found_at','')[-60:]}")


async def _try_google(
    state: SharedState, c: httpx.AsyncClient, types: set[str], web_search_fn,
) -> None:
    """Para cada target no cubierto del track, Google queries dirigidas."""
    fund_name = state.identity.get("nombre_oficial", "") or state.isin
    for dt, periodo in state.missing_doc_targets():
        if dt not in types:
            continue
        if state.coverage(dt, periodo):
            continue
        queries = build_queries(dt, periodo, state.isin, fund_name)
        for q in queries:
            if state.budget.google_remaining <= 0:
                return
            results = await search_google(state, q, web_search_fn)
            # Probar los top-5 resultados (ya scored)
            for r in results[:5]:
                url = r["url"]
                if state.coverage(dt, periodo):
                    break
                if state.already_downloaded(url):
                    continue
                doc = await download_and_register(
                    state, c, url, dt, periodo,
                    source="google",
                    source_detail=q,
                )
                if doc:
                    kb.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
                    console.log(f"[cyan]google {doc.doc_type}@{doc.periodo}[/cyan] <- {url[-80:]}")
                    break  # avanzar al siguiente target una vez cubierto
            if state.coverage(dt, periodo):
                break


async def run_track(
    state: SharedState,
    c: httpx.AsyncClient,
    types: set[str],
    candidates: list[dict],
    web_search_fn,
) -> None:
    """Ejecuta la cascada para un pool de doc_types."""
    await _try_kb_hits(state, c, types)
    if state.is_fully_covered():
        return
    await _try_candidates(state, c, types, candidates)
    if state.is_fully_covered():
        return
    await _try_google(state, c, types, web_search_fn)


async def run_reports_track(state, c, candidates, web_search_fn):
    await run_track(state, c, REPORTS_TYPES, candidates, web_search_fn)


async def run_commercial_track(state, c, candidates, web_search_fn):
    await run_track(state, c, COMMERCIAL_TYPES, candidates, web_search_fn)
