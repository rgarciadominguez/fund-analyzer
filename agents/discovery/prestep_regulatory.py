"""
Pre-step regulatorio: descarga Prospectus + KIID/KID ANTES del flujo principal.

Razón: son documentos estables, contienen la estructura de comisiones (mgmt
fee, performance fee, HWM) y la ficha regulatoria completa. Si no se cogen
primero, el budget puede agotarse en factsheets históricos y quedarnos
sin la parte más crítica.

Fuentes, en orden de preferencia:
  1. Finect (finect.com/fondos-inversion/{ISIN})
  2. Morningstar retail (morningstar.es, morningstar.com)
  3. Web gestora (via bootstrap later)
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from rich.console import Console

from agents.discovery import kb as kb_mod
from agents.discovery.downloader import download_and_register
from agents.discovery.state import SharedState

console = Console()


def _search_pdfs_in_page(html: str, isin: str, base_url: str) -> list[dict]:
    """Extrae links a PDFs de una página, priorizando los que contienen ISIN."""
    soup = BeautifulSoup(html, "html.parser")
    from urllib.parse import urljoin
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        text = a.get_text(strip=True)
        combined = f"{text} {full}".lower()

        if not (".pdf" in full.lower() or "download" in full.lower()):
            continue

        # Clasificar intención del link
        doc_type = None
        priority = 0
        if re.search(r"\b(kid|kiid|priips|dic\b|datos fundamentales|key information)\b", combined):
            doc_type = "kid"
            priority = 10
        elif re.search(r"\b(prospec|prospekt|folleto|verkaufsprospekt|vkp)\b", combined):
            doc_type = "prospectus"
            priority = 9

        if not doc_type:
            continue

        # Bonus si contiene ISIN
        if isin.lower() in combined:
            priority += 5

        candidates.append({
            "url": full,
            "text": text,
            "doc_type": doc_type,
            "priority": priority,
        })
    candidates.sort(key=lambda x: x["priority"], reverse=True)
    return candidates


async def _try_source(
    state: SharedState,
    c: httpx.AsyncClient,
    source_name: str,
    url: str,
) -> int:
    """Visita una página de fuente, extrae candidates y los descarga. Devuelve nº docs obtenidos."""
    if state.already_fetched(url):
        return 0
    if not state.budget.try_http():
        return 0
    try:
        r = await c.get(url, timeout=20)
        await state.mark_fetched(url)
        if r.status_code != 200:
            return 0
    except Exception:
        return 0

    candidates = _search_pdfs_in_page(r.text, state.isin, url)
    if not candidates:
        return 0

    got = 0
    for cand in candidates[:6]:  # máx 6 intentos por fuente
        if state.coverage(cand["doc_type"], ""):
            continue
        if state.already_downloaded(cand["url"]):
            continue
        doc = await download_and_register(
            state, c, cand["url"], cand["doc_type"], "",
            source="prestep", source_detail=source_name,
        )
        if doc:
            kb_mod.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
            console.log(f"[bold green]prestep {doc.doc_type}[/bold green] <- {source_name}: {cand['url'][-80:]}")
            got += 1
            if got >= 2:  # prospectus + KIID obtenidos
                break
    return got


async def _google_hunt(
    state: SharedState,
    c: httpx.AsyncClient,
    web_search_fn,
    doc_type: str,
) -> int:
    """
    Para prospectus / KIID, lanza queries dorked sobre Finect, Morningstar y
    la web en general. El filetype:pdf nos lleva directo al binario saltándose
    las SPAs.
    """
    if web_search_fn is None:
        return 0
    isin = state.isin
    fund = state.identity.get("nombre_oficial", "") or isin

    if doc_type == "prospectus":
        kws = '"prospectus" OR "prospekt" OR "folleto" OR "verkaufsprospekt"'
    else:  # kid
        kws = '"KID" OR "KIID" OR "PRIIPS" OR "datos fundamentales"'

    queries = [
        f'site:finect.com "{isin}" filetype:pdf',
        f'site:morningstar.es "{isin}" filetype:pdf',
        f'site:morningstar.com "{isin}" filetype:pdf',
        f'"{isin}" {kws} filetype:pdf',
    ]

    got = 0
    for q in queries:
        if state.budget.google_remaining <= 0:
            break
        if state.coverage(doc_type, ""):
            break
        if state.google_done(q):
            continue
        await state.mark_google_done(q)
        state.budget.try_google()
        try:
            results = await web_search_fn(q) or []
        except Exception:
            continue
        for r in results[:5]:
            url = (r.get("url") or "").strip()
            if not url or state.already_downloaded(url):
                continue
            doc = await download_and_register(
                state, c, url, doc_type, "",
                source="prestep", source_detail=f"google:{q[:40]}",
            )
            if doc:
                kb_mod.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
                console.log(f"[bold green]prestep {doc.doc_type}[/bold green] <- {url[-80:]}")
                got += 1
                break
    return got


async def run_prestep(
    state: SharedState,
    c: httpx.AsyncClient,
    web_search_fn=None,
) -> None:
    """
    Intenta obtener prospectus + KIID ANTES del flujo principal.
    Orden:
      1. KB hit (instant)
      2. Google dorks (saltan SPAs, van directo al PDF)
    No usamos page-fetches a Finect/Quefondos porque son SPAs lentas que
    bloquean el flujo. El bootstrap crawl posterior pillará lo que falte.
    """
    for dt in ("prospectus", "kid"):
        url = kb_mod.lookup(state.kb, dt, "")
        if url and not state.already_downloaded(url):
            doc = await download_and_register(
                state, c, url, dt, "",
                source="knowledge_base", source_detail="prestep-kb",
            )
            if doc:
                console.log(f"[dim]prestep kb-hit {dt}[/dim] <- {url[-60:]}")

    if state.coverage("prospectus", "") and state.coverage("kid", ""):
        return

    if web_search_fn is not None:
        if not state.coverage("prospectus", ""):
            await _google_hunt(state, c, web_search_fn, "prospectus")
        if not state.coverage("kid", ""):
            await _google_hunt(state, c, web_search_fn, "kid")
