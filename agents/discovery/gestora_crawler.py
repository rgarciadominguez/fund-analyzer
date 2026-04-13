"""
Crawler de la web de la gestora.

Estrategia:
  1. Resolver dominio(s) de la gestora a partir del nombre.
  2. Intentar rutas conocidas de documentos (/documents, /fund-documents,
     /publications, /reports, /downloads, /en/*).
  3. Si la web de la gestora es un patrón conocido (Natixis CDN para DNCA,
     fundsmith.co.uk/media, groupama-am.com/publication/FundDoc), construir
     URL directa al PDF.
  4. Scraping BFS limitado (max profundidad 2) de la página documents
     para extraer links a PDFs/HTML de reports.

Todo usa SharedState → no fetches duplicados.
"""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from agents.discovery.state import SharedState


# ── Paterns de documento por KEYWORD en URL o texto del link ────────────────
DOC_KEYWORDS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(annual[_\-\s]*report|rapport[_\-\s]*annuel|jahresbericht|informe[_\-\s]*anual|memoria[_\-\s]*anual|ar[_\-\s]+\d{4}|_AR_|ANNREP)\b", re.I), "annual_report"),
    (re.compile(r"\b(semi[_\-\s]?annual|half[_\-\s]?year|rapport[_\-\s]*semestriel|halbjahresbericht|_SAR_|SEMIREP)\b", re.I), "semi_annual_report"),
    (re.compile(r"\b(quarterly[_\-\s]+letter|letter[_\-\s]+to[_\-\s]+(share|unit)holders?|carta[_\-\s]+trimestral|lettre[_\-\s]+trimestrielle|[-_]QR[-_]|commentary)\b", re.I), "quarterly_letter"),
    (re.compile(r"\b(factsheet|fact[_\-\s]sheet|monthly[_\-\s]report|ficha[_\-\s]mensual|reporting[_\-\s]mensuel|[-_]MR[-_]|MMF)\b", re.I), "factsheet"),
    (re.compile(r"\b(prospectus|prospekt|folleto)\b", re.I), "prospectus"),
    (re.compile(r"\b(kid|kiid|priips|wesentliche[_\-\s]*anlegerinformationen|dic[_\-\s]priips)\b", re.I), "kid"),
    (re.compile(r"\b(presentation|pitch[_\-\s]deck|webinar|conference)\b", re.I), "manager_presentation"),
]

# ── Rutas candidatas donde suelen estar los documentos en webs de gestoras ──
DOCS_PATHS = [
    "/documents", "/en/documents", "/fr/documents", "/es/documentos",
    "/publications", "/en/publications",
    "/fund-documents", "/documentation",
    "/reports", "/en/reports", "/regulatory-documents",
    "/downloads", "/en/downloads",
    "/media",
]


def classify_link(text: str, href: str) -> str | None:
    """Clasifica un link (text+href) como doc_type si matchea alguna keyword."""
    combined = f"{text or ''} {href or ''}"
    for pat, kind in DOC_KEYWORDS:
        if pat.search(combined):
            return kind
    return None


def gestora_domain_candidates(gestora_name: str) -> list[str]:
    """
    Genera dominios candidatos a partir del nombre de la gestora.
    Estrategia: probar PRIMERO la primera palabra significativa con sufijos
    típicos del sector (-am, -investments, asset-management), luego cascadas
    más amplias.
    """
    if not gestora_name:
        return []

    # Tokens significativos (>2 chars, en minúsculas, sin sufijos ruidosos)
    NOISE = {
        "ltd", "limited", "sa", "sarl", "sas", "plc", "ag", "gmbh", "kvg",
        "sgiic", "sgr", "asset", "management", "managers", "investment",
        "investments", "fund", "funds", "the", "of", "europe",
    }
    raw_tokens = re.findall(r"[a-z0-9]+", gestora_name.lower())
    sig_tokens = [t for t in raw_tokens if len(t) > 2 and t not in NOISE]
    full_slug = re.sub(r"[^a-z0-9]+", "-", gestora_name.lower()).strip("-")

    # PRIMERA PRIORIDAD: la primera palabra significativa
    first = sig_tokens[0] if sig_tokens else (raw_tokens[0] if raw_tokens else "")
    variants = []
    if first:
        for tld in ("com", "lu", "fr", "de", "co.uk", "eu", "es"):
            variants.append(f"www.{first}.{tld}")
        for suffix in ("am", "investments"):
            variants.append(f"www.{first}-{suffix}.com")
            variants.append(f"www.{first}{suffix}.com")
        variants.append(f"www.{first}-asset-management.com")

    # SEGUNDA PRIORIDAD: dos primeras palabras
    if len(sig_tokens) >= 2:
        two = "-".join(sig_tokens[:2])
        variants += [f"www.{two}.com", f"www.{two}-am.com"]
        variants.append(f"www.{''.join(sig_tokens[:2])}.com")

    # FALLBACK: slug completo
    name_nodash = full_slug.replace("-", "")
    variants += [
        f"www.{full_slug}.com",
        f"www.{name_nodash}.com",
        f"www.{full_slug}.lu",
        f"www.{full_slug}.fr",
    ]

    # Dedup manteniendo orden
    seen = set()
    out = []
    for v in variants:
        if v not in seen and "." in v[4:]:  # filtra cosas mal formadas
            seen.add(v)
            out.append(v)
    return out


async def find_gestora_base_urls(
    state: SharedState, c: httpx.AsyncClient, gestora: str, max_bases: int = 3,
) -> list[str]:
    """
    Resuelve hasta max_bases URLs base de la gestora que respondan 200.
    Devuelve lista (no solo la primera, porque la primera puede ser una
    página corporate sin docs y la segunda la de asset management).
    """
    # Si KB tiene páginas confirmadas, reusar como base
    kb_pages = state.kb.get("gestora_pages_worth_crawling", [])
    bases = []
    if kb_pages:
        for p in kb_pages:
            base = p.split("/", 3)[:3]
            base_url = "/".join(base)
            if base_url not in bases:
                bases.append(base_url)
        if bases:
            return bases[:max_bases]

    for domain in gestora_domain_candidates(gestora):
        if len(bases) >= max_bases:
            break
        if not state.budget.try_http():
            return bases
        url = f"https://{domain}/"
        if state.already_fetched(url):
            continue
        try:
            # HEAD primero (barato); fallback a GET si HEAD no soportado
            r = await c.head(url, follow_redirects=True, timeout=8)
            await state.mark_fetched(url)
            if r.status_code in (200, 301, 302):
                bases.append(f"https://{domain}".rstrip("/"))
        except Exception:
            continue
    return bases


async def fetch_page(
    state: SharedState, c: httpx.AsyncClient, url: str,
) -> str | None:
    """GET con dedup via SharedState (cache + fetched_urls)."""
    cached = state.page_cached(url)
    if cached is not None:
        return cached
    if state.already_fetched(url):
        return None
    if not state.budget.try_http():
        return None
    try:
        r = await c.get(url, timeout=20)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("text/html"):
            await state.cache_page(url, r.text)
            return r.text
        await state.mark_fetched(url)
    except Exception:
        await state.mark_fetched(url)
    return None


async def crawl_gestora(
    state: SharedState, c: httpx.AsyncClient, gestora: str,
) -> list[dict]:
    """
    Crawl BFS limitado a las rutas de documentos de la gestora.
    Prueba hasta 3 dominios candidatos (algunas gestoras tienen sitio
    corporate + sitio AM separados).

    Devuelve [{url, text, doc_type, depth, page_found_at}, ...] sin
    descargar los PDFs.
    """
    bases = await find_gestora_base_urls(state, c, gestora, max_bases=3)
    if not bases:
        return []

    found: list[dict] = []
    queue: list[tuple[str, int]] = []
    for base in bases:
        queue.append((base, 0))
        for p in DOCS_PATHS:
            queue.append((urljoin(base + "/", p.lstrip("/")), 0))
    visited = set()
    seen_pdfs: set[str] = set()
    base_prefixes = tuple(bases)

    while queue and state.budget.http_remaining > 0:
        page_url, depth = queue.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)

        html = await fetch_page(state, c, page_url)
        if html is None:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(strip=True)
            if not href:
                continue
            full = urljoin(page_url, href)

            # Descartar externos y anchors
            if not full.startswith(base) and not _is_known_cdn(full):
                continue
            if full == page_url or "#" == href[:1]:
                continue

            # Si apunta a PDF/HTML de documento, clasificar y guardar
            if _looks_like_doc(full):
                if full in seen_pdfs:
                    continue
                seen_pdfs.add(full)
                dt = classify_link(text, full)
                if dt:
                    found.append({
                        "url": full,
                        "text": text,
                        "doc_type": dt,
                        "depth": depth,
                        "page_found_at": page_url,
                    })
            # Si es una sub-página candidata a tener más docs, encolar (depth<=1)
            elif depth < 1 and _is_docs_subpage(full, base):
                queue.append((full, depth + 1))

    return found


def _looks_like_doc(url: str) -> bool:
    """Heurística: ¿apunta a un archivo de documento?"""
    u = url.lower()
    return (
        u.endswith(".pdf")
        or u.endswith(".xml")
        or ".pdf?" in u
        or "/download" in u
        or "download_doc" in u
    )


def _is_docs_subpage(url: str, base: str) -> bool:
    """Sub-página que pueda contener más links de documentos."""
    u = url.lower().replace(base.lower(), "")
    return any(kw in u for kw in [
        "document", "publication", "report", "regulatory",
        "download", "media", "investor",
    ])


# ── CDN conocidos que SÍ aceptamos aunque no estén en el dominio de la gestora ──
KNOWN_CDN_HOSTS = [
    "im.natixis.com",       # DNCA y toda la cuadra Natixis
    "api.fundinfo.com",
    "fefundinfo.com",
    "bundesanzeiger.de",
    "luxse.com", "dl.luxse.com", "dl.bourse.lu",
]


def _is_known_cdn(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return any(cdn in host for cdn in KNOWN_CDN_HOSTS)
