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


# Patterns de doc_type. ORDEN IMPORTA: los más específicos arriba para no
# ser robados por matches genéricos.
#
# Cada tipo incluye:
#   - Palabras completas (annual report, jahresbericht, etc.)
#   - Abreviaturas industria (AR-, SAR-, MR-, KID-, PRS-) como prefijo en paths
#     de URL — las usan la mayoría de CDNs de distribuidores (Natixis, BlackRock,
#     Amundi, etc.) con formato `/{TYPE}-{share_slug}/...`
DOC_KEYWORDS: list[tuple[re.Pattern, str]] = [
    # Semi-annual primero (contiene "annual" como substring)
    (re.compile(r"\b(semi[_\-\s]?annual|half[_\-\s]?year|rapport[_\-\s]*semestriel|halbjahresbericht|halbjahres|SEMIREP|informe[_\-\s]*semestral)\b", re.I), "semi_annual_report"),
    # Prefijo industrial /SAR- en URL path (Natixis/DNCA y otros)
    (re.compile(r"[/_-]SAR[_\-][a-z]", re.I), "semi_annual_report"),
    # KID/KIID
    (re.compile(r"\b(kiid|priips|wesentliche[_\-\s]*anlegerinformationen|dic[_\-\s]priips|datos[_\-\s]+fundamentales)\b", re.I), "kid"),
    (re.compile(r"\bkid\b", re.I), "kid"),
    (re.compile(r"[/_-]KI?ID[_\-]", re.I), "kid"),
    # Prospectus (Verkaufsprospekt en alemán = VKP)
    (re.compile(r"\b(prospectus|prospekt|verkaufsprospekt|folleto[_\-\s]*informativo)\b", re.I), "prospectus"),
    # VKP abreviatura — rodeada de no-alfanumérico (incluye underscore)
    (re.compile(r"(?<![A-Za-z0-9])VKP(?![A-Za-z0-9])"), "prospectus"),
    (re.compile(r"[/_-]PRS?[_\-]", re.I), "prospectus"),
    # Factsheet / Monthly report
    (re.compile(r"\b(factsheet|fact[_\-\s]sheet|monthly[_\-\s]report|monatsbericht|ficha[_\-\s]mensual|reporting[_\-\s]mensuel|MMF)\b", re.I), "factsheet"),
    (re.compile(r"[/_-]MR[_\-][a-z]", re.I), "factsheet"),  # /MR-share_slug/ pattern
    # Quarterly letter
    (re.compile(r"\b(quarterly[_\-\s]+letter|letter[_\-\s]+to[_\-\s]+(share|unit)holders?|carta[_\-\s]+trimestral|lettre[_\-\s]+trimestrielle|investor[_\-\s]+letter|commentary)\b", re.I), "quarterly_letter"),
    # Prefijo industrial /LETTER- (DNCA y otros)
    (re.compile(r"[/_-]LETTER[_\-][a-z]", re.I), "quarterly_letter"),
    # Annual report
    (re.compile(r"\b(annual[_\-\s]*report|rapport[_\-\s]*annuel|jahresbericht|jahresabschluss|rechenschaftsbericht|RECHENSCHAFT|informe[_\-\s]*anual|memoria[_\-\s]*anual|ANNREP)\b", re.I), "annual_report"),
    (re.compile(r"[/_-]AR[_\-][a-z]", re.I), "annual_report"),  # /AR-share_slug/
    # Presentation
    (re.compile(r"\b(presentation|pitch[_\-\s]deck|webinar|conference|investor[_\-\s]+day)\b", re.I), "manager_presentation"),
]

# ── Rutas candidatas donde suelen estar los documentos en webs de gestoras ──
DOCS_PATHS = [
    "/documents", "/en/documents", "/fr/documents", "/es/documentos",
    "/publications", "/en/publications",
    "/fund-documents", "/documentation",
    "/reports", "/en/reports", "/regulatory-documents",
    "/downloads", "/en/downloads",
    "/media",
    # Gestoras que exponen docs via páginas de fondo
    "/funds", "/our-funds", "/portfolio", "/portfolios", "/products", "/strategies",
]


def classify_link(text: str, href: str) -> str | None:
    """Clasifica un link (text+href) como doc_type si matchea alguna keyword."""
    combined = f"{text or ''} {href or ''}"
    for pat, kind in DOC_KEYWORDS:
        if pat.search(combined):
            return kind
    # Fallback: patrón "YYYY-MM-{fund}.pdf" → factsheet mensual
    # (ej. 2018-01-Storm-Bond-Fund.pdf, 2024-02_Fund_Name.pdf)
    if re.search(r"\b(19|20)\d{2}[-_.](0[1-9]|1[0-2])\b.*\.pdf", combined, re.I):
        return "factsheet"
    return None


def detect_factsheet_month(url: str, text: str = "") -> tuple[str, str]:
    """
    Devuelve (year, month) del factsheet basándose en el FILENAME.
    El filename suele tener patrón `YYYY-MM-{name}.pdf` que indica el
    mes fiscal — incluso si el path dice otro mes de subida.

    Ej: `stormcapital.no/wp-content/uploads/2024/01/2023-12-Fund.pdf`
      → path dice 2024/01 (mes de subida)
      → filename dice 2023-12 (mes fiscal) ← este prevalece
      → devuelve ("2023", "12")
    """
    # Extraer solo el filename (última parte después del slash)
    filename = url.rsplit("/", 1)[-1]

    # Patrón YYYY-MM en filename (prioridad)
    m = re.search(r"\b((?:19|20)\d{2})[-_.](0[1-9]|1[0-2])\b", filename)
    if m:
        return (m.group(1), m.group(2))

    # Fallback: cualquier texto asociado al link (poco fiable)
    if text:
        m = re.search(r"\b((?:19|20)\d{2})[-_.](0[1-9]|1[0-2])\b", text)
        if m:
            return (m.group(1), m.group(2))

    return ("", "")


def factsheet_subtype(month: str) -> str:
    """
    Dado un mes (MM), devuelve el subtipo del factsheet:
      "eoy"       → diciembre (year-end snapshot, sustituto parcial de AR)
      "mid_year"  → junio (snapshot semestral, sustituto parcial de SAR)
      "monthly"   → cualquier otro
    """
    if month == "12":
        return "eoy"
    if month == "06":
        return "mid_year"
    if month:
        return "monthly"
    return ""


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
    """GET con dedup via SharedState + fallback Cloudflare (curl_cffi)."""
    from agents.discovery.cloudflare_bypass import fetch_with_fallback
    cached = state.page_cached(url)
    if cached is not None:
        return cached
    if state.already_fetched(url):
        return None
    if not state.budget.try_http():
        return None
    try:
        status, body, hdrs = await fetch_with_fallback(c, url, timeout=20)
        if status == 200:
            ct = (hdrs.get("content-type") or "").lower()
            if "text/html" in ct or body[:100].lower().startswith((b"<!doctype", b"<html")):
                text = body.decode("utf-8", errors="ignore")
                await state.cache_page(url, text)
                return text
        await state.mark_fetched(url)
    except Exception:
        await state.mark_fetched(url)
    return None


async def crawl_gestora(
    state: SharedState, c: httpx.AsyncClient, gestora: str,
    extra_bases: list[str] | None = None,
) -> list[dict]:
    """
    Crawl BFS limitado a las rutas de documentos de la gestora.
    Prueba hasta 3 dominios candidatos (algunas gestoras tienen sitio
    corporate + sitio AM separados).

    extra_bases: dominios adicionales (con o sin scheme) descubiertos por
    otros métodos. Se añaden al pool de crawl.

    Devuelve [{url, text, doc_type, depth, page_found_at}, ...] sin
    descargar los PDFs.
    """
    bases: list[str] = []
    if gestora:
        bases = await find_gestora_base_urls(state, c, gestora, max_bases=3)

    if extra_bases:
        for b in extra_bases:
            if not b.startswith("http"):
                b = f"https://{b}"
            base_clean = b.rstrip("/")
            if base_clean not in bases:
                bases.append(base_clean)

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

            # Descartar externos (que no estén en ninguno de los bases) y anchors
            if not full.startswith(base_prefixes) and not _is_known_cdn(full):
                continue
            if full == page_url or "#" == href[:1]:
                continue

            # PDFs/docs candidatos: clasificar pero NO descartar si no hay keyword.
            # El validator post-download decide. Esto permite ADAPTACIÓN a
            # cualquier formato de URL que use la gestora.
            if _looks_like_doc(full):
                if full in seen_pdfs:
                    continue
                seen_pdfs.add(full)
                dt = classify_link(text, full) or "unknown_pdf"
                found.append({
                    "url": full,
                    "text": text,
                    "doc_type": dt,
                    "depth": depth,
                    "page_found_at": page_url,
                })
            # Si es una sub-página candidata a tener más docs, encolar (depth<=1)
            elif depth < 1 and any(_is_docs_subpage(full, b) for b in bases):
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
        # Las gestoras suelen exponer docs por fondo en /funds/{slug}/,
        # /portfolios/{slug}/, /products/{slug}/ o /strateg(y|ies)/.
        "fund", "portfolio", "product", "strateg",
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
