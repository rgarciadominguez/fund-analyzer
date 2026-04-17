"""
Discovery v2 — versión simplificada en un solo archivo.

Pipeline:
  1. Identity (reusa enrich_from_local_docs de v1)
  2. Harvest de web gestora (BFS depth 2, curl_cffi, URL-first classification)
  3. Wayback sólo para AR/SAR años faltantes
  4. Download con quotas por tipo
  5. Email draft si gap>50% + beep final

Firma pública:
    DiscoveryV2(isin, identity, gap, fund_dir, web_search_fn=None)
    await pipeline.run() -> SharedState

Consumida por agents/intl_discovery_agent.py.
"""
from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from rich.console import Console

from agents.discovery import kb as kb_mod
from agents.discovery.cloudflare_bypass import fetch_with_fallback
from agents.discovery.downloader import download_and_register
from agents.discovery.identity_resolver import enrich_from_local_docs
from agents.discovery.state import SharedState

console = Console()


# ═══════════════════════════════════════════════════════════════════════════
# 1. CLASIFICADOR URL-FIRST
# ═══════════════════════════════════════════════════════════════════════════

_MONTHS = r"(?:january|february|march|april|may|june|july|august|september|october|november|december)"
_MONTHS_ABBR = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"

# Reglas ordenadas por especificidad. Primera que matchea el filename gana.
_RULES: list[tuple[str, re.Pattern, str, int, str]] = [
    # (rule_id, pattern, doc_type, confidence, subtype_key)

    # ── SKIP (primero, cortan antes que nada) ──
    ("skip_legal", re.compile(r"\b(privacy|cookies?|terms|conditions|legal[-_ ]notice|aviso[-_ ]legal|disclaimer|gdpr)\b", re.I), "skip", 0, "legal"),
    ("skip_promo", re.compile(r"\b(application[-_ ]form|formulario|brochure|leaflet|glossary|application)\b", re.I), "skip", 0, "promotional"),

    # ── Annual Report ──
    ("ar_dated_full", re.compile(rf"annual[-_ ]report[-_ ]\d{{1,2}}[-_ ]{_MONTHS}[-_ ](?:19|20)\d{{2}}", re.I), "annual_report", 95, ""),
    ("ar_year", re.compile(r"(?:annual[-_ ]report|jahresbericht|rechenschaftsbericht|rapport[-_ ]annuel|informe[-_ ]anual|memoria[-_ ]anual|annrep|annual[-_ ]accounts)", re.I), "annual_report", 90, ""),
    ("ar_prefix", re.compile(r"(?:^|/)AR[-_][a-z0-9]", re.I), "annual_report", 75, ""),

    # ── Semi-Annual / Interim ──
    ("interim_dated", re.compile(rf"interim[-_ ]report[-_ ]{_MONTHS}[-_ ](?:19|20)\d{{2}}", re.I), "semi_annual_report", 95, "interim"),
    ("interim", re.compile(r"interim[-_ ]report|interim[-_ ]accounts", re.I), "semi_annual_report", 88, "interim"),
    ("sar_year", re.compile(r"(?:semi[-_ ]?annual|halbjahres|rapport[-_ ]semestriel|informe[-_ ]semestral|semirep)", re.I), "semi_annual_report", 90, ""),
    ("sar_prefix", re.compile(r"(?:^|/)SAR[-_][a-z0-9]", re.I), "semi_annual_report", 75, ""),

    # ── Letters ──
    ("letter_no", re.compile(r"(?:investor[-_ ]letter|investment[-_ ]report)[-_ ](?:no[-_. ]?)?(\d{1,3})", re.I), "quarterly_letter", 90, ""),
    ("letter_dated_eu", re.compile(r"(?:investor[-_ ]letter|letter|carta)[-_ ]\d{1,2}[-_.]\d{1,2}[-_.](?:19|20)\d{2}", re.I), "quarterly_letter", 95, ""),
    ("letter_kw", re.compile(r"\b(quarterly[-_ ]letter|carta[-_ ]trimestral|lettre[-_ ]trimestrielle|investor[-_ ]letter|commentary|letter[-_ ]to[-_ ](?:share|unit)holders)\b", re.I), "quarterly_letter", 80, ""),
    ("letter_prefix", re.compile(r"(?:^|/)LETTER[-_][a-z0-9]", re.I), "quarterly_letter", 75, ""),

    # ── KID / Prospectus ──
    ("kid", re.compile(r"\b(kid|kiid|priips|wesentliche[-_ ]anlegerinformationen|datos[-_ ]fundamentales|dic[-_ ]priips)\b", re.I), "kid", 90, ""),
    ("kid_prefix", re.compile(r"(?:^|/)KI?ID[-_]", re.I), "kid", 85, ""),
    ("prospectus", re.compile(r"\b(prospectus|prospekt|verkaufsprospekt|folleto)\b", re.I), "prospectus", 90, ""),
    ("prospectus_vkp", re.compile(r"(?<![A-Za-z0-9])VKP(?![A-Za-z0-9])"), "prospectus", 85, ""),
    ("prospectus_prefix", re.compile(r"(?:^|/)PRS?(?:EN)?[-_]", re.I), "prospectus", 80, ""),

    # ── Factsheet ──
    ("factsheet_month_named", re.compile(rf"fact[-_ ]?sheet[-_ ]{_MONTHS}[-_ ](?:19|20)\d{{2}}", re.I), "factsheet", 90, "_month"),
    ("factsheet_iso_start", re.compile(r"(?:^|/)(?:19|20)\d{2}[-_.](0[1-9]|1[0-2])[-_. ]", re.I), "factsheet", 75, "_month_iso"),
    ("factsheet_kw", re.compile(r"\b(factsheet|fact[-_ ]sheet|monthly[-_ ]report|monatsbericht|ficha[-_ ]mensual|reporting[-_ ]mensuel)\b", re.I), "factsheet", 70, ""),
    ("factsheet_mr", re.compile(r"(?:^|/)MR[-_][a-z0-9]", re.I), "factsheet", 70, ""),

    # ── Presentation ──
    ("presentation", re.compile(r"\b(presentation|pitch[-_ ]deck|investor[-_ ]day|webinar|conference)\b", re.I), "manager_presentation", 70, ""),
    ("special_paper", re.compile(r"\b(special[-_ ]paper|white[-_ ]paper|thought[-_ ]piece)\b", re.I), "manager_presentation", 60, "paper"),

    # ── Fallback ──
    ("unknown_pdf", re.compile(r".*\.pdf$", re.I), "unknown_pdf", 25, ""),
]


_MONTH_TO_NUM = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _extract_periodo(s: str) -> tuple[str, str]:
    """
    Devuelve (periodo, month) donde periodo='YYYY' o 'YYYY-MM' o 'YYYY-MM-DD'
    y month es 2-digit si detectado, vacío si no.
    """
    s_l = s.lower()

    # DD Month YYYY: "31-january-2025"
    m = re.search(rf"(\d{{1,2}})[-_ ]({'|'.join(_MONTH_TO_NUM)})[-_ ]((?:19|20)\d{{2}})", s_l)
    if m:
        dd, mon, yyyy = m.group(1).zfill(2), _MONTH_TO_NUM[m.group(2)], m.group(3)
        return (f"{yyyy}-{mon}-{dd}", mon)

    # Month YYYY: "december-2024"
    m = re.search(rf"({'|'.join(_MONTH_TO_NUM)})[-_ ]((?:19|20)\d{{2}})", s_l)
    if m:
        mon, yyyy = _MONTH_TO_NUM[m.group(1)], m.group(2)
        return (f"{yyyy}-{mon}", mon)

    # DD.MM.YYYY  (europeo): "27.05.2025"
    m = re.search(r"\b(\d{1,2})[.](\d{1,2})[.]((?:19|20)\d{2})\b", s_l)
    if m:
        dd, mon, yyyy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return (f"{yyyy}-{mon}-{dd}", mon)

    # YYYY-MM-DD
    m = re.search(r"\b((?:19|20)\d{2})[-_.](0[1-9]|1[0-2])[-_.](\d{2})\b", s_l)
    if m:
        return (f"{m.group(1)}-{m.group(2)}-{m.group(3)}", m.group(2))

    # YYYY-MM or YYYY_MM
    m = re.search(r"\b((?:19|20)\d{2})[-_.](0[1-9]|1[0-2])\b", s_l)
    if m:
        return (f"{m.group(1)}-{m.group(2)}", m.group(2))

    # Solo año
    m = re.search(r"\b((?:19|20)\d{2})\b", s_l)
    if m:
        return (m.group(1), "")

    return ("", "")


def _factsheet_subtype(month: str) -> str:
    if month == "12":
        return "eoy"
    if month == "06":
        return "mid_year"
    if month:
        return "monthly"
    return ""


def classify_url(url: str, link_text: str = "") -> dict:
    """
    Clasifica un PDF URL por su filename + link text ANTES de descargar.

    Returns:
        {
          "doc_type": "annual_report|semi_annual_report|quarterly_letter|
                       factsheet|kid|prospectus|manager_presentation|
                       unknown_pdf|skip",
          "periodo": "YYYY" | "YYYY-MM" | "YYYY-MM-DD" | "no-N" | "",
          "subtype": "eoy|mid_year|monthly|interim|paper|" ,
          "confidence": 0..100,
          "skip_reason": "",
          "matched_rule": "rule_id",
        }
    """
    # Filename + link_text combined (mantener URL completo por si hay pistas en path)
    fname = url.rsplit("/", 1)[-1]
    combined = f"{link_text} {fname} {url}"

    # Defaults
    result = {
        "doc_type": "skip",
        "periodo": "",
        "subtype": "",
        "confidence": 0,
        "skip_reason": "no_pattern",
        "matched_rule": "",
    }

    for rule_id, pattern, doc_type, confidence, subtype_key in _RULES:
        if not pattern.search(combined):
            continue

        result["matched_rule"] = rule_id
        result["doc_type"] = doc_type
        result["confidence"] = confidence

        if doc_type == "skip":
            result["skip_reason"] = subtype_key
            return result

        # Periodo extraction
        periodo, month = _extract_periodo(combined)

        # Letter No-N special case
        if rule_id == "letter_no":
            n_match = re.search(r"no[-_. ]?(\d{1,3})", fname, re.I)
            if n_match:
                periodo = periodo or f"no-{n_match.group(1)}"

        result["periodo"] = periodo

        # Subtype logic
        if subtype_key == "_month" or subtype_key == "_month_iso":
            result["subtype"] = _factsheet_subtype(month)
        elif subtype_key:
            result["subtype"] = subtype_key

        # GUARDA POST-CLASIFICACIÓN: cualquier factsheet con periodo YYYY-MM
        # debe cumplir mes=06 o mes=12. Monthly (jan-may, jul-nov) se skippa.
        # Cubre casos como "Fact-Sheet-Ireland-January-2025" donde el mes no
        # está inmediatamente tras "fact-sheet" pero sí en el periodo detectado.
        if doc_type == "factsheet":
            m_in_periodo = re.search(r"(?:19|20)\d{2}-(\d{2})", periodo)
            if m_in_periodo:
                mm = m_in_periodo.group(1)
                if mm not in ("06", "12"):
                    result["doc_type"] = "skip"
                    result["skip_reason"] = "monthly_not_eoy_or_midyear"
                    result["confidence"] = 0
                    return result
                result["subtype"] = _factsheet_subtype(mm)

        # Interim ya viene con subtype
        if rule_id.startswith("interim"):
            result["subtype"] = "interim"

        result["skip_reason"] = ""
        return result

    return result


# ═══════════════════════════════════════════════════════════════════════════
# 2. HARVESTER DE WEB GESTORA
# ═══════════════════════════════════════════════════════════════════════════

_FUND_SUBPAGE_KW = re.compile(
    r"(document|publication|report|regulatory|download|literature|investor|"
    r"insights|fund|portfolio|product|strateg)", re.I
)

_DOCS_PATHS = [
    "", "/funds", "/our-funds", "/fund-range", "/products",
    "/documents", "/literature", "/publications", "/insights",
    "/reports", "/regulatory-documents", "/fund-documents",
    "/en/documents", "/en/literature", "/en/publications",
]


def _slugify(name: str) -> list[str]:
    """Devuelve 2-3 slugs candidatos del nombre del fondo."""
    if not name:
        return []
    # Limpia paréntesis y tokens ruido
    clean = re.sub(r"\([^)]*\)", "", name).lower()
    clean = re.sub(r"[^a-z0-9 ]", " ", clean)
    tokens = [t for t in clean.split() if len(t) > 2 and t not in {
        "fund", "funds", "the", "plc", "ltd", "sa", "sicav", "ucits",
        "acc", "inc", "eur", "usd", "gbp", "class", "share", "shares",
    }]
    out = []
    if len(tokens) >= 2:
        out.append("-".join(tokens[:3]))
        out.append("-".join(tokens[:2]))
    if tokens:
        out.append(tokens[0])
    # Dedup preservando orden
    seen = set()
    return [s for s in out if not (s in seen or seen.add(s))]


def _looks_like_pdf(url: str) -> bool:
    u = url.lower()
    return u.endswith(".pdf") or ".pdf?" in u or "/download" in u or "download_doc" in u


def _is_sibling_fund_doc(filename: str, fund_slugs: list[str], sicav_slug: str = "") -> bool:
    """
    True si el filename refiere claramente a OTRO sub-fondo de la misma familia
    (no al fondo objetivo). Heurística: el fondo target tiene un slug
    distintivo (ej. 'trojan-fund'); si filename tiene un slug fund-like distinto
    (ej. 'trojan-ethical-income-fund', 'trojan-global-equity-fund'), descartar.

    Reglas:
      1. Buscar segmentos {Word}-Fund / {Word}-Income / {Word}-Equity / etc.
      2. Si los tokens distintivos (Ethical, Income, Equity, Global, Growth, Value...)
         aparecen en filename pero NO en fund_slugs[0] → es de otro sub-fondo.
      3. Si SICAV slug distinto aparece (ej. trojan-investment-funds vs trojan-funds-ireland),
         descartar.
    """
    fname = filename.lower()
    # SICAV hermano: distinguir "investment-funds" (UK) vs "funds-ireland" (IE)
    if sicav_slug:
        # Conjunto canonical de palabras del SICAV correcto
        sicav_tokens = {t for t in re.split(r"[-_ ]+", sicav_slug.lower()) if len(t) > 3}
        # SICAV alternativos típicos: investment-funds, funds-uk, funds-lux, etc.
        SIBLING_SICAV_PATTERNS = [
            r"investment[-_]funds", r"funds[-_]uk", r"funds[-_]lux",
            r"funds[-_]global", r"funds[-_]plc",
        ]
        for pat in SIBLING_SICAV_PATTERNS:
            if re.search(pat, fname):
                # ¿matchea con nuestro sicav_slug?
                if not any(t in pat for t in sicav_tokens):
                    return True

    # Tokens distintivos típicos de NOMBRES de sub-fondos
    DISTINCTIVE_SUBFUND_TOKENS = [
        "ethical", "income", "equity", "growth", "value", "bond",
        "global", "europe", "asia", "emerging", "small", "smid",
        "esg", "sustainable", "climate", "alpha", "beta",
    ]
    target_name = " ".join(fund_slugs).lower() if fund_slugs else ""
    for tok in DISTINCTIVE_SUBFUND_TOKENS:
        # Si el filename incluye el token + sufijo "-fund"/"-fonds" claramente
        # marcando otro fondo, y el target name NO incluye ese token → es hermano
        if re.search(rf"\b{tok}\b", fname) and tok not in target_name:
            # Pero solo si el filename tiene estructura de NAME-fund
            if re.search(rf"{tok}[-_ ](?:income|equity|fund|bond|growth|value)", fname):
                return True
            if re.search(rf"(?:income|equity|growth|fund|bond|value)[-_ ]{tok}", fname):
                return True
    return False


async def harvest_website(
    state: SharedState,
    c: httpx.AsyncClient,
    base_url: str,
    fund_slugs: list[str],
) -> list[dict]:
    """
    BFS depth-2 desde base_url + rutas típicas. Extrae hrefs *.pdf y
    clasifica cada uno con classify_url. Devuelve candidatos (no descarga).
    """
    base_url = base_url.rstrip("/")
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    base_host = urlparse(base_url).netloc.lower()

    # Seeds: rutas tipicas + rutas específicas de cada slug
    seeds: list[str] = []
    for path in _DOCS_PATHS:
        seeds.append(base_url + path)
    for slug in fund_slugs:
        for prefix in ("/funds", "/our-funds", "/products", "/portfolio"):
            seeds.append(f"{base_url}{prefix}/{slug}")
            seeds.append(f"{base_url}{prefix}/{slug}/documents")

    # BFS
    visited: set[str] = set()
    queue: list[tuple[str, int]] = [(u, 0) for u in seeds]
    candidates: list[dict] = []
    seen_pdfs: set[str] = set()

    while queue and state.budget.http_remaining > 0:
        page_url, depth = queue.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)

        if not state.budget.try_http():
            break

        try:
            status, body, hdrs = await fetch_with_fallback(c, page_url, timeout=15)
        except Exception:
            continue

        if status != 200 or not body:
            continue

        ct = (hdrs.get("content-type") or "").lower()
        if "text/html" not in ct and not body[:200].lower().startswith((b"<!doctype", b"<html")):
            continue

        try:
            soup = BeautifulSoup(body, "html.parser")
        except Exception:
            continue

        # CDNs conocidos: aceptar PDFs aunque no sean del mismo dominio
        from agents.discovery.gestora_crawler import KNOWN_CDN_HOSTS

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(strip=True) or ""
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full = urljoin(page_url, href)
            h_netloc = urlparse(full).netloc.lower()
            # Aceptar: mismo dominio, subdominio, o CDN conocido (im.natixis.com, etc.)
            same_domain = (h_netloc == base_host
                           or base_host in h_netloc
                           or h_netloc in base_host)
            is_cdn = any(cdn in h_netloc for cdn in KNOWN_CDN_HOSTS)
            if not same_domain and not is_cdn:
                continue

            if _looks_like_pdf(full):
                if full in seen_pdfs:
                    continue
                seen_pdfs.add(full)
                # Filtrar PDFs claramente de OTROS sub-fondos / SICAVs hermanos
                if _is_sibling_fund_doc(full.rsplit("/", 1)[-1], fund_slugs,
                                        sicav_slug=state.identity.get("sicav_paraguas", "")):
                    continue
                cls = classify_url(full, text)
                if cls["doc_type"] == "skip":
                    continue
                candidates.append({
                    "url": full, "text": text, "classification": cls,
                    "source": "gestora_web", "source_page": page_url,
                    "host": h_netloc,
                })
            elif depth < 2 and _FUND_SUBPAGE_KW.search(full):
                queue.append((full, depth + 1))

    console.log(f"[blue]harvest {base_host}: {len(candidates)} candidates de {len(visited)} pages")
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# 3. WAYBACK HISTÓRICO
# ═══════════════════════════════════════════════════════════════════════════

async def harvest_wayback(
    c: httpx.AsyncClient,
    domain: str,
    missing_years: list[int],
) -> list[dict]:
    """
    CDX domain-wide filtrado por mimetype PDF y rango de años faltantes.
    Devuelve candidatos clasificados como AR/SAR para años missing.
    La URL devuelta es la archivada directamente (flag id_ del wayback).
    """
    if not missing_years:
        return []
    domain_clean = domain.split("/")[0].replace("www.", "")
    min_y, max_y = min(missing_years), max(missing_years)
    cdx_url = (
        f"https://web.archive.org/cdx/search/cdx"
        f"?url={domain_clean}&matchType=domain"
        f"&filter=mimetype:application/pdf"
        f"&filter=statuscode:200"
        f"&from={min_y}0101&to={max_y + 1}1231"
        f"&collapse=urlkey&output=json&limit=1000"
    )
    try:
        r = await c.get(cdx_url, timeout=30)
        if r.status_code != 200:
            return []
        rows = r.json()
    except Exception:
        return []

    if not rows or len(rows) < 2:
        return []

    candidates: list[dict] = []
    seen: set[tuple] = set()
    for row in rows[1:]:
        # [urlkey, timestamp, original, mimetype, status, digest, length]
        ts = row[1]
        original = row[2]
        cls = classify_url(original)
        if cls["doc_type"] not in {"annual_report", "semi_annual_report"}:
            continue
        periodo = cls["periodo"][:4]
        try:
            year = int(periodo)
        except ValueError:
            continue
        if year not in missing_years:
            continue
        key = (cls["doc_type"], year)
        if key in seen:
            continue
        seen.add(key)

        # URL archivada con flag id_ → devuelve el PDF crudo sin chrome wayback
        archived = f"https://web.archive.org/web/{ts}id_/{original}"
        candidates.append({
            "url": archived, "text": "",
            "classification": cls,
            "source": "wayback",
            "source_page": f"cdx:{domain_clean}",
            "host": urlparse(original).netloc,
            "original_url": original,
            "timestamp": ts,
        })

    console.log(f"[blue]wayback {domain_clean}: {len(candidates)} AR/SAR candidates para {missing_years}")
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# 4. ORQUESTADOR con QUOTAS POR TIPO
# ═══════════════════════════════════════════════════════════════════════════

class DiscoveryV2:
    """Pipeline simplificado de discovery para fondos internacionales."""

    # Prioridades:
    #   P1 (sin límite): AR, SAR, factsheet Jun/Dec — material cuantitativo y
    #       cualitativo del fondo. Coger todos los años que haya.
    #   P2 (≥1/año desde inception): cartas / commentary del gestor.
    #   P3 (limitado): special papers, presentations, unknown.
    #   Latest-only: KID + prospectus (1 cada uno, ya garantizado en state.add_doc).
    QUOTAS = {
        "annual_report":       999,   # P1: sin límite práctico
        "semi_annual_report":  999,   # P1
        "factsheet":           999,   # P1 (monthly se skippan en classify_url)
        "quarterly_letter":     50,   # P2: ~1/año × 2 inception a hoy, margen
        "kid":                   1,
        "prospectus":            1,
        "manager_presentation":  6,   # P3
        "unknown_pdf":           3,
    }

    def __init__(
        self,
        isin: str,
        identity: dict,
        gap: dict,
        fund_dir: Path,
        web_search_fn=None,
        config: dict | None = None,
    ):
        self.isin = isin
        self.identity = dict(identity or {"isin": isin})
        self.gap = gap or {}
        self.fund_dir = fund_dir
        self.web_search_fn = web_search_fn
        self.config = config or {}
        self.spent_by_type: Counter = Counter()

    # ── Identity resolution ───────────────────────────────────────────────
    def _resolve_websites(self, client_sync: httpx.AsyncClient) -> list[str]:
        """
        Devuelve websites gestora candidatos (con https://).
        Cascada:
          1. identity._gestora_website si ya existe
          2. enrich_from_local_docs (regex Investment Manager website)
          3. parse KID/prospectus buscando cualquier domain/URL y filtrando
             plataformas/administradores conocidos
        """
        sites: list[str] = []
        if self.identity.get("_gestora_website"):
            sites.append(self.identity["_gestora_website"])

        try:
            enrich_from_local_docs(self.identity, self.fund_dir)
            w = self.identity.get("_gestora_website")
            if w and w not in sites:
                sites.append(w)
        except Exception as e:
            console.log(f"[yellow]identity enrich: {e}")

        # Fallback: extraer domains de KID/prospectus
        sites += self._extract_domains_from_docs()

        # Fallback adicional: candidatos derivados del nombre de la gestora
        gestora = self.identity.get("gestora_oficial", "")
        if gestora:
            try:
                from agents.discovery.gestora_crawler import gestora_domain_candidates
                for cand in gestora_domain_candidates(gestora)[:6]:
                    if cand not in sites:
                        sites.append(cand)
            except Exception:
                pass

        # Normaliza a https://host
        out = []
        for s in sites:
            if not s:
                continue
            if not s.startswith("http"):
                s = "https://" + s.lstrip("/")
            host = urlparse(s).netloc.lower().rstrip("/")
            if not host:
                continue
            url = f"https://{host}"
            if url not in out:
                out.append(url)
        return out

    async def _probe_websites(self, c: httpx.AsyncClient, sites: list[str]) -> list[str]:
        """HEAD check cada candidato. Keep los que respondan 200-399. Máx 3."""
        if not sites:
            return []
        confirmed: list[str] = []
        for site in sites:
            if len(confirmed) >= 3:
                break
            try:
                status, _, _ = await fetch_with_fallback(c, site, timeout=8)
                if 200 <= status < 400:
                    confirmed.append(site)
            except Exception:
                continue
        return confirmed

    def _extract_domains_from_docs(self) -> list[str]:
        """Parse KID/prospectus/factsheet locales y devuelve dominios candidatos."""
        disc_dir = self.fund_dir / "raw" / "discovery"
        if not disc_dir.exists():
            return []
        try:
            import pdfplumber
        except ImportError:
            return []

        PLATFORM_BLACKLIST = {
            "fundsquare.net", "luxse.com", "bourse.lu", "bundesanzeiger.de",
            "morningstar.com", "morningstar.es", "morningstar.co.uk",
            "finect.com", "quefondos.com", "funds.cssf.lu",
            "centralbank.ie", "amf-france.org", "cnmv.es",
            "fundinfo.com", "fefundinfo.com", "kneip.com",
            "universal-investment.com", "universal-investment.lu",
            "waystone.com", "linkgroup.eu", "caceis.com",
            "linkedin.com", "twitter.com", "facebook.com", "youtube.com",
            "google.com", "adobe.com", "microsoft.com",
            "sfdr.eu", "priips.eu",
        }
        candidates: Counter = Counter()
        for pdf in disc_dir.glob("*.pdf"):
            try:
                with pdfplumber.open(pdf) as p:
                    text = ""
                    for pg in p.pages[:15]:
                        text += (pg.extract_text() or "") + " "
            except Exception:
                continue
            # Busca URLs https:// y domains sueltos (palabra.tld con tlds comunes)
            for m in re.finditer(
                r"(?:https?://)?(?:www\.)?"
                r"([a-z][a-z0-9]{2,40}(?:[\-.][a-z0-9]{2,40})*"
                r"\.(?:com|co\.uk|co|lu|fr|de|es|it|ch|ie|nl|be|eu|at|se|no|dk|fi|pt|pl))",
                text, re.I,
            ):
                d = m.group(1).lower()
                # Descartar tokens mal parseados (terminando en '-' o '.')
                if "-." in d or d.endswith("-") or d.startswith("-"):
                    continue
                # Descartar blacklist
                if any(b in d for b in PLATFORM_BLACKLIST):
                    continue
                candidates[d] += 1

        # Ordenar por frecuencia y devolver top 3
        return [d for d, _ in candidates.most_common(3)]

    # ── Budget por tipo ───────────────────────────────────────────────────
    def _can_download(self, doc_type: str) -> bool:
        quota = self.QUOTAS.get(doc_type, 2)
        return self.spent_by_type[doc_type] < quota

    # ── Ordenar candidatos ────────────────────────────────────────────────
    def _score(self, cand: dict, state: SharedState) -> float:
        cls = cand["classification"]
        doc_type = cls["doc_type"]
        periodo = cls["periodo"]
        confidence = cls["confidence"]

        # Base por tipo (AR/SAR prioridad)
        base = {
            "annual_report": 100, "semi_annual_report": 95,
            "quarterly_letter": 80, "prospectus": 75, "kid": 70,
            "factsheet": 60, "manager_presentation": 50,
            "unknown_pdf": 20,
        }.get(doc_type, 10)

        # Bonus si cubre un target exacto
        if not state.coverage(doc_type, periodo):
            base += 30

        # Bonus por dominio gestora confirmado
        host = cand.get("host", "").lower()
        if host in state.discovered_gestora_domains or any(
            host in d or d in host for d in state.discovered_gestora_domains
        ):
            base += 20

        # Bonus ISIN en URL
        if self.isin.lower() in cand["url"].lower():
            base += 30
        elif self.identity.get("nombre_oficial"):
            name = self.identity["nombre_oficial"].lower()
            tokens = [t for t in re.findall(r"[a-z]{4,}", name) if t not in {"fund", "funds"}]
            if any(t in cand["url"].lower() for t in tokens):
                base += 10

        # Recency bonus (periodos recientes ganan)
        y_match = re.search(r"(20\d{2})", periodo)
        if y_match:
            years_ago = datetime.now().year - int(y_match.group(1))
            base += max(0, 20 - years_ago * 3)

        # Confidence multiplier
        return base * (confidence / 100.0)

    # ── MAIN ──────────────────────────────────────────────────────────────
    async def run(self) -> SharedState:
        kb_data = kb_mod.load_kb(self.fund_dir, self.isin)
        state = SharedState(
            isin=self.isin, identity=self.identity, gap=self.gap,
            fund_dir=self.fund_dir, kb=kb_data,
        )

        console.log(f"[bold cyan]DiscoveryV2 start[/bold cyan] ISIN {self.isin}")
        console.log(f"Targets: {len(state.missing_doc_targets())}")

        browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        }
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=60, headers=browser_headers,
        ) as c:
            # Phase 0 — Identity + websites
            # Si prestep docs no existen aún, descargamos KID via prestep ligero
            # (reutilizamos prestep v1 por simplicidad, pero solo una vez).
            if not self.identity.get("gestora_oficial") or not self.identity.get("_gestora_website"):
                try:
                    from agents.discovery.prestep_regulatory import run_prestep
                    await run_prestep(state, c, web_search_fn=self.web_search_fn)
                except Exception as e:
                    console.log(f"[yellow]prestep: {e}")
            websites = self._resolve_websites(c)
            websites = await self._probe_websites(c, websites)
            console.log(f"[blue]websites confirmados: {websites}")

            # Phase 1 — Harvest LIVE
            fund_slugs = _slugify(self.identity.get("nombre_oficial", ""))
            if self.identity.get("sicav_paraguas"):
                fund_slugs += _slugify(self.identity["sicav_paraguas"])
            fund_slugs = list(dict.fromkeys(fund_slugs))  # dedup preserve order
            console.log(f"[blue]fund slugs: {fund_slugs[:4]}")

            all_candidates: list[dict] = []
            for site in websites[:3]:
                all_candidates += await harvest_website(state, c, site, fund_slugs)

            # Phase 2 — Wayback para AR/SAR que siguen faltando
            missing_years: list[int] = []
            for dt, periodo in state.missing_doc_targets():
                if dt in {"annual_report", "semi_annual_report"}:
                    m = re.search(r"(20\d{2})", periodo)
                    if m:
                        missing_years.append(int(m.group(1)))
            missing_years = sorted(set(missing_years))
            if missing_years and websites:
                for site in websites[:2]:
                    all_candidates += await harvest_wayback(c, site, missing_years)

            console.log(f"[blue]total candidates: {len(all_candidates)}")

            # Phase 3 — Score + dispatch por quotas
            all_candidates.sort(key=lambda cd: self._score(cd, state), reverse=True)

            for cand in all_candidates:
                if state.budget.download_remaining <= 0:
                    console.log("[yellow]download budget agotado")
                    break
                cls = cand["classification"]
                doc_type = cls["doc_type"]
                periodo = cls["periodo"]

                if not self._can_download(doc_type):
                    continue
                if state.coverage(doc_type, periodo):
                    continue
                if state.already_downloaded(cand["url"]):
                    continue

                doc = await download_and_register(
                    state, c, cand["url"], doc_type, periodo,
                    source=cand["source"],
                    source_detail=cand.get("source_page", "")[-80:],
                )
                if doc:
                    self.spent_by_type[doc.doc_type] += 1
                    kb_mod.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
                    console.log(
                        f"[green]{doc.source} {doc.doc_type}@{doc.periodo}[/green] "
                        f"[{self.spent_by_type[doc.doc_type]}/{self.QUOTAS.get(doc.doc_type,2)}]"
                    )

            # Phase 3.5 — URL template learning (para CDNs con pattern /{TYPE}-{slug}/..)
            # Si tras harvest ya hay ≥2 docs del mismo template, inferimos tipos faltantes.
            if not state.is_fully_covered():
                try:
                    from agents.discovery.url_template_learner import learn_and_enumerate
                    await learn_and_enumerate(state, c)
                except Exception as e:
                    console.log(f"[yellow]template learner: {e}")

            # Phase 4 — Email draft si gap significativo
            kb_mod.save_kb(self.fund_dir, state.kb)
            try:
                from agents.email_agent import maybe_draft_request
                maybe_draft_request(state)
            except Exception as e:
                console.log(f"[yellow]email_agent: {e}")

        return state
