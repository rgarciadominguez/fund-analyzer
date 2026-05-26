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
# Registry helper — compartido entre G7 (PDFs) y BUG-D (HTML fallback)
# ═══════════════════════════════════════════════════════════════════════════


def _registry_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "gestoras_registry.json"


def _write_registry_entry(
    isin: str,
    gestora: str,
    domain: str,
    doc_count: int = 0,
    html_fallback_useful_domains: list[str] | None = None,
) -> bool:
    """Escribe/mergea una entrada en gestoras_registry.json.

    Compartido por:
      - G7 (PDFs descargados): doc_count >= 1, html_fallback_useful_domains None
      - BUG-D (HTML fallback exitoso, sin PDFs): doc_count == 0,
        html_fallback_useful_domains = ["dom1", "dom2", ...]

    Respeta manuales (auto_learned=false → no toca).
    Devuelve True si escribió cambios, False si fue un skip.
    """
    if not gestora:
        return False
    registry_path = _registry_path()
    if registry_path.exists():
        with registry_path.open(encoding="utf-8") as f:
            reg = json.load(f)
    else:
        reg = {"_description": "Auto-aprendido por discovery_v2", "gestoras": {}}
    gestoras = reg.setdefault("gestoras", {})
    existing = gestoras.get(gestora) or {}
    if existing.get("auto_learned") is False:
        console.log(f"[yellow]G7 skip: '{gestora}' es manual (auto_learned=false)")
        return False

    merged = dict(existing)
    if domain:
        inferred_url = f"https://{domain}"
        merged.setdefault("web", inferred_url)
        if not merged.get("reports_url"):
            merged["reports_url"] = inferred_url
    merged["auto_learned"] = True
    merged["learned_at"] = datetime.now().isoformat()
    merged["from_isin"] = isin
    if doc_count > 0:
        merged["discovered_doc_count"] = doc_count
    merged.setdefault("letters_pages", existing.get("letters_pages", []) or [])
    merged.setdefault("successful_pdf_urls", existing.get("successful_pdf_urls", []) or [])
    funds_list = list(existing.get("funds", []) or [])
    if isin not in funds_list:
        funds_list.append(isin)
    merged["funds"] = sorted(funds_list)
    merged.setdefault("notes", existing.get("notes") or "")

    # BUG-D: merge html_fallback_useful_domains (unión, no overwrite)
    if html_fallback_useful_domains:
        prev = list(existing.get("html_fallback_useful_domains") or [])
        new_set = set(prev) | {
            d.lower().strip() for d in html_fallback_useful_domains if d
        }
        merged["html_fallback_useful_domains"] = sorted(new_set)

    gestoras[gestora] = merged
    registry_path.write_text(
        json.dumps(reg, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    tag = f"docs={doc_count}" if doc_count else "html-fb"
    domain_label = domain or (
        (html_fallback_useful_domains or [""])[0]
        if html_fallback_useful_domains else ""
    )
    console.log(f"[green]G7 persist registry: '{gestora}' → {domain_label} ({tag})")
    return True


def persist_html_fallback_to_registry(
    isin: str,
    gestora: str,
    useful_domains: list[str],
) -> bool:
    """BUG-D entry point: llamado por intl_extractor_v2._fallback_html_extract
    cuando el fallback HTML rellena AUM/posiciones. Persiste los dominios
    como `html_fallback_useful_domains` en la entrada de la gestora del registry,
    para que futuros runs de la misma gestora prioricen esos dominios.

    Devuelve True si escribió cambios.
    """
    if not gestora or not useful_domains:
        return False
    # Si la gestora no existe aún en el registry, usar el primer dominio como
    # `domain` semilla (no es web oficial pero es lo más informativo que
    # tenemos en este punto). _write_registry_entry no sobrescribirá `web` si
    # ya existe en una entrada previa.
    seed_domain = useful_domains[0]
    try:
        return _write_registry_entry(
            isin=isin,
            gestora=gestora,
            domain="",  # no inventamos un web oficial — solo persistimos html_fallback_useful_domains
            doc_count=0,
            html_fallback_useful_domains=useful_domains,
        )
    except Exception as e:
        console.log(f"[yellow]G7 html-fb persist failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# G5/G6/G8 — Discovery genérico multi-idioma (sin per-gestora hardcode)
# ═══════════════════════════════════════════════════════════════════════════

# Replica subset de readings_collector para no acoplar; mismas keys.
ISIN_PREFIX_TO_REGION = {
    "FR": "FR", "DE": "DE", "AT": "DE", "GB": "GB",
    "IT": "IT", "CH": "CH", "LI": "CH",
    "BE": "BE", "NL": "NL", "ES": "ES",
}
GESTORA_REGION_HINTS = {
    "FR": ["dnca", "carmignac", "rothschild", "amiral", "comgest", "ostrum",
           "groupama", "amundi", "bnp paribas", "natixis", "tikehau", "axiom",
           "lazard", "edmond de rothschild"],
    "DE": ["dws", "allianz global investors", "union investment", "dje kapital",
           "deka", "metzler", "lupus alpha", "flossbach"],
    "GB": ["fundsmith", "lindsell train", "troy asset", "guinness asset",
           "evenlode", "polar capital", "baillie gifford", "schroders", "jupiter",
           "marlborough", "liontrust", "fidelity international", "m&g", "ruffer"],
    "IT": ["azimut", "anima", "eurizon", "mediolanum", "generali"],
    "CH": ["pictet", "lombard odier", "vontobel", "gam ", "ubs ",
           "credit suisse", "julius baer", "swisscanto", "banque syz"],
}

# Keywords doc tipo annual report por región — usados por G5
REGION_AR_KEYWORDS = {
    "FR": ["rapport annuel", "rapport semestriel", "reporting mensuel"],
    "DE": ["Jahresbericht", "Halbjahresbericht", "Monatsbericht"],
    "GB": ["annual report", "interim report", "factsheet"],
    "IT": ["relazione annuale", "relazione semestrale"],
    "ES": ["informe anual", "informe semestral"],
    "CH": ["Jahresbericht", "annual report", "rapport annuel"],
    "BE": ["rapport annuel", "jaarverslag"],
    "NL": ["jaarverslag", "halfjaarverslag"],
    "EN_GLOBAL": ["annual report", "semi-annual report", "factsheet"],
}

# TLDs probables por región — usados por G6
REGION_TLDS = {
    "FR": [".fr", ".com"],
    "DE": [".de", ".com"],
    "GB": [".co.uk", ".com"],
    "IT": [".it", ".com"],
    "CH": [".ch", ".com"],
    "LU": [".lu", ".com"],
    "IE": [".ie", ".com"],
    "BE": [".be", ".com"],
    "NL": [".nl", ".com"],
    "ES": [".es", ".com"],
    "EN_GLOBAL": [".com", ".net"],
}


def _detect_region_from_isin(isin: str, gestora: str = "") -> str:
    """Replica simplificada de readings_collector._detect_region.
    Devuelve código región FR/DE/GB/IT/CH/BE/NL/ES/EN_GLOBAL."""
    prefix = (isin or "")[:2].upper()
    explicit = ISIN_PREFIX_TO_REGION.get(prefix)
    if explicit:
        return explicit
    g = (gestora or "").lower().strip()
    if g:
        for region, hints in GESTORA_REGION_HINTS.items():
            if any(h in g for h in hints):
                return region
    return "EN_GLOBAL"


def _slugify_gestora(gestora: str) -> list[str]:
    """Genera slugs candidatos del nombre de la gestora.
    'Amiral Gestion' → ['amiralgestion','amiral-gestion','amiral']."""
    if not gestora:
        return []
    g = re.sub(r"[^a-z0-9 ]", "", gestora.lower()).strip()
    parts = [p for p in g.split() if p]
    if not parts:
        return []
    slugs: list[str] = []
    full_concat = "".join(parts)
    full_dash = "-".join(parts)
    if full_concat:
        slugs.append(full_concat)
    if full_dash and full_dash != full_concat:
        slugs.append(full_dash)
    if len(parts) > 1:
        slugs.append(parts[0])  # primer token como fallback (ej. 'amiral')
    return list(dict.fromkeys(slugs))[:3]


def candidate_domains(gestora: str, region: str = "EN_GLOBAL") -> list[str]:
    """G6: genera candidatos de dominio para una gestora dada.
    Combina slugs + TLDs probables según región. Max 10 candidates.
    NO hace IO — sólo composición. La validación va por _probe_candidate_domains."""
    slugs = _slugify_gestora(gestora)
    tlds = REGION_TLDS.get(region, REGION_TLDS["EN_GLOBAL"])
    out: list[str] = []
    for slug in slugs:
        for tld in tlds:
            out.append(f"{slug}{tld}")
    return list(dict.fromkeys(out))[:10]


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

    def _reached_critical_minimums(self, state: "SharedState") -> bool:
        """Minimo absoluto: ≥1 AR, ≥1 factsheet, ≥1 carta.
        Pero este check se usa para ACTIVAR Opus hints, NO para parar la busqueda."""
        counts: dict[str, int] = {}
        for d in state.downloaded_docs:
            if d.validated:
                counts[d.doc_type] = counts.get(d.doc_type, 0) + 1
        return (
            counts.get("annual_report", 0) >= 1
            and counts.get("factsheet", 0) >= 1
            and counts.get("quarterly_letter", 0) >= 1
        )

    def _coverage_report(self, state: "SharedState") -> dict:
        """Genera coverage report: que años tenemos, que años faltan, por doc_type."""
        import re as _re
        inception = state.identity.get("anio_creacion") or state.identity.get("inception_year")
        if not inception:
            # Intentar extraer de identity
            for k in ("fecha_autorizacion", "fecha_lanzamiento"):
                v = state.identity.get(k, "")
                m = _re.search(r'(20[012]\d|19\d{2})', str(v))
                if m:
                    inception = int(m.group(1))
                    break
        current = 2026
        if not inception:
            inception = current - 10  # fallback: 10 años

        target_years = list(range(inception, current + 1))
        covered_by_type: dict[str, set] = {}

        for d in state.downloaded_docs:
            if not d.validated:
                continue
            dt = d.doc_type
            periodo = d.periodo or ""
            years = _re.findall(r'(20[012]\d|19\d{2})', str(periodo))
            if years:
                y = int(max(years))
                covered_by_type.setdefault(dt, set()).add(y)

        report = {
            "inception_year": inception,
            "target_years": target_years,
            "by_doc_type": {},
            "summary": {},
        }
        for dt in ("annual_report", "factsheet", "quarterly_letter", "semi_annual_report"):
            covered = sorted(covered_by_type.get(dt, set()))
            missing = sorted(set(target_years) - set(covered))
            report["by_doc_type"][dt] = {
                "covered": covered,
                "missing": missing,
                "coverage_pct": round(len(covered) * 100 / max(1, len(target_years)), 1),
            }

        # Score global
        total_covered = sum(len(v["covered"]) for v in report["by_doc_type"].values())
        total_target = len(target_years) * 4  # 4 tipos de doc
        report["summary"]["coverage_score"] = round(total_covered * 100 / max(1, total_target), 1)
        return report

    async def _probe_candidate_domains(self, httpc, candidates: list[str], fund_name: str) -> list[str]:
        """G6: prueba cada candidate con `site:domain "fund_name"` (1 query Google).
        El que devuelve >0 resultados ES un dominio real. Cache en memoria por sesión.
        Cap 5 candidates → max 5 queries Google → coste ≈ free (Serper plan).
        """
        if not fund_name or not candidates:
            return []
        if not hasattr(self, "_domain_probe_cache"):
            self._domain_probe_cache: dict[str, bool] = {}
        try:
            from tools.google_search import SearchEngine
            engine = SearchEngine(isin=self.isin)
        except Exception as e:
            console.log(f"[yellow]G6 SearchEngine init: {e}")
            return []
        confirmed: list[str] = []
        for domain in candidates[:5]:
            if domain in self._domain_probe_cache:
                if self._domain_probe_cache[domain]:
                    confirmed.append(domain)
                continue
            query = f'site:{domain} "{fund_name}"'
            try:
                results = await engine.search(query, num=3, agent="discovery_g6")
            except Exception as e:
                console.log(f"[yellow]G6 probe {domain}: {e}")
                results = []
            hit = bool(results)
            self._domain_probe_cache[domain] = hit
            if hit:
                confirmed.append(domain)
                console.log(f"[green]G6 dominio detectado: {domain} ({len(results)} hits)")
        return confirmed

    def _generate_g5_queries(self, region: str, fund_name: str, gestora: str = "") -> list[str]:
        """G5: queries universales multi-idioma para encontrar PDFs del fondo.
        Combina fund_name + keyword en idioma local + filetype:pdf (+ gestora opcional)."""
        if not fund_name:
            return []
        keywords = REGION_AR_KEYWORDS.get(region, REGION_AR_KEYWORDS["EN_GLOBAL"])
        queries: list[str] = []
        for kw in keywords[:3]:
            queries.append(f'"{fund_name}" "{kw}" filetype:pdf')
            if gestora:
                queries.append(f'"{fund_name}" "{kw}" "{gestora}" filetype:pdf')
        return queries[:6]

    async def _universal_doc_search(self, httpc, fund_name: str) -> list[dict]:
        """G5: ejecuta queries multi-idioma vía Serper. Devuelve candidates
        compatibles con harvest pipeline ({url, text, classification, source, source_page, host})."""
        out: list[dict] = []
        if not fund_name:
            return out
        gestora = self.identity.get("gestora_oficial", "") or ""
        region = _detect_region_from_isin(self.isin, gestora)
        try:
            from tools.google_search import SearchEngine
            engine = SearchEngine(isin=self.isin)
        except Exception as e:
            console.log(f"[yellow]G5 SearchEngine init: {e}")
            return out
        queries = self._generate_g5_queries(region, fund_name, gestora)
        seen_urls: set[str] = set()
        for q in queries:
            try:
                results = await engine.search(q, num=5, agent="discovery_g5")
            except Exception as e:
                console.log(f"[yellow]G5 query '{q[:60]}': {e}")
                results = []
            for r in results:
                url = (r.get("url") or "").strip()
                if not url or url in seen_urls:
                    continue
                if not url.lower().endswith(".pdf"):
                    # Skip non-PDF — G5 sólo persigue PDFs
                    continue
                seen_urls.add(url)
                cls = classify_url(url, r.get("title", "") or "")
                if cls.get("doc_type") == "skip":
                    continue
                try:
                    host = urlparse(url).netloc.lower()
                except Exception:
                    host = ""
                out.append({
                    "url": url,
                    "text": r.get("title", "") or "",
                    "classification": cls,
                    "source": "g5_universal_search",
                    "source_page": "",
                    "host": host,
                })
        if out:
            console.log(f"[green]G5 universal search ({region}): {len(out)} candidates PDF")
        return out

    def _persist_to_registry(
        self,
        domain: str,
        doc_count: int,
        html_fallback_useful_domains: list[str] | None = None,
    ) -> None:
        """G7: auto-aprende dominio en gestoras_registry.json tras descarga exitosa
        (≥1 doc útil). Respeta entries con auto_learned=false (manuales).

        BUG-D (2026-05-20): cuando se llama desde el HTML fallback (sin PDFs
        descargados) se pasa `doc_count=0` pero `html_fallback_useful_domains`
        no vacío. La entrada en registry se persiste igual con esos dominios.
        """
        gestora = (self.identity.get("gestora_oficial") or "").strip()
        # BUG-D: aceptar también el caso de HTML fallback (doc_count=0 pero
        # html_fallback_useful_domains populated).
        useful_html_doms = list(html_fallback_useful_domains or [])
        if not gestora:
            return
        if doc_count <= 0 and not useful_html_doms:
            return
        if doc_count <= 0 and not domain:
            # html-fallback only path: usar el primer dominio útil como `domain`
            domain = useful_html_doms[0]
        try:
            _write_registry_entry(
                isin=self.isin,
                gestora=gestora,
                domain=domain,
                doc_count=doc_count,
                html_fallback_useful_domains=useful_html_doms,
            )
        except Exception as e:
            console.log(f"[yellow]G7 persist failed: {e}")

    def _load_registry_hints(self) -> dict:
        """F3: lee data/gestoras_registry.json y devuelve hints opcionales
        (reports_url, wayback_slug, annual_report_pattern) para self.identity.gestora_oficial.

        Devuelve {} si no hay match o el fichero no existe. Match es case-insensitive
        y tolera espacios extra. Pensado para ejecutarse en cada run() — el coste es
        irrisorio (registry típicamente <50 entradas).
        """
        gestora = (self.identity.get("gestora_oficial") or "").strip()
        if not gestora:
            return {}
        try:
            registry_path = Path(__file__).resolve().parent.parent / "data" / "gestoras_registry.json"
            if not registry_path.exists():
                return {}
            with registry_path.open(encoding="utf-8") as f:
                reg = json.load(f)
        except Exception as e:
            console.log(f"[yellow]registry load: {e}")
            return {}

        gestoras = reg.get("gestoras", {}) or {}
        # Match exacto primero, luego case-insensitive
        entry = gestoras.get(gestora)
        if entry is None:
            g_lower = gestora.lower()
            for k, v in gestoras.items():
                if k.lower() == g_lower:
                    entry = v
                    break
        if not entry:
            return {}

        hints = {}
        for key in ("reports_url", "wayback_slug", "annual_report_pattern"):
            v = entry.get(key)
            if v:
                hints[key] = v
        if hints:
            console.log(f"[green]registry hints para '{gestora}': {list(hints.keys())}")
        return hints

    def _opus_suggest_sources(self, httpc, missing: list) -> list[str]:
        """Opus sugiere dominios adicionales donde buscar docs del fondo.
        1 call, ~$0.02. Util cuando los targets criticos (AR, carta) siguen faltando."""
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        except Exception:
            return []

        missing_types = sorted(set(t[0] for t in missing))
        nombre = self.identity.get("nombre_oficial", "")
        gestora = self.identity.get("gestora_oficial", "")

        try:
            # Fase M (2026-05-04): Opus → Haiku. Tarea de clasificación pura
            # (lista de dominios). Haiku 4.5 basta. ~5x más barato.
            from tools.llm_models import HAIKU_HINTS
            r = client.messages.create(
                model=HAIKU_HINTS,
                max_tokens=400,
                temperature=0,  # K5 Fase K: determinismo
                messages=[{"role": "user", "content": (
                    f"Fondo: {nombre} ({self.isin})\n"
                    f"Gestora: {gestora}\n"
                    f"Faltan: {missing_types}\n\n"
                    f"Para encontrar {missing_types} historicos de este fondo, "
                    f"¿en que 3-6 dominios web especificos buscarias? Piensa en:\n"
                    f"- Web oficial de la gestora (dominio actual y subdominios tipo /funds/)\n"
                    f"- Distribuidores especializados (fundinfo, allfunds, im.natixis, etc.)\n"
                    f"- Plataformas jurisdiccionales (morningstar.{{cc}}, trustnet, etc.)\n"
                    f"- Sites especializados por clase de activo si aplica\n\n"
                    f"Devuelve SOLO dominios (uno por linea). NO URLs completas."
                )}],
            )
            console.log(f"[cyan]Haiku hints ({r.usage.input_tokens}+"
                        f"{r.usage.output_tokens} tok)")
            # Cost-Opt Fase 2 (2026-05-03): instrumentar coste
            try:
                from tools.llm_logger import log_llm_response
                log_llm_response(r, agent="discovery_v2_haiku_hints",
                                  isin=self.isin, model=HAIKU_HINTS,
                                  provider="anthropic")
            except Exception:
                pass
            text = r.content[0].text
            sites = []
            for line in text.split("\n"):
                w = line.strip().strip(",.;:()-*").replace("www.", "").lower()
                if ("." in w and len(w) > 4 and len(w) < 60
                    and "/" not in w
                    and not any(bad in w for bad in ["google", "wikipedia",
                                                     "linkedin", "facebook", "twitter"])):
                    sites.append(w)
            return sites[:6]
        except Exception as e:
            console.log(f"[yellow]Opus suggest failed: {e}")
            return []

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

            # F3: hints del registry (gestoras_registry.json) — PRIORIDAD sobre Opus.
            # Si la gestora está registrada con reports_url o wayback_slug, los
            # prependemos para que Phase 1 (LIVE) y Phase 2 (Wayback) los exploten
            # antes de que Phase 2b (Opus suggest) gaste un LLM call.
            registry_hints = self._load_registry_hints()
            registry_wayback_slugs: list[str] = []
            ar_pattern_from_registry: str | None = registry_hints.get("annual_report_pattern")
            if registry_hints.get("reports_url"):
                from urllib.parse import urlparse as _urlparse
                reports_url = registry_hints["reports_url"]
                # Prepend el dominio completo a websites para que harvest_website lo cubra
                if reports_url not in websites:
                    websites = [reports_url] + websites
                # Y el host raíz como fallback de exploración
                try:
                    host = _urlparse(reports_url).netloc
                    host_url = f"https://{host}" if host else None
                    if host_url and host_url not in websites:
                        websites.append(host_url)
                except Exception:
                    pass
            if registry_hints.get("wayback_slug"):
                registry_wayback_slugs.append(registry_hints["wayback_slug"])
            if ar_pattern_from_registry:
                console.log(
                    f"[green]registry annual_report_pattern: {ar_pattern_from_registry} "
                    "(disponible para filtrado downstream)"
                )

            # Phase 1 — Harvest LIVE
            fund_slugs = _slugify(self.identity.get("nombre_oficial", ""))
            if self.identity.get("sicav_paraguas"):
                fund_slugs += _slugify(self.identity["sicav_paraguas"])
            fund_slugs = list(dict.fromkeys(fund_slugs))  # dedup preserve order
            console.log(f"[blue]fund slugs: {fund_slugs[:4]}")

            all_candidates: list[dict] = []
            for site in websites[:3]:
                all_candidates += await harvest_website(state, c, site, fund_slugs)

            # ── Phase 1b — G5+G6: discovery genérico (multi-idioma + auto-domain) ──
            # Sin per-gestora hardcode. Sin LLM. Sólo Google CSE (Serper).
            # Corre ANTES de Phase 2b Opus para evitar el LLM call si no hace falta.
            confirmed_g6_domains: list[str] = []
            _fund_name_for_search = self.identity.get("nombre_oficial", "") or ""
            _g_for_region = self.identity.get("gestora_oficial", "") or ""
            if _fund_name_for_search:
                # G6 — candidate_domains + probe via site:domain "fund_name"
                region = _detect_region_from_isin(self.isin, _g_for_region)
                g6_candidates = candidate_domains(_g_for_region, region)
                if g6_candidates:
                    try:
                        confirmed_g6_domains = await self._probe_candidate_domains(
                            c, g6_candidates, _fund_name_for_search,
                        )
                    except Exception as e:
                        console.log(f"[yellow]G6 probe: {e}")
                    for d in confirmed_g6_domains:
                        url = f"https://{d}"
                        if url not in websites:
                            websites.append(url)
                            try:
                                all_candidates += await harvest_website(state, c, url, fund_slugs)
                            except Exception as e:
                                console.log(f"[yellow]G6 harvest {d}: {e}")
                # G5 — queries universales multi-idioma
                try:
                    g5_candidates = await self._universal_doc_search(c, _fund_name_for_search)
                    all_candidates += g5_candidates
                except Exception as e:
                    console.log(f"[yellow]G5 universal: {e}")

            # Phase 2 — Wayback para AR/SAR que siguen faltando
            missing_years: list[int] = []
            for dt, periodo in state.missing_doc_targets():
                if dt in {"annual_report", "semi_annual_report"}:
                    m = re.search(r"(20\d{2})", periodo)
                    if m:
                        missing_years.append(int(m.group(1)))
            # Expandir missing_years a TODO el rango desde inicio del fondo
            inception = self.identity.get("anio_creacion")
            if not inception:
                for k in ("fecha_autorizacion", "fecha_lanzamiento"):
                    v = self.identity.get(k, "")
                    m = re.search(r"(20\d{2}|19\d{2})", str(v))
                    if m:
                        inception = int(m.group(1))
                        break
            if inception:
                all_target_years = list(range(int(inception), 2027))
                # Añadir todos los años target a missing (si no cubiertos)
                already_covered = set()
                for d in state.downloaded_docs:
                    m = re.search(r"(20\d{2})", str(d.periodo or ""))
                    if m:
                        already_covered.add(int(m.group(1)))
                missing_years = sorted(set(all_target_years) - already_covered)
                console.log(f"[blue]expanded missing_years (hasta {inception}): {len(missing_years)} años")

            missing_years = sorted(set(missing_years))
            if missing_years and websites:
                for site in websites[:2]:
                    all_candidates += await harvest_wayback(c, site, missing_years)
                # F3: wayback_slug del registry como hint adicional, ANTES de Opus
                for slug in registry_wayback_slugs:
                    try:
                        all_candidates += await harvest_wayback(c, slug, missing_years)
                    except Exception as e:
                        console.log(f"[yellow]wayback registry slug {slug}: {e}")
                # G8: Wayback iterativo sobre dominios CONFIRMADOS por G6
                # (los que el registry no conocía pero G6 detectó vía site: probe).
                # Cubre el caso "live 404 / 503" — Wayback rescata el listado histórico.
                for d in confirmed_g6_domains:
                    if any(d in w for w in websites[:2]):
                        # Ya cubierto arriba como website principal
                        continue
                    try:
                        all_candidates += await harvest_wayback(c, d, missing_years)
                    except Exception as e:
                        console.log(f"[yellow]G8 wayback {d}: {e}")

            # Phase 2b — Opus hints: si faltan AR criticos, preguntar a Opus
            # por dominios adicionales / URLs especificas (1 call, ~$0.02)
            missing_critical = [t for t in state.missing_doc_targets()
                                if t[0] in {"annual_report", "factsheet", "quarterly_letter"}]
            if missing_critical and not self._reached_critical_minimums(state):
                opus_sites = self._opus_suggest_sources(c, missing_critical)
                for site in opus_sites:
                    try:
                        all_candidates += await harvest_website(state, c, site, fund_slugs)
                    except Exception as e:
                        console.log(f"[yellow]harvest {site}: {e}")
                # También probar Wayback en los nuevos dominios sugeridos
                if missing_years and opus_sites:
                    for site in opus_sites[:2]:
                        try:
                            all_candidates += await harvest_wayback(c, site, missing_years)
                        except Exception as e:
                            console.log(f"[yellow]wayback {site}: {e}")

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

        # Coverage report final
        try:
            coverage = self._coverage_report(state)
            score = coverage["summary"]["coverage_score"]
            console.log(f"[bold magenta]Coverage: {score}% (inception {coverage['inception_year']})")
            for dt, info in coverage["by_doc_type"].items():
                if info["covered"]:
                    console.log(f"  {dt}: {info['coverage_pct']}% "
                                f"({len(info['covered'])}/{len(coverage['target_years'])} años)")
            # Guardar en el state para que el dashboard lo consuma
            try:
                disc_path = self.fund_dir / "intl_discovery_data.json"
                if disc_path.exists():
                    disc = json.loads(disc_path.read_text(encoding="utf-8"))
                    disc["coverage_report"] = coverage
                    disc_path.write_text(json.dumps(disc, ensure_ascii=False, indent=2),
                                          encoding="utf-8")
            except Exception as e:
                console.log(f"[yellow]coverage save: {e}")
        except Exception as e:
            console.log(f"[yellow]coverage report: {e}")

        # ── G7 — auto-aprendizaje del registry ──────────────────────────────
        # Si se descargaron ≥1 docs útiles desde un dominio detectado por G6
        # (o desde cualquier host nuevo), persistir en gestoras_registry.json
        # con auto_learned=true. Manuales (auto_learned=false) NO se tocan.
        try:
            confirmed = getattr(self, "_domain_probe_cache", {}) or {}
            confirmed_hosts = {d for d, hit in confirmed.items() if hit}
            if confirmed_hosts:
                _UTIL_DT = {"annual_report", "semi_annual_report", "factsheet", "quarterly_letter"}
                per_host_count: dict[str, int] = {}
                for d in state.downloaded_docs:
                    if d.doc_type not in _UTIL_DT:
                        continue
                    try:
                        host = urlparse(d.url).netloc.lower()
                    except Exception:
                        host = ""
                    # match exacto o subdomain del confirmado
                    matched = next(
                        (h for h in confirmed_hosts if h == host or host.endswith("." + h)),
                        None,
                    )
                    if matched:
                        per_host_count[matched] = per_host_count.get(matched, 0) + 1
                for domain, count in per_host_count.items():
                    self._persist_to_registry(domain, count)
        except Exception as e:
            console.log(f"[yellow]G7 wrap-up: {e}")

        return state
