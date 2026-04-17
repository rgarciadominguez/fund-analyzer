"""
Scoring genérico para PDFs descubiertos en cualquier fuente (crawl,
Wayback, Google). El validator post-download decide si es válido y
clasifica el contenido. NO se filtran candidatos por keyword/año
antes de descargar — solo se ordenan por probabilidad.

Principio: el discovery debe ADAPTARSE a la gestora, no asumir formato.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse


# Palabras clave que aumentan score (multi-idioma, multi-formato)
_KEYWORD_BOOSTS = [
    # Annual report
    (re.compile(r"\b(annual[_\-\s]*report|jahresbericht|rechenschaftsbericht|rapport[_\-\s]*annuel|informe[_\-\s]*anual|memoria[_\-\s]*anual|annrep|_AR_|\bAR\d{4})\b", re.I), 60),
    # Semi-annual report
    (re.compile(r"\b(semi[_\-\s]?annual|halbjahres|rapport[_\-\s]*semestriel|informe[_\-\s]*semestral|_SAR_|semirep)\b", re.I), 55),
    # Factsheet / monthly report
    (re.compile(r"\b(factsheet|fact[_\-\s]sheet|monthly[_\-\s]*report|monatsbericht|reporting[_\-\s]*mensuel|fiches[_\-\s]*reporting|MMF|[-_]MR[-_])\b", re.I), 35),
    # YYYY-MM filename pattern (factsheet implícito)
    (re.compile(r"\b(19|20)\d{2}[-_.](0[1-9]|1[0-2])[-_]"), 30),
    # Prospectus / KID
    (re.compile(r"\b(prospectus|prospekt|verkaufsprospekt|VKP|folleto)\b", re.I), 50),
    (re.compile(r"\b(kid|kiid|priips|wesentliche[_\-\s]*anlegerinformationen)\b", re.I), 45),
    # Manager presentation / commentary
    (re.compile(r"\b(presentation|pitch[_\-\s]*deck|webinar|investor[_\-\s]*day|commentary)\b", re.I), 25),
    # Year in URL
    (re.compile(r"\b(20\d{2})\b"), 10),
]

# Penalizaciones (señales negativas)
_KEYWORD_PENALTIES = [
    # Páginas legales/admin/promo
    (re.compile(r"\b(privacy|cookies?|terms|conditions|legal[_\-\s]*notice|aviso[_\-\s]*legal)\b", re.I), -50),
    (re.compile(r"\b(application|formulario|brochure|leaflet|guide|manual|conference)\b", re.I), -20),
    # NOTA: penalty de factsheets mensuales se aplica en score_pdf_url() tras
    # detectar que NO hay keyword de AR/SAR/Interim (fiscal-year-end con mes).
    # Otros años de ISIN incrustados (probable doc de OTRO fondo)
    # Esto se manejará a nivel orquestador con isin-specific logic
]


def score_pdf_url(
    url: str,
    text_link: str = "",
    isin: str = "",
    sicav: str = "",
) -> int:
    """
    Score genérico para un PDF candidato.
    >50 = alta probabilidad de ser doc del fondo
    20-50 = candidato razonable
    <20 = baja probabilidad
    """
    score = 0
    combined = f"{text_link} {url}".lower()

    # Boosts por keywords
    for pat, pts in _KEYWORD_BOOSTS:
        if pat.search(combined):
            score += pts

    # Penalizaciones
    for pat, pts in _KEYWORD_PENALTIES:
        if pat.search(combined):
            score += pts

    # Penalty mensual condicional: factsheets mensuales (ene-mayo, jul-nov) que
    # NO son fiscal-year-end de un AR/SAR/Interim. Evita basura pero deja pasar
    # "Annual-Report-31-January-2025" (fiscal y/e enero) o "Interim-July-2025".
    month_match = re.search(
        r"\b(january|february|march|april|may|july|august|september|october|november)[-_ ](19|20)\d{2}\b",
        combined, re.I,
    )
    if month_match:
        has_major = re.search(
            r"\b(annual[_\-\s]*report|semi[_\-\s]?annual|interim|rapport[_\-\s]*annuel|jahresbericht|halbjahres|informe[_\-\s]*anual)\b",
            combined, re.I,
        )
        if not has_major:
            score -= 40

    # Bonus si contiene ISIN exacto
    if isin and isin.lower() in url.lower():
        score += 80
    elif isin and isin.lower() in combined:
        score += 30

    # Bonus si contiene SICAV name (varios tokens)
    if sicav:
        sicav_tokens = re.findall(r"[A-Za-z]{4,}", sicav.lower())
        sig = [t for t in sicav_tokens if t not in {
            "fund", "funds", "fonds", "invest", "asset", "management",
            "sicav", "fcp", "ucits", "ltd", "limited", "sa", "gmbh",
        }]
        if sig and any(t in combined for t in sig):
            score += 25

    return score


def is_gestora_like_domain(host: str, gestora_name: str = "") -> bool:
    """
    ¿El host parece de la gestora (no plataforma distribuidora)?

    Es genérico: detecta por (a) host coincide con nombre gestora normalizado,
    (b) host contiene segmento típico de fund manager (-am, -investments,
    -asset, -fund), o (c) NO está en lista de plataformas conocidas.
    """
    if not host:
        return False
    host_l = host.lower()

    # Excluir plataformas conocidas
    PLATFORMS = {
        "avl-investmentfonds.de", "ebnbanco.com", "vwdservices.com",
        "fundinfo.com", "fefundinfo.com", "ancoria.com",
        "morningstar.com", "morningstar.es", "morningstar.de",
        "comdirect.de", "finanzen.net", "dasinvestment.com",
        "fondsweb.com", "justetf.com", "trustnet.com", "rankia.com",
        "openbank.es", "myinvestor.es", "boursorama.com",
        "bankinter.com", "investing.com", "yahoo.com",
        "bundesanzeiger.de", "cssf.lu", "amf-france.org", "geco.amf-france.org",
        "centralbank.ie", "registers.centralbank.ie",
        "luxse.com", "dl.luxse.com", "bourse.lu", "dl.bourse.lu",
        "deutsche-bank.es", "scribd.com", "yumpu.com",
        "ethe.org.gr", "bimvita.it",  # plataformas insurance/distribuidor
    }
    for plat in PLATFORMS:
        if host_l == plat or host_l.endswith("." + plat) or plat in host_l:
            return False

    # Bonus por segmentos típicos de gestora
    GESTORA_HINTS = ["-am.", "-investments.", "-asset.", "-fund.", "-funds.",
                     "asset-management.", "investments.", "im."]
    if any(h in host_l for h in GESTORA_HINTS):
        return True

    # Match con nombre gestora
    if gestora_name:
        tokens = re.findall(r"[a-z0-9]{3,}", gestora_name.lower())
        for t in tokens:
            if len(t) >= 4 and t in host_l:
                return True

    # Default: aceptar (mejor incluir y validar que filtrar)
    return True


def is_parent_cdn(host: str) -> bool:
    """
    ¿Es CDN de un parent group conocido (Natixis, Allianz, etc.)?
    Detección genérica: host contiene "im.", "content/dam/", "cdn." o
    el dominio raíz es de un grupo financiero global.
    """
    if not host:
        return False
    host_l = host.lower()
    # Subdomain patterns típicos de CDN B2B financiero
    if host_l.startswith(("im.", "cdn.", "static.", "files.", "documents.", "api-esb.")):
        return True
    # Major financial group root domains
    GROUPS = ["natixis.com", "allianz.com", "blackrock.com", "jpmorgan.com",
              "vanguard.com", "fidelity.com", "schroders.com", "ubs.com",
              "credit-suisse.com", "bnpparibas.com", "amundi.com", "pictet.com"]
    return any(g in host_l for g in GROUPS)
