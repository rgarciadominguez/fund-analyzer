"""
Downloader: descarga + valida + indexa contenido.

Usado por ambos tracks. Deduplica via SharedState.attempted_downloads.
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx

from agents.discovery.state import DiscoveredDoc, SharedState
from agents.discovery.validator import (
    detect_isins_in_doc,
    detect_language,
    detect_manager_commentary,
    guess_fecha_publicacion,
    validate_file,
)


def _safe_filename(url: str, doc_type: str, periodo: str) -> str:
    base = url.rsplit("/", 1)[-1].split("?", 1)[0]
    if not base or "." not in base:
        ext = "pdf"
        if url.lower().endswith(".html"):
            ext = "html"
        elif url.lower().endswith(".xml"):
            ext = "xml"
        base = f"{doc_type}_{periodo or 'latest'}.{ext}"
    safe = re.sub(r"[^\w\-_.]", "_", base)[:120]
    return safe


async def download_and_register(
    state: SharedState,
    c: httpx.AsyncClient,
    url: str,
    doc_type: str,
    periodo: str,
    source: str,
    source_detail: str = "",
) -> DiscoveredDoc | None:
    """
    Descarga el archivo, valida, indexa y lo registra en state.downloaded_docs.
    Devuelve el DiscoveredDoc si es válido, None si falla.
    """
    # Dedup
    if url in state.attempted_downloads:
        return state.already_downloaded(url)
    await state.mark_download_attempted(url)

    # Las descargas tienen su propio budget (no comparten con probing/crawl)
    if not state.budget.try_download():
        return None

    target_dir = state.fund_dir / "raw" / "discovery"
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _safe_filename(url, doc_type, periodo)
    target = target_dir / filename

    # Timeouts agresivos: 8s connect, 30s read. Un PDF tarda <10s normalmente;
    # si tarda más, hay un servidor colgado y no perdemos minutos esperándolo.
    try:
        from agents.discovery.cloudflare_bypass import fetch_with_fallback
        status, content, headers = await fetch_with_fallback(c, url, timeout=30.0)
        if status != 200:
            return None
        ct = (headers.get("content-type") or "").lower()
        if not any(k in ct for k in ("pdf", "html", "xml", "octet-stream")):
            if not content.startswith(b"%PDF") and b"<html" not in content[:200].lower():
                return None
        target.write_bytes(content)
    except Exception:
        return None

    # Validar
    fund_name = state.identity.get("nombre_oficial", "")
    sicav_paraguas = state.identity.get("sicav_paraguas", "")
    is_valid, contains = validate_file(
        target, state.isin, fund_name=fund_name, sicav_paraguas=sicav_paraguas,
    )

    # Excepción: si el URL contiene el ISIN EXACTO del fondo, confiamos
    # (typical en CDNs de gestoras: ancoria.com/FundFactSheets/LU0840158819.pdf).
    # Aceptamos siempre que el archivo sea parseable (>20KB, no corrupto).
    if not is_valid and state.isin.upper() in url.upper():
        try:
            from agents.discovery.validator import _extract_text_for_validation
            txt = _extract_text_for_validation(target)
            if txt.strip():
                is_valid = True
                from agents.discovery.validator import CONTENT_PATTERNS
                contains = {k for pat, k in CONTENT_PATTERNS if pat.search(txt)}
        except Exception:
            pass

    # Excepción 2 — UMBRELLA AR/SAR: SICAVs irlandesas/luxemburguesas publican
    # AR umbrella (114+ págs) cubriendo varios sub-fondos, pero el ISIN de la
    # clase específica puede NO aparecer en el texto (va en prospectus/KID).
    # Si la URL es de un dominio gestora-like ya descubierto Y el filename dice
    # annual/semi-annual/interim report → confiamos y marcamos como umbrella.
    if not is_valid:
        from urllib.parse import urlparse
        from agents.discovery.scoring import is_gestora_like_domain, is_parent_cdn
        from agents.discovery.validator import _extract_text_for_validation, CONTENT_PATTERNS
        host = urlparse(url).netloc.lower()
        host_ok = (
            host in state.discovered_gestora_domains
            or any(host in d or d in host for d in state.discovered_gestora_domains)
            or is_gestora_like_domain(host, state.identity.get("gestora_oficial", ""))
            or is_parent_cdn(host)
        )
        url_lower = url.lower()
        is_trusted_pattern = any(kw in url_lower for kw in [
            # AR / SAR / Interim
            "annual-report", "annual_report", "annualreport",
            "semi-annual", "semiannual", "semi_annual",
            "interim-report", "interim_report", "interimreport",
            "rapport-annuel", "jahresbericht", "halbjahresbericht",
            # Umbrella letters / commentary / factsheets / special papers
            "investor-letter", "investor_letter",
            "investment-report", "investment_report",
            "quarterly-letter", "carta-trimestral",
            "special-paper", "white-paper",
            "fact-sheet", "factsheet",
            "presentation", "pitch-deck",
        ])
        # Prefijos industriales de CDN (Natixis IM, Amundi, BlackRock, etc.):
        # /AR-{slug}/, /SAR-{slug}/, /MR-{slug}/, /LETTER-{slug}/, /KID-/, /PRS-/
        if not is_trusted_pattern:
            is_trusted_pattern = bool(re.search(
                r"/(AR|SAR|MR|LETTER|KID|KIID|PRS|PRSEN|VKP|ANNREP|SEMIREP|QR)[-_][a-z0-9]",
                url, re.I,
            ))
        if host_ok and is_trusted_pattern:
            try:
                txt = _extract_text_for_validation(target)
                if txt.strip() and target.stat().st_size > 50_000:
                    is_valid = True
                    contains = {k for pat, k in CONTENT_PATTERNS if pat.search(txt)}
            except Exception:
                pass

    if not is_valid:
        try:
            target.unlink()
        except Exception:
            pass
        return None

    # Metadata
    fecha = guess_fecha_publicacion(target)
    lang = detect_language(target)
    content_type = "html" if target.suffix.lower() in (".html", ".htm") else (
        "xml" if target.suffix.lower() == ".xml" else "pdf"
    )

    # Decisión de doc_type con jerarquía:
    #   1. URL/filename tiene keyword fuerte → autoritativo
    #   2. Si no, content-classification dominante
    #   3. Fallback: el doc_type que pidió el caller
    from agents.discovery.gestora_crawler import (
        classify_link, detect_factsheet_month, factsheet_subtype,
    )
    from agents.discovery.validator import _extract_text_for_validation, classify_content
    actual_type = doc_type
    url_class = classify_link(text="", href=url)
    if url_class:
        actual_type = url_class
    elif contains:
        text_for_class = _extract_text_for_validation(target)
        _, dominant = classify_content(text_for_class)
        if dominant and dominant != doc_type:
            actual_type = dominant

    # Subtype para factsheets: eoy/mid_year/monthly según el mes en filename
    subtype = ""
    month = ""
    fiscal_year = ""
    if actual_type == "factsheet":
        fiscal_year, month = detect_factsheet_month(url)
        subtype = factsheet_subtype(month)

    # IMPORTANTE: registrar el dominio gestora-like ANTES del filtro monthly.
    # También registramos parent-CDNs (im.natixis.com, blackrock.com, etc.)
    # y rutas padre con segmentos típicos (/content/dam/, /fund-documents/).
    from urllib.parse import urlparse
    from agents.discovery.scoring import is_gestora_like_domain, is_parent_cdn
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    gestora_name = state.identity.get("gestora_oficial", "")
    if host and (is_gestora_like_domain(host, gestora_name) or is_parent_cdn(host)):
        async with state._lock:
            state.discovered_gestora_domains.add(host)
            # Si la URL tiene segmentos canónicos de directorio de docs,
            # registrar también el path padre como crawl target.
            for marker in ("/content/dam/", "/fund-documents/", "/documents/", "/uploads/"):
                if marker in url:
                    parent_path = url.split(marker)[0] + marker
                    state.discovered_gestora_domains.add(parent_path)
                    break

    # Filtro: solo conservamos factsheets Jun y Dic (sustitutos de SAR/AR).
    # Los mensuales intermedios se descartan del listado final.
    if actual_type == "factsheet" and subtype == "monthly":
        try:
            target.unlink()
        except Exception:
            pass
        return None

    # Detectar ISINs distintos → umbrella AR si cubre múltiples sub-fondos
    isins_inside = detect_isins_in_doc(target)
    is_umbrella = (
        actual_type in ("annual_report", "semi_annual_report")
        and len(isins_inside) >= 3  # ≥3 ISINs distintos = SICAV con varios sub-fondos
    )

    # Override periodo si es factsheet eoy/mid_year: usar año+mes del FILENAME
    # (no del path, ya que el path refleja fecha de subida, no fiscal).
    if subtype in ("eoy", "mid_year") and fiscal_year and month:
        periodo = f"{fiscal_year}-{month}"

    # Notas para el extractor (evitar confusión sobre rentabilidades)
    extractor_notes = ""
    if actual_type == "factsheet" and subtype == "eoy":
        extractor_notes = (
            "Factsheet diciembre: snapshot AUM/NAV/cartera a cierre del año "
            "(puede sustituir parcialmente al AR). IMPORTANTE: rendimientos "
            "mostrados suelen ser mensuales (Dec) / YTD / 1-3-5y — para año "
            "completo usar YTD, NO el return mensual."
        )
    elif actual_type == "factsheet" and subtype == "mid_year":
        extractor_notes = (
            "Factsheet junio: snapshot semestral (puede sustituir parcialmente "
            "al semi-annual report). Los rendimientos son MTD/YTD/6M — verificar "
            "periodo antes de usar."
        )

    # Detectar si hay sección de commentary del gestor (usado como fallback
    # cuando no hay cartas trimestrales)
    has_commentary = detect_manager_commentary(target)

    doc = DiscoveredDoc(
        doc_type=actual_type,
        periodo=periodo or fecha[:4],
        url=url,
        local_path=str(target),
        source=source,
        source_detail=source_detail,
        content_type=content_type,
        size_bytes=target.stat().st_size,
        fecha_publicacion=fecha,
        validated=True,
        contains=contains,
        lang=lang,
        isins_inside=isins_inside,
        is_umbrella=is_umbrella,
        subtype=subtype,
        extractor_notes=extractor_notes,
        contains_manager_commentary=has_commentary,
    )
    await state.add_doc(doc)

    # Side-effects: actualizar state global
    if is_umbrella:
        async with state._lock:
            state.umbrella_mode = True

    return doc


# Plataformas distribuidoras / info sites que NO son la gestora
_PLATFORM_DOMAINS = {
    # Distribuidores fondos
    "avl-investmentfonds.de", "dl.avl-investmentfonds.de",
    "ebnbanco.com", "marketdata.ebnbanco.com",
    "vwdservices.com", "solutions.vwdservices.com",
    "fundinfo.com", "api.fundinfo.com", "fefundinfo.com",
    "ancoria.com",
    # Info aggregators
    "morningstar.com", "morningstar.es", "morningstar.de",
    "comdirect.de", "finanzen.net", "dasinvestment.com",
    "fondsweb.com", "justetf.com", "trustnet.com", "rankia.com",
    "openbank.es", "myinvestor.es", "boursorama.com",
    "bankinter.com", "investing.com", "yahoo.com",
    # Reguladores / oficial
    "bundesanzeiger.de", "cssf.lu", "amf-france.org", "geco.amf-france.org",
    "centralbank.ie", "registers.centralbank.ie",
    "luxse.com", "dl.luxse.com",
    # Bancos custodios y depositarios típicos
    "deutsche-bank.es", "servicios.deutsche-bank.es",
    # Misc
    "scribd.com", "yumpu.com",
    "natixis.com", "im.natixis.com", "api-esb.im.natixis.com",
    "ethe.org.gr",
}


def _is_platform_domain(host: str) -> bool:
    """¿Es un dominio de plataforma/info, NO de gestora?"""
    host_l = host.lower()
    for plat in _PLATFORM_DOMAINS:
        if host_l == plat or host_l.endswith("." + plat) or plat in host_l:
            return True
    return False
