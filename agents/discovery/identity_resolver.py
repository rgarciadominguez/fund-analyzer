"""
Enriquece identity cuando el regulador dejó huecos.

Fuente principal: **el KID / prospectus que ya tenemos localmente** tras el
prestep. Son documentos regulatorios obligatorios que citan:
  - Investment Manager (gestor real de cartera)
  - Management Company (ManCo legal, puede ser distinta — administrador)
  - Website oficial del gestor

Criterio: si Investment Manager existe, ése es el gestora_oficial para discovery
y para el email. Si no, caer a Management Company.

Se llama DESPUÉS del prestep (cuando ya están prospectus/KID en raw/) y ANTES
del crawl de gestora.
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx
from rich.console import Console

console = Console()


def _fetch_title(url: str) -> str:
    """GET sync corto + extract <title>."""
    try:
        with httpx.Client(follow_redirects=True, timeout=10,
                          headers={"User-Agent": "Mozilla/5.0"}) as c:
            r = c.get(url)
            if r.status_code != 200:
                return ""
            m = re.search(r"<title[^>]*>([^<]{3,200})</title>", r.text, re.I)
            if not m:
                return ""
            title = m.group(1).strip()
            # Normalizar: quitar sufijos típicos
            title = re.sub(r"\s*[\|\-–—]\s*(Home|Homepage|Welcome|Asset Management).*$", "", title, flags=re.I)
            return title.strip()
    except Exception:
        return ""


_PATTERNS = {
    "investment_manager": [
        # "Investment Manager: Troy Asset Management Ltd"
        re.compile(r"Investment Manager[\s:]*\n?\s*([A-Z][A-Za-z0-9&.,\- ()']{3,80}(?:Ltd|Limited|LLP|LLC|Inc|plc|AG|S\.A\.|SA|GmbH|KGaA|Capital|Asset Management|Investments))",
                   re.IGNORECASE),
        # "managed by Troy Asset Management Limited"
        re.compile(r"managed by\s+([A-Z][A-Za-z0-9&.,\- ()']{3,80}(?:Ltd|Limited|LLP|plc|AG|GmbH|Capital|Asset Management|Investments))",
                   re.IGNORECASE),
    ],
    "management_company": [
        re.compile(r"Management Company[\s:]*\n?\s*([A-Z][A-Za-z0-9&.,\- ()']{3,90}(?:Ltd|Limited|LLP|plc|AG|S\.A\.|SA|GmbH|KGaA|Capital|Management))",
                   re.IGNORECASE),
        re.compile(r"Manager[\s:]+([A-Z][A-Za-z0-9&.,\- ()']{3,90}(?:Fund Management|ManagementLimited|FundManagementLimited))",
                   re.IGNORECASE),
    ],
}

# Websites pattern: "Manager's website at https://www.taml.co.uk"
_WEBSITE_PATTERN = re.compile(
    r"(?:Investment Manager|Manager)(?:['\u2019]s)?\s*website[^\n]*?(https?://[a-z0-9.\-]+\.[a-z]{2,10})",
    re.IGNORECASE,
)


def _clean_name(raw: str) -> str:
    """Repara PDFs que strippean espacios ('TroyAssetManagement' → 'Troy Asset Management')."""
    s = raw.strip().rstrip(".,;:")
    # Insertar espacio antes de mayúscula que sigue a minúscula/letra
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_pdf_text(path: Path, max_pages: int = 20) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(path) as p:
            txt = ""
            for pg in p.pages[:max_pages]:
                txt += (pg.extract_text() or "") + "\n"
            return txt
    except Exception:
        return ""


def enrich_from_local_docs(identity: dict, fund_dir: Path) -> dict:
    """
    Intenta rellenar gestora_oficial leyendo KID/prospectus locales.
    Modifica y devuelve el dict identity.
    También devuelve el dominio del Investment Manager si lo encuentra
    (para que el caller lo añada a discovered_gestora_domains).
    """
    if identity.get("gestora_oficial") and identity.get("nombre_oficial"):
        # Si ya tenemos ambos, solo intentamos sacar website
        pass

    disc_dir = fund_dir / "raw" / "discovery"
    if not disc_dir.exists():
        return identity

    # Priorizar KID (más corto, Investment Manager suele ir claro)
    candidates = []
    for name in ("kid_latest.pdf", "prospectus_latest.pdf"):
        p = disc_dir / name
        if p.exists():
            candidates.append(p)
    # También buscar cualquier KID/prospectus si los nombres difieren
    for p in disc_dir.glob("*.pdf"):
        nm = p.name.lower()
        if ("kid" in nm or "prospect" in nm) and p not in candidates:
            candidates.append(p)

    if not candidates:
        return identity

    investment_mgr = ""
    management_co = ""
    website = ""

    for p in candidates:
        text = _extract_pdf_text(p)
        if not text:
            continue

        # Investment Manager
        if not investment_mgr:
            for pat in _PATTERNS["investment_manager"]:
                m = pat.search(text)
                if m:
                    investment_mgr = _clean_name(m.group(1))
                    break

        # Management Company
        if not management_co:
            for pat in _PATTERNS["management_company"]:
                m = pat.search(text)
                if m:
                    management_co = _clean_name(m.group(1))
                    break

        # Website
        if not website:
            m = _WEBSITE_PATTERN.search(text)
            if m:
                website = m.group(1).strip().rstrip(".,;)")

    # Si hay website del Investment Manager, priorizar: fetch homepage y sacar
    # el nombre legal de la <title> — más fiable que regex sobre KID.
    if website and not investment_mgr:
        title = _fetch_title(website)
        if title:
            # p.ej. "Troy Asset Management" o "Troy Asset Management Ltd"
            # Filtrar tokens genéricos al final
            title = re.sub(r"[:|].*$", "", title).strip()
            if 3 < len(title) < 80:
                investment_mgr = title

    # Priorizar Investment Manager sobre Management Company
    real_gestora = investment_mgr or management_co
    if real_gestora and not identity.get("gestora_oficial"):
        identity["gestora_oficial"] = real_gestora

    if website:
        identity.setdefault("_gestora_website", website)

    if real_gestora or website:
        console.log(f"[blue]identity enriched from KID/prospectus:[/blue] "
                    f"gestora='{real_gestora or identity.get('gestora_oficial','')}' "
                    f"website='{website}'")

    return identity
