"""Loader único para data/trusted_sources.json.

Consumido por:
- agents/readings_agent.py    (PRIORITY_SOURCES)
- agents/manager_google_snippets.py (TRUSTED_DOMAINS)
- dashboard/generate_dashboard.py (pro_sources, logo_map)

Si el JSON falta o está corrupto, devuelve listas vacías y loggea — pero
los consumers tienen fallback hardcoded por si acaso.
"""
import json
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).parent.parent
TRUSTED_PATH = ROOT / "data" / "trusted_sources.json"


@lru_cache(maxsize=1)
def _load() -> dict:
    if not TRUSTED_PATH.exists():
        return {"pro_sources": [], "international_media": [], "trusted_extra_for_managers": []}
    try:
        return json.loads(TRUSTED_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"pro_sources": [], "international_media": [], "trusted_extra_for_managers": []}


def _matches_region(src: dict, region: str | None) -> bool:
    """True si la source aplica a la región pedida (o si no se filtra)."""
    if region is None:
        return True
    src_regions = src.get("region", ["ES", "INT"])  # default backward-compat
    return region in src_regions or "all" in src_regions


def get_priority_sources(region: str | None = None) -> list[tuple[str, str]]:
    """Lista (domain, label) para readings_agent. Pro + medios internacionales.
    Si region='ES' o 'INT', filtra solo sources aplicables a esa región.
    Si region=None, devuelve todas (compat).
    """
    data = _load()
    out = []
    for src in data.get("pro_sources", []):
        if _matches_region(src, region):
            out.append((src["domain"], src["label"]))
    for src in data.get("international_media", []):
        if _matches_region(src, region):
            out.append((src["domain"], src["label"]))
    return out


def get_trusted_domains(region: str | None = None) -> list[str]:
    """Lista plana de dominios trusted para manager_google_snippets.
    Incluye pro_sources + international_media + trusted_extra_for_managers.
    Si region especificado, filtra pro/internacional pero mantiene trusted_extra
    (las webs de gestoras siempre son válidas independientemente de región).
    """
    data = _load()
    domains = []
    for src in data.get("pro_sources", []):
        if _matches_region(src, region):
            domains.append(src["domain"])
    for src in data.get("international_media", []):
        if _matches_region(src, region):
            domains.append(src["domain"])
    domains.extend(data.get("trusted_extra_for_managers", []))
    # Dedup preservando orden
    seen = set()
    out = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def get_pro_source_domains(region: str | None = None) -> list[str]:
    """Dominios considerados 'pro' por el dashboard (sección 'Análisis profesionales')."""
    data = _load()
    return [src["domain"] for src in data.get("pro_sources", []) if _matches_region(src, region)]


def get_logo_map() -> dict[str, tuple[str, str]]:
    """Mapa {keyword_lowercase: (color, initials)} para el dashboard.
    Usa label en lowercase como key (matchea contra fuente del reading).
    """
    data = _load()
    out = {}
    for src in data.get("pro_sources", []) + data.get("international_media", []):
        # Key principal: label en lowercase (palabras clave)
        for word in src["label"].lower().split():
            if len(word) >= 3 and word not in out:
                out[word] = (src["logo_color"], src["initials"])
        # También el subdomain principal
        domain_key = src["domain"].split(".")[0]
        if domain_key not in out:
            out[domain_key] = (src["logo_color"], src["initials"])
    return out
