"""sector_classifier.py — Clasificación de posiciones por SECTOR canónico, con
caché GLOBAL reutilizable entre todos los fondos (INT + ESP).

Diseño (Rafa, 2026-06-10, Opción B):
- Taxonomía fija de 11 sectores (pocos, limpio). Nada fuera de la lista.
- Caché global `data/company_sectors.json` keyed por NOMBRE NORMALIZADO de la
  empresa (sin sufijos societarios ni clase de acción) → la misma empresa se
  clasifica UNA vez y se reutiliza en todo el catálogo (coste decreciente,
  consistencia total).
- La clasificación de empresas NUEVAS la hace Claude (conocimiento de empresa)
  vía `classify_unknowns` en una sesión Cowork; el pipeline (Python) solo APLICA
  el caché (determinista) y agrega el desglose. Si una empresa no está en caché
  y no se puede clasificar con certeza → 'Otros' (editable).

API determinista (pipeline):
  apply_sectors(positions) -> (n_set, n_unknown)   # rellena pos['sector'] desde caché
  build_sector_allocation(positions) -> list[{sector, peso_pct}]
  unknown_companies(positions) -> [nombres sin clasificar]
API de clasificación (sesión Claude):
  add_classifications({nombre_original: sector_canonico})   # valida + cachea
CLI:
  python -m tools.sector_classifier --report          # cobertura en todo el catálogo
  python -m tools.sector_classifier --unknowns ISIN    # empresas sin clasificar de un fondo
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
CACHE_PATH = ROOT / "data" / "company_sectors.json"

# Taxonomía CANÓNICA (11 + Otros). NADA fuera de aquí.
CANONICAL_SECTORS = [
    "Tecnología", "Servicios financieros", "Salud", "Consumo cíclico",
    "Consumo defensivo", "Industria", "Energía", "Materiales",
    "Servicios públicos", "Inmobiliario", "Comunicación", "Otros",
]
# Sinónimos/idiomas → canónico (para validar lo que clasifique Claude/CNMV)
_SECTOR_SYNONYMS = {
    "technology": "Tecnología", "information technology": "Tecnología", "tech": "Tecnología",
    "tecnologia": "Tecnología", "it": "Tecnología",
    "financials": "Servicios financieros", "financial services": "Servicios financieros",
    "finance": "Servicios financieros", "banks": "Servicios financieros",
    "financiero": "Servicios financieros", "servicios financieros": "Servicios financieros",
    "seguros": "Servicios financieros", "insurance": "Servicios financieros",
    "health care": "Salud", "healthcare": "Salud", "health": "Salud", "salud": "Salud",
    "pharma": "Salud", "biotech": "Salud",
    "consumer discretionary": "Consumo cíclico", "consumer cyclical": "Consumo cíclico",
    "consumo ciclico": "Consumo cíclico", "consumo cíclico": "Consumo cíclico",
    "cyclical consumer goods": "Consumo cíclico", "retail": "Consumo cíclico",
    "consumer staples": "Consumo defensivo", "consumer defensive": "Consumo defensivo",
    "non-cyclical consumer goods": "Consumo defensivo", "consumo defensivo": "Consumo defensivo",
    "consumo basico": "Consumo defensivo", "consumo básico": "Consumo defensivo",
    "industrials": "Industria", "industrial": "Industria", "industria": "Industria",
    "energy": "Energía", "energia": "Energía", "energía": "Energía", "oil": "Energía", "oil & gas": "Energía",
    "materials": "Materiales", "basic materials": "Materiales", "raw materials": "Materiales",
    "materiales": "Materiales", "materias primas": "Materiales", "mining": "Materiales",
    "metals & mining": "Materiales", "chemicals": "Materiales", "construction materials": "Materiales",
    "pharmaceuticals": "Salud", "biotechnology": "Salud", "diversified financials": "Servicios financieros",
    "capital goods": "Industria", "transportation": "Industria", "semiconductors": "Tecnología",
    "software & services": "Tecnología", "automobiles": "Consumo cíclico",
    "food beverage & tobacco": "Consumo defensivo", "food & staples retailing": "Consumo defensivo",
    "consumer durables & apparel": "Consumo cíclico", "telecommunication services": "Comunicación",
    "utilities": "Servicios públicos", "servicios publicos": "Servicios públicos",
    "servicios públicos": "Servicios públicos", "utilidades": "Servicios públicos",
    "real estate": "Inmobiliario", "inmobiliario": "Inmobiliario", "reit": "Inmobiliario",
    "communication services": "Comunicación", "communication": "Comunicación",
    "telecom": "Comunicación", "telecommunications": "Comunicación", "comunicacion": "Comunicación",
    "comunicación": "Comunicación", "media": "Comunicación",
    "other": "Otros", "others": "Otros", "otros": "Otros",
}

# Sufijos societarios / ruido a quitar del nombre para la clave de caché
_SUFFIXES = (
    r"s\.?a\.?", r"s\.?a\.?u\.?", r"plc", r"inc\.?", r"corp\.?", r"corporation",
    r"co\.?", r"ltd\.?", r"limited", r"ag", r"n\.?v\.?", r"se", r"a/s", r"asa",
    r"ab", r"abp", r"oyj", r"spa", r"s\.?p\.?a\.?", r"sca", r"saca", r"scsa",
    r"bhd", r"tbk", r"pjsc", r"kgaa",
    r"holdings?", r"group", r"grupo", r"company", r"the", r"reit", r"adr", r"gdr",
    r"sicav", r"class\s+[a-z0-9]+", r"reg\.?", r"pref\.?", r"-rights?", r"wts?",
)
_SUFFIX_RE = re.compile(r"\b(" + "|".join(_SUFFIXES) + r")\b", re.IGNORECASE)


def _norm_company(name: str) -> str:
    """Clave de caché: minúsculas, sin sufijos societarios/clase, sin puntuación."""
    s = (name or "").lower().strip()
    s = re.sub(r"['\".,()/&]", " ", s)
    s = _SUFFIX_RE.sub(" ", s)
    s = re.sub(r"\b[a-z]\b", " ", s)            # letras sueltas (clases 'A','B')
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonical_sector(value: str) -> str | None:
    """Valida/normaliza un sector a la taxonomía canónica. None si no reconoce."""
    if not value:
        return None
    v = str(value).strip()
    if v in CANONICAL_SECTORS:
        return v
    return _SECTOR_SYNONYMS.get(v.lower())


def load_cache() -> dict:
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def sector_for(name: str, cache: dict | None = None) -> str | None:
    cache = cache if cache is not None else load_cache()
    return cache.get(_norm_company(name))


def clean_positions(positions: list) -> int:
    """Limpia artefactos de extracción de nombres (caso AR umbrella GS):
    - extrae el sector del paréntesis final '(Banks)'/'(Mining)' → pos['sector'];
    - re-espacia nombres pegados en CamelCase ('RaiffeisenBankInternationalAG'
      → 'Raiffeisen Bank International AG'; 'BHPBilliton' → 'BHP Billiton').
    Devuelve nº de nombres modificados. Idempotente."""
    n = 0
    for p in positions or []:
        if not isinstance(p, dict):
            continue
        nm = p.get("nombre", "") or ""
        orig = nm
        # sector entre paréntesis al final
        m = re.search(r"\(([^()]{3,40})\)\s*$", nm)
        if m:
            sec = canonical_sector(m.group(1).strip())
            if sec and not canonical_sector(p.get("sector")):
                p["sector"] = sec
            nm = nm[:m.start()].strip()
        # separar sufijo legal pegado al final ('DanoneSA'->'Danone SA',
        # 'NokiaOYJ'->'Nokia OYJ', 'MowiASA'->'Mowi ASA') → _norm_company lo quita
        # y la empresa casa con la caché.
        nm = re.sub(r"(?<=[A-Za-z])(ASA|SpA|OYJ|GmbH|KGaA|Abp|PLC|Ltd|SA|SE|AS|AG|NV)(?=$|\s|\.|,)", r" \1", nm)
        # re-espaciar si está pegado (sin espacios y con mayúsculas internas)
        if " " not in nm and len(nm) > 8 and re.search(r"[a-z][A-Z]|[A-Z]{2,}[a-z]", nm):
            nm = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", nm)
            nm = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", nm)
        nm = re.sub(r"\s+", " ", nm).strip()
        if nm and nm != orig:
            p["nombre"] = nm
            n += 1
    return n


def apply_sectors(positions: list, cache: dict | None = None) -> tuple[int, int]:
    """Rellena pos['sector'] desde el caché (canónico). Devuelve (n_set, n_unknown).
    Respeta un sector ya canónico que traiga la posición (p.ej. CNMV)."""
    cache = cache if cache is not None else load_cache()
    n_set = n_unknown = 0
    for p in positions or []:
        if not isinstance(p, dict):
            continue
        existing = canonical_sector(p.get("sector"))
        if existing:
            p["sector"] = existing
            n_set += 1
            continue
        sec = cache.get(_norm_company(p.get("nombre", "")))
        if sec:
            p["sector"] = sec
            n_set += 1
        else:
            n_unknown += 1
    return n_set, n_unknown


def unknown_companies(positions: list, cache: dict | None = None) -> list:
    """Nombres (originales) de posiciones sin sector canónico ni en caché."""
    cache = cache if cache is not None else load_cache()
    out, seen = [], set()
    for p in positions or []:
        if not isinstance(p, dict):
            continue
        if canonical_sector(p.get("sector")):
            continue
        nm = p.get("nombre", "")
        key = _norm_company(nm)
        if key and not cache.get(key) and key not in seen:
            seen.add(key)
            out.append(nm)
    return out


def add_classifications(mapping: dict) -> int:
    """Añade {nombre_original: sector} al caché global (valida canónico). Devuelve nº añadidos."""
    cache = load_cache()
    n = 0
    for name, sector in (mapping or {}).items():
        sec = canonical_sector(sector)
        key = _norm_company(name)
        if sec and key:
            cache[key] = sec
            n += 1
    save_cache(cache)
    return n


def build_sector_allocation(positions: list, cache: dict | None = None) -> list:
    """Desglose [{sector, peso_pct}] sumando pesos de las posiciones por sector."""
    cache = cache if cache is not None else load_cache()
    agg: dict = {}
    for p in positions or []:
        if not isinstance(p, dict):
            continue
        sec = canonical_sector(p.get("sector")) or cache.get(_norm_company(p.get("nombre", ""))) or "Otros"
        w = p.get("peso_pct") or 0
        if w:
            agg[sec] = round(agg.get(sec, 0) + w, 2)
    return [{"sector": s, "peso_pct": w} for s, w in sorted(agg.items(), key=lambda x: -x[1])]


def _all_positions(isin: str) -> list:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return []
    try:
        return (json.loads(p.read_text(encoding="utf-8")).get("posiciones", {}) or {}).get("actuales", []) or []
    except Exception:
        return []


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Clasificador de sectores (caché global)")
    ap.add_argument("--report", action="store_true", help="cobertura del caché en todo el catálogo")
    ap.add_argument("--unknowns", help="lista empresas sin clasificar de un ISIN")
    args = ap.parse_args(argv)
    cache = load_cache()
    if args.unknowns:
        unk = unknown_companies(_all_positions(args.unknowns.strip().upper()), cache)
        print(f"{len(unk)} empresas sin clasificar en {args.unknowns}:")
        for u in unk:
            print(f"  {u}")
        return 0
    # report
    print(f"Caché global: {len(cache)} empresas clasificadas")
    tot = tot_unk = 0
    for d in sorted(FUNDS_DIR.iterdir()):
        if not d.is_dir() or "." in d.name:
            continue
        pos = _all_positions(d.name)
        if not pos:
            continue
        unk = unknown_companies(pos, cache)
        tot += len(pos)
        tot_unk += len(unk)
        if unk:
            print(f"  {d.name}: {len(unk)}/{len(pos)} sin clasificar")
    print(f"\nTotal posiciones: {tot} | sin clasificar: {tot_unk} | cobertura: {round(100*(tot-tot_unk)/max(1,tot),1)}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
