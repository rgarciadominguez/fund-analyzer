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
COUNTRY_CACHE_PATH = ROOT / "data" / "company_countries.json"   # país de RIESGO por emisor (1-oct-2026)

# Taxonomía CANÓNICA (11 + Otros). NADA fuera de aquí.
CANONICAL_SECTORS = [
    "Tecnología", "Servicios financieros", "Salud", "Consumo cíclico",
    "Consumo defensivo", "Industria", "Energía", "Materiales",
    "Servicios públicos", "Inmobiliario", "Comunicación",
    "Gobierno", "Supranacional",          # deuda soberana / de agencias públicas y de organismos supranacionales (1-oct-2026)
    "Titulizaciones",                     # ABS / MBS / CLO / RMBS / CMBS (renta fija sin sector de empresa)
    "Liquidez y monetarios", "Fondos",    # liquidez, repos, fondos monetarios / otros fondos y ETFs en cartera
    "Otros",
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
    "government": "Gobierno", "governments": "Gobierno", "sovereign": "Gobierno", "sovereigns": "Gobierno",
    "treasury": "Gobierno", "treasuries": "Gobierno", "govt": "Gobierno", "gobierno": "Gobierno",
    "soberano": "Gobierno", "deuda pública": "Gobierno", "deuda publica": "Gobierno", "agency": "Gobierno",
    "agencies": "Gobierno", "municipal": "Gobierno", "public sector": "Gobierno",
    "supranational": "Supranacional", "supranationals": "Supranacional", "supranacional": "Supranacional",
    "supra": "Supranacional",
    "cash": "Liquidez y monetarios", "liquidez": "Liquidez y monetarios", "money market": "Liquidez y monetarios",
    "monetario": "Liquidez y monetarios", "liquidez y monetarios": "Liquidez y monetarios",
    "funds": "Fondos", "fondos": "Fondos", "fund": "Fondos", "etf": "Fondos", "etfs": "Fondos",
    "abs": "Titulizaciones", "mbs": "Titulizaciones", "clo": "Titulizaciones", "rmbs": "Titulizaciones",
    "cmbs": "Titulizaciones", "securitized": "Titulizaciones", "securitised": "Titulizaciones",
    "titulizaciones": "Titulizaciones", "titulización": "Titulizaciones", "titulizacion": "Titulizaciones",
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
    # Bonos (1-oct-2026): la clave es el EMISOR → fuera cupón, vencimiento y etiquetas del instrumento
    # ("Grifols SA 'REGS' 3.875% 15-Oct-2028" → "grifols"), así todos sus bonos comparten sector.
    s = re.sub(r"\d+(?:[.,]\d+)?\s*%", " ", s)
    s = re.sub(r"\b\d{1,2}[-/ ](?:[a-z]{3}|\d{1,2})[-/ ]\d{2,4}\b", " ", s)
    s = re.sub(r"\b(?:19|20)\d{2}\b", " ", s)
    s = re.sub(r"\b(?:regs|reg s|144a|frn|perp|perpetual|var|float(?:ing)?|fixed|sr|snr|sub|unsec|secured|notes?|bonds?|"
               r"debentures?|mtn|emtn|callable|step[- ]?up|zero|coupon|due|finco|bidco|topco|midco|opco|issuer|gmb h|gmbh)\b", " ", s)
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
        nm = re.sub(r"(?<=[A-Za-z])(ASA|SpA|OYJ|GmbH|KGaA|Abp|ADR|PLC|Ltd|SA|SE|AS|AG|NV)(?=$|\s|\.|,)", r" \1", nm)
        nm = re.sub(r"(?<=[A-Za-z])(ADR)(?=$|\s|\.|,)", r" \1", nm)  # 'PLCADR'->'PLC ADR'
        # quitar marcadores que no son parte del nombre (ADR/ADS/Reg. S) al final
        nm = re.sub(r"\s*\b(ADR|ADS|Reg\.?\s*S)\b\.?\s*$", "", nm, flags=re.I).strip()
        # re-espaciar CamelCase pegado en CUALQUIER palabra (aunque ya haya espacios,
        # p.ej. 'ASMLHolding NV'->'ASML Holding NV', 'AirLiquide SA'->'Air Liquide SA')
        if re.search(r"[a-z][A-Z]|[A-Z]{2,}[a-z]", nm):
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


def relevant_positions(positions: list, top_n: int = 50, min_weight: float = 1.0) -> list:
    """Posiciones que MERECEN clasificación de sector: las `top_n` mayores por peso
    + cualquiera con peso >= `min_weight`% (alta convicción) aunque esté más abajo.
    La cola diminuta (pos. 50+ y poco peso) se deja en blanco a propósito — no
    perder tiempo clasificándola (regla Rafa 2026-06-11)."""
    pos = [p for p in (positions or []) if isinstance(p, dict)]
    by_w = sorted(pos, key=lambda p: p.get("peso_pct") or 0, reverse=True)
    rel = list(by_w[:top_n])
    ids = {id(p) for p in rel}
    for p in by_w[top_n:]:
        if (p.get("peso_pct") or 0) >= min_weight and id(p) not in ids:
            rel.append(p)
    return rel


def unknown_companies(positions: list, cache: dict | None = None,
                      only_relevant: bool = True) -> list:
    """Nombres (originales) de posiciones sin sector canónico ni en caché.
    Por defecto SOLO entre las posiciones relevantes (top-50 + peso alto)."""
    cache = cache if cache is not None else load_cache()
    pool = relevant_positions(positions) if only_relevant else (positions or [])
    out, seen = [], set()
    for p in pool:
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


def _load_json(p) -> dict:
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _guardar_resultado(mapping) -> int:
    """{nombre: sector} o {nombre: {sector, pais_riesgo}} → cachés de sector y de país de riesgo."""
    if not isinstance(mapping, dict):
        return 0
    sect, ctry = {}, _load_json(COUNTRY_CACHE_PATH)
    for nm, v in mapping.items():
        if isinstance(v, dict):
            if v.get("sector"):
                sect[nm] = v["sector"]
            if v.get("pais_riesgo") and _norm_company(nm):
                ctry[_norm_company(nm)] = str(v["pais_riesgo"]).strip()
        elif isinstance(v, str):
            sect[nm] = v
    COUNTRY_CACHE_PATH.write_text(json.dumps(ctry, ensure_ascii=False, indent=1, sort_keys=True), encoding="utf-8")
    return add_classifications(sect)


def _apply_countries(positions: list, ccache: dict) -> None:
    for p in positions or []:
        if isinstance(p, dict) and not p.get("pais_riesgo"):
            c = ccache.get(_norm_company(p.get("nombre", "")))
            if c:
                p["pais_riesgo"] = c


def _posiciones_todas(o: dict) -> list:
    """Posiciones de la cartera actual y de los años anteriores (para la evolución por sector)."""
    pos = list(((o.get("posiciones") or {}).get("actuales")) or [])
    for h in ((o.get("posiciones") or {}).get("historicas")) or []:
        if isinstance(h, dict):
            pos += list(h.get("todas") or h.get("holdings") or h.get("top10") or h.get("posiciones") or [])
    return [x for x in pos if isinstance(x, dict) and x.get("nombre")]


def classify_auto(isin: str, model: str | None = None, log=print) -> dict:
    """Clasifica con Claude los emisores sin sector del fondo, cachea y aplica. Idempotente y barato
    (solo los que no están en la caché global)."""
    import os as _os
    import subprocess as _sp
    isin = isin.strip().upper()
    fd = FUNDS_DIR / isin
    op = fd / "output.json"
    if not op.exists():
        return {"ok": False, "motivo": "sin output.json"}
    o = json.loads(op.read_text(encoding="utf-8"))
    pos = _posiciones_todas(o)
    for x in pos:
        clean_positions([x])
    cache = load_cache()
    ccache = _load_json(COUNTRY_CACHE_PATH)
    unk = unknown_companies(pos, cache, only_relevant=False)
    vistos = {_norm_company(u) for u in unk}
    for x in pos:            # también los que tienen sector pero aún no país de riesgo
        k = _norm_company(x.get("nombre", ""))
        if k and k not in ccache and k not in vistos:
            vistos.add(k); unk.append(x["nombre"])
    res = {"ok": True, "pendientes": len(unk), "clasificados": 0}
    if unk:
        pend = fd / "_sectores_pendientes.json"
        outf = fd / "_sectores_clasificados.json"
        outf.unlink(missing_ok=True)
        pend.write_text(json.dumps({"isin": isin, "fondo": o.get("nombre"), "emisores": unk,
                                    "sectores": CANONICAL_SECTORS}, ensure_ascii=False, indent=1), encoding="utf-8")
        prompt = (
            f"Clasificación de sectores para un análisis de fondo. Lee el fichero data/funds/{isin}/_sectores_pendientes.json: "
            "trae 'emisores' (nombres de posiciones de la cartera: acciones o bonos) y la lista cerrada 'sectores'. "
            "Para CADA emisor decide (1) el sector de la empresa u organismo (en un bono, el del GRUPO que lo emite: un "
            "vehículo 'Bidco', 'Finco', 'Topco', 'Lux Sarl' o 'BV' se clasifica por el negocio del grupo al que pertenece; "
            "quita del nombre cupones, vencimientos y tipo de instrumento) y (2) su país de RIESGO: el país donde está el "
            "negocio o la sede del grupo, NO el domicilio del vehículo emisor. Bonos de estados, tesoros, agencias públicas y "
            "administraciones regionales o municipales → 'Gobierno' (país = ese estado). Organismos multilaterales (BEI/EIB, "
            "Banco Mundial/IBRD, ESM, BAD, AIIB…) → 'Supranacional' (país 'Supranacional'). ABS/MBS/CLO → 'Titulizaciones'. "
            "Liquidez, repos, depósitos y fondos monetarios → 'Liquidez y monetarios'; otros fondos y ETFs → 'Fondos' "
            "(país 'Liquidez y fondos'). Derivados → 'Otros'. Si de verdad no reconoces la empresa, 'Otros'. "
            f"Escribe SOLO un JSON en data/funds/{isin}/_sectores_clasificados.json con la forma "
            "{\"nombre exacto\": {\"sector\": \"sector de la lista\", \"pais_riesgo\": \"país en español\"}} con TODOS los "
            "emisores. No escribas nada más ni preguntes."
        ).replace('\\"', '"')
        logf = ROOT / "logs" / f"skill_sectores_{isin}.log"
        rc = _sp.call([sys.executable, "-m", "tools.claude_cowork", str(logf), prompt,
                       "--model", model or _os.environ.get("MODEL_EXTRACT", "claude-sonnet-5"),
                       "--allowedTools", "Read,Write"], cwd=str(ROOT))
        try:
            mapping = json.loads(outf.read_text(encoding="utf-8"))
        except Exception:
            mapping = {}
            log(f"[SECTORES] {isin}: sin resultado de la clasificación (rc={rc}); ver {logf.name}")
        res["clasificados"] = _guardar_resultado(mapping)
        pend.unlink(missing_ok=True)
        outf.unlink(missing_ok=True)
        cache = load_cache()
        ccache = _load_json(COUNTRY_CACHE_PATH)
        # 2ª pasada CON WEB para lo que siga en 'Otros' con peso relevante (vehículos opacos)
        opacos = []
        for x in pos:
            k = _norm_company(x.get("nombre", ""))
            if cache.get(k) == "Otros" and (x.get("peso_pct") or 0) >= 0.3 and \
                    str(x.get("tipo") or "").lower() not in ("fondo", "liquidez", "cash", "future", "swap", "forward", "option") \
                    and x["nombre"] not in opacos:
                opacos.append(x["nombre"])
        if opacos:
            pend.write_text(json.dumps({"isin": isin, "fondo": o.get("nombre"), "emisores": opacos,
                                        "sectores": CANONICAL_SECTORS}, ensure_ascii=False, indent=1), encoding="utf-8")
            prompt2 = (prompt + " Estos emisores no se pudieron identificar sin buscar: usa la búsqueda web para averiguar "
                       "a qué grupo pertenece cada vehículo y a qué se dedica. Si tras buscar sigues sin saberlo, 'Otros'.")
            _sp.call([sys.executable, "-m", "tools.claude_cowork", str(logf).replace(".log", "_web.log"), prompt2,
                      "--model", model or _os.environ.get("MODEL_EXTRACT", "claude-sonnet-5"),
                      "--allowedTools", "Read,Write,WebSearch,WebFetch"], cwd=str(ROOT))
            try:
                res["identificados_web"] = _guardar_resultado(json.loads(outf.read_text(encoding="utf-8")))
            except Exception:
                res["identificados_web"] = 0
            pend.unlink(missing_ok=True)
            outf.unlink(missing_ok=True)
            cache = load_cache()
            ccache = _load_json(COUNTRY_CACHE_PATH)
    # aplicar a output.json (cartera actual + años anteriores)
    act = ((o.get("posiciones") or {}).get("actuales")) or []
    n_set, n_unk = apply_sectors(act, cache)
    ccache = _load_json(COUNTRY_CACHE_PATH)
    _apply_countries(act, ccache)
    for h in ((o.get("posiciones") or {}).get("historicas")) or []:
        if isinstance(h, dict):
            _rows = h.get("todas") or h.get("holdings") or h.get("top10") or []
            apply_sectors(_rows, cache)
            _apply_countries(_rows, ccache)
    tmp = op.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(o, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(op)
    res.update({"con_sector": n_set, "sin_sector": n_unk})
    try:
        from tools.build_cartera_breakdowns import build_for, apply_to_output
        apply_to_output(isin, build_for(isin), overwrite=True)
    except Exception as e:  # noqa: BLE001
        log(f"[SECTORES] {isin}: desgloses de cartera no rehechos: {str(e)[:100]}")
    try:
        _sp.run([sys.executable, str(ROOT / "dashboard" / "generate_dashboard.py"), isin], cwd=str(ROOT),
                capture_output=True, text=True, timeout=300)
    except Exception:
        pass
    log(f"[SECTORES] {isin}: {res}")
    return res


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Clasificador de sectores (caché global)")
    ap.add_argument("--report", action="store_true", help="cobertura del caché en todo el catálogo")
    ap.add_argument("--unknowns", help="lista empresas sin clasificar de un ISIN")
    ap.add_argument("--auto", help="clasifica con Claude los emisores sin sector del ISIN, cachea y aplica")
    args = ap.parse_args(argv)
    if args.auto:
        r = classify_auto(args.auto)
        return 0 if r.get("ok") else 1
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
