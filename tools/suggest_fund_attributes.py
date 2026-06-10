"""suggest_fund_attributes.py — Sugiere tipo_activo / geografia / estilo para los
fund_groups, de forma DETERMINISTA (reglas, sin API), a partir de:
  - nombre_base + gestora (de fund_groups, siempre disponible)
  - kpis.clasificacion + sector/geographic_allocation + texto del análisis
    (de data/funds/{ISIN}/output.json, si existe)

Diseño (2026-06-09, petición Rafa):
  - SUGERENCIA editable: escribe SOLO campos vacíos (fill-if-empty). Nunca pisa
    lo que ya hay (marcado manual o sugerido antes).
  - Valores acotados a los válidos del catálogo:
      tipo_activo ∈ {RV, RF, Mixtos, Materias_primas, Alternativos}
      geografia   ∈ {Europa, Nordics, Global, Developed, USA, Emerging, Pacifico, España}
      estilo      = texto libre (Value, Growth, Quality, SmallCaps, Floating rate, ...)
  - Ante la duda, deja el campo en None (mejor vacío que mal — el usuario lo edita).

CLI:
    python -m tools.suggest_fund_attributes            # DRY-RUN (no escribe)
    python -m tools.suggest_fund_attributes --apply    # escribe fill-if-empty en Supabase
    python -m tools.suggest_fund_attributes --isin ES0159259011   # un fondo
"""
from __future__ import annotations

import argparse
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

# ── Gestoras value reconocidas (señal fuerte de estilo) ───────────────────────
_VALUE_GESTORAS = (
    "magallanes", "cobas", "azvalor", "az valor", "horos", "cartesio", "true value",
    "bestinver", "metagestion", "metavalor", "valentum", "sigma", "boston partners",
    "amiral", "moerus", "equam", "numantia",
)
_QUALITY_HINTS = ("quality", "calidad", "compounders", "high-quality", "high quality",
                  "moat", "wide moat", "negocios de calidad", "nordic quality")
_GROWTH_HINTS = ("growth", "crecimiento", "disruption", "innovation", "tecnológic")
_VALUE_HINTS = ("value", "valor intrínseco", "deep value", "infravalorad", "descuento",
                "contrarian", "margen de seguridad")


def _norm_geo(text: str) -> str | None:
    t = text.lower()
    # Orden = prioridad (más específico primero)
    if re.search(r"\bib[eé]ric|iberia|espa[ñn]a|spanish|spain\b", t):
        return "España"
    if re.search(r"\bnordic|n[oó]rdic", t):
        return "Nordics"
    if re.search(r"emerging|emergent|\bindia\b|\bchina\b|latam|latinoam|brazil|brasil|asia ex|frontier|mercados emergentes", t):
        return "Emerging"
    if re.search(r"\busa\b|\bu\.?s\.?\b|ee\.?uu|estados unidos|american|s&p ?500|norteam|north america", t):
        return "USA"
    if re.search(r"\bjap[oó]n|japan|pacific|pac[ií]fico|asia\b", t):
        return "Pacifico"
    if re.search(r"europ", t):
        return "Europa"
    if re.search(r"global|world|mundial|internacional|international|worldwide", t):
        return "Global"
    if re.search(r"developed|desarrollad", t):
        return "Developed"
    return None


def _norm_tipo(text: str, clasif: str, has_sectors: bool) -> str | None:
    t = (text + " " + clasif).lower()
    if re.search(r"materias primas|commodit|\boro\b|\bgold\b|metales preciosos", t):
        return "Materias_primas"
    if re.search(r"retorno absoluto|absolute return|market neutral|long/short|long short|market-neutral|alternativ|hedge fund|event driven", t):
        return "Alternativos"
    if re.search(r"\bmixto|mixed|multiactivo|multi-asset|multiasset|balanced|asignaci[oó]n de activos|allocation|patrimonio global|flexible.*(renta|asset)", t):
        return "Mixtos"
    if re.search(r"renta fija|fixed income|\bbond|\bbonos|credit|cr[eé]dito|deuda|monetar|money market|treasury|high yield|investment grade|short.?term|ultra.?short|floating rate|aggregate", t):
        return "RF"
    if has_sectors or re.search(r"renta variable|\bequit|equities|\bacciones|\bstock|\bRV\b|large.?cap|small.?cap|micro.?cap", t):
        return "RV"
    return None


def _norm_estilo(nombre: str, analysis: str, gestora: str, tipo: str | None) -> str | None:
    """Tags ESPECIALES → solo desde el NOMBRE del fondo (evita falsos positivos
    por menciones genéricas en el cuerpo del análisis, p.ej. AzValor Managers que
    cita 'small caps' pero es value multi-gestor). Estilo de RV (Value/Growth/
    Quality) → gestora + lenguaje del análisis."""
    # SUB-fondo = parte tras " - " (evita que el nombre del PARAGUAS, p.ej.
    # "Robeco CAPITAL GROWTH FUNDS - ..." o "DNCA Invest - ...", marque estilo).
    sub = nombre.split(" - ")[-1].lower() if " - " in nombre else nombre.lower()
    n = nombre.lower()
    a = analysis.lower()
    g = gestora.lower()
    # Tags especiales por nombre del sub-fondo
    if re.search(r"floating rate|tipo flotante|\bfrn\b", n):
        return "Floating rate"
    if re.search(r"retorno absoluto|absolute return|market neutral", n):
        return "Retorno absoluto"
    if re.search(r"small.?cap|microcap|micro.?cap|micro.?caps", sub):
        return "SmallCaps"
    if re.search(r"sector financ|financials sector", sub):
        return "Sector financiero"
    # Pasivos / cuánticos: NO asignar estilo activo (Value/Growth/Quality).
    if re.search(r"[ií]ndice|enhanced index|\bindex\b|tracker|etf", sub):
        return "Indexado"
    if re.search(r"\bqi\b|quant|momentum|smart beta|factor", sub):
        # Robeco QI / quant / momentum → factor, no value/growth/quality
        return "Momentum" if "momentum" in sub else None
    # Estilo de RV (gestión activa). El SUB-fondo manda sobre la gestora:
    # p.ej. "Sextant Quality Focus" (Amiral, casa value) → Quality, no Value.
    if tipo == "RV":
        if "quality" in sub:
            return "Quality"
        if "growth" in sub or "crecimiento" in sub:
            return "Growth"
        if any(v in g for v in _VALUE_GESTORAS):
            return "Value"
        if any(h in a for h in _QUALITY_HINTS):
            return "Quality"
        if any(h in a for h in _VALUE_HINTS):
            return "Value"
        if any(h in a for h in _GROWTH_HINTS):
            return "Growth"
    return None


def _load_output(isin: str) -> dict:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def suggest_for(isin: str, nombre: str, gestora: str) -> dict:
    """Devuelve {tipo_activo, geografia, estilo} sugeridos (None donde no hay señal)."""
    out = _load_output(isin)
    kpis = out.get("kpis", {}) if isinstance(out, dict) else {}
    clasif = str(kpis.get("clasificacion") or "")
    a = out.get("analyst_synthesis", {}) if isinstance(out, dict) else {}
    analysis_txt = ""
    if isinstance(a, dict):
        for s in ("resumen", "estrategia", "cartera"):
            node = a.get(s, {})
            if isinstance(node, dict):
                analysis_txt += " " + (node.get("texto", "") or "")[:2000]
    has_sectors = bool(out.get("sector_allocation")) if isinstance(out, dict) else False

    # geografia: preferir geographic_allocation dominante si existe
    geo = None
    galloc = out.get("geographic_allocation") if isinstance(out, dict) else None
    if isinstance(galloc, list) and galloc:
        top = max(galloc, key=lambda r: r.get("peso_pct") or 0)
        geo = _norm_geo(str(top.get("region", "")))
    if not geo:
        geo = _norm_geo(nombre) or _norm_geo(clasif) or _norm_geo(analysis_txt[:500])

    name_clasif = f"{nombre} {clasif}"
    tipo = _norm_tipo(name_clasif, "", has_sectors) or _norm_tipo(analysis_txt[:800], clasif, has_sectors)
    estilo = _norm_estilo(nombre, analysis_txt[:1500], gestora, tipo)
    return {"tipo_activo": tipo, "geografia": geo, "estilo": estilo}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Sugiere tipo_activo/geografia/estilo (fill-if-empty)")
    ap.add_argument("--apply", action="store_true", help="Escribe en Supabase (default: dry-run)")
    ap.add_argument("--isin", help="Solo este ISIN")
    args = ap.parse_args(argv)

    from tools.supabase_client import get_client
    c = get_client()
    funds = c.table("funds").select("isin,fund_group_id").limit(2000).execute().data
    groups = {g["fund_group_id"]: g for g in
              c.table("fund_groups").select("fund_group_id,nombre_base,gestora,tipo_activo,geografia,estilo").limit(2000).execute().data}

    rows = []
    for f in funds:
        isin = f["isin"]
        if args.isin and isin != args.isin:
            continue
        g = groups.get(f["fund_group_id"])
        if not g:
            continue
        prop = suggest_for(isin, g.get("nombre_base") or "", g.get("gestora") or "")
        # fill-if-empty
        upd = {k: v for k, v in prop.items() if v and not g.get(k)}
        rows.append((isin, g.get("nombre_base", "")[:34], g, prop, upd))

    n_fill = {"tipo_activo": 0, "geografia": 0, "estilo": 0}
    applied = 0
    print(f"{'ISIN':13} {'nombre':34} {'tipo':6} {'geo':9} {'estilo':14} (vacío→relleno)")
    for isin, nb, g, prop, upd in rows:
        if not upd:
            continue
        for k in upd:
            n_fill[k] += 1
        marks = " ".join(f"{k}={upd[k]}" for k in upd)
        print(f"{isin:13} {nb:34} {str(prop['tipo_activo'] or '-'):6} {str(prop['geografia'] or '-'):9} {str(prop['estilo'] or '-'):14} -> {marks}")
        if args.apply:
            try:
                c.table("fund_groups").update(upd).eq("fund_group_id", g["fund_group_id"]).execute()
                applied += 1
            except Exception as e:
                print(f"   [ERROR] {isin}: {str(e)[:100]}")

    print(f"\nRellenos propuestos: tipo_activo={n_fill['tipo_activo']}, geografia={n_fill['geografia']}, estilo={n_fill['estilo']}")
    print(f"{'APLICADO' if args.apply else 'DRY-RUN (usa --apply para escribir)'}: {applied} grupos actualizados" if args.apply else "DRY-RUN — nada escrito. Usa --apply para aplicar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
