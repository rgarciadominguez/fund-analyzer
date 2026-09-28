"""
build_cartera_breakdowns.py — Sectores y geografía agregados DESDE las posiciones.

La mejor info posible sin depender de MyInvestor (conector caído) ni de la SAL de Morningstar
(bloqueada): las posiciones reales del fondo (CNMV cartera de valores / AR top holdings) traen
`pais` (58/60 fondos, también en el histórico) y `sector` (30/60, solo actual), con `peso_pct`.

Alimenta exactamente las claves que el dashboard renderiza:
  - SECTOR (snapshot actual) → analisis_cuantitativo.sectores = [{sector, peso_pct}]
    → lo lee build_quant_panel (gráfico de sector de la pestaña Cartera).
  - GEOGRAFÍA (serie multi-año) → geographic_allocation_history (top-level)
    = [{periodo, zonas:{region:pct}}], agregando cada periodo de posiciones.historicas por
    país→región. El gráfico de evolución exige ≥2 periodos → se construye del histórico real,
    no de un único snapshot.

Regla: FILL-IF-EMPTY (no pisa datos ya buenos) + "mejor vacío que inventado" (países genéricos
tipo "Internacional" se descartan; si no queda geografía real suficiente, no se produce nada).

CLI:
    python -m tools.build_cartera_breakdowns --isin ES0112231008
    python -m tools.build_cartera_breakdowns --all [--apply]   (--apply escribe output.json)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# país (como sale en CNMV/AR) → región para el gráfico de geografía. Best-effort; lo que no
# mapee cae en "Otros" (no se inventa).
_PAIS_REGION = {
    "españa": "Europa", "spain": "Europa", "francia": "Europa", "france": "Europa",
    "alemania": "Europa", "germany": "Europa", "italia": "Europa", "italy": "Europa",
    "reino unido": "Europa", "united kingdom": "Europa", "uk": "Europa", "gran bretaña": "Europa",
    "países bajos": "Europa", "paises bajos": "Europa", "netherlands": "Europa", "holanda": "Europa",
    "suiza": "Europa", "switzerland": "Europa", "españa/francia": "Europa",
    "bélgica": "Europa", "belgica": "Europa", "belgium": "Europa", "austria": "Europa",
    "irlanda": "Europa", "ireland": "Europa", "portugal": "Europa", "luxemburgo": "Europa",
    # Nórdicos aparte de Europa (Rafa, 28-sep-2026): el high yield nórdico se comporta distinto.
    "suecia": "Nórdicos", "sweden": "Nórdicos", "dinamarca": "Nórdicos", "denmark": "Nórdicos",
    "noruega": "Nórdicos", "norway": "Nórdicos", "finlandia": "Nórdicos", "finland": "Nórdicos",
    "islandia": "Nórdicos", "iceland": "Nórdicos", "nórdicos": "Nórdicos", "nordics": "Nórdicos",
    "nordic": "Nórdicos", "escandinavia": "Nórdicos", "scandinavia": "Nórdicos",
    "estados unidos": "USA", "united states": "USA", "usa": "USA", "eeuu": "USA", "u.s.a.": "USA",
    "canadá": "Norteamérica", "canada": "Norteamérica",
    "japón": "Japón", "japon": "Japan", "japan": "Japón",
    "china": "Asia emergente", "corea": "Asia emergente", "corea del sur": "Asia emergente",
    "korea": "Asia emergente", "taiwán": "Asia emergente", "taiwan": "Asia emergente",
    "india": "Asia emergente", "hong kong": "Asia emergente", "indonesia": "Asia emergente",
    "brasil": "Latinoamérica", "brazil": "Latinoamérica", "méxico": "Latinoamérica",
    "mexico": "Latinoamérica", "chile": "Latinoamérica",
    "australia": "Pacífico", "nueva zelanda": "Pacífico",
    "supranacional": "Supranacional", "supranational": "Supranacional",
}


# Valores de país GENÉRICOS que NO son geografía real (no sirven para el gráfico).
_PAIS_GENERICO = {"internacional", "varios", "global", "diversos", "diversificado", "n/a",
                  "na", "-", "", "otros", "no especificado", "mundial", "world"}

# Prefijo ISIN del VALOR (país de emisión, ISO 3166 alpha-2) → región. Fallback cuando la
# posición no trae país real. XS/EU (eurobonos, supranacional) NO son país → no se resuelven.
_ISIN_PREFIX_REGION = {
    "US": "USA", "CA": "Norteamérica",
    "JP": "Japón",
    "AU": "Pacífico", "NZ": "Pacífico",
    "BR": "Latinoamérica", "MX": "Latinoamérica", "CL": "Latinoamérica",
    "AR": "Latinoamérica", "CO": "Latinoamérica", "PE": "Latinoamérica",
    "CN": "Asia emergente", "KR": "Asia emergente", "TW": "Asia emergente",
    "IN": "Asia emergente", "HK": "Asia emergente", "ID": "Asia emergente",
    "TH": "Asia emergente", "MY": "Asia emergente", "SG": "Asia emergente", "PH": "Asia emergente",
    "ZA": "África/Oriente Medio", "IL": "África/Oriente Medio",
    "AE": "África/Oriente Medio", "SA": "África/Oriente Medio",
}
# Europa (mismo destino de región): países europeos por prefijo ISIN.
for _c in ("ES", "FR", "DE", "IT", "GB", "NL", "CH", "BE", "AT", "IE", "PT", "LU",
           "PL", "GR", "CZ", "HU", "LI", "RU", "TR"):
    _ISIN_PREFIX_REGION[_c] = "Europa"
for _c in ("SE", "DK", "NO", "FI", "IS"):
    _ISIN_PREFIX_REGION[_c] = "Nórdicos"

# tipos de posición donde el ISIN es el DOMICILIO del vehículo, no la geografía subyacente
# (fondos, ETFs, participaciones) → no usar el prefijo ISIN para geografía.
_TIPOS_VEHICULO = {"fondo", "fondos", "etf", "etn", "participaciones", "sicav", "ic", "iic"}

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def _region(pais: str) -> str:
    return _PAIS_REGION.get((pais or "").strip().lower(), "Otros")


def _region_from_isin(pos: dict):
    """Región deducida del prefijo del ISIN del VALOR (ticker/isin), solo si la posición
    NO es un vehículo (fondo/ETF) y el prefijo es un país real (no XS). None si no aplica."""
    tipo = str(pos.get("tipo", "")).strip().lower()
    if tipo in _TIPOS_VEHICULO:
        return None
    for campo in ("ticker", "isin"):
        val = str(pos.get(campo) or "").strip().upper()
        if _ISIN_RE.match(val):
            return _ISIN_PREFIX_REGION.get(val[:2])
    return None


def _agg(pos: list, key: str) -> list:
    """Agrega posiciones por `key` (sector/pais) ponderando por peso_pct. Devuelve
    [{key, peso_pct}] ordenado desc. Ignora posiciones sin ese campo."""
    acc = defaultdict(float)
    for p in pos:
        if not isinstance(p, dict):
            continue
        k = (p.get(key) or "").strip()
        w = p.get("peso_pct")
        if not k or not isinstance(w, (int, float)):
            continue
        acc[k] += float(w)
    out = [{key: k, "peso_pct": round(v, 2)} for k, v in acc.items() if v > 0]
    out.sort(key=lambda x: -x["peso_pct"])
    return out


def _zonas_from_positions(pos: list) -> dict:
    """Agrega posiciones por región (ponderado). Región por posición:
       1) país real de la posición (no genérico), si lo trae;
       2) si no, prefijo ISIN del valor (país de emisión) — salvo vehículos (fondo/ETF) y XS.
    Devuelve {region: pct} o {} si <25% del peso quedó geolocalizado de forma fiable."""
    reg = defaultdict(float)
    geoloc = 0.0
    for p in pos:
        if not isinstance(p, dict):
            continue
        w = p.get("peso_pct")
        if not isinstance(w, (int, float)) or w <= 0:
            continue
        pais = (p.get("pais") or "").strip()
        if pais and pais.lower() not in _PAIS_GENERICO:
            reg[_region(pais)] += w
            geoloc += w
        else:
            r = _region_from_isin(p)          # fallback: prefijo ISIN del valor
            if r:
                reg[r] += w
                geoloc += w
    if geoloc < 25:
        return {}
    zonas = {k: round(v, 2) for k, v in sorted(reg.items(), key=lambda x: -x[1])}
    if len(zonas) == 1 and "Otros" in zonas:
        return {}
    return zonas


def position_coverage(isin: str):
    """Suma de `peso_pct` de las posiciones actuales (% del patrimonio cubierto por la
    cartera extraída). None si no hay posiciones. Sirve de guard: <50% cartera parcial,
    <75% incompleta. Para ES viene completa de CNMV; para INT depende de que el AR traiga
    el 'Securities Portfolio' completo (no solo el top-10)."""
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return None
    d = json.loads(p.read_text(encoding="utf-8"))
    pos = (d.get("posiciones", {}) or {}).get("actuales") or []
    if not pos:
        return None
    tot = sum((x.get("peso_pct") or 0) for x in pos if isinstance(x, dict))
    return round(tot, 1)


def build_for(isin: str) -> dict:
    """Devuelve {sectores, geo_history} desde output.json. {} si no hay posiciones útiles.
       - sectores: snapshot actual [{sector, peso_pct}]  (posiciones.actuales)
       - geo_history: serie [{periodo, zonas:{region:pct}}] de posiciones.historicas (≥2)."""
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return {}
    d = json.loads(p.read_text(encoding="utf-8"))
    posw = d.get("posiciones", {}) or {}
    out = {}

    # 1) Sectores — snapshot actual
    sect = _agg(posw.get("actuales") or [], "sector")
    if sect:
        out["sectores"] = [{"sector": s["sector"], "peso_pct": s["peso_pct"], "_fuente": "cartera"}
                           for s in sect[:15]]

    # 2) Geografía — serie multi-año de posiciones.historicas (país→región por periodo).
    #    Usa `todas` (cartera completa) si está; si no, `top10`. Añade el periodo ACTUAL
    #    (posiciones.actuales) como último punto si tiene geografía real.
    geo = []
    for entry in posw.get("historicas") or []:
        per = entry.get("periodo")
        top = entry.get("todas") or entry.get("top10") or entry.get("posiciones") or []
        if not per or not top:
            continue
        zonas = _zonas_from_positions(top)
        if zonas:
            geo.append({"periodo": str(per)[:7], "zonas": zonas, "_fuente": "cartera"})
    # periodo actual como punto adicional
    z_act = _zonas_from_positions(posw.get("actuales") or [])
    if z_act:
        serie = (d.get("cuantitativo", {}) or {}).get("serie_aum") or []
        per_act = str(serie[-1].get("periodo", "actual"))[:7] if serie else "actual"
        if per_act not in {g["periodo"] for g in geo}:
            geo.append({"periodo": per_act, "zonas": z_act, "_fuente": "cartera"})
    # dedup por periodo (quedarse con el último) + ordenar
    dedup = {}
    for g in geo:
        dedup[g["periodo"]] = g
    geo = sorted(dedup.values(), key=lambda g: g["periodo"])
    if len(geo) >= 2:                       # el gráfico de evolución exige ≥2 periodos
        out["geo_history"] = geo
    return out


def apply_to_output(isin: str, bd: dict, overwrite: bool = False) -> list:
    """Escribe en output.json. Por defecto fill-if-empty; con overwrite=True (modo update
    anual) REFRESCA los gráficos de cartera con el año nuevo (geografía siempre; sectores
    solo si hay dato nuevo, sin dejar el previo). Devuelve lista de campos escritos.
    También limpia claves muertas de una versión previa del tool."""
    p = ROOT / "data" / "funds" / isin / "output.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    written = []
    aq = d.get("analisis_cuantitativo")
    if not isinstance(aq, dict):
        aq = {}
        d["analisis_cuantitativo"] = aq
    cu = d.setdefault("cuantitativo", {})

    # limpieza de claves muertas de la iteración previa
    if aq.pop("geografia_pais", None) is not None:
        written.append("-analisis_cuantitativo.geografia_pais")
    mgh = cu.get("mix_geografico_historico")
    if isinstance(mgh, list) and mgh and all(isinstance(e, dict) and e.get("_fuente") == "cartera" for e in mgh):
        cu.pop("mix_geografico_historico", None)
        written.append("-cuantitativo.mix_geografico_historico")

    # Sectores: fill-if-empty, o refresco con dato nuevo en modo overwrite (no lo blanquea).
    if bd.get("sectores") and (overwrite or not aq.get("sectores")):
        aq["sectores"] = bd["sectores"]
        written.append("analisis_cuantitativo.sectores")
    # Geografía: escribir si está vacía, O mejorar la que YO mismo escribí antes
    # (_fuente="cartera"); en overwrite SIEMPRE refresca (año nuevo → posiciones nuevas).
    if bd.get("geo_history"):
        cur = d.get("geographic_allocation_history") or []
        es_mia = cur and all(isinstance(e, dict) and e.get("_fuente") == "cartera" for e in cur)
        if overwrite or not cur or es_mia:
            d["geographic_allocation_history"] = bd["geo_history"]
            written.append("geographic_allocation_history")
    if written:
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return written


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if a.isin:
        bd = build_for(a.isin)
        print(json.dumps(bd, ensure_ascii=False, indent=1)[:1500])
        if a.apply:
            print("escrito:", apply_to_output(a.isin, bd))
        return

    import glob
    # ISIN real = 12 alfanuméricos; los dirs de backup llevan sufijo ".bak_..." → se excluyen.
    isins = [Path(f).parent.name for f in glob.glob(str(ROOT / "data" / "funds" / "*" / "output.json"))
             if "." not in Path(f).parent.name]
    n_sect = n_geo = n_any = 0
    for isin in isins:
        bd = build_for(isin)
        if bd.get("sectores"):
            n_sect += 1
        if bd.get("geo_history"):
            n_geo += 1
        if a.apply:
            w = apply_to_output(isin, bd)
            if w:
                n_any += 1
    print(f"con sectores derivables: {n_sect} | con geografía (serie ≥2 periodos) derivable: {n_geo}")
    if a.apply:
        print(f"output.json modificados: {n_any}")


if __name__ == "__main__":
    main()
