"""build_historical_series.py — Puebla el HISTÓRICO multi-año de un fondo INT desde los
extracts por-año del Annual Report (`extracted/*annual_subfund*.json`, uno por año).

Problema: el `_consume_extracted` del pipeline solo conserva el año MÁS RECIENTE en
`posiciones.actuales` (gana por prioridad) → los años anteriores (que SÍ se extraen, ~100
holdings cada uno) se descartan. El analyst tiene el motor de consistencia / cambios
estructurales (compara `posiciones.historicas[year-1]` vs `[year]`) pero se queda sin datos.

Este módulo recupera esos años y estructura:
  - `posiciones.historicas`     : [{periodo, top10, holdings, num_posiciones, aum_meur}]  (1/año)
  - `cuantitativo.mix_activos_historico`   : [{periodo, renta_variable_pct, renta_fija_pct, liquidez_pct, otros_pct}]
  - `cuantitativo.mix_geografico_historico`: [{periodo, zonas:{region:pct}}]
  - `cuantitativo.serie_rentabilidad`      : acumulada de las tablas `performance`
  - `cuantitativo.serie_aum`               : acumulada de `statistics` (cada AR trae 3 años)

Upsert por `periodo` (idempotente). Para el MISMO año, el AR gana sobre el SAR (semestral).
Datos DERIVADOS (regenerables) → no marca `_manual_edits`. Best-effort, nunca lanza en apply.

CLI:
    python -m tools.build_historical_series LU0203975437            # dry-run (muestra)
    python -m tools.build_historical_series LU0203975437 --apply    # escribe output.json
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _year_of(name: str, data: dict) -> str | None:
    """Periodo del extract: `data.periodo` si es YYYY[-Hx]; si no, del nombre de fichero."""
    per = str(data.get("periodo") or "").strip()
    if re.match(r"^\d{4}(-H[12])?$", per):
        return per
    m = re.search(r"(\d{4})", name)
    return m.group(1) if m else None


def _is_ar(name: str) -> bool:
    return "semi_annual" not in name.lower()


def _norm_geo(geo: list) -> dict:
    """{region: peso_pct} normalizado y agregado por país (USA=Estados Unidos...)."""
    if not isinstance(geo, list):
        return {}
    try:
        from tools.region_normalizer import aggregate_by_country
        return aggregate_by_country({it.get("region"): it.get("peso_pct")
                                     for it in geo if isinstance(it, dict) and it.get("region")})
    except Exception:
        out = {}
        for it in geo:
            if isinstance(it, dict) and it.get("region"):
                out[it["region"]] = it.get("peso_pct")
        return out


# Clasificación de tipo de activo (sin excesivo detalle, capturando lo posible). Se mapea el
# `tipo` de cada holding a un bucket legible. SOLO por `tipo` (NO por nombre: "Barrick Gold"
# es una acción, no materias primas — usar el nombre misclasificaba).
def _asset_bucket(tipo: str) -> str:
    t = str(tipo or "").lower().strip()
    if not t:
        return "Otros"
    if any(k in t for k in ("materia", "commodit", "gold", "oro", "silver", "plata")):
        return "Materias primas"
    if any(k in t for k in ("liquid", "cash", "repo", "monetar", "efectivo", "letras")):
        return "Monetario/Liquidez"
    if any(k in t for k in ("forward", "future", "futur", "option", "opcion", "swap", "warrant", "derivad", "cds")):
        return "Derivados"
    if any(k in t for k in ("fondo", "fund", "etf", "sicav", "ucits")):
        return "Fondos/ETF"
    if any(k in t for k in ("rf", "bond", "bono", "note", "debt", "deuda", "obligac", "fija", "fixed", "cédula", "cedula", "papel")):
        return "Renta fija"
    if any(k in t for k in ("rv", "equit", "share", "accion", "acción", "stock", "variable")):
        return "Renta variable"
    return "Otros"


def _asset_mix_from_holdings(holdings: list) -> dict:
    """{bucket: peso_pct} agregando los holdings por `tipo`. Solo fiable si los holdings traen
    `tipo` poblado (fondos de RF/mixtos sí; equity puro a menudo no → cae a asset_allocation)."""
    out: dict = {}
    n_tipado = 0
    for h in (holdings or []):
        if not isinstance(h, dict):
            continue
        w = h.get("peso_pct")
        if not w:
            continue
        if h.get("tipo"):
            n_tipado += 1
        b = _asset_bucket(h.get("tipo"))
        out[b] = round(out.get(b, 0) + w, 2)
    # Si casi nada tenía tipo → todo cae en "Otros", inútil: mejor devolver vacío y usar
    # el asset_allocation del AR (limpio).
    if n_tipado < max(3, 0.5 * len([h for h in holdings if isinstance(h, dict) and h.get("peso_pct")])):
        return {}
    return out


def _asset_mix_from_allocation(aa: dict) -> dict:
    """{bucket: pct} desde el asset_allocation del AR (equity/bonds/cash/otros)."""
    if not isinstance(aa, dict):
        return {}
    m = {"Renta variable": aa.get("equity_pct"), "Renta fija": aa.get("bonds_pct"),
         "Monetario/Liquidez": aa.get("cash_pct"), "Otros": aa.get("otros_pct")}
    return {k: v for k, v in m.items() if v}


def _geo_from_holdings(holdings: list) -> dict:
    """{region: peso_pct} agregando holdings por país (normalizado a región/país). Fallback
    cuando el AR no trae tabla de geografía. Solo si ≥30% del peso tiene país resoluble."""
    if not holdings:
        return {}
    try:
        from tools.region_normalizer import aggregate_by_country
    except Exception:
        aggregate_by_country = None
    raw, cubierto = {}, 0.0
    for h in holdings:
        if not isinstance(h, dict):
            continue
        w = h.get("peso_pct")
        pais = (h.get("pais") or "").strip()
        if not w or not pais or pais.lower() in ("internacional", "n/a", "-", "varios", "global"):
            continue
        raw[pais] = raw.get(pais, 0) + w
        cubierto += w
    if cubierto < 25:                       # muy poca cobertura → no fiable
        return {}
    if aggregate_by_country:
        try:
            return {k: round(v, 2) for k, v in aggregate_by_country(raw).items()}
        except Exception:
            pass
    return {k: round(v, 2) for k, v in raw.items()}


def _sector_from_holdings(holdings: list) -> dict:
    """{sector: peso_pct} agregando holdings por sector. Fallback cuando el AR no trae tabla
    de sectores. Solo si ≥25% del peso tiene sector."""
    if not holdings:
        return {}
    raw, cubierto = {}, 0.0
    for h in holdings:
        if not isinstance(h, dict):
            continue
        w = h.get("peso_pct")
        sec = (h.get("sector") or "").strip()
        if not w or not sec or sec.lower() in ("n/a", "-", "otros", "otro"):
            continue
        raw[sec] = raw.get(sec, 0) + w
        cubierto += w
    if cubierto < 25:
        return {}
    return {k: round(v, 2) for k, v in raw.items()}


def _compact_holdings(pos: list, limit: int = 200) -> list:
    """Holdings compactos para comparación año-a-año (nombre + peso + sector + país)."""
    out = []
    for p in (pos or [])[:limit]:
        if not isinstance(p, dict) or not p.get("nombre"):
            continue
        out.append({k: p.get(k) for k in ("nombre", "peso_pct", "sector", "pais", "tipo")
                    if p.get(k) is not None})
    return out


def build(isin: str) -> dict:
    """Construye las estructuras históricas desde extracted/. No escribe. Devuelve dict con
    posiciones_historicas, mix_activos_historico, mix_geografico_historico,
    serie_rentabilidad, serie_aum (y n_años)."""
    fd = ROOT / "data" / "funds" / isin.upper()
    ed = fd / "extracted"
    files = sorted(glob.glob(str(ed / "*annual_subfund*.json")))
    # AR primero para que gane sobre SAR del mismo año en el upsert
    files.sort(key=lambda f: (0 if _is_ar(Path(f).name) else 1, f))

    hist_by_per: dict[str, dict] = {}
    mix_act_by_per: dict[str, dict] = {}
    mix_geo_by_per: dict[str, dict] = {}
    sector_by_per: dict[str, dict] = {}
    asset_hist_by_per: dict[str, dict] = {}
    rent_by_key: dict[tuple, dict] = {}
    aum_by_per: dict[str, float] = {}

    for f in files:
        name = Path(f).name
        data = _load(Path(f)).get("data") or {}
        if not isinstance(data, dict):
            continue
        per = _year_of(name, data)
        if not per:
            continue
        yr = per[:4]

        # posiciones.historicas: 1 entrada por AÑO. Como se procesa el AR antes que el SAR
        # (sort AR-first), el AR anual gana; el SAR del mismo año no re-pisa (guard por yr).
        pos = data.get("posiciones") or []
        top10 = data.get("top_10") or []
        if yr not in hist_by_per and (pos or top10):
            entry = {"periodo": yr, "top10": top10[:10],
                     "num_posiciones": len(pos),
                     "holdings": _compact_holdings(pos)}
            aum = ((data.get("kpis") or {}).get("aum_actual_meur"))
            if aum is not None:
                entry["aum_meur"] = aum
            hist_by_per[yr] = entry

        # mix_activos_historico (asset_allocation {equity/bonds/cash/otros})
        aa = data.get("asset_allocation") or {}
        if isinstance(aa, dict) and any(v for v in aa.values() if v) and yr not in mix_act_by_per:
            mix_act_by_per[yr] = {
                "periodo": yr,
                "renta_variable_pct": aa.get("equity_pct"),
                "renta_fija_pct": aa.get("bonds_pct"),
                "liquidez_pct": aa.get("cash_pct"),
                "otros_pct": aa.get("otros_pct"),
            }

        # mix_geografico_historico: tabla del AR primero; si no, derivar de los holdings del año.
        zonas = _norm_geo(data.get("geographic_allocation") or []) or _geo_from_holdings(pos)
        if zonas and yr not in mix_geo_by_per:
            mix_geo_by_per[yr] = {"periodo": yr, "zonas": zonas}

        # sector_allocation_history: tabla del AR primero; si no, derivar de los holdings.
        secs = {it.get("sector"): it.get("peso_pct")
                for it in (data.get("sector_allocation") or [])
                if isinstance(it, dict) and it.get("sector") and it.get("peso_pct")}
        if not secs:
            secs = _sector_from_holdings(pos)
        if secs and yr not in sector_by_per:
            sector_by_per[yr] = {"periodo": yr, "sectores": secs}

        # asset_allocation_history (tipo de activo): PRIMERO el asset_allocation del AR (limpio,
        # equity/bonds/cash/otros); si el AR no lo trae, agrega holdings por tipo (solo si vienen
        # tipados). Para el gráfico de evolución por tipo de activo.
        tipos = _asset_mix_from_allocation(aa) or _asset_mix_from_holdings(pos)
        if tipos and yr not in asset_hist_by_per:
            asset_hist_by_per[yr] = {"periodo": yr, "tipos": tipos}

        # serie_rentabilidad (tabla performance: puede haber varias clases/año)
        for pr in (data.get("performance") or []):
            if not isinstance(pr, dict):
                continue
            p_yr = str(pr.get("periodo") or yr)[:4]
            if not re.match(r"^\d{4}$", p_yr):
                continue
            clase = str(pr.get("clase") or "")
            rent_by_key[(p_yr, clase)] = {
                "periodo": p_yr, "clase": clase,
                "rentabilidad_pct": pr.get("rentabilidad_pct"),
                "benchmark_pct": pr.get("benchmark_pct"),
            }

        # serie_aum (statistics: cada AR trae ~3 años de aum_meur)
        for st in (data.get("statistics") or []):
            if not isinstance(st, dict):
                continue
            s_yr = str(st.get("periodo") or "")[:4]
            v = st.get("aum_meur")
            if re.match(r"^\d{4}$", s_yr) and v:
                aum_by_per[s_yr] = v          # último gana (AR más reciente da el dato más fiable)

    def _sorted(d):
        return [d[k] for k in sorted(d.keys())]

    geo_hist = _sorted(mix_geo_by_per)
    return {
        "posiciones_historicas": [hist_by_per[k] for k in sorted(hist_by_per.keys())],
        "mix_activos_historico": _sorted(mix_act_by_per),
        "mix_geografico_historico": geo_hist,
        "serie_rentabilidad": [rent_by_key[k] for k in sorted(rent_by_key.keys())],
        "serie_aum": [{"periodo": k, "valor_meur": aum_by_per[k]} for k in sorted(aum_by_per.keys())],
        # Claves que consume el dashboard (build_allocation_evolution_chart) para los gráficos
        # de evolución de exposición en la pestaña Cartera:
        "geographic_allocation_history": geo_hist,               # subkey "zonas"
        "sector_allocation_history": _sorted(sector_by_per),     # subkey "sectores"
        "asset_allocation_history": _sorted(asset_hist_by_per),  # subkey "tipos"
        "n_anios": len(hist_by_per),
    }


def _upsert_by_periodo(existing: list, nuevos: list) -> list:
    """Fusiona por `periodo`: los nuevos rellenan/actualizan; conserva lo que no toquen."""
    by = {}
    for e in (existing or []):
        if isinstance(e, dict) and e.get("periodo") is not None:
            by[str(e["periodo"])] = e
    for n in nuevos:
        by[str(n["periodo"])] = n           # el histórico reconstruido es autoritativo
    return [by[k] for k in sorted(by.keys())]


def _apply_to_file(p: Path, isin: str, built: dict, log=print) -> dict:
    """Upsert de las estructuras históricas en un fichero (output.json o intl_data.json).
    Mismo schema (posiciones.historicas + cuantitativo.*). Best-effort."""
    if not p.exists():
        return {"ok": False, "error": f"sin {p.name}"}
    d = _load(p)
    manual = set(d.get("_manual_edits") or [])
    pos = d.setdefault("posiciones", {})
    cuant = d.setdefault("cuantitativo", {})
    changed = []

    if built["posiciones_historicas"] and "posiciones.historicas" not in manual:
        pos["historicas"] = _upsert_by_periodo(pos.get("historicas"), built["posiciones_historicas"])
        changed.append(f"posiciones.historicas={len(pos['historicas'])}")
    for key, src in (("mix_activos_historico", "mix_activos_historico"),
                     ("mix_geografico_historico", "mix_geografico_historico"),
                     ("serie_rentabilidad", "serie_rentabilidad"),
                     ("serie_aum", "serie_aum")):
        if built[src] and f"cuantitativo.{key}" not in manual:
            cuant[key] = _upsert_by_periodo(cuant.get(key), built[src])
            changed.append(f"{key}={len(cuant[key])}")
    # Claves top-level que consume el dashboard para los gráficos de evolución de exposición.
    for key in ("geographic_allocation_history", "sector_allocation_history",
                "asset_allocation_history"):
        if built.get(key) and key not in manual:
            d[key] = _upsert_by_periodo(d.get(key), built[key])
            changed.append(f"{key}={len(d[key])}")

    if not changed:
        return {"ok": True, "changed": [], "n_anios": built["n_anios"]}
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    log(f"[HIST] {isin} ({p.name}): {', '.join(changed)} (años={built['n_anios']})")
    return {"ok": True, "changed": changed, "n_anios": built["n_anios"]}


def apply_to_output(isin: str, built: dict | None = None, log=print) -> dict:
    """Escribe el histórico en output.json (para dashboard + persistencia). Best-effort."""
    fd = ROOT / "data" / "funds" / isin.upper()
    return _apply_to_file(fd / "output.json", isin, built or build(isin), log=log)


def apply_to_intl_data(isin: str, built: dict | None = None, log=print) -> dict:
    """Escribe el histórico en intl_data.json → el bundle_exporter lo copia a fund_data.json
    (lo que lee la skill analyst-cowork) → el analyst puede narrar la evolución/consistencia
    en el MISMO run. Se llama en _consume_extracted, ANTES de la re-exportación del bundle."""
    fd = ROOT / "data" / "funds" / isin.upper()
    return _apply_to_file(fd / "intl_data.json", isin, built or build(isin), log=log)


def main(argv=None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="Puebla histórico multi-año desde extracts del AR")
    ap.add_argument("isin")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    built = build(a.isin)
    print(json.dumps({k: (len(v) if isinstance(v, list) else v)
                      for k, v in built.items()}, ensure_ascii=False, indent=1))
    if a.apply:
        print(apply_to_output(a.isin, built))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
