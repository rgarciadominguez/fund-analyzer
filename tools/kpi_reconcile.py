"""Conciliación de KPIs de cabecera por CONFIANZA DE FUENTE (regla de Rafa, 2026-09-24).

Por qué: los KPIs de `output.json` se rellenaban por orden de llegada y luego eran aditivos. En un
aporte o update anual podían quedarse con el dato del primer análisis (Gamma: partícipes 112 de 2021
con 8.110 en el informe de 2025; rating vacío con 5★ en MyInvestor; nº de activos y concentración
vacíos con 79 posiciones en cartera). Aquí cada KPI se toma de la fuente más fiable disponible:

  Fondos ES:  CNMV (cnmv_data.json: informes periódicos + XML, ya anualizados por el agente)
              > informe anual / extractos (lo que ya haya en output.json)
              > Morningstar / MyInvestor (myinvestor_data.json, Supabase `estrellas`).
  Fondos INT: informe anual / extractos (output.json) > Morningstar / MyInvestor.
  Derivados de la propia cartera (siempre): nº de activos y concentración top 10.

Nunca escribe vacío sobre un valor; deja rastro en `output["kpis_origen"]` (fuente por KPI) y en
`output["kpis_reconciliacion"]` (qué cambió). Se ejecuta al final de --consume-all-cowork (todos los
modos) y como CLI: `python -m tools.kpi_reconcile ISIN` (dry-run) / `--apply`.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _num(v):
    try:
        f = float(v)
        return f if f == f else None   # NaN
    except (TypeError, ValueError):
        return None


def _last_by_period(serie: list, key: str):
    """Último valor no nulo de una serie [{periodo, key}] ordenando por periodo (texto)."""
    best = None
    for e in serie or []:
        if not isinstance(e, dict) or _num(e.get(key)) is None:
            continue
        per = str(e.get("periodo") or "")
        if best is None or per > best[0]:
            best = (per, _num(e.get(key)))
    return best   # (periodo, valor) | None


def _supabase_estrellas(isin: str):
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        r = get_client().table("funds").select("estrellas").eq("isin", isin).execute().data
        return int(r[0]["estrellas"]) if r and r[0].get("estrellas") else None
    except Exception:
        return None


def propose(isin: str, use_supabase: bool = True) -> list[dict]:
    """Lista de cambios propuestos: [{campo, antes, despues, fuente}] (sin escribir nada)."""
    isin = isin.upper().strip()
    fd = FUNDS / isin
    out = _load(fd / "output.json") or {}
    cnmv = _load(fd / "cnmv_data.json") or {}
    mi = _load(fd / "myinvestor_data.json") or {}
    kpis = out.get("kpis") or {}
    es = (out.get("tipo") or "").upper() == "ES" or isin.startswith("ES")
    cambios: list[dict] = []

    def prop(campo, valor, fuente, solo_si_vacio=False):
        if valor is None:
            return
        antes = kpis.get(campo)
        if solo_si_vacio and antes not in (None, "", 0):
            return
        if isinstance(valor, float):
            valor = round(valor, 4)
        if antes == valor:
            return
        cambios.append({"campo": campo, "antes": antes, "despues": valor, "fuente": fuente})

    if es and cnmv:
        ck = cnmv.get("kpis") or {}
        cq = cnmv.get("cuantitativo") or {}
        origen = (cnmv.get("_kpi_origen") or ck.get("_kpi_origen") or {})
        est_h1 = set(cnmv.get("_kpi_estimado_h1") or ck.get("_kpi_estimado_h1") or [])
        # partícipes: informe CNMV más reciente que lo tenga; si no, última serie
        n = _num(ck.get("num_participes"))
        if n and n > 0:
            prop("num_participes", int(n), f"CNMV informe {origen.get('num_participes', '')}".strip())
        else:
            last = _last_by_period(cq.get("serie_participes"), "valor")
            if last and last[1] > 0:
                prop("num_participes", int(last[1]), f"CNMV serie partícipes {last[0]}")
        # patrimonio: último de la serie CNMV (PDF pisa XML en el agente)
        last = _last_by_period(cq.get("serie_aum"), "valor_meur")
        if last and last[1] > 0:
            prop("aum_actual_meur", float(last[1]), f"CNMV patrimonio {last[0]}")
        # comisiones y TER: anualizados por el agente; si vienen de un H1 se marca estimación
        for campo in ("coste_gestion_pct", "ter_pct"):
            v = _num(ck.get(campo))
            if v is not None and v > 0:
                tag = f"CNMV informe {origen.get(campo, '')}".strip() + (" (H1 anualizado)" if campo in est_h1 else "")
                prop(campo, float(v), tag)
    if not es:
        # INT: el informe anual/semestral más reciente manda para el patrimonio (convertido a EUR desde la
        # divisa base con el tipo de la fecha) y para el TER si lo trae.
        try:
            import glob as _glob
            best = None
            for f in _glob.glob(str(fd / "extracted" / "*.json")):
                d = _load(Path(f)) or {}
                d = d.get("data") or d
                k = d.get("kpis") or {}
                aum, fa = _num(k.get("aum_actual_meur")), str(k.get("fecha_aum") or d.get("periodo") or "")
                if aum and aum > 0 and fa and (best is None or fa > best[0]):
                    best = (fa, aum, str(k.get("divisa_base") or "EUR").upper(), _num(k.get("ter_pct")))
            if best:
                fa, aum, cur, ter = best
                if cur != "EUR":
                    from tools.fx import to_eur
                    aum, fxsrc = to_eur(aum, cur, fa)
                    prop("aum_actual_meur", round(aum, 2), f"informe {fa} ({cur} → EUR, {fxsrc})")
                else:
                    prop("aum_actual_meur", round(aum, 2), f"informe {fa}")
                if ter and ter > 0:
                    prop("ter_pct", float(ter), f"informe {fa}", solo_si_vacio=True)
        except Exception:
            pass
    # rating: MyInvestor (Morningstar) > Supabase estrellas; solo si el análisis no lo tiene o difiere
    rating = _num(mi.get("mstar_rating"))
    src = "MyInvestor (Morningstar)"
    if rating is None and use_supabase:
        rating, src = _supabase_estrellas(isin), "Supabase estrellas"
    if rating and 1 <= rating <= 5:
        prop("rating_morningstar", int(rating), src)
    # TER en INT sin dato: MyInvestor
    if not es and _num(kpis.get("ter_pct")) in (None, 0) and _num(mi.get("ter")):
        prop("ter_pct", float(mi["ter"]), "MyInvestor TER", solo_si_vacio=True)
    # derivados de la cartera
    pos = ((out.get("posiciones") or {}).get("actuales") or [])
    pesos = sorted([p for p in (_num(x.get("peso_pct")) for x in pos if isinstance(x, dict)) if p and p > 0], reverse=True)
    if len(pos) >= 5:
        prop("num_activos_cartera", len(pos), "cartera (posiciones actuales)")
    if len(pesos) >= 10 and sum(pesos) <= 105:
        prop("concentracion_top10_pct", round(sum(pesos[:10]), 2), "cartera (suma top 10)")
    # benchmark: solo relleno desde lo que mencione el informe CNMV
    if es and cnmv and not (kpis.get("benchmark") or "").strip():
        bm = (cnmv.get("benchmark_mencionado") or (cnmv.get("kpis") or {}).get("benchmark_mencionado") or "").strip()
        if bm:
            prop("benchmark", bm, "CNMV informe (benchmark mencionado)", solo_si_vacio=True)
    return cambios


def reconcile(isin: str, apply: bool = True, log=print, use_supabase: bool = True) -> dict:
    isin = isin.upper().strip()
    cambios = propose(isin, use_supabase=use_supabase)
    if not cambios:
        log(f"[KPI] {isin}: sin cambios (KPIs coherentes con la fuente más fiable)")
        return {"cambios": []}
    for c in cambios:
        log(f"[KPI] {isin}: {c['campo']}: {c['antes']} → {c['despues']}  [{c['fuente']}]")
    if apply:
        p = FUNDS / isin / "output.json"
        out = _load(p) or {}
        kpis = out.setdefault("kpis", {})
        origen = out.setdefault("kpis_origen", {})
        for c in cambios:
            kpis[c["campo"]] = c["despues"]
            origen[c["campo"]] = c["fuente"]
        out["kpis_reconciliacion"] = cambios
        p.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[KPI] {isin}: {len(cambios)} KPIs conciliados y guardados")
    return {"cambios": cambios}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print("uso: python -m tools.kpi_reconcile ISIN [--apply]")
        sys.exit(1)
    reconcile(args[0], apply="--apply" in sys.argv)
