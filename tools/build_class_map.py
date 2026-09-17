"""build_class_map.py — Genera dashboard/_class_map.json para el Worker de Cloudflare.

Contrato FONDO vs CLASE (CLAUDE.md §0.9) llevado al WORKER estático:
  El portal embebe `https://fund-analyzer.<sub>.workers.dev/fund-{ISIN}` pasando el ISIN de la
  CLASE pulsada. El Worker sirve `./dashboard/` como assets estáticos SIN routing → una clase sin
  su propio HTML da 404 (iframe vacío) y una clase con HTML viejo muestra un análisis stale.

  Este mapa permite al `_worker.js` (modo avanzado) ROUTEAR cualquier clase → el HTML del PRIMARIO
  del grupo (el último análisis bueno) e INYECTAR el contexto de la clase pulsada (cabecera +
  selector). Un análisis por grupo; todas las clases resuelven a él, sea cual sea la que se pulse.

Salida `dashboard/_class_map.json`:
{
  "generated": "ISO",
  "aliases": { "<isin_clase>": "<isin_primario>", ... },   # SOLO clases NO-primarias
  "groups":  { "<isin_primario>": {
                  "nombre": "<nombre del fondo>",
                  "primary": "<isin_primario>",
                  "classes": [ {isin,nombre_clase,divisa,hedge,fecha_inicio,comision,ter,
                                anios,es_primario,tiene_dashboard}, ... ] } }
}

El Worker usa `aliases` para routear; el dashboard usa `groups[primary].classes` para el selector.
Solo se crea alias hacia un primario que TENGA fichero local `dashboard/fund-{primary}.html`
(si no, `env.ASSETS.fetch` daría 404 igualmente).

CLI: python -m tools.build_class_map            (escribe dashboard/_class_map.json)
     python -m tools.build_class_map --dry       (imprime resumen, no escribe)
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
OUT = DASH / "_class_map.json"


def _years(fc) -> int:
    try:
        return 2026 - int(str(fc)[:4])
    except Exception:
        return 0


def _has_dash(isin: str) -> bool:
    return (DASH / f"fund-{isin}.html").exists()


def _pick_primary(members: list[dict]) -> dict | None:
    """Primario del grupo según CLAUDE.md §0.9. SOLO clases con dashboard local (el Worker sirve
    ficheros de ./dashboard). Si ninguna candidata tiene fichero → None.

    Regla §0.9 (Rafa): (1) clase EUR con ≥3 años → la de más track entre ellas. (2) Si hay EUR pero
    <3 años → quedarse con el EUR de más track, SALVO que otra divisa tenga ≥7 años Y ≥3 años MÁS que
    ese EUR (entonces esa divisa, avisando en UI). "No saltar de divisa por 1 año": si ambas son
    cortas → EUR con aviso de histórico corto. (3) Sin EUR → el de histórico más largo.
    """
    cand = [m for m in members if _has_dash(m["isin"])]
    if not cand:
        return None
    yrs = lambda m: _years(m.get("fecha_creacion_clase"))
    is_eur = lambda m: (m.get("divisa") or "").upper() == "EUR"
    tie = lambda m: (-yrs(m), 0 if m.get("has_qualitative_analysis") else 1, m["isin"])

    eur = sorted([m for m in cand if is_eur(m)], key=tie)
    eur3 = [m for m in eur if yrs(m) >= 3]
    if eur3:
        return eur3[0]
    if eur:
        best_eur = eur[0]
        others = sorted([m for m in cand if not is_eur(m)], key=tie)
        if others and yrs(others[0]) >= 7 and yrs(others[0]) - yrs(best_eur) >= 3:
            return others[0]  # otra divisa con MUCHO más histórico (la UI avisa de la divisa)
        return best_eur       # ambas cortas / diferencia pequeña → EUR (aviso "histórico corto")
    return sorted(cand, key=tie)[0]


def _doc_classes(primary_isin: str) -> dict[str, dict]:
    """Tabla de clases del DOCUMENTO (output.clases_documento del primario), por ISIN. Es literal
    del folleto/presentación: manda sobre lo inferido (cobertura, comisión de éxito, código)."""
    try:
        op = ROOT / "data" / "funds" / primary_isin / "output.json"
        cl = json.loads(op.read_text(encoding="utf-8")).get("clases_documento") or []
        return {(c.get("isin") or "").upper(): c for c in cl if isinstance(c, dict) and c.get("isin")}
    except Exception:
        return {}


def _common_prefix(names: list[str]) -> str:
    """Prefijo común (por palabras) de los nombres de clase del grupo = nombre del fondo; lo que
    queda es el CÓDIGO de la clase ('ML Alpha Fixed Inc UCITS FIEHA H EUR Acc' → 'FIEHA H EUR Acc')."""
    toks = [n.split() for n in names if n]
    if len(toks) < 2:
        return ""
    pre = []
    for ws in zip(*toks):
        if len(set(ws)) == 1:
            pre.append(ws[0])
        else:
            break
    return " ".join(pre)


def _class_row(m: dict, primary_isin: str, doc: dict | None = None, prefix: str = "") -> dict:
    from tools.morningstar_daily import is_hedged_class
    name = m.get("nombre_clase") or ""
    d = (doc or {}).get((m["isin"] or "").upper()) or {}
    resto = name[len(prefix):].strip() if prefix and name.startswith(prefix) else ""
    rep = (d.get("reparto") or "").lower() or (
        "acc" if " acc" in name.lower() else "dist" if any(t in name.lower() for t in (" inc", " dist", " dis")) else "")
    return {
        "isin": m["isin"],
        "nombre_clase": name,
        # código: el del documento; si no, lo que queda tras el nombre del fondo, solo si ese prefijo
        # es fiable (≥3 palabras comunes a TODAS las clases). Si no, None → la UI usa el nombre entero.
        "codigo": d.get("codigo") or (resto.split()[0] if (resto and len(prefix.split()) >= 3) else None),
        "divisa": (m.get("divisa") or d.get("divisa") or "").upper().replace("HEDGED", "").strip() or None,
        # cobertura: documento > flag de la taxonomía > nombre de la clase ("… H EUR", "Hedged")
        "hedge": bool(d.get("cubierta")) if "cubierta" in d else (bool(m.get("divisa_hedge_bool")) or is_hedged_class(name)),
        "reparto": "Acc" if rep.startswith("acc") else "Dist" if rep else None,
        "fecha_inicio": m.get("fecha_creacion_clase"),
        "anios": _years(m.get("fecha_creacion_clase")),
        "comision": d.get("comision_gestion_pct") if d.get("comision_gestion_pct") is not None else m.get("comision_gestion_pct"),
        "exito": d.get("comision_exito_pct"),
        "minimo": d.get("inversion_minima"),
        "ter": m.get("ter_pct"),
        "es_primario": m["isin"] == primary_isin,
        "tiene_dashboard": _has_dash(m["isin"]),
    }


def build(dry: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()

    cols = ("isin,nombre_clase,fund_group_id,divisa,divisa_hedge_bool,fecha_creacion_clase,"
            "comision_gestion_pct,ter_pct,dashboard_storage_path,has_qualitative_analysis")
    rows = c.table("funds").select(cols).limit(8000).execute().data or []

    # nombre del grupo (fund_groups)
    gnames: dict[str, str] = {}
    try:
        gr = c.table("fund_groups").select("id,nombre").limit(8000).execute().data or []
        for g in gr:
            if g.get("nombre"):
                gnames[g["id"]] = g["nombre"]
    except Exception:
        pass

    groups_raw: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("fund_group_id"):
            groups_raw[r["fund_group_id"]].append(r)

    aliases: dict[str, str] = {}
    groups_out: dict[str, dict] = {}
    n_multi = n_alias = 0

    for gid, members in groups_raw.items():
        if len(members) < 2:
            continue
        prim = _pick_primary(members)
        if not prim:
            continue  # ningún miembro tiene dashboard local → no se puede routear
        n_multi += 1
        pisin = prim["isin"]
        _doc = _doc_classes(pisin)
        _pre = _common_prefix([m.get("nombre_clase") or "" for m in members])
        classes = sorted(
            (_class_row(m, pisin, _doc, _pre) for m in members),
            key=lambda x: (not x["es_primario"], -(x["anios"] or 0), x["isin"]),
        )
        groups_out[pisin] = {
            "nombre": gnames.get(gid) or prim.get("nombre_clase") or "",
            "primary": pisin,
            "classes": classes,
        }
        for m in members:
            if m["isin"] != pisin:
                aliases[m["isin"]] = pisin
                n_alias += 1

    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "aliases": aliases,
        "groups": groups_out,
    }
    print(f"[class_map] grupos multi-clase enrutables: {n_multi} | aliases: {n_alias} | "
          f"clases totales en {len(groups_out)} grupos")
    if not dry:
        OUT.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"[class_map] escrito {OUT} ({OUT.stat().st_size} bytes)")
    return out


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    build(dry="--dry" in (argv or sys.argv[1:]))


if __name__ == "__main__":
    main()
