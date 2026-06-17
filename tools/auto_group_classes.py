"""Auto-agrupado de clases del mismo fondo tras el análisis.

Regla (Rafa, 2026-06-17): en cuanto un análisis identifica dos ISINs como el
MISMO fondo (otra clase), hay que AGRUPARLOS SIEMPRE — aunque ninguno esté en
Supabase todavía. La fuente de verdad es el registro local `class_links`
(`data/fund_class_links.json`); la propagación a Supabase es best-effort (solo
aplica cuando el primario ya está sincronizado, pero el vínculo local persiste y
se aplicará en cuanto se suba).

Señales de "misma clase" (en orden de autoridad):
  1. ISIN compartido del folleto: la lista `clases[].isin` / `_int_clases[].isin`
     de un fondo incluye el ISIN del otro (o viceversa). Autoritativo.
  2. Nombre EXACTO (normalizado, específico, válido) + mismo stem de ISIN (primeros
     8 chars = país + emisor). Cubre el caso en que el folleto no listó las clases
     (p.ej. BNY Mellon Short-Dated HY: IE00BD5CTX77 / IE00BD5CV310, ambos IE00BD5C).

Nunca agrupa por prefijo de nombre (los paraguas vienen truncados): exige igualdad
EXACTA del nombre completo, de modo que dos sub-fondos distintos del mismo paraguas
(nombres distintos) jamás se fusionan.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
_STEM_LEN = 8  # país + emisor; las clases de un fondo comparten este prefijo


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _class_isins(data: dict) -> set[str]:
    """ISINs de clase que el folleto del fondo lista (clases[].isin / _int_clases)."""
    out: set[str] = set()
    for key in ("clases", "_int_clases"):
        for c in (data.get(key) or []):
            if isinstance(c, dict):
                v = (c.get("isin") or c.get("ISIN") or "").upper().strip()
                if _ISIN_RE.match(v):
                    out.add(v)
    return out


def _is_specific_name(name: str, isin: str) -> bool:
    """Nombre válido y específico (no vacío, no ISIN, no etiqueta genérica)."""
    try:
        from tools.fund_name_utils import is_valid_fund_name
        ok, _ = is_valid_fund_name(name, isin)
    except Exception:
        ok = bool(name) and len(name) >= 6
    return ok and len(_norm(name)) >= 8


def _build_index() -> dict:
    """{isin: {name, gestora, stem, class_isins:set, anio, in_supabase:None}}."""
    idx: dict[str, dict] = {}
    for p in FUNDS_DIR.glob("*/output.json"):
        isin = p.parent.name.upper()
        if not _ISIN_RE.match(isin):
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        name = (d.get("nombre") or "").strip()
        idx[isin] = {
            "name": name,
            "nname": _norm(name),
            "gestora": _norm(d.get("gestora") or ""),
            "stem": isin[:_STEM_LEN],
            "class_isins": _class_isins(d),
            "anio": (d.get("kpis") or {}).get("anio_creacion"),
            "specific": _is_specific_name(name, isin),
        }
    return idx


def find_same_fund(isin: str, idx: dict | None = None) -> set[str]:
    """ISINs de OTROS fondos analizados que son el MISMO fondo que `isin`."""
    isin = (isin or "").upper().strip()
    idx = idx if idx is not None else _build_index()
    me = idx.get(isin)
    if not me:
        return set()
    sibs: set[str] = set()
    for other, o in idx.items():
        if other == isin:
            continue
        # Señal 1: ISIN compartido del folleto (autoritativo, bidireccional).
        if other in me["class_isins"] or isin in o["class_isins"]:
            sibs.add(other)
            continue
        # Señal 2: nombre EXACTO + mismo stem de ISIN, ambos específicos.
        if (me["specific"] and o["specific"]
                and me["nname"] and me["nname"] == o["nname"]
                and me["stem"] == o["stem"]):
            # si hay gestora en ambos, debe coincidir (evita homónimos de emisores)
            if me["gestora"] and o["gestora"] and me["gestora"] != o["gestora"]:
                continue
            sibs.add(other)
    return sibs


def _pick_primary(cluster: set[str], idx: dict, supa_present: set[str]) -> str:
    """Primario del cluster: el que esté en Supabase > más antiguo > ISIN menor."""
    def key(i):
        anio = idx.get(i, {}).get("anio")
        anio = anio if isinstance(anio, int) else 9999
        return (0 if i in supa_present else 1, anio, i)
    return sorted(cluster, key=key)[0]


def _supabase_present(isins: set[str]) -> set[str]:
    try:
        from tools.supabase_client import get_client
        c = get_client()
        rows = c.table("funds").select("isin").in_("isin", list(isins)).execute().data
        return {r["isin"].upper() for r in rows if r.get("isin")}
    except Exception:
        return set()


def auto_group(isin: str, log=None) -> dict:
    """Agrupa `isin` con sus clases hermanas ya analizadas. Idempotente.

    Vincula (class_links) cada hermana → primario y propaga a Supabase best-effort.
    Devuelve {grouped:[...], primary, linked:[...]} o {grouped:[]} si no hay hermanas.
    """
    from tools import class_links
    isin = (isin or "").upper().strip()
    idx = _build_index()
    sibs = find_same_fund(isin, idx)
    if not sibs:
        return {"grouped": [], "primary": isin}
    cluster = {isin} | sibs
    # incluir clases ya vinculadas a cualquier miembro (cluster transitivo)
    existing = class_links.all_links()
    for a, v in existing.items():
        pr = (v.get("primary_isin") or "").upper()
        if a in cluster or pr in cluster:
            cluster.add(a.upper()); cluster.add(pr)
    cluster = {i for i in cluster if _ISIN_RE.match(i)}
    supa = _supabase_present(cluster)
    primary = _pick_primary(cluster, idx, supa)
    linked = []
    for alias in sorted(cluster - {primary}):
        if class_links.resolve_primary(alias) == primary:
            continue  # ya vinculado a la raíz correcta
        label = idx.get(alias, {}).get("name", "")
        res = class_links.add_link(alias, primary, label)
        if res.get("ok"):
            linked.append(alias)
            class_links.sync_link_to_supabase(alias, primary)  # best-effort
    if log:
        if linked:
            log("AUTO_GROUP", "OK",
                f"{len(linked)} clase(s) agrupadas bajo {primary}: {', '.join(linked)}")
        else:
            log("AUTO_GROUP", "OK", f"cluster ya agrupado bajo {primary}")
    return {"grouped": sorted(cluster), "primary": primary, "linked": linked}


def backfill_all(log=None) -> dict:
    """Recorre todos los fondos analizados y agrupa los que sean el mismo fondo."""
    idx = _build_index()
    seen: set[str] = set()
    clusters = []
    for isin in sorted(idx):
        if isin in seen:
            continue
        sibs = find_same_fund(isin, idx)
        if sibs:
            res = auto_group(isin, log=log)
            for i in res.get("grouped", []):
                seen.add(i)
            if res.get("linked"):
                clusters.append(res)
        seen.add(isin)
    return {"clusters": clusters, "n_clusters": len(clusters)}


def main():
    import sys
    log = lambda a, lvl, m: print(f"[{lvl}] {m}")
    if len(sys.argv) > 1 and sys.argv[1] not in ("--all", "--backfill"):
        print(json.dumps(auto_group(sys.argv[1], log=log), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(backfill_all(log=log), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
