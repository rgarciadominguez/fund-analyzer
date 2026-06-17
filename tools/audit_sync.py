"""Auditoría de publicación: ¿qué fondos analizados localmente NO están en el catálogo?

Reconcilia los `data/funds/{ISIN}/output.json` locales contra la tabla `funds` de
Supabase. Reporta:
  - ANALIZADO PERO NO PUBLICADO: tiene output.json pero no está en Supabase. Muestra
    el motivo del último sync (data/sync_status.json) y RE-VALIDA ahora (¿pasaría?).
  - PUBLICADO SIN LOCAL: fila en Supabase sin output.json local (fila zombi).

Uso:
    python -m tools.audit_sync            # informe legible
    python -m tools.audit_sync --json     # salida JSON
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
STATUS_PATH = ROOT / "data" / "sync_status.json"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _local_funds() -> dict:
    """{isin: nombre} de cada output.json local."""
    out = {}
    for p in FUNDS_DIR.glob("*/output.json"):
        isin = p.parent.name.upper()
        if not _ISIN.match(isin):
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            out[isin] = (d.get("nombre") or "").strip()
        except Exception:
            out[isin] = "(output.json ilegible)"
    return out


def _supabase_funds() -> dict:
    """{isin: has_qualitative_analysis} de todas las filas de funds."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    rows = c.table("funds").select("isin,has_qualitative_analysis").limit(10000).execute().data
    return {(r["isin"] or "").upper(): bool(r.get("has_qualitative_analysis"))
            for r in rows if r.get("isin")}


def _status_map() -> dict:
    if STATUS_PATH.exists():
        try:
            return json.loads(STATUS_PATH.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}
    return {}


def _revalidate(isin: str) -> tuple[bool, list]:
    """¿output.json pasaría el guard del sync AHORA?"""
    try:
        from tools.sync_to_supabase import _validate_before_sync
        d = json.loads((FUNDS_DIR / isin / "output.json").read_text(encoding="utf-8"))
        return _validate_before_sync(d, isin)
    except Exception as e:
        return False, [f"error releyendo: {str(e)[:80]}"]


def audit() -> dict:
    local = _local_funds()
    try:
        supa = _supabase_funds()
        supa_ok = True
    except Exception as e:
        supa = {}
        supa_ok = False
        print(f"[WARN] no se pudo leer Supabase: {str(e)[:120]}", file=sys.stderr)
    if not supa_ok:
        # Sin Supabase no se puede reconciliar — no inventar "no publicados".
        return {"n_local": len(local), "n_supabase": 0, "not_published": [],
                "zombies": [], "n_catalog_only": 0, "supabase_ok": False}
    status = _status_map()

    not_published = []
    for isin, nombre in sorted(local.items()):
        if isin not in supa:
            st = status.get(isin, {})
            ok_now, reasons_now = _revalidate(isin)
            not_published.append({
                "isin": isin, "nombre": nombre,
                "last_sync": st.get("status", "(nunca sincronizado)"),
                "last_reasons": st.get("reasons", []),
                "ts": st.get("ts"),
                "pasaria_ahora": ok_now,
                "motivos_ahora": reasons_now,
            })
    # Inconsistencia REAL: la fila dice tener análisis cualitativo pero no hay
    # output.json local que lo respalde. (Las filas SIN has_qualitative_analysis
    # — clases insertadas / fondos importados al catálogo — NO son zombis: es el
    # estado esperado de un catálogo más amplio que el set analizado.)
    zombies = sorted([i for i, has_q in supa.items()
                      if has_q and i not in local]) if supa_ok else []
    n_catalog_only = sum(1 for i, has_q in supa.items()
                         if not has_q and i not in local) if supa_ok else 0
    return {
        "n_local": len(local), "n_supabase": len(supa),
        "not_published": not_published, "zombies": zombies,
        "n_catalog_only": n_catalog_only, "supabase_ok": supa_ok,
    }


def main():
    res = audit()
    if "--json" in sys.argv:
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return
    print(f"\n=== AUDITORÍA DE PUBLICACIÓN ===")
    print(f"Local (output.json): {res['n_local']}   |   Supabase (funds): {res['n_supabase']}")
    np = res["not_published"]
    print(f"\n── ANALIZADOS PERO NO PUBLICADOS: {len(np)} ──")
    for f in np:
        flag = "✓ pasaría ahora" if f["pasaria_ahora"] else "✗ seguiría bloqueado"
        print(f"  {f['isin']}  {f['nombre'][:38]!r}")
        print(f"      último sync: {f['last_sync']} {f['last_reasons'] or ''}  →  {flag}")
        if not f["pasaria_ahora"]:
            print(f"      motivos ahora: {f['motivos_ahora']}")
    if res["zombies"]:
        print(f"\n── ZOMBIS (dicen tener análisis pero sin output.json local): {len(res['zombies'])} ──")
        print("  " + ", ".join(res["zombies"]))
    print(f"\n(info: {res.get('n_catalog_only', 0)} filas solo-catálogo sin análisis "
          f"— clases/importados, estado esperado)")
    if not np and not res["zombies"]:
        print("✓ Todo cuadra: cada análisis local está publicado y sin filas zombi.")


if __name__ == "__main__":
    main()
