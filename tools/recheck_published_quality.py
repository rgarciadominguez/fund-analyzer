"""recheck_published_quality.py — Re-evalúa la calidad de los análisis ya
publicados en Supabase y corrige las filas rotas (B1/B2, 2026-06-04).

Problema: sync_to_supabase marcaba has_qualitative_analysis=true sin validar el
contenido, así que análisis vacíos o fabricados quedaron live como "✓ ver".

Este script recorre los fondos con output.json local, evalúa cada uno con
tools.analysis_quality.assess_analysis_quality, y para los que tienen blockers:
  - pone has_qualitative_analysis = false  (vuelven a "▶ Analizar" en el catálogo)

Por defecto es DRY-RUN (no escribe). Usa --apply para ejecutar los cambios.

    python -m tools.recheck_published_quality            # dry-run
    python -m tools.recheck_published_quality --apply     # escribe en Supabase
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"

from tools.analysis_quality import assess_analysis_quality  # noqa: E402


def _iter_local_funds():
    for d in sorted(FUNDS_DIR.iterdir()):
        if not d.is_dir() or "." in d.name:
            continue
        out = d / "output.json"
        if out.is_file():
            yield d.name, out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Re-evalúa análisis publicados en Supabase")
    ap.add_argument("--apply", action="store_true", help="Escribe los cambios (default: dry-run)")
    args = ap.parse_args(argv)

    client = None
    published = {}
    if args.apply or True:
        try:
            from tools.supabase_client import get_client
            client = get_client()
        except Exception as e:
            print(f"[WARN] Supabase no disponible ({e}). Solo evaluación local.")
            client = None

    if client is not None:
        rows = client.table("funds").select("isin,has_qualitative_analysis").execute().data
        published = {r["isin"]: bool(r.get("has_qualitative_analysis")) for r in rows}

    to_unpublish = []
    for isin, out_path in _iter_local_funds():
        try:
            data = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[SKIP] {isin}: output.json ilegible ({e})")
            continue
        q = assess_analysis_quality(data)
        is_pub = published.get(isin)
        if not q["ok"]:
            mark = "PUBLICADO" if is_pub else ("no-pub" if is_pub is False else "?")
            print(f"[ROTO] {isin}: {'; '.join(q['blockers'])}  (catálogo: {mark})")
            if is_pub:
                to_unpublish.append(isin)
        elif q["warnings"]:
            print(f"[WARN] {isin}: {'; '.join(q['warnings'])}")

    print(f"\nFondos rotos a despublicar (has_qualitative_analysis -> false): {to_unpublish or 'ninguno'}")

    if not to_unpublish:
        return 0
    if not args.apply:
        print("\n(DRY-RUN) Re-ejecuta con --apply para escribir los cambios.")
        return 0
    if client is None:
        print("[ERROR] --apply pero Supabase no disponible.")
        return 1

    for isin in to_unpublish:
        client.table("funds").update({"has_qualitative_analysis": False}).eq("isin", isin).execute()
        print(f"[APPLIED] {isin}: has_qualitative_analysis -> false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
