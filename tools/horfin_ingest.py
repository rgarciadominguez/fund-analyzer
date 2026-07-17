"""
horfin_ingest.py — Ingiere `aporte_horfin.json` en Supabase. Cruce SOLO por ISIN.

REGLA DURA: se cruza siempre por ISIN, nunca por nombre. `nombre_horfin` es clave de
unión en el lado de Horizonte (hay tablas que enlazan por el nombre literal) → un rename
les huerfana filas. No se usa para casar, no se da por bueno, no se corrige.

CAMPOS BLINDADOS (la escritura de Horizonte GANA; nosotros solo rellenamos huecos):
    descripcion, opinion (-> opinion_user), categoria_activo

Los fondos del aporte que aún no existen en nuestro catálogo (los 78) se quedan en
`pendientes`: se aplicarán cuando se analicen, sin pisarlos.

CLI:
    python -m tools.horfin_ingest --dry-run
    python -m tools.horfin_ingest --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

APORTE = Path(r"C:\Users\RafaelGarcía\horizonte-datos\aporte_horfin.json")
REPORT = ROOT / "data" / "horfin_ingest_report.json"

VALID_CATEGORIA = {"Indexado", "Gestionado", "Hedgefund"}


def load_aporte() -> dict:
    d = json.loads(APORTE.read_text(encoding="utf-8"))
    return {a["isin"]: a for a in d["activos"] if a.get("isin")}


def ingest(apply: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client

    c = get_client()
    rows = {r["isin"]: r for r in
            c.table("funds").select("isin,opinion_user,horfin_id").execute().data}
    aporte = load_aporte()

    plan, pendientes, avisos = [], [], []
    for isin, a in aporte.items():
        if isin not in rows:
            pendientes.append(isin)
            continue
        upd, why = {}, []

        desc = (a.get("descripcion") or "").strip()
        if desc:
            upd["descripcion"] = desc
            why.append("descripcion=horfin")

        op = (a.get("opinion") or "").strip()
        if op:
            mine = (rows[isin].get("opinion_user") or "").strip()
            if op != mine:
                upd["opinion_user"] = op
                why.append("opinion=horfin(gana)" if mine else "opinion=horfin(hueco)")

        cat = (a.get("categoria_activo") or "").strip()
        if cat:
            if cat in VALID_CATEGORIA:
                upd["categoria_activo"] = cat
                why.append("categoria=horfin")
            else:
                avisos.append(f"{isin}: categoria_activo fuera de lista: {cat!r}")

        hid = str(a.get("horfin_id") or "").strip()
        if hid and not (rows[isin].get("horfin_id") or "").strip():
            upd["horfin_id"] = hid
            why.append("horfin_id")

        if upd:
            plan.append({"isin": isin, "update": upd, "why": why})

    rep = {
        "aporte": len(aporte),
        "en_catalogo": len(aporte) - len(pendientes),
        "pendientes_de_analizar": len(pendientes),
        "a_actualizar": len(plan),
        "pendientes": sorted(pendientes),
        "avisos": avisos,
        "plan": plan,
    }
    REPORT.write_text(json.dumps(rep, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"aporte: {len(aporte)} | en catálogo: {rep['en_catalogo']} | "
          f"pendientes (78): {len(pendientes)} | filas a tocar: {len(plan)}")
    for w in avisos:
        print(f"  [AVISO] {w}")

    if apply:
        n = 0
        for p in plan:
            c.table("funds").update(p["update"]).eq("isin", p["isin"]).execute()
            n += 1
        print(f"escritas {n} filas en Supabase")
    else:
        print("(dry-run — nada escrito). Muestra:")
        for p in plan[:5]:
            print(f"  {p['isin']}: {', '.join(p['why'])}")
    return rep


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    ingest(apply=a.apply)
