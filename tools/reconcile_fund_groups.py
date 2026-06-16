"""Reconcilia clases del mismo fondo partidas en distintos `fund_group_id`.

Causa raíz: un `nombre_base` basura (del bug de nombres) produce un
`fund_group_id` distinto para clases del MISMO fondo → en el catálogo aparecen
separadas (una clasificada y otra no). Este script detecta esos splits
(misma gestora + mismo nombre base limpio, pero distinto grupo) y los FUSIONA en
un grupo canónico: el del "primario" (con análisis cualitativo / más
track-record). Escribe en Supabase → el catálogo público agrupa.

Casos NO recuperables automáticamente (nombre base vacío/1 letra, p.ej. Trojan
'O EUR Acc') se LISTAN para vínculo manual (botón 🔗) — no se tocan.

Uso:
    python -m tools.reconcile_fund_groups            # DRY-RUN (solo reporta)
    python -m tools.reconcile_fund_groups --apply    # aplica en Supabase
"""
from __future__ import annotations

import re
import sys
from collections import defaultdict

_DROP = {'fi', 'sicav', 'fund', 'funds', 'plc', 'icav', 'oeic', 'fcp', 'ucits',
         'acc', 'inc', 'cap', 'dist', 'hedged', 'hedge', 'eurhdg', 'class', 'clase',
         'action', 'part', 'retail', 'institutional', 'eur', 'usd', 'gbp', 'chf',
         'jpy', 'cad', 'sek', 'i', 'a', 'b', 'c', 'd', 'e', 'f', 'r', 'z', 'p',
         'n', 'w', 'o', 'h', 'x', 's', 't'}


def clean_base(s: str) -> list:
    """Tokens significativos del nombre (sin ruido de clase). NO trunca: las
    palabras que distinguen fondos (Europe/Emerging, Corporate/Government...)
    suelen ir al final del nombre."""
    s = re.sub(r'"[^"]*"', ' ', s or '')      # quita "I"
    s = re.sub(r'\([^)]*\)', ' ', s)          # quita (EURHDG)
    s = re.sub(r'[^a-z0-9 ]+', ' ', s.lower())
    return [t for t in s.split() if t not in _DROP and len(t) > 1]


def gnorm(g: str) -> str:
    g = re.sub(r'[^a-z0-9 ]', ' ', (g or '').lower())
    return ' '.join(g.split()[:2])


def _is_prefix(short: list, long: list) -> bool:
    """True si `short` es prefijo de tokens de `long` (mismo fondo, otra clase).
    No basta compartir 4 tokens: hay que coincidir hasta agotar el más corto."""
    return len(short) >= 2 and short == long[:len(short)]


def detect(funds: list, groups: dict):
    """Devuelve (splits, garbage). splits = [{key, members:[fund]}].

    Clustering por prefijo de tokens DENTRO de cada gestora: dos clases del mismo
    fondo comparten el nombre completo salvo el sufijo de clase. Fondos distintos
    que comparten prefijo (Amundi IS Core MSCI *Europe* vs *Emerging Markets*) NO
    se agrupan porque divergen antes de agotar el token-prefix.
    """
    by_gestora = defaultdict(list)
    garbage = []
    for f in funds:
        gb = groups.get(f.get("fund_group_id"), {})
        toks = clean_base(f.get("nombre_clase") or gb.get("nombre_base") or "")
        if len(toks) < 2:
            garbage.append(f)
            continue
        f["_toks"] = toks
        by_gestora[gnorm(gb.get("gestora") or "")].append(f)

    splits = []
    for gest, fs in by_gestora.items():
        fs.sort(key=lambda x: len(x["_toks"]))      # base más corta = representante
        clusters = []  # [{rep:[toks], members:[]}]
        for f in fs:
            for c in clusters:
                if _is_prefix(c["rep"], f["_toks"]):
                    c["members"].append(f)
                    break
            else:
                clusters.append({"rep": f["_toks"], "members": [f]})
        for c in clusters:
            gids = {m.get("fund_group_id") for m in c["members"]}
            if len(c["members"]) > 1 and len(gids) > 1:
                splits.append({"key": (gest, ' '.join(c["rep"])), "members": c["members"]})
    return splits, garbage


def _primary(members: list) -> dict:
    """Primario = con análisis cualitativo > con dashboard > grupo más poblado > isin."""
    def score(m):
        return ((1 if m.get("has_qualitative_analysis") else 0),
                (1 if m.get("dashboard_storage_path") else 0))
    return sorted(members, key=lambda m: (score(m), m.get("isin", "")), reverse=True)[0]


def resolve_group_for_new_fund(client, nombre: str, gestora: str, fallback_gid: str):
    """Antes de crear un grupo nuevo para una clase entrante, busca una hermana ya
    en Supabase (misma gestora + nombre prefijo-compatible) y reutiliza SU grupo.
    Así una clase nueva de un fondo ya analizado se agrupa sola. Devuelve el
    fund_group_id a usar (el de la hermana, o `fallback_gid` si no hay)."""
    toks = clean_base(nombre or "")
    if len(toks) < 2:
        return fallback_gid
    gkey = gnorm(gestora or "")
    try:
        funds = client.table("funds").select(
            "fund_group_id,nombre_clase").limit(5000).execute().data
        groups = {g["fund_group_id"]: g for g in client.table("fund_groups").select(
            "fund_group_id,nombre_base,gestora").limit(5000).execute().data}
    except Exception:
        return fallback_gid
    for f in funds:
        gb = groups.get(f.get("fund_group_id"), {})
        if gnorm(gb.get("gestora") or "") != gkey:
            continue
        sib = clean_base(f.get("nombre_clase") or gb.get("nombre_base") or "")
        if len(sib) < 2:
            continue
        if _is_prefix(sib, toks) or _is_prefix(toks, sib):
            return f["fund_group_id"]
    return fallback_gid


def main():
    apply = "--apply" in sys.argv
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    client = get_client()

    funds = client.table("funds").select(
        "isin,nombre_clase,fund_group_id,has_qualitative_analysis,dashboard_storage_path"
    ).limit(5000).execute().data
    groups = {g["fund_group_id"]: g for g in client.table("fund_groups").select(
        "fund_group_id,nombre_base,gestora").limit(5000).execute().data}

    splits, garbage = detect(funds, groups)
    print(f"{'APLICAR' if apply else 'DRY-RUN'} — {len(funds)} fondos, "
          f"{len(splits)} splits recuperables, {len(garbage)} con base basura\n")

    n_updates = 0
    for s in splits:
        members = s["members"]
        primary = _primary(members)
        gid = primary.get("fund_group_id")
        g, base = s["key"]
        print(f"GRUPO [{g} | {base}]  primario={primary['isin']} (grupo {gid[:8]})")
        for m in members:
            if m.get("fund_group_id") == gid:
                print(f"    = {m['isin']}  {m.get('nombre_clase','')!r}  (ya en el grupo)")
                continue
            print(f"    -> {m['isin']}  {m.get('nombre_clase','')!r}  : {str(m.get('fund_group_id'))[:8]} -> {gid[:8]}")
            if apply:
                client.table("funds").update({"fund_group_id": gid}).eq("isin", m["isin"]).execute()
                n_updates += 1
        print()

    if garbage:
        print(f"=== NO agrupables auto (vínculo manual 🔗 o arreglar nombre): {len(garbage)} ===")
        for f in garbage:
            gb = groups.get(f.get("fund_group_id"), {})
            print(f"  {f['isin']} | clase={f.get('nombre_clase')!r} | gestora={gb.get('gestora')!r}")

    if apply:
        print(f"\nAPLICADO: {n_updates} fondos re-asignados a su grupo canónico.")
    else:
        print("\nDRY-RUN: nada modificado. Revisa los merges y ejecuta con --apply.")


if __name__ == "__main__":
    main()
