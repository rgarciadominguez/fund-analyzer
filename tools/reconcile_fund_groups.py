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


_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def load_class_isins(funds_dir: str = "data/funds") -> dict:
    """Lee `clases[].isin` de cada fondo analizado (extraído del folleto/AR) →
    {primary_isin: set(sibling_isins)}. Es la señal AUTORITATIVA por ISIN: dos
    ISINs son la misma cartera si aparecen juntos en la lista de clases del
    folleto, no por parecido de nombre."""
    import glob
    import json
    import os
    out = {}
    for f in glob.glob(os.path.join(funds_dir, "*", "output.json")):
        d = os.path.basename(os.path.dirname(f))
        if "." in d or not _ISIN_RE.match(d.upper()):   # salta dirs .bak
            continue
        try:
            data = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        sibs = {d.upper()}
        for c in data.get("clases") or []:
            iv = (c.get("isin") or "").strip().upper()
            if _ISIN_RE.match(iv):
                sibs.add(iv)
        if len(sibs) > 1:
            out[d.upper()] = sibs
    return out


def reconcile_by_isin(client, apply: bool = False):
    """Agrupa por ISIN usando la lista de clases del folleto de cada fondo
    analizado. El grupo canónico = el del fondo analizado (tiene el análisis
    cualitativo). Las clases hermanas presentes en Supabase heredan ese grupo."""
    class_map = load_class_isins()
    supa = {f["isin"].upper(): f for f in client.table("funds").select(
        "isin,nombre_clase,fund_group_id,has_qualitative_analysis").limit(5000).execute().data}

    print(f"{'APLICAR' if apply else 'DRY-RUN'} — {len(class_map)} fondos analizados con "
          f"lista de clases, {len(supa)} fondos en Supabase\n")
    n = 0
    for primary, sibs in sorted(class_map.items()):
        pf = supa.get(primary)
        if not pf or not pf.get("fund_group_id"):
            continue
        gid = pf["fund_group_id"]
        moves = [s for s in sorted(sibs) if s != primary and s in supa
                 and supa[s].get("fund_group_id") != gid]
        if not moves:
            continue
        print(f"FONDO {primary} (grupo {gid[:8]}, {pf.get('nombre_clase','')!r})")
        for s in moves:
            sf = supa[s]
            print(f"    -> {s}  {sf.get('nombre_clase','')!r}  : "
                  f"{str(sf.get('fund_group_id'))[:8]} -> {gid[:8]}")
            if apply:
                client.table("funds").update({"fund_group_id": gid}).eq("isin", s).execute()
                n += 1
        print()
    # Clases del folleto que aún NO están en el catálogo (info, no acción)
    known = {s for sibs in class_map.values() for s in sibs}
    missing = sorted(known - set(supa))
    if missing:
        print(f"=== {len(missing)} clases en folletos AÚN no en catálogo (se agruparán al entrar) ===")
        print("  " + ", ".join(missing[:30]) + (" ..." if len(missing) > 30 else ""))
    print(f"\n{'APLICADO: ' + str(n) + ' clases reagrupadas por ISIN.' if apply else 'DRY-RUN: nada modificado.'}")
    return n


def group_for_isin(new_isin: str) -> str | None:
    """Busca `new_isin` en la lista de clases (folleto) de algún fondo analizado.
    Si aparece, devuelve el ISIN primario de ese fondo (mismo grupo). Señal por
    ISIN, autoritativa. None si no aparece en ningún folleto."""
    new_isin = (new_isin or "").upper().strip()
    for primary, sibs in load_class_isins().items():
        if new_isin in sibs:
            return primary
    return None


def group_for_isin_online(client, new_isin: str) -> str | None:
    """Igual que group_for_isin pero ONLINE: busca `new_isin` en
    `fund_groups.class_isins_known` (poblado desde los folletos). Devuelve el
    fund_group_id, o None."""
    new_isin = (new_isin or "").upper().strip()
    if not _ISIN_RE.match(new_isin):
        return None
    try:
        r = client.table("fund_groups").select("fund_group_id").contains(
            "class_isins_known", [new_isin]).execute()
        if r.data:
            return r.data[0]["fund_group_id"]
    except Exception:
        pass
    return None


def push_class_isins_to_supabase(client, apply: bool = False) -> int:
    """Vuelca los ISINs de clase del folleto de cada fondo analizado a
    `fund_groups.class_isins_known` (union) → ground-truth ONLINE por ISIN, para
    que una clase nueva se agrupe sola consultando Supabase (sin ficheros)."""
    class_map = load_class_isins()
    supa = {f["isin"].upper(): f for f in client.table("funds").select(
        "isin,fund_group_id").limit(5000).execute().data}
    n = 0
    for primary, sibs in sorted(class_map.items()):
        pf = supa.get(primary)
        if not pf or not pf.get("fund_group_id"):
            continue
        gid = pf["fund_group_id"]
        cur = client.table("fund_groups").select("class_isins_known").eq(
            "fund_group_id", gid).execute().data
        known = set(cur[0].get("class_isins_known") or []) if cur else set()
        merged = known | sibs
        if merged != known:
            print(f"  {primary} grupo {gid[:8]}: class_isins_known {len(known)} -> {len(merged)}")
            if apply:
                client.table("fund_groups").update(
                    {"class_isins_known": sorted(merged)}).eq("fund_group_id", gid).execute()
            n += 1
    return n


def resolve_group_for_new_fund(client, nombre: str, gestora: str, fallback_gid: str,
                               new_isin: str = "") -> str:
    """Grupo a usar para una clase entrante. Prioridad:
    1) ISIN en la lista de clases (folleto) de un fondo ya analizado → su grupo.
    2) Hermana en Supabase con nombre prefijo-compatible (misma gestora).
    3) `fallback_gid` (uuid5 determinista del nombre_base)."""
    # 1a) Señal por ISIN ONLINE (fund_groups.class_isins_known del folleto)
    gid_online = group_for_isin_online(client, new_isin)
    if gid_online:
        return gid_online
    # 1b) Señal por ISIN desde folletos locales (fallback si aún no está en Supabase)
    primary = group_for_isin(new_isin)
    if primary:
        try:
            r = client.table("funds").select("fund_group_id").eq("isin", primary).execute()
            if r.data and r.data[0].get("fund_group_id"):
                return r.data[0]["fund_group_id"]
        except Exception:
            pass
    # 2) Heurística de nombre (secundaria)
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
    name_mode = "--name-heuristic" in sys.argv   # opt-in: método de nombre (frágil)
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    client = get_client()

    if not name_mode:
        # Por defecto: agrupar por ISIN desde la lista de clases del folleto.
        print(">>> Volcando ISINs de clase del folleto a fund_groups.class_isins_known (online)")
        pushed = push_class_isins_to_supabase(client, apply=apply)
        print(f"    {pushed} grupos {'actualizados' if apply else 'a actualizar'}\n")
        reconcile_by_isin(client, apply=apply)
        return

    print(">>> MODO NOMBRE (heurístico, frágil — puede fusionar fondos paraguas distintos)\n")
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
