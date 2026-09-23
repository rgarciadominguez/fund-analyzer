"""Nombre CANÓNICO de una clase de fondo — una sola fuente de verdad para todo lo que se publica.

Por qué (2026-09-23, Rafa: "Carmignac guardado sin nombre en fund-dashboard"): el puente
fund-dashboard tomaba `output.json["nombre"]` en el PRIMER sync y luego era "aditivo" (nunca
corregía). Ese campo, en las primeras fases del pipeline, puede ser el ISIN, la GESTORA
("Fortune Financial Strategies SA"), un BENCHMARK ("SOFR compuesto") o un nombre de otra clase.
Resultado: 19 fondos con nombre vacío/erróneo en fund-dashboard y 9 clases con nombre-basura
("Z", "I", "MASTER") en nuestra BDD.

Precedencia (de más a menos fiable), saltando lo que no pase `is_garbage`:
  1. override del usuario (data/funds/{ISIN}/funddash_meta.json → name)
  2. Supabase funds.nombre_clase (Morningstar/folleto, curado)
  3. Morningstar screener (Name por ISIN exacto) — repara también nuestra BDD
  4. fund_groups.nombre_base (+ código de clase si se conoce)
  5. output.json["nombre"]
  6. el ISIN (último recurso, y se marca)

CLI: python -m tools.fund_names ISIN            → muestra decisión y fuente
     python -m tools.fund_names --fix-catalog   → repara nombre_clase basura en Supabase (funds)
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_BENCH = re.compile(r"\b(sofr|€str|ester|euribor|libor|compuesto|compounded|msci|s&p|stoxx|ibex|index total return)\b", re.I)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def is_garbage(name: str, isin: str = "", gestora: str = "") -> str | None:
    """Motivo por el que `name` NO sirve como nombre de clase, o None si sirve."""
    n = (name or "").strip()
    if not n:
        return "vacío"
    if _ISIN.match(n.upper()) or _norm(n) == _norm(isin):
        return "es el ISIN"
    if len(n) < 8 or len(n.split()) < 2:
        return "demasiado corto (código de clase suelto)"
    if gestora and _norm(n) == _norm(gestora):
        return "es la gestora"
    if _BENCH.search(n) and not re.search(r"\b(fund|fondo|fi|fcp|sicav|ucits|icav|portfolio|bond|equity|acc|inc)\b", n, re.I):
        return "parece un benchmark"
    if " desde " in n.lower() or n.endswith((":", "...")):
        return "es prosa"
    return None


def _supabase_row(isin: str):
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        c = get_client()
        r = c.table("funds").select("isin,nombre_clase,fund_group_id").eq("isin", isin).execute().data
        if not r:
            return None, None
        g = c.table("fund_groups").select("nombre_base,gestora").eq("fund_group_id", r[0]["fund_group_id"]).execute().data
        return r[0], (g[0] if g else {})
    except Exception:
        return None, None


def morningstar_name(isin: str) -> str | None:
    try:
        from tools.morningstar_daily import resolve_security
        sec = resolve_security(isin)
        return (sec or {}).get("name") or None
    except Exception:
        return None


def canonical_name(isin: str, output: dict | None = None, log=None) -> dict:
    """→ {name, fuente, motivo_descartes:[...]}"""
    isin = (isin or "").upper().strip()
    desc = []
    ov = ROOT / "data" / "funds" / isin / "funddash_meta.json"
    if ov.exists():
        try:
            n = (json.loads(ov.read_text(encoding="utf-8")).get("name") or "").strip()
            if n and not is_garbage(n, isin):
                return {"name": n, "fuente": "override usuario", "descartes": desc}
        except Exception:
            pass
    row, grp = _supabase_row(isin)
    gestora = (grp or {}).get("gestora") or ""
    if row:
        n = (row.get("nombre_clase") or "").strip()
        g = is_garbage(n, isin, gestora)
        if not g:
            return {"name": n, "fuente": "supabase funds.nombre_clase", "descartes": desc}
        desc.append(f"supabase '{n}': {g}")
    n = morningstar_name(isin)
    if n and not is_garbage(n, isin, gestora):
        return {"name": n, "fuente": "morningstar", "descartes": desc}
    if n:
        desc.append(f"morningstar '{n}': {is_garbage(n, isin, gestora)}")
    base = ((grp or {}).get("nombre_base") or "").strip()
    if base and not is_garbage(base, isin, gestora):
        code = (row or {}).get("nombre_clase") or ""
        code = code if (code and len(code) <= 8 and code.upper() == code) else ""
        return {"name": (base + (" " + code if code else "")).strip(), "fuente": "fund_groups.nombre_base", "descartes": desc}
    if output is None:
        op = ROOT / "data" / "funds" / isin / "output.json"
        if op.exists():
            try:
                output = json.loads(op.read_text(encoding="utf-8"))
            except Exception:
                output = None
    if output:
        n = (output.get("nombre") or "").strip()
        g = is_garbage(n, isin, gestora or (output.get("gestora") or ""))
        if not g:
            return {"name": n, "fuente": "output.json", "descartes": desc}
        desc.append(f"output '{n}': {g}")
    return {"name": isin, "fuente": "ISIN (sin nombre fiable)", "descartes": desc}


def fix_catalog(log=print) -> dict:
    """Repara en Supabase `funds.nombre_clase` cuando es basura, usando Morningstar (nombre por ISIN
    exacto) y, si no, nombre_base + código. Aditivo: solo toca filas con nombre malo."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()
    rows, off = [], 0
    while True:
        b = c.table("funds").select("isin,nombre_clase,fund_group_id").range(off, off + 999).execute().data or []
        rows += b
        if len(b) < 1000:
            break
        off += 1000
    groups = {g["fund_group_id"]: g for g in c.table("fund_groups").select("fund_group_id,nombre_base,gestora").limit(8000).execute().data or []}
    fixed, left = [], []
    for r in rows:
        isin = r["isin"].upper()
        g = groups.get(r.get("fund_group_id")) or {}
        why = is_garbage(r.get("nombre_clase") or "", isin, g.get("gestora") or "")
        if not why:
            continue
        n = morningstar_name(isin)
        src = "morningstar"
        if not n or is_garbage(n, isin, g.get("gestora") or ""):
            base = (g.get("nombre_base") or "").strip()
            code = (r.get("nombre_clase") or "").strip()
            n = (base + (" " + code if code and len(code) <= 8 else "")).strip() if base and not is_garbage(base, isin) else None
            src = "nombre_base+código"
        if not n:
            left.append((isin, r.get("nombre_clase"), why)); continue
        c.table("funds").update({"nombre_clase": n}).eq("isin", isin).execute()
        try:
            c.table("catalogo_activos").update({"nombre": n}).eq("isin", isin).execute()
        except Exception:
            pass
        fixed.append((isin, r.get("nombre_clase"), n, src))
        log(f"  {isin}: '{r.get('nombre_clase')}' → '{n}' ({src})")
    log(f"[fund_names] reparados {len(fixed)} | sin solución {len(left)}: {left}")
    return {"fixed": fixed, "left": left}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    a = sys.argv[1:]
    if "--fix-catalog" in a:
        fix_catalog()
    else:
        for isin in a:
            print(isin, json.dumps(canonical_name(isin), ensure_ascii=False))
