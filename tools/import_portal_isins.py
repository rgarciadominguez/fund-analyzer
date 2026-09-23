"""Alta en el catálogo de los ISIN que Rafa tiene en el PORTAL y que fund-analyzer no conoce.

Por qué (2026-09-23): el cruce por ISIN portal ↔ fund-analyzer dejó 58 activos del portal (con
clasificación de Rafa) sin fila en `funds`/`catalogo_activos`: ni reciben brokers, ni análisis, ni
pasan al fund-dashboard, y sus inputs del portal (clasificación, opinión, encaje…) no llegan porque
`consume_inputs_rafa` solo actualiza ISIN existentes. Regla de Rafa: meterlos en el catálogo SIN
clasificación cualitativa (no la tienen); cuando se clasifiquen en el portal, el consumidor horario
los actualiza solo (ya funciona así para cualquier ISIN presente en `funds`).

Qué hace por ISIN (determinista, sin LLM):
  1. Morningstar screener (tools.morningstar_classes.fetch_classes): nombre de clase, divisa,
     comisión, TER, fecha de inicio, gestora y categoría Morningstar. Sin resultado → se LISTA y no
     se da de alta (mejor fuera que inventado).
  2. ETF (nombre contiene "ETF") → fuera del catálogo de fondos (se lista).
  3. Grupo: `reconcile_fund_groups.resolve_group_for_new_fund` (ISIN del folleto de un fondo ya
     analizado > hermana por nombre y gestora > uuid5 determinista del nombre base). Si el grupo ya
     existe, la clase se cuelga de él (así el ISIN del portal resuelve al análisis que ya tenemos).
  4. Inserta `fund_groups` (solo nombre_base, gestora, categoria_morningstar; tipo_activo/geografia
     vacíos) y `funds` (cuantitativos de la clase). Nunca pisa filas existentes.
  5. Al final: export_horfin_catalog + catalog_publish (→ catalogo_activos, webhook al portal) y
     consume_inputs_rafa (→ trae YA la clasificación/opinión/broker que Rafa tenga en el portal).

CLI:
    python -m tools.import_portal_isins                 # dry-run: solo lista qué haría
    python -m tools.import_portal_isins --apply         # ejecuta
    python -m tools.import_portal_isins --isin LU... [--apply]
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def _portal_isins() -> list[str]:
    cfg = json.load(open(os.path.expanduser("~/.horizonte-portal.json"), encoding="utf-8"))
    hdr = {"Authorization": "Basic " + base64.b64encode(f"{cfg['usuario']}:{cfg['app_password']}".encode()).decode()}
    d = json.load(urllib.request.urlopen(urllib.request.Request(cfg["base_url"] + "/wp-json/horizonte/v1/inputs-rafa", headers=hdr), timeout=60))
    items = d if isinstance(d, list) else next((v for v in d.values() if isinstance(v, list)), [])
    return sorted({str(x.get("isin") or "").upper() for x in items if _ISIN.match(str(x.get("isin") or "").upper())})


def _hedged(name: str) -> bool:
    return bool(re.search(r"\b(h|hdg|hedged|hedge|cubierta|cubierto)\b|\(h\)|\bH\b", name or "", re.I))


def import_isins(isins: list[str], apply: bool = False, log=print) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    from tools.morningstar_classes import fetch_classes
    from tools.import_taxonomy import normalize_nombre_base, _deterministic_uuid
    from tools.reconcile_fund_groups import resolve_group_for_new_fund
    client = get_client()
    existing = {r["isin"].upper(): r["fund_group_id"] for r in client.table("funds").select("isin,fund_group_id").limit(5000).execute().data}
    groups = {g["fund_group_id"]: g for g in client.table("fund_groups").select("fund_group_id,nombre_base,gestora").limit(8000).execute().data}
    res = {"altas": [], "etf": [], "sin_datos": [], "ya_existian": [], "grupos_nuevos": 0, "grupos_reutilizados": 0}
    for isin in isins:
        if isin in existing:
            res["ya_existian"].append(isin)
            continue
        try:
            clases = fetch_classes(isin)
        except Exception as e:  # noqa: BLE001
            clases = []
            log(f"  {isin}: Morningstar ERROR {str(e)[:60]}")
        time.sleep(0.7)
        me = next((c for c in clases if c.get("isin") == isin), None)
        if not me or not (me.get("nombre_clase") or "").strip():
            res["sin_datos"].append(isin)
            log(f"  {isin}: sin datos en Morningstar → NO se da de alta")
            continue
        nombre = me["nombre_clase"].strip()
        if re.search(r"\bETF\b", nombre, re.I):
            res["etf"].append((isin, nombre))
            log(f"  {isin}: ETF ({nombre}) → fuera del catálogo de fondos")
            continue
        gestora = (me.get("gestora") or "").strip()
        if not gestora:   # fund_groups.gestora es NOT NULL: se deriva del nombre (Waverton Global... → "Waverton")
            try:
                from tools.import_taxonomy import extract_gestora
                gestora = (extract_gestora(nombre) or "").strip()
            except Exception:
                gestora = ""
            gestora = gestora or nombre.split()[0]
        nombre_base = normalize_nombre_base(nombre)
        fallback = _deterministic_uuid(nombre_base, gestora)
        gid = resolve_group_for_new_fund(client, nombre, gestora, fallback, new_isin=isin)
        # ¿alguna hermana (misma FundId en Morningstar) ya está en el catálogo? → su grupo manda
        sib = next((existing[c["isin"]] for c in clases if c.get("isin") in existing), None)
        if sib:
            gid = sib
        nuevo_grupo = gid not in groups
        res["grupos_nuevos" if nuevo_grupo else "grupos_reutilizados"] += 1
        row_group = {"fund_group_id": gid, "nombre_base": nombre_base, "gestora": gestora,
                     "categoria_morningstar": me.get("categoria_morningstar") or None}
        row_fund = {"isin": isin, "fund_group_id": gid, "nombre_clase": nombre,
                    "divisa": me.get("divisa") or None, "divisa_hedge_bool": _hedged(nombre),
                    "comision_gestion_pct": me.get("comision_gestion_pct"), "ter_pct": me.get("ter_pct"),
                    "fecha_creacion_clase": me.get("fecha_inicio"), "has_qualitative_analysis": False}
        log(f"  {isin}: {nombre[:55]} | {gestora[:25]} | {'GRUPO NUEVO' if nuevo_grupo else 'grupo existente: ' + (groups[gid].get('nombre_base') or '')[:40]}")
        res["altas"].append((isin, nombre, gestora, gid, nuevo_grupo))
        if apply:
            if nuevo_grupo:
                client.table("fund_groups").insert({k: v for k, v in row_group.items() if v not in (None, "")}).execute()
                groups[gid] = row_group
            client.table("funds").insert({k: v for k, v in row_fund.items() if v is not None}).execute()
            existing[isin] = gid
    return res


_SIN_DATOS = ROOT / "data" / ".portal_isins_sin_datos.json"   # isin → ts del último intento fallido
_RETRY_DAYS = 30


def _sin_datos_load() -> dict:
    try:
        return json.loads(_SIN_DATOS.read_text(encoding="utf-8"))
    except Exception:
        return {}


def import_missing_from_portal(portal_isins: list[str], apply: bool, log=print, max_n: int = 40) -> dict:
    """Entrada AUTOMÁTICA (consume_inputs_rafa, cada hora en el servidor): ISIN del portal que no
    están en `funds` → alta. Los que Morningstar no conoce se anotan y no se reintentan hasta pasados
    _RETRY_DAYS (ETF, clases institucionales sin ficha, códigos que no son ISIN). Tope por pasada."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    existing = {r["isin"].upper() for r in get_client().table("funds").select("isin").limit(8000).execute().data}
    skip = _sin_datos_load()
    now = time.time()
    cand = [i for i in portal_isins if _ISIN.match(i) and i not in existing
            and (now - float(skip.get(i, 0))) > _RETRY_DAYS * 86400][:max_n]
    if not cand:
        return {"altas": [], "sin_datos": [], "etf": []}
    log(f"[alta-portal] {len(cand)} ISIN del portal no están en el catálogo → alta {'(dry-run)' if not apply else ''}")
    res = import_isins(cand, apply=apply, log=log)
    if apply:
        for i in res["sin_datos"] + [e[0] for e in res["etf"]]:
            skip[i] = now
        try:
            _SIN_DATOS.write_text(json.dumps(skip, indent=1), encoding="utf-8")
        except Exception:
            pass
        if res["altas"]:
            _post_steps(log, steps=("export", "publish"))   # el consumer que nos llama aplica los inputs después
    return res


def _post_steps(log=print, steps=("export", "publish", "consume")) -> None:
    """Catálogo → tabla → portal, y traer los inputs de Rafa para los ISIN recién dados de alta."""
    exe = sys.executable
    cmds = {"export": ["-m", "tools.export_horfin_catalog"], "publish": ["-m", "tools.catalog_publish", "--apply"],
            "consume": ["-m", "tools.consume_inputs_rafa"]}
    for cmd in (cmds[k] for k in steps):
        log(f"[post] {' '.join(cmd)}")
        r = subprocess.run([exe, *cmd], cwd=str(ROOT), capture_output=True, text=True, timeout=900, encoding="utf-8", errors="replace")
        tail = "\n".join((r.stdout or "").strip().splitlines()[-3:])
        log(f"       rc={r.returncode} {tail[:300]}")


def main(argv=None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--isin", action="append", help="solo estos ISIN (repetible)")
    a = ap.parse_args(argv)
    isins = [i.upper() for i in (a.isin or [])] or _portal_isins()
    print(f"{'APLICAR' if a.apply else 'DRY-RUN'}: {len(isins)} ISIN candidatos")
    res = import_isins(isins, apply=a.apply)
    print(f"\naltas: {len(res['altas'])} (grupos nuevos {res['grupos_nuevos']}, colgadas de grupo existente {res['grupos_reutilizados']}) | "
          f"ETF fuera: {len(res['etf'])} | sin datos: {len(res['sin_datos'])} {res['sin_datos']} | ya existían: {len(res['ya_existian'])}")
    if a.apply and res["altas"]:
        _post_steps()
    return 0


if __name__ == "__main__":
    sys.exit(main())
