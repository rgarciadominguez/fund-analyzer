"""consume_inputs_rafa.py — el PORTAL es la fuente de verdad de 5 campos cualitativos que Rafa
valida en el modal de cada fondo: broker (disponibilidad), opinión, encaje, horizonte (plazo) y
clasificación. Este consumidor los LEE del portal por `/inputs-rafa` y los ESCRIBE en Supabase.

Flujo:  portal (Rafa valida) → GET /inputs-rafa → funds/fund_groups (fuente durable) + catalogo_activos
        (reflejo inmediato del catálogo) → el export los propaga (no los pisa).

Reglas INNEGOCIABLES:
  1. El portal manda: si un campo viene con valor, sobrescribe Supabase.
  2. Vacío NO borra: si viene vacío/ausente, NO se toca lo que ya hay (jamás null encima).
Idempotente por ISIN. Incremental con `?since` (mayor updated_at procesado, en _inputs_rafa_state.json).

Mapeo /inputs-rafa → Supabase:
  brokers[].broker (CSV)  → broker_disponible (jsonb array)     [split(',')]
  rows[].opinion          → opinion_user     (+ opinion_origen='portal')
  rows[].encaje           → encaje_texto
  rows[].horizonte        → plazo            (grupo: fund_groups.plazo; + catalogo_activos.plazo)
  rows[].clasificacion    → clasificacion_user (+ clasificacion_origen='portal')
  · SE IGNORA rows[].clasificacion_user entrante (es nuestro propio valor volviendo → evita bucle).

Uso:
  python -m tools.consume_inputs_rafa            # incremental (usa ?since del estado)
  python -m tools.consume_inputs_rafa --full     # ignora ?since, procesa todo
  python -m tools.consume_inputs_rafa --dry-run   # no escribe, solo informa
"""
from __future__ import annotations
import time

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "data" / "_inputs_rafa_state.json"
ENDPOINT_PATH = "/wp-json/horizonte/v1/inputs-rafa"


def _log(msg: str) -> None:
    print(f"[inputs-rafa] {msg}", flush=True)


def _empty(v) -> bool:
    """Vacío = None, '' o solo espacios. Un vacío NUNCA sobrescribe (regla 2)."""
    return v is None or (isinstance(v, str) and v.strip() == "")


def _load_since() -> str | None:
    try:
        return json.loads(STATE.read_text(encoding="utf-8")).get("last_updated_at")
    except Exception:
        return None


def _save_since(ts: str | None) -> None:
    if not ts:
        return
    try:
        STATE.parent.mkdir(parents=True, exist_ok=True)
        STATE.write_text(json.dumps({"last_updated_at": ts,
                                     "saved_at": datetime.utcnow().isoformat()},
                                    ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude guardar estado: {e}")


def fetch(since: str | None) -> dict:
    from tools.portal_analyze_worker import _cfg, _auth_header
    import httpx
    base, user, pwd = _cfg()
    url = f"{base}{ENDPOINT_PATH}"
    params = {"since": since} if since else None
    r = httpx.get(url, headers=_auth_header(user, pwd), params=params, timeout=90)
    r.raise_for_status()
    return r.json()


BROKER_CANON = ("MyInvestor", "Mapfre", "Ironia", "Renta4", "BBVA", "Caixa", "ABANCA", "Santander", "CJRS", "EBN")


def parse_brokers(raw) -> list:
    """CSV o lista → lista LIMPIA de brokers canónicos. Tolera el formato roto que circuló entre
    portal y Supabase (2026-09-17): un JSON '["MyInvestor","Renta4"]' partido por comas daba
    elementos como '["MyInvestor"' y '"Renta4"]' (197 filas). Nunca más: se quitan corchetes y
    comillas de cada trozo, se canonizan mayúsculas y se deduplica conservando el orden."""
    import json as _json
    if raw is None:
        return []
    items = []
    if isinstance(raw, (list, tuple)):
        for x in raw:
            items += parse_brokers(x)
        raw = None
    else:
        s = str(raw).strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                return parse_brokers(_json.loads(s))
            except Exception:
                pass
        items = [t.strip().strip("[]\"' ") for t in s.split(",")]
    canon = {b.lower(): b for b in BROKER_CANON}
    out = []
    for t in items:
        t = (t or "").strip().strip("[]\"' ")
        if not t:
            continue
        t = canon.get(t.lower(), t)
        if t not in out:
            out.append(t)
    return out


def _brokers_by_isin(payload: dict) -> dict:
    """brokers[] ya viene resuelto por ISIN (fondo si tiene, si no la clase). broker = CSV."""
    out: dict[str, list] = {}
    for b in (payload.get("brokers") or []):
        isin = (b.get("isin") or "").upper().strip()
        raw = b.get("broker")
        if not isin or _empty(raw):
            continue
        arr = parse_brokers(raw)
        if arr:
            out[isin] = arr
    return out


_FULL_SCAN_STAMP = Path(__file__).resolve().parent.parent / "data" / ".portal_import_full_scan.json"


def _full_scan_due(hours: int = 24) -> bool:
    try:
        return (time.time() - float(json.loads(_FULL_SCAN_STAMP.read_text(encoding="utf-8")).get("ts", 0))) > hours * 3600
    except Exception:
        return True


def _mark_full_scan() -> None:
    try:
        _FULL_SCAN_STAMP.write_text(json.dumps({"ts": time.time()}), encoding="utf-8")
    except Exception:
        pass


def consume(dry_run: bool = False, full: bool = False) -> dict:
    since = None if full else _load_since()
    _log(f"GET /inputs-rafa {'(full)' if full else f'since={since}'}")
    payload = fetch(since)
    if not payload.get("ok"):
        _log(f"[WARN] respuesta ok=false: {str(payload)[:150]}")
        return {"ok": False}
    rows = payload.get("rows") or []
    brokers = _brokers_by_isin(payload)
    _log(f"recibido: {len(rows)} rows · {len(brokers)} brokers")

    # ── Alta automática (2026-09-23, Rafa): un fondo nuevo en el portal entra en el catálogo solo,
    #    con o sin análisis y con o sin clasificación. Incremental: los ISIN de esta tanda; y una vez
    #    al día, TODOS los del portal (por si un alta antigua se quedó fuera). ──
    try:
        from tools.import_portal_isins import import_missing_from_portal
        isins_tanda = [(r.get("isin") or "").upper().strip() for r in rows]
        if not full and _full_scan_due():
            isins_tanda = sorted({(r.get("isin") or "").upper().strip() for r in (fetch(None).get("rows") or [])} | set(isins_tanda))
            _mark_full_scan()
        alta = import_missing_from_portal(isins_tanda, apply=not dry_run, log=_log)
        if alta.get("altas"):
            _log(f"[alta-portal] dadas de alta {len(alta['altas'])} clases nuevas del portal")
            if not dry_run:   # y al fund-dashboard (todo el catálogo vive allí, con o sin análisis)
                try:
                    from tools.funddash_sync import sync_any, _repo_isins
                    repo_fd = _repo_isins()
                    for a in alta["altas"]:
                        sync_any(a[0], repo_fd)
                except Exception as e:  # noqa: BLE001
                    _log(f"[WARN] fund-dashboard tras alta: {str(e)[:100]}")
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] alta-portal falló (se sigue con los inputs): {str(e)[:120]}")

    from tools.supabase_client import get_client
    client = get_client()

    # Mapa isin → fund_group_id (para reflejar `plazo` en fund_groups, la fuente coarse).
    grp: dict[str, str] = {}
    try:
        off = 0
        while True:
            r = client.table("funds").select("isin,fund_group_id").range(off, off + 999).execute()
            data = r.data or []
            for f in data:
                grp[(f.get("isin") or "").upper()] = f.get("fund_group_id")
            if len(data) < 1000:
                break
            off += 1000
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude cargar fund_group_id: {e}")

    max_ts = since or ""
    n_cat = n_funds = n_groups = 0
    groups_done: set[str] = set()

    for row in rows:
        isin = (row.get("isin") or "").upper().strip()
        if not isin:
            continue
        max_ts = max(max_ts, str(row.get("updated_at") or ""))

        # ── Construir los updates SOLO con campos con valor (regla 2: vacío no borra) ──
        cat: dict = {}
        fnd: dict = {}
        if not _empty(row.get("opinion")):
            cat["opinion_user"] = row["opinion"]; cat["opinion_origen"] = "portal"
            fnd["opinion_user"] = row["opinion"]
        if not _empty(row.get("encaje")):
            cat["encaje_texto"] = row["encaje"]; fnd["encaje_texto"] = row["encaje"]
        # clasificación HORFIN validada por Rafa = rows[].clasificacion (NO clasificacion_user, que es
        # nuestro valor volviendo). Ver cabecera.
        if not _empty(row.get("clasificacion")):
            cat["clasificacion_user"] = row["clasificacion"]; cat["clasificacion_origen"] = "portal"
            fnd["clasificacion_user"] = row["clasificacion"]
        # horizonte → plazo (catálogo por ISIN + fund_groups por grupo)
        if not _empty(row.get("horizonte")):
            cat["plazo"] = row["horizonte"]
        # broker por ISIN (ya resuelto fondo/clase)
        if isin in brokers:
            cat["broker_disponible"] = brokers[isin]
            fnd["broker_disponible"] = brokers[isin]

        if not cat and not fnd:
            continue

        if dry_run:
            _log(f"  [dry] {isin}: cat={list(cat)} funds={list(fnd)}")
        else:
            # UPDATE (no insert): solo tocamos ISINs ya presentes; no creamos filas vacías.
            try:
                if cat:
                    rr = client.table("catalogo_activos").update(cat).eq("isin", isin).execute()
                    n_cat += len(getattr(rr, "data", []) or [])
            except Exception as e:  # noqa: BLE001
                _log(f"  [WARN] catalogo_activos {isin}: {str(e)[:80]}")
            try:
                if fnd:
                    client.table("funds").update(fnd).eq("isin", isin).execute()
                    n_funds += 1
            except Exception as e:  # noqa: BLE001
                _log(f"  [WARN] funds {isin}: {str(e)[:80]}")
            # plazo → fund_groups (una vez por grupo)
            gid = grp.get(isin)
            if not _empty(row.get("horizonte")) and gid and gid not in groups_done:
                try:
                    client.table("fund_groups").update({"plazo": row["horizonte"]}).eq("fund_group_id", gid).execute()
                    groups_done.add(gid); n_groups += 1
                except Exception as e:  # noqa: BLE001
                    _log(f"  [WARN] fund_groups {gid}: {str(e)[:80]}")

    # brokers de ISINs que quizá no vengan en rows (broker sin opinión/etc.)
    for isin, arr in brokers.items():
        if any((r.get("isin") or "").upper() == isin for r in rows):
            continue  # ya procesado arriba
        if dry_run:
            _log(f"  [dry] {isin}: solo broker={arr}")
            continue
        try:
            client.table("catalogo_activos").update({"broker_disponible": arr}).eq("isin", isin).execute()
            client.table("funds").update({"broker_disponible": arr}).eq("isin", isin).execute()
            n_cat += 1
        except Exception as e:  # noqa: BLE001
            _log(f"  [WARN] broker-only {isin}: {str(e)[:80]}")

    # actualizar el mayor updated_at de brokers también (para el ?since)
    for b in (payload.get("brokers") or []):
        max_ts = max(max_ts, str(b.get("updated_at") or ""))

    _log(f"escrito: catalogo_activos {n_cat} · funds {n_funds} · fund_groups {n_groups}")
    if not dry_run and max_ts:
        _save_since(max_ts)
        _log(f"estado: since → {max_ts}")
    return {"ok": True, "rows": len(rows), "brokers": len(brokers),
            "cat": n_cat, "funds": n_funds, "groups": n_groups, "since": max_ts}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="ignora ?since, procesa todo")
    ap.add_argument("--dry-run", action="store_true", help="no escribe, solo informa")
    a = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    res = consume(dry_run=a.dry_run, full=a.full)
    _log(f"FIN: {res}")
    return 0 if res.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
