"""
sync_to_supabase.py — Sube outputs de un análisis cualitativo a Supabase
tras cada ejecución de analizar_fondo.bat.

Se invoca al final del bat con el ISIN:
    python -m tools.sync_to_supabase ES0119199000

Acciones:
1. Lee data/funds/{ISIN}/output.json + companions
2. Sube dashboard/fund-{ISIN}.html a Supabase Storage (bucket 'funds-data')
3. Sube output.json + cnmv_data.json + letters_data.json + manager_profile.json
4. Upsert/update en tabla `funds`:
   - has_qualitative_analysis = true
   - dashboard_storage_path
   - output_json_storage_path
   - fecha_ultimo_analisis
5. Update en tabla `fund_groups`:
   - gestores_nombres, gestores_perfiles_json (del manager_profile)
   - top_holdings_json (de posiciones)
   - filosofia, estrategia, historia (de analyst_synthesis)
   - aum_meur, num_participes (de KPIs CNMV)
   - fecha_ultimo_analisis, cost_run_eur
6. Insert en tabla `analysis_runs` (audit trail)

REQUISITOS:
- Bucket `funds-data` debe existir en Supabase Storage (privado)
- .env con SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (para escribir)

Uso desde el bat (paso 8 nuevo del flow):
    python -m tools.sync_to_supabase %ISIN%

Uso manual:
    python -m tools.sync_to_supabase ES0119199000
    python -m tools.sync_to_supabase ES0119199000 --dry-run
    python -m tools.sync_to_supabase --backfill-all   # sube todos los análisis existentes
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Forzar UTF-8 en stdout/stderr para Windows PowerShell
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, Exception):
    pass

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FUNDS_DIR = DATA_DIR / "funds"
DASHBOARD_DIR = ROOT / "dashboard"

BUCKET_NAME = "funds-data"
ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")
ISIN_FULL_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _validate_before_sync(output_data: dict, isin: str) -> tuple[bool, list[str]]:
    """T2.9 (2026-05-28): comprueba que output.json no tiene fallos críticos
    antes de subir a Supabase.

    Criterios bloqueantes (cualquier de ellos aborta el sync):
      - nombre vacío
      - nombre == ISIN
      - nombre parece otro ISIN (regex)
      - nombre < 5 caracteres
      - gestora vacía
      - gestora == ISIN

    Devuelve (ok, reasons). Si `ok` es False, el sync NO debe ejecutarse
    (a menos que el usuario pase `--force` para bypass).
    """
    reasons: list[str] = []
    nombre = (output_data.get("nombre") or "").strip()
    gestora = (output_data.get("gestora") or "").strip()
    isin_upper = isin.upper()

    if not nombre:
        reasons.append("nombre vacío")
    elif nombre.upper() == isin_upper:
        reasons.append(f"nombre == ISIN ({isin})")
    elif ISIN_FULL_REGEX.match(nombre.upper()):
        reasons.append(f"nombre parece otro ISIN: {nombre!r}")
    elif len(nombre) < 5:
        reasons.append(f"nombre muy corto: {nombre!r}")

    if not gestora:
        reasons.append("gestora vacía")
    elif gestora.upper() == isin_upper:
        reasons.append("gestora == ISIN")

    return (len(reasons) == 0, reasons)


def _safe_read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] No se pudo leer {path}: {e}")
        return None


def _upload_file_to_storage(client, bucket: str, dest_path: str, local_path: Path, content_type: str = None) -> str | None:
    """Sube un archivo local a Supabase Storage. Devuelve dest_path o None.

    Fix 2026-05-28: usar API REST directa con header HTTP `Content-Type`
    correcto. El SDK Python (file_options.contentType) no actualizaba el
    Content-Type al hacer upsert sobre archivos existentes, así que
    Supabase servía todo como text/plain y los dashboards HTML salían
    como código fuente en lugar de renderizarse.
    """
    if not local_path.exists():
        print(f"[SYNC] [SKIP] No existe: {local_path}")
        return None
    try:
        import os, urllib.request
        body = local_path.read_bytes()
        # Construir URL REST: <base>/storage/v1/object/<bucket>/<path>
        base = os.environ.get("SUPABASE_URL", "").rstrip("/")
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
        if not base or not key:
            raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY no en env")
        url = f"{base}/storage/v1/object/{bucket}/{dest_path}"
        ct = content_type or "application/octet-stream"
        req = urllib.request.Request(
            url, data=body, method="PUT",
            headers={
                "Authorization": f"Bearer {key}",
                "apikey": key,
                "Content-Type": ct,
                "x-upsert": "true",
            },
        )
        urllib.request.urlopen(req, timeout=60)
        return dest_path
    except Exception as e:
        print(f"[SYNC] [ERROR] Subiendo {dest_path}: {e}")
        return None


def sync_fund(
    isin: str,
    dry_run: bool = False,
    verbose: bool = True,
    force: bool = False,
) -> dict:
    """Sync completo de un fondo a Supabase. Devuelve resumen.

    T2.9 (2026-05-28): bloquea por defecto si output.json tiene fallos críticos
    (nombre==ISIN/vacío, gestora==ISIN/vacía). Pasa `force=True` para bypass.
    """

    def log(msg):
        if verbose:
            print(msg)

    isin = isin.strip().upper()
    if not ISIN_REGEX.match(isin):
        raise ValueError(f"ISIN inválido: {isin}")

    fund_dir = FUNDS_DIR / isin
    if not fund_dir.exists():
        raise FileNotFoundError(f"No existe {fund_dir}")

    output_json_path = fund_dir / "output.json"
    if not output_json_path.exists():
        raise FileNotFoundError(f"No existe {output_json_path}")

    output_data = _safe_read_json(output_json_path) or {}

    # T2.9: guard pre-sync — bloquea si datos críticos están corruptos
    ok, reasons = _validate_before_sync(output_data, isin)
    if not ok and not force:
        log("=" * 60)
        log(f"[SYNC] [ABORT] {isin} NO se sincroniza — fallos críticos en output.json:")
        for r in reasons:
            log(f"  ✗ {r}")
        log("")
        log("Arregla manualmente (botón ✏ en el catalog) o re-ejecuta el")
        log(f"análisis (🔄). Para forzar sync con los datos actuales:")
        log(f"  python -m tools.sync_to_supabase {isin} --force")
        log("=" * 60)
        return {
            "isin": isin,
            "aborted": True,
            "reasons": reasons,
            "uploaded": {},
            "funds_updated": 0,
            "fund_groups_updated": False,
        }
    elif not ok and force:
        log(f"[SYNC] [WARN] --force activo — subiendo aunque hay {len(reasons)} fallo(s):")
        for r in reasons:
            log(f"  ⚠ {r}")

    cnmv_data = _safe_read_json(fund_dir / "cnmv_data.json") or {}
    letters_data = _safe_read_json(fund_dir / "letters_data.json") or {}
    manager_profile = _safe_read_json(fund_dir / "manager_profile.json") or {}

    dashboard_html_path = DASHBOARD_DIR / f"fund-{isin}.html"

    log(f"[SYNC] Iniciando sync de {isin}...")

    # Cliente Supabase (lazy import)
    if dry_run:
        log("[SYNC] [DRY-RUN] No se sube nada, solo simula")
        client = None
    else:
        from tools.supabase_client import get_client
        client = get_client()
        if client is None:
            raise RuntimeError("Supabase client no disponible. Revisa .env")

    # Storage paths (estructura del bucket)
    storage_paths = {
        "dashboard": f"dashboards/fund-{isin}.html",
        "output": f"analyses/{isin}/output.json",
        "cnmv": f"analyses/{isin}/cnmv_data.json",
        "letters": f"analyses/{isin}/letters_data.json",
        "manager": f"analyses/{isin}/manager_profile.json",
    }

    uploaded = {}
    if not dry_run and client:
        # Subir cada archivo
        for key, dest in storage_paths.items():
            local_map = {
                "dashboard": dashboard_html_path,
                "output": output_json_path,
                "cnmv": fund_dir / "cnmv_data.json",
                "letters": fund_dir / "letters_data.json",
                "manager": fund_dir / "manager_profile.json",
            }
            content_types = {
                "dashboard": "text/html",
                "output": "application/json",
                "cnmv": "application/json",
                "letters": "application/json",
                "manager": "application/json",
            }
            local = local_map[key]
            if local.exists():
                result = _upload_file_to_storage(
                    client, BUCKET_NAME, dest, local, content_types[key]
                )
                uploaded[key] = result
                log(f"[SYNC] [{'OK' if result else 'FAIL'}] {dest} ({local.stat().st_size} bytes)")

    # Construir update para tabla `funds`
    funds_update = {
        "has_qualitative_analysis": True,
        "dashboard_storage_path": storage_paths["dashboard"] if uploaded.get("dashboard") else None,
        "output_json_storage_path": storage_paths["output"] if uploaded.get("output") else None,
        "cnmv_data_storage_path": storage_paths["cnmv"] if uploaded.get("cnmv") else None,
        "letters_data_storage_path": storage_paths["letters"] if uploaded.get("letters") else None,
        "manager_profile_storage_path": storage_paths["manager"] if uploaded.get("manager") else None,
    }

    # T2.6 (2026-05-28): sync nombre_clase si output.json tiene un nombre real
    # (no el ISIN crudo). Sin esto, fund_groups quedaba con nombre_base=ISIN
    # aunque output.json ya tuviera el nombre correcto tras name_recovery.
    nombre_out = (output_data.get("nombre") or "").strip()
    if nombre_out and nombre_out.upper() != isin.upper():
        funds_update["nombre_clase"] = nombre_out

    # KPIs específicos de clase desde output.json (CNMV)
    kpis = output_data.get("kpis", {}) or {}
    if kpis.get("ter_pct") is not None:
        funds_update["ter_pct"] = kpis.get("ter_pct")
    if kpis.get("coste_gestion_pct") is not None:
        funds_update["comision_gestion_pct"] = kpis.get("coste_gestion_pct")
    if kpis.get("divisa"):
        funds_update["divisa"] = kpis.get("divisa")

    # Construir update para tabla `fund_groups` — defensive: algunos output.json
    # antiguos tienen campos heterogéneos (string en lugar de dict).
    def _safe_get_dict(d, key):
        """Devuelve d[key] si es dict, sino {}"""
        if not isinstance(d, dict):
            return {}
        v = d.get(key)
        return v if isinstance(v, dict) else {}

    def _safe_get_list(d, key):
        """Devuelve d[key] si es list, sino []"""
        if not isinstance(d, dict):
            return []
        v = d.get(key)
        return v if isinstance(v, list) else []

    def _safe_get_str(d, key):
        """Devuelve d[key] si es str non-empty, sino None"""
        if not isinstance(d, dict):
            return None
        v = d.get(key)
        if isinstance(v, str) and v.strip():
            return v
        return None

    analyst = _safe_get_dict(output_data, "analyst_synthesis")
    gestores = _safe_get_dict(output_data, "gestores")

    # gestores_perfiles: analyst.gestores.perfiles (con safe getters)
    analyst_gestores = _safe_get_dict(analyst, "gestores")
    gestores_perfiles = _safe_get_list(analyst_gestores, "perfiles")

    # posiciones: output_data.posiciones.actuales
    posiciones_dict = _safe_get_dict(output_data, "posiciones")
    posiciones = _safe_get_list(posiciones_dict, "actuales")

    # gestores_nombres: gestores.equipo (lista de dicts con campo nombre)
    gestores_equipo = _safe_get_list(gestores, "equipo")
    gestores_nombres = []
    for g in gestores_equipo:
        if isinstance(g, dict) and g.get("nombre"):
            gestores_nombres.append(str(g["nombre"]))
        elif isinstance(g, str) and g.strip():
            gestores_nombres.append(g.strip())

    # filosofia / estrategia / historia: pueden estar en analyst_synthesis.{seccion}.texto
    resumen = _safe_get_dict(analyst, "resumen")
    estrategia_dict = _safe_get_dict(analyst, "estrategia")
    historia_dict = _safe_get_dict(analyst, "historia")

    fund_groups_update = {
        "aum_meur": kpis.get("aum_actual_meur") if isinstance(kpis, dict) else None,
        "num_participes": kpis.get("num_participes") if isinstance(kpis, dict) else None,
        "gestores_nombres": gestores_nombres,
        "gestores_perfiles_json": gestores_perfiles if gestores_perfiles else None,
        "top_holdings_json": posiciones[:25] if posiciones else None,
        "filosofia": _safe_get_str(resumen, "filosofia_inversion"),
        "estrategia": _safe_get_str(estrategia_dict, "texto"),
        "historia": _safe_get_str(historia_dict, "texto"),
        "fecha_ultimo_analisis": datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(output_data, dict) and output_data.get("gestora"):
        gestora_val = str(output_data["gestora"]).strip()
        # No sobreescribir con el ISIN (caso INT identity card vacía)
        if gestora_val and gestora_val.upper() != isin.upper():
            fund_groups_update["gestora"] = gestora_val

    # T2.6 (2026-05-28): sync nombre_base si output.json tiene un nombre real.
    # Sin esto, el catalog mostraba el ISIN aunque output.json ya tuviera
    # nombre correcto (gracias a regulator_router, intl_extractor, o
    # name_recovery). Usa normalize_nombre_base para extraer la parte
    # sin clase comercial ("X - Action A" → "X").
    if nombre_out and nombre_out.upper() != isin.upper():
        try:
            from tools.import_taxonomy import normalize_nombre_base
            fund_groups_update["nombre_base"] = normalize_nombre_base(nombre_out)
        except Exception:
            fund_groups_update["nombre_base"] = nombre_out

    if dry_run:
        log(f"[SYNC] [DRY-RUN] Sample funds update: {list(funds_update.keys())}")
        log(f"[SYNC] [DRY-RUN] Sample fund_groups update: {list(fund_groups_update.keys())}")
        return {"dry_run": True, "uploaded": uploaded, "funds_update_keys": list(funds_update.keys())}

    # Update tabla `funds`
    log(f"[SYNC] Updating funds[{isin}]...")
    r1 = client.table("funds").update(funds_update).eq("isin", isin).execute()
    funds_updated = len(getattr(r1, "data", []) or [])

    # Si funds no tenía esta fila (no estaba en taxonomía), insertar
    if funds_updated == 0:
        # Necesitamos fund_group_id — inferimos uno determinístico
        from tools.import_taxonomy import (
            normalize_nombre_base, extract_gestora, _deterministic_uuid
        )
        nombre = output_data.get("nombre") or isin
        gestora = output_data.get("gestora") or extract_gestora(nombre)
        nombre_base = normalize_nombre_base(nombre)
        fg_id = _deterministic_uuid(nombre_base, gestora)

        # Insertar fund_group si no existe
        client.table("fund_groups").upsert({
            "fund_group_id": fg_id,
            "nombre_base": nombre_base,
            "gestora": gestora,
            **fund_groups_update,
        }, on_conflict="fund_group_id").execute()

        # Insertar fund
        client.table("funds").upsert({
            "isin": isin,
            "fund_group_id": fg_id,
            "nombre_clase": nombre,
            **funds_update,
        }, on_conflict="isin").execute()
        log(f"[SYNC] Insertado nuevo fund + fund_group: {isin}")
    else:
        # Update también fund_groups
        log(f"[SYNC] Updating fund_groups del ISIN {isin}...")
        # Obtener fund_group_id del fund actualizado
        r_get = client.table("funds").select("fund_group_id").eq("isin", isin).execute()
        if r_get.data:
            fg_id = r_get.data[0]["fund_group_id"]
            client.table("fund_groups").update(fund_groups_update).eq("fund_group_id", fg_id).execute()

    log(f"[SYNC] [OK] Sync OK: {isin} | uploaded={sum(1 for v in uploaded.values() if v)}/{len(uploaded)} archivos")

    return {
        "isin": isin,
        "uploaded": uploaded,
        "funds_updated": funds_updated,
        "fund_groups_updated": True,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Sync output de análisis a Supabase (Storage + tables)"
    )
    parser.add_argument("isin", nargs="?", help="ISIN del fondo a sincronizar")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass validación pre-sync (sube aunque nombre==ISIN, gestora==ISIN, etc.). "
             "Solo para emergencias.",
    )
    parser.add_argument(
        "--backfill-all",
        action="store_true",
        help="Sube todos los fondos con output.json existente (no acepta ISIN). "
             "Los abortados por validación cuentan como skip, no como error.",
    )
    args = parser.parse_args()

    verbose = not args.quiet

    if args.backfill_all:
        if not FUNDS_DIR.exists():
            print(f"[ERROR] No existe {FUNDS_DIR}")
            return 1
        n_ok, n_err, n_aborted = 0, 0, 0
        aborted_list = []
        for fund_dir in sorted(FUNDS_DIR.iterdir()):
            if not fund_dir.is_dir():
                continue
            isin = fund_dir.name
            if not ISIN_REGEX.match(isin):
                continue
            if not (fund_dir / "output.json").exists():
                continue
            try:
                res = sync_fund(isin, dry_run=args.dry_run,
                                verbose=verbose, force=args.force)
                if res.get("aborted"):
                    n_aborted += 1
                    aborted_list.append(isin)
                else:
                    n_ok += 1
            except Exception as e:
                print(f"[ERROR] {isin}: {e}", file=sys.stderr)
                n_err += 1
        print(f"\n[BACKFILL] OK: {n_ok}, errores: {n_err}, abortados: {n_aborted}")
        if aborted_list:
            print(f"[BACKFILL] Abortados (datos críticos malos): {', '.join(aborted_list)}")
            print(f"[BACKFILL] Re-ejecuta esos análisis o usa --force para forzar.")
        return 0 if n_err == 0 else 1

    if not args.isin:
        parser.error("isin requerido (o usa --backfill-all)")

    try:
        res = sync_fund(args.isin, dry_run=args.dry_run,
                       verbose=verbose, force=args.force)
        if res.get("aborted"):
            # Exit 2: bloqueado por validación (distinto de 1 = error técnico)
            return 2
        return 0
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
