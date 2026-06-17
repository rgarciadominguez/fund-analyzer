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


import re as _re_mod

# Marcadores de texto de test/placeholder que NUNCA deben publicarse (ni con --force).
_TEST_PLACEHOLDER_RE = _re_mod.compile(
    r"\b(BASURA|FALSO|LOREM\s+IPSUM|PLACEHOLDER|XXXX+|FAKE\s+FUND|FONDO\s+FALSO|DUMMY\s+FUND)\b",
    _re_mod.IGNORECASE,
)


def _norm_cmp(s: str) -> str:
    return _re_mod.sub(r"[^a-z0-9]", "", (s or "").lower())


def _has_test_placeholder(output_data: dict) -> bool:
    """True si nombre/gestora o el texto del resumen contienen placeholders de test."""
    parts = [output_data.get("nombre") or "", output_data.get("gestora") or ""]
    try:
        res = ((output_data.get("analyst_synthesis") or {}).get("resumen") or {}).get("texto") or ""
        parts.append(res[:2000])
    except Exception:
        pass
    return bool(_TEST_PLACEHOLDER_RE.search(" \n ".join(parts)))


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
      - análisis cualitativo roto (vacío, todas las secciones vacías o
        contenido fabricado/ilustrativo) — ver tools/analysis_quality.py

    Devuelve (ok, reasons). Si `ok` es False, el sync NO debe ejecutarse
    (a menos que el usuario pase `--force` para bypass).
    """
    reasons: list[str] = []
    nombre = (output_data.get("nombre") or "").strip()
    gestora = (output_data.get("gestora") or "").strip()
    isin_upper = isin.upper()

    # Nombre: validador robusto (detecta prosa, etiquetas/stub, fragmentos, ISIN
    # embebido) en vez de los 4 checks inline débiles que dejaban pasar la
    # gestora/categoría/clase/placeholder como nombre.
    if not nombre:
        reasons.append("nombre vacío")
    else:
        try:
            from tools.fund_name_utils import is_valid_fund_name
            ok_name, motivo = is_valid_fund_name(nombre, isin)
            if not ok_name:
                reasons.append(f"nombre inválido ({motivo}): {nombre!r}")
        except Exception:
            # fallback a los checks mínimos si el validador no carga
            if nombre.upper() == isin_upper or ISIN_FULL_REGEX.match(nombre.upper()) or len(nombre) < 5:
                reasons.append(f"nombre inválido: {nombre!r}")
        # nombre == gestora → casi siempre es la gestora colada como nombre
        if gestora and _norm_cmp(nombre) == _norm_cmp(gestora):
            reasons.append(f"nombre == gestora ({nombre!r}) — probable gestora-como-nombre")

    if not gestora:
        reasons.append("gestora vacía")
    elif gestora.upper() == isin_upper:
        reasons.append("gestora == ISIN")

    # Placeholder de test: bloqueo DURO (lo honra incluso --force, ver sync_fund).
    if _has_test_placeholder(output_data):
        reasons.append("contiene texto de TEST/placeholder (BASURA/FALSO/LOREM/…)")

    # B1/B2/B3 (2026-06-04): no publicar análisis roto como has_qualitative_analysis=true.
    from tools.analysis_quality import assess_analysis_quality
    quality = assess_analysis_quality(output_data)
    for b in quality["blockers"]:
        reasons.append(f"análisis: {b}")

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


SYNC_STATUS_PATH = Path(__file__).resolve().parent.parent / "data" / "sync_status.json"


def _record_sync_status(isin: str, status: str, reasons: list | None = None) -> None:
    """Persiste el último resultado del sync por ISIN (audit trail local).

    Permite responder 'qué fondos analizados NO están publicados y por qué' sin
    depender de leer la consola del run. Best-effort (no rompe el sync si falla)."""
    try:
        from datetime import datetime, timezone
        data = {}
        if SYNC_STATUS_PATH.exists():
            try:
                data = json.loads(SYNC_STATUS_PATH.read_text(encoding="utf-8")) or {}
            except Exception:
                data = {}
        data[(isin or "").upper()] = {
            "status": status,
            "reasons": reasons or [],
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        SYNC_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = SYNC_STATUS_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(SYNC_STATUS_PATH)
    except Exception:
        pass


def sync_fund(
    isin: str,
    dry_run: bool = False,
    verbose: bool = True,
    force: bool = False,
) -> dict:
    """Wrapper de _sync_fund_impl que PERSISTE el estado del sync (ok/aborted/error)
    en data/sync_status.json. Así `tools/audit_sync.py` puede listar en cualquier
    momento los fondos analizados que NO llegaron al catálogo y por qué."""
    try:
        res = _sync_fund_impl(isin, dry_run=dry_run, verbose=verbose, force=force)
    except Exception as e:
        if not dry_run:
            _record_sync_status(isin, "error", [str(e)[:200]])
        raise
    if not dry_run:
        _record_sync_status(isin, "aborted" if res.get("aborted") else "ok",
                            res.get("reasons") or [])
    return res


def _sync_fund_impl(
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

    # Bloqueo DURO de placeholders de test: ni --force lo salta (un análisis de
    # prueba con 'BASURA NOMBRE FALSO TEST' jamás debe llegar al catálogo).
    if _has_test_placeholder(output_data):
        log("=" * 60)
        log(f"[SYNC] [HARD-ABORT] {isin} contiene texto de TEST/placeholder — NO se publica")
        log("  (re-ejecuta el análisis del fondo; esto no se salta ni con --force)")
        log("=" * 60)
        return {"isin": isin, "aborted": True, "reasons": ["test_placeholder"],
                "uploaded": {}, "funds_updated": 0, "fund_groups_updated": False}

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

    # B-INT (2026-06-04): para fondos INT el equipo raw (gestores.equipo) suele
    # estar vacío y los perfiles viven en analyst_synthesis.gestores.perfiles.
    # Fallback: derivar los nombres de ahí para no dejar gestores_nombres vacío
    # en el catálogo cuando el análisis sí tiene equipo.
    if not gestores_nombres and gestores_perfiles:
        for p in gestores_perfiles:
            if isinstance(p, dict) and p.get("nombre"):
                gestores_nombres.append(str(p["nombre"]))

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

        # Si ya hay una clase HERMANA de este fondo en Supabase (misma gestora +
        # nombre prefijo-compatible), reutiliza SU grupo → la clase nueva se
        # agrupa sola sin depender de que el nombre_base coincida exacto.
        try:
            from tools.reconcile_fund_groups import resolve_group_for_new_fund
            fg_id = resolve_group_for_new_fund(client, nombre, gestora, fg_id)
        except Exception as _e:
            log(f"[SYNC] sibling-group lookup falló (no crítico): {str(_e)[:80]}")

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

    # Volcar los ISINs de clase del folleto (clases[].isin) a class_isins_known
    # del grupo → ground-truth ONLINE por ISIN para agrupar clases nuevas.
    try:
        import re as _re
        sibs = sorted({(c.get("isin") or "").upper() for c in (output_data.get("clases") or [])
                       if _re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", (c.get("isin") or "").upper())})
        if len(sibs) > 1:
            r_g = client.table("funds").select("fund_group_id").eq("isin", isin).execute()
            if r_g.data and r_g.data[0].get("fund_group_id"):
                fgid = r_g.data[0]["fund_group_id"]
                cur = client.table("fund_groups").select("class_isins_known").eq(
                    "fund_group_id", fgid).execute().data
                known = set(cur[0].get("class_isins_known") or []) if cur else set()
                merged = sorted(known | set(sibs))
                if merged != sorted(known):
                    client.table("fund_groups").update(
                        {"class_isins_known": merged}).eq("fund_group_id", fgid).execute()
                    log(f"[SYNC] class_isins_known {isin}: {len(known)} -> {len(merged)} ISINs")
    except Exception as _e:
        log(f"[SYNC] class_isins_known update falló (no crítico): {str(_e)[:80]}")

    # Insertar las clases del folleto como filas funds (comparten grupo+cualitativo
    # del primario; cuantitativos por clase) → catálogo las agrupa y despliega.
    try:
        from tools.reconcile_fund_groups import populate_fund_classes
        clases = output_data.get("clases") or []
        if len(clases) > 1:
            ins = populate_fund_classes(client, isin, clases, apply=True,
                                        fund_name=output_data.get("nombre") or "")
            if ins:
                log(f"[SYNC] {ins} clases del folleto insertadas/actualizadas en funds")
    except Exception as _e:
        log(f"[SYNC] populate_fund_classes falló (no crítico): {str(_e)[:80]}")

    # Consumir myinvestor_data.json (creado por la skill cowork myinvestor-enrich)
    # → Supabase (distribución robusta, broker MyInvestor, allocation/sectores/docs).
    try:
        from tools.myinvestor_consume import consume as _mi_consume
        _mi_consume(isin, client=client, log=log)
    except Exception as _e:
        log(f"[SYNC] myinvestor_consume falló (no crítico): {str(_e)[:80]}")

    # Taxonomía derivada (tipo/geo/categoria_rf/srri/tags) de Morningstar+MyInvestor
    # para fondos nuevos — rellena SOLO campos vacíos (no pisa lo curado del Excel).
    try:
        from tools.derive_taxonomy import derive_and_fill
        derive_and_fill(client, isin, log=log)
    except Exception as _e:
        log(f"[SYNC] derive_taxonomy falló (no crítico): {str(_e)[:80]}")

    # Quant desde la serie diaria de Morningstar (consistente con fund-dashboard).
    try:
        from tools.morningstar_daily import compute_metrics as _ms_daily
        r_g = client.table("funds").select("fund_group_id").eq("isin", isin).execute()
        fgid = r_g.data[0]["fund_group_id"] if r_g.data else None
        if fgid:
            m = _ms_daily(isin)
            if m:
                client.table("fund_groups").update({"rendimiento_jsonb": m}).eq("fund_group_id", fgid).execute()
                log(f"[SYNC] quant diario Morningstar: cagr={m.get('cagr_desde_inicio')} vol={m.get('volatilidad')}")
    except Exception as _e:
        log(f"[SYNC] quant diario falló (no crítico): {str(_e)[:80]}")

    # Categoría Morningstar (+ estilo inferido) en el grupo, si falta.
    try:
        from tools.morningstar_quant import fetch_category
        r_g = client.table("funds").select("fund_group_id").eq("isin", isin).execute()
        fgid = r_g.data[0]["fund_group_id"] if r_g.data else None
        if fgid:
            cur = client.table("fund_groups").select(
                "categoria_morningstar,estilo").eq("fund_group_id", fgid).execute().data
            g0 = cur[0] if cur else {}
            if not (g0.get("categoria_morningstar") or "").strip():
                cat = fetch_category(isin)
                upd = {}
                if cat.get("categoria_morningstar"):
                    upd["categoria_morningstar"] = cat["categoria_morningstar"]
                if cat.get("estilo") and not (g0.get("estilo") or "").strip():
                    upd["estilo"] = cat["estilo"]
                if upd:
                    client.table("fund_groups").update(upd).eq("fund_group_id", fgid).execute()
                    log(f"[SYNC] categoria Morningstar: {upd}")
    except Exception as _e:
        log(f"[SYNC] categoria Morningstar falló (no crítico): {str(_e)[:80]}")

    # Fondos ES: listado oficial de clases desde el registro CNMV (por NIF).
    if isin.upper().startswith("ES"):
        try:
            from pathlib import Path as _P
            import json as _json
            nif = output_data.get("nif")
            if not nif:
                _cf = _P("data") / "funds" / isin / "cnmv_data.json"
                if _cf.exists():
                    nif = (_json.loads(_cf.read_text(encoding="utf-8")).get("nif") or "").strip()
            if nif:
                from tools.cnmv_classes import fetch_classes
                from tools.reconcile_fund_groups import populate_fund_classes
                cnmv_cl = fetch_classes(nif)
                if len(cnmv_cl) > 1:
                    ins = populate_fund_classes(client, isin, cnmv_cl, apply=True,
                                                fund_name=output_data.get("nombre") or "")
                    if ins:
                        log(f"[SYNC] {ins} clases CNMV (NIF {nif}) insertadas en funds")
        except Exception as _e:
            log(f"[SYNC] CNMV classes falló (no crítico): {str(_e)[:80]}")
    else:
        # Fondos no-ES: listado de clases desde el screener de Morningstar (por ISIN).
        try:
            from tools.morningstar_classes import fetch_classes as _ms_classes
            from tools.reconcile_fund_groups import populate_fund_classes
            ms_cl = _ms_classes(isin)
            if len(ms_cl) > 1:
                ins = populate_fund_classes(client, isin, ms_cl, apply=True,
                                            fund_name=output_data.get("nombre") or "")
                if ins:
                    log(f"[SYNC] {ins} clases Morningstar insertadas en funds")
        except Exception as _e:
            log(f"[SYNC] Morningstar classes falló (no crítico): {str(_e)[:80]}")

    # años_antiguedad = clase MÁS ANTIGUA. AL FINAL, tras insertar todas las clases
    # (CNMV/Morningstar), para que vea la clase más antigua del grupo aunque la
    # analizada sea más nueva.
    try:
        from tools.anios_antiguedad import fill_anios
        fill_anios(client, isin, output_data=output_data, log=log)
    except Exception as _e:
        log(f"[SYNC] años_antiguedad falló (no crítico): {str(_e)[:80]}")

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
