"""
cleanup_supabase_isins.py — Borra ISINs de Supabase (funds + fund_groups + Storage).

Uso:
    python -m tools.cleanup_supabase_isins FR001400CEK6 FR001400CEG4 LU0168736675 \
        IE00BDR0JY05 LU0289214628

Por seguridad solo borra entries cuyo `is_curated_universe = false`. Si el ISIN
está en el universo curado (Excel maestro), NO lo borra de funds — solo limpia
los campos del backfill (has_qualitative_analysis, dashboard_storage_path, etc.)
para que vuelva a aparecer como "pendiente de analizar".

Borra del Storage:
    dashboards/fund-{ISIN}.html
    analyses/{ISIN}/output.json
    analyses/{ISIN}/letters_data.json
    analyses/{ISIN}/manager_profile.json
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Reconfigure stdout for UTF-8 (Windows cp1252 puede romper con tildes/check marks)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Campos del backfill que se resetean al limpiar (no borrar la fila si is_curated)
BACKFILL_FIELDS_TO_RESET = {
    "has_qualitative_analysis": False,
    "dashboard_storage_path": None,
    "output_json_storage_path": None,
    "letters_data_storage_path": None,
    "manager_profile_storage_path": None,
    "cnmv_data_storage_path": None,
    "fecha_ultimo_analisis": None,
    "cost_run_eur": None,
    "completitud_pct": None,
    "gestores_nombres": None,
    "gestores_perfiles_json": None,
    "top_holdings_json": None,
    "filosofia": None,
    "estrategia": None,
    "historia": None,
    "rendimiento_jsonb": None,
    "portfolio_metrics_jsonb": None,
    "equipo_metrics_jsonb": None,
    "_verified_fields": None,
}

STORAGE_BUCKET = "funds-data"
STORAGE_FILES_PER_ISIN = (
    "dashboards/fund-{isin}.html",
    "analyses/{isin}/output.json",
    "analyses/{isin}/letters_data.json",
    "analyses/{isin}/manager_profile.json",
    "analyses/{isin}/cnmv_data.json",
)


def _get_is_curated(client, fund_group_id: str | None) -> bool:
    """Lee is_curated_universe de la fila fund_groups asociada."""
    if not fund_group_id:
        return False
    try:
        res = client.table("fund_groups").select("is_curated_universe").eq("fund_group_id", fund_group_id).execute()
        rows = res.data or []
        if rows:
            return bool(rows[0].get("is_curated_universe"))
    except Exception:
        pass
    return False


def cleanup_isin(client, isin: str) -> dict:
    """Limpia un ISIN de Supabase. Devuelve dict con stats."""
    stats = {
        "isin": isin,
        "fund_row_action": None,
        "fund_group_action": None,
        "storage_deleted": [],
        "storage_errors": [],
    }

    # 1. Leer fila actual de funds (sin asumir columnas que pueden no existir)
    try:
        res = client.table("funds").select("isin,fund_group_id").eq("isin", isin).execute()
        rows = res.data or []
    except Exception as e:
        stats["fund_row_action"] = f"error_read: {e}"
        return stats

    if not rows:
        stats["fund_row_action"] = "not_found"
        # No hay nada que limpiar de DB, intentamos Storage solo
    else:
        row = rows[0]
        fund_group_id = row.get("fund_group_id")
        is_curated = _get_is_curated(client, fund_group_id)

        if is_curated:
            # Reset campos del backfill, NO borrar fila
            try:
                client.table("funds").update(BACKFILL_FIELDS_TO_RESET).eq("isin", isin).execute()
                stats["fund_row_action"] = "reset_backfill_fields"
            except Exception as e:
                stats["fund_row_action"] = f"error_update: {e}"
        else:
            # Borrar fila completa (huérfano del backfill, no curado)
            try:
                client.table("funds").delete().eq("isin", isin).execute()
                stats["fund_row_action"] = "deleted_row"
            except Exception as e:
                stats["fund_row_action"] = f"error_delete: {e}"

        # Si era huérfano, verificar si fund_group queda sin otras clases → borrar
        if not is_curated and fund_group_id:
            try:
                siblings = client.table("funds").select("isin").eq("fund_group_id", fund_group_id).execute()
                if not (siblings.data or []):
                    client.table("fund_groups").delete().eq("fund_group_id", fund_group_id).execute()
                    stats["fund_group_action"] = "deleted_orphan_group"
                else:
                    stats["fund_group_action"] = f"kept_has_{len(siblings.data)}_siblings"
            except Exception as e:
                stats["fund_group_action"] = f"error_check_group: {e}"

    # 2. Storage
    for tmpl in STORAGE_FILES_PER_ISIN:
        path = tmpl.format(isin=isin)
        try:
            client.storage.from_(STORAGE_BUCKET).remove([path])
            stats["storage_deleted"].append(path)
        except Exception as e:
            msg = str(e)
            # "not found" es OK, no es error
            if "not found" in msg.lower() or "does not exist" in msg.lower():
                stats["storage_deleted"].append(f"{path} (already gone)")
            else:
                stats["storage_errors"].append(f"{path}: {msg[:80]}")

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Limpia ISINs de Supabase (DB + Storage)")
    parser.add_argument("isins", nargs="+", help="ISINs a limpiar")
    parser.add_argument("--dry-run", action="store_true", help="No modifica, solo muestra qué haría")
    args = parser.parse_args()

    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a Supabase: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("=== DRY RUN — no modifica nada ===")
        for isin in args.isins:
            try:
                res = client.table("funds").select("isin,fund_group_id").eq("isin", isin).execute()
                rows = res.data or []
                if not rows:
                    print(f"  {isin}: not_found in funds")
                else:
                    r = rows[0]
                    fund_group_id = r.get("fund_group_id")
                    is_curated = _get_is_curated(client, fund_group_id)
                    action = "reset_backfill" if is_curated else "delete_row + delete_group_if_orphan"
                    print(f"  {isin}: fund_group_id={fund_group_id[:8] if fund_group_id else '?'}… is_curated={is_curated} → {action}")
            except Exception as e:
                print(f"  {isin}: error {e}")
        return 0

    print(f"=== Limpiando {len(args.isins)} ISINs de Supabase ===")
    summary = []
    for isin in args.isins:
        print(f"\n--- {isin} ---")
        s = cleanup_isin(client, isin.upper().strip())
        print(f"  funds row:    {s['fund_row_action']}")
        if s["fund_group_action"]:
            print(f"  fund_group:   {s['fund_group_action']}")
        print(f"  storage:      {len(s['storage_deleted'])} borrados, {len(s['storage_errors'])} errores")
        for p in s["storage_deleted"]:
            print(f"     [OK]   {p}")
        for p in s["storage_errors"]:
            print(f"     [ERR]  {p}")
        summary.append(s)

    print("\n=== Resumen ===")
    print(f"  ISINs procesados:    {len(summary)}")
    n_reset = sum(1 for s in summary if s["fund_row_action"] == "reset_backfill_fields")
    n_deleted = sum(1 for s in summary if s["fund_row_action"] == "deleted_row")
    n_notfound = sum(1 for s in summary if s["fund_row_action"] == "not_found")
    n_storage = sum(len(s["storage_deleted"]) for s in summary)
    print(f"  Filas curated (reset): {n_reset}")
    print(f"  Filas huérfanas (delete): {n_deleted}")
    print(f"  No encontrados:      {n_notfound}")
    print(f"  Storage borrados:    {n_storage}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
