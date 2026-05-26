"""
fix_storage_content_types.py — Re-sube los HTMLs en Supabase Storage con
content-type correcto.

Problema: archivos subidos previamente quedaron servidos con Content-Type:
text/plain, por lo que el browser los muestra como código fuente en lugar
de renderizarlos.

Uso:
    python -m tools.fix_storage_content_types               # todos los fund-*.html
    python -m tools.fix_storage_content_types FR001400CEK6  # solo uno
    python -m tools.fix_storage_content_types --dry-run     # listar sin tocar
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BUCKET = "funds-data"


def fix_isin(client, isin: str, dry_run: bool = False) -> tuple[bool, str]:
    """Re-sube dashboards/fund-{ISIN}.html con content-type correcto."""
    storage_path = f"dashboards/fund-{isin}.html"
    local_path = ROOT / "dashboard" / f"fund-{isin}.html"

    if not local_path.exists():
        return False, f"local missing: {local_path}"

    if dry_run:
        return True, f"DRY: would re-upload {storage_path}"

    try:
        with local_path.open("rb") as f:
            file_bytes = f.read()
        # IMPORTANTE: file_options usa kebab-case "content-type"
        # y "upsert" como string "true" en supabase-py 2.x
        options = {
            "content-type": "text/html; charset=utf-8",
            "upsert": "true",
            "cache-control": "no-cache, no-store, must-revalidate",
        }
        try:
            client.storage.from_(BUCKET).update(
                path=storage_path,
                file=file_bytes,
                file_options=options,
            )
            return True, f"updated {storage_path} ({len(file_bytes)} bytes)"
        except Exception:
            client.storage.from_(BUCKET).upload(
                path=storage_path,
                file=file_bytes,
                file_options=options,
            )
            return True, f"uploaded {storage_path} ({len(file_bytes)} bytes)"
    except Exception as e:
        return False, f"error: {e}"


def list_local_isins() -> list[str]:
    """Lista los ISINs con dashboard HTML local."""
    isins = []
    for f in (ROOT / "dashboard").glob("fund-*.html"):
        name = f.name
        if name.startswith("fund-") and name.endswith(".html"):
            isins.append(name[5:-5])
    return sorted(isins)


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-sube dashboards a Supabase Storage con content-type correcto")
    parser.add_argument("isins", nargs="*", help="ISINs a fixear (vacío = todos los dashboards locales)")
    parser.add_argument("--dry-run", action="store_true", help="No modifica, solo lista")
    args = parser.parse_args()

    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as e:
        print(f"[ERROR] Supabase: {e}", file=sys.stderr)
        return 2

    targets = args.isins if args.isins else list_local_isins()
    print(f"=== Re-subiendo {len(targets)} dashboard(s) a Supabase Storage ===")
    if args.dry_run:
        print("[DRY RUN]")

    n_ok = n_fail = 0
    for isin in targets:
        ok, msg = fix_isin(client, isin, dry_run=args.dry_run)
        tag = "[OK]" if ok else "[FAIL]"
        print(f"  {tag} {isin}: {msg}")
        if ok:
            n_ok += 1
        else:
            n_fail += 1

    print(f"\n=== Resumen ===")
    print(f"  OK:   {n_ok}")
    print(f"  FAIL: {n_fail}")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
