"""
register_tunnel.py — Registra la URL del túnel Cloudflare en Supabase.

El catalog (servido desde Cloudflare Pages/Workers) lee esta URL al cargar
y se auto-conecta al server local vía el túnel HTTPS. Así el usuario NO tiene
que copiar/pegar la URL manualmente cada vez.

Uso:
    python -m tools.register_tunnel https://xxx.trycloudflare.com
    python -m tools.register_tunnel --clear      # borra la URL (modo offline)

Requiere tabla app_config en Supabase:
    create table if not exists app_config (
      key text primary key, value text, updated_at timestamptz default now()
    );
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def register(url: str) -> bool:
    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as e:
        print(f"[register_tunnel] ERROR Supabase: {e}", file=sys.stderr)
        return False

    url = (url or "").strip().rstrip("/")
    try:
        client.table("app_config").upsert({
            "key": "tunnel_url",
            "value": url,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        if url:
            print(f"[register_tunnel] OK — tunnel_url = {url}")
        else:
            print("[register_tunnel] OK — tunnel_url limpiado (modo offline)")
        return True
    except Exception as e:
        print(f"[register_tunnel] ERROR escribiendo: {e}", file=sys.stderr)
        # Si la tabla no existe, avisar claro
        if "app_config" in str(e) and ("does not exist" in str(e) or "not find" in str(e).lower()):
            print("[register_tunnel] La tabla 'app_config' no existe. Créala con el SQL del setup.", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Registra URL del túnel en Supabase")
    parser.add_argument("url", nargs="?", default="", help="URL del túnel (vacío o --clear para limpiar)")
    parser.add_argument("--clear", action="store_true", help="Limpia la URL (modo offline)")
    args = parser.parse_args()
    url = "" if args.clear else args.url
    ok = register(url)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
