"""
supabase_client.py — Cliente Supabase para fund-analyzer (schema v2-cowork).

Carga credenciales desde .env (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY) y
expone:

- get_client() -> Client
- test_connection() -> int | None   (devuelve el count de fund_groups, o None si falla)

CLI:
    python -m tools.supabase_client     # imprime "OK: X fund_groups, Y funds" o el error

La dependencia `supabase` es opcional: se importa de forma perezosa para que
el resto del repo siga funcionando aunque el paquete no esté instalado
(p. ej. en CI o en máquinas donde aún no se ha configurado Supabase).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class SupabaseNotConfigured(RuntimeError):
    """SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY no presentes en el entorno."""


class SupabaseLibMissing(RuntimeError):
    """El paquete `supabase` no está instalado en el venv actual."""


def _load_env() -> tuple[str, str]:
    """Carga .env del repo y devuelve (url, service_role_key). Lanza si faltan."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass

    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        missing = [n for n, v in (("SUPABASE_URL", url), ("SUPABASE_SERVICE_ROLE_KEY", key)) if not v]
        raise SupabaseNotConfigured(f"Faltan en .env: {', '.join(missing)}")
    return url, key


def get_client():
    """Devuelve un cliente supabase-py listo para usar. Lanza si falta config o lib."""
    url, key = _load_env()
    try:
        from supabase import create_client
    except ImportError as e:
        raise SupabaseLibMissing(
            "Paquete `supabase` no instalado. Instala con: pip install supabase"
        ) from e
    return create_client(url, key)


def _count_table(client, table: str) -> int:
    """Hace SELECT count(*) eficiente vía head=True + count='exact'."""
    res = client.table(table).select("*", count="exact", head=True).execute()
    return res.count or 0


def test_connection() -> int | None:
    """Verifica conexión leyendo count de fund_groups.

    Devuelve el nº de filas en `fund_groups`, o None si la conexión falla
    por cualquier motivo (faltan envs, lib no instalada, error de red, etc.).
    """
    try:
        client = get_client()
        return _count_table(client, "fund_groups")
    except Exception:
        return None


def main() -> int:
    try:
        client = get_client()
    except SupabaseNotConfigured as e:
        print(f"[supabase_client] ERROR: {e}", file=sys.stderr)
        return 2
    except SupabaseLibMissing as e:
        print(f"[supabase_client] ERROR: {e}", file=sys.stderr)
        return 3

    try:
        n_groups = _count_table(client, "fund_groups")
        n_funds = _count_table(client, "funds")
    except Exception as e:
        print(f"[supabase_client] ERROR consultando: {e}", file=sys.stderr)
        return 1

    print(f"OK: {n_groups} fund_groups, {n_funds} funds")
    return 0


if __name__ == "__main__":
    sys.exit(main())


_PROBE_CACHE: dict = {}


def probe(timeout: float = 8.0, cache_s: int = 60) -> tuple[bool, str]:
    """¿Responde Supabase? (True, "") o (False, motivo). Sonda REST mínima con el service key, cacheada
    `cache_s` segundos. Un 402 (proyecto restringido por cuota) o un fallo de red devuelven False:
    el sync se aplaza (tools/supabase_pending) en vez de tumbar el pipeline (29-sep-2026)."""
    import time
    import urllib.request
    now = time.time()
    hit = _PROBE_CACHE.get("r")
    if hit and now - hit[0] < cache_s:
        return hit[1]
    try:
        url, key = _load_env()
    except Exception as e:  # noqa: BLE001
        res = (False, f"sin configurar: {str(e)[:60]}")
        _PROBE_CACHE["r"] = (now, res)
        return res
    req = urllib.request.Request(url.rstrip("/") + "/rest/v1/funds?select=isin&limit=1",
                                 headers={"apikey": key, "Authorization": f"Bearer {key}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            res = (200 <= r.status < 300, "" if 200 <= r.status < 300 else f"HTTP {r.status}")
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "402" in msg:
            msg = "HTTP 402: proyecto restringido (cuota)"
        res = (False, msg[:100])
    _PROBE_CACHE["r"] = (now, res)
    return res
