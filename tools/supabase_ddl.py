"""
supabase_ddl.py — Ejecuta SQL (incluido DDL) contra el Supabase de fund-analyzer.

supabase-py solo habla PostgREST (DML). Para ALTER TABLE hace falta la
Management API, que autentica con SUPABASE_ACCESS_TOKEN (token de cuenta, no
la service_role key).

    POST https://api.supabase.com/v1/projects/{ref}/database/query
    body: {"query": "..."}

Uso:
    from tools.supabase_ddl import run_sql
    run_sql("ALTER TABLE funds ADD COLUMN IF NOT EXISTS benchmark text")

CLI:
    python -m tools.supabase_ddl "select count(*) from funds"
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
_API = "https://api.supabase.com/v1/projects/{ref}/database/query"


def _cfg() -> tuple[str, str]:
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    token = os.environ.get("SUPABASE_ACCESS_TOKEN", "").strip()
    url = os.environ.get("SUPABASE_URL", "").strip()
    if not token:
        raise RuntimeError("Falta SUPABASE_ACCESS_TOKEN en .env")
    if not url:
        raise RuntimeError("Falta SUPABASE_URL en .env")
    # https://<ref>.supabase.co  ->  <ref>
    ref = url.split("//", 1)[-1].split(".", 1)[0]
    return token, ref


def run_sql(query: str, timeout: float = 60.0):
    """Ejecuta SQL arbitrario. Devuelve la lista de filas (o [] si no retorna)."""
    token, ref = _cfg()
    r = httpx.post(
        _API.format(ref=ref),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": query},
        timeout=timeout,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"SQL HTTP {r.status_code}: {r.text[:400]}")
    try:
        return r.json()
    except Exception:
        return []


def columns(table: str = "funds") -> list[str]:
    rows = run_sql(
        "select column_name from information_schema.columns "
        f"where table_schema='public' and table_name='{table}' order by ordinal_position"
    )
    return [x["column_name"] for x in rows]


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "select count(*) from funds"
    import json
    print(json.dumps(run_sql(q), ensure_ascii=False, indent=1)[:3000])
