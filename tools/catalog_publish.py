"""
catalog_publish.py — Publica el export del catálogo a una TABLA Supabase para el webhook.

Por qué: el export (catalogo_supabase.json) es un JOB PYTHON con la lógica del contrato
(tipo_activo granular, benchmark nulificado, estado, herencia...), NO una vista SQL. Para que
el portal tenga webhook + lectura viva EXACTA, materializamos el export en `catalogo_activos`
con un `updated_at` real que SOLO cambia cuando la fila cambia de verdad (hash de contenido)
→ el webhook dispara solo en cambios reales, no en cada re-publicación.

Flujo: export_horfin_catalog → catalogo_supabase.json → catalog_publish → tabla catalogo_activos.

DDL: data/migrations/2026-07-23_catalogo_activos.sql
Cruce: ISIN (PK). Lectura pública (anon + RLS select). El portal NUNCA escribe.

CLI:
    python -m tools.catalog_publish --apply       # publica el JSON actual a la tabla
    python -m tools.catalog_publish --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
EXPORT = Path(r"C:\Users\RafaelGarcía\horizonte-datos\catalogo_supabase.json")

# Campos que son listas → van como jsonb
_JSONB = {"caracteristicas_especiales", "broker_disponible", "class_isins_known"}


def _row_hash(row: dict) -> str:
    payload = {k: v for k, v in row.items() if k not in ("updated_at", "content_hash")}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


def publish(apply: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    client = get_client()

    data = json.loads(EXPORT.read_text(encoding="utf-8"))
    activos = data["activos"]

    # hashes actuales en la tabla (para no tocar filas sin cambios → webhook silencioso)
    existing = {}
    try:
        for r in client.table("catalogo_activos").select("isin,content_hash").execute().data:
            existing[r["isin"]] = r.get("content_hash")
    except Exception as e:
        print(f"[WARN] no pude leer catalogo_activos (¿falta DDL?): {str(e)[:80]}")

    now = datetime.now(timezone.utc).isoformat()
    to_upsert = []
    for a in activos:
        row = dict(a)
        # normaliza tipos jsonb (dejar como están; supabase-py serializa listas a jsonb)
        h = _row_hash(row)
        if existing.get(a["isin"]) == h:
            continue  # sin cambios → no tocar (no dispara webhook)
        row["content_hash"] = h
        row["updated_at"] = now
        to_upsert.append(row)

    print(f"export: {len(activos)} filas | cambian: {len(to_upsert)} | sin cambio: {len(activos)-len(to_upsert)}")
    if apply and to_upsert:
        for i in range(0, len(to_upsert), 100):
            client.table("catalogo_activos").upsert(to_upsert[i:i+100], on_conflict="isin").execute()
        print(f"publicadas {len(to_upsert)} filas en catalogo_activos (updated_at={now})")
    elif not apply:
        print("(dry-run) nada escrito")
    return {"total": len(activos), "cambian": len(to_upsert)}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    publish(apply=a.apply)
