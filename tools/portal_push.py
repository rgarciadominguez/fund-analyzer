"""
portal_push.py — Empuja nuestros JSON a los endpoints admin del portal Horizonte.

El portal expone (auth admin = WordPress Application Password, Basic Auth):
  POST {base}/wp-json/horizonte/v1/admin/assets/sync-metricas  ← metricas.json
  POST {base}/wp-json/horizonte/v1/admin/assets/sync-clases    ← asset_classes_por_isin.json

Idempotentes. Sustituyen el paso manual de pasar ficheros.

Config en .env (NO en el chat):
  PORTAL_BASE_URL=https://<dominio-portal>        (cambia cuando Rafa mueve la web)
  PORTAL_APP_USER=<usuario admin WP>
  PORTAL_APP_PASSWORD=xxxx xxxx xxxx xxxx xxxx xxxx   (app password de WordPress, con espacios)

CLI:
  python -m tools.portal_push --metricas         # push metricas.json
  python -m tools.portal_push --clases           # push asset_classes_por_isin.json
  python -m tools.portal_push --all              # ambos
  python -m tools.portal_push --file X.json --endpoint sync-metricas
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
HORFIN = Path(r"C:\Users\RafaelGarcía\horizonte-datos")
_ENDPOINTS = {
    "sync-metricas": HORFIN / "metricas.json",
    "sync-clases": HORFIN / "asset_classes_por_isin.json",
}


def _cfg():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    base = os.environ.get("PORTAL_BASE_URL", "").strip().rstrip("/")
    user = os.environ.get("PORTAL_APP_USER", "").strip()
    pwd = os.environ.get("PORTAL_APP_PASSWORD", "").strip()
    missing = [n for n, v in (("PORTAL_BASE_URL", base), ("PORTAL_APP_USER", user),
                              ("PORTAL_APP_PASSWORD", pwd)) if not v]
    if missing:
        raise RuntimeError(f"Faltan en .env: {', '.join(missing)}")
    return base, user, pwd


def push(endpoint: str, file_path: Path) -> dict:
    base, user, pwd = _cfg()
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    # WordPress app password → Basic Auth (los espacios del password se conservan)
    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    url = f"{base}/wp-json/horizonte/v1/admin/assets/{endpoint}"
    payload = file_path.read_bytes()
    r = httpx.post(url, content=payload,
                   headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
                   timeout=120)
    ok = 200 <= r.status_code < 300
    print(f"POST {endpoint} ({len(payload)/1024:.0f} KB) → HTTP {r.status_code} "
          f"{'OK' if ok else 'FALLO: ' + r.text[:200]}")
    return {"endpoint": endpoint, "status": r.status_code, "ok": ok, "resp": r.text[:300]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metricas", action="store_true")
    ap.add_argument("--clases", action="store_true")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--file")
    ap.add_argument("--endpoint")
    a = ap.parse_args()

    jobs = []
    if a.file and a.endpoint:
        jobs.append((a.endpoint, Path(a.file)))
    if a.metricas or a.all:
        jobs.append(("sync-metricas", _ENDPOINTS["sync-metricas"]))
    if a.clases or a.all:
        jobs.append(("sync-clases", _ENDPOINTS["sync-clases"]))
    if not jobs:
        ap.print_help()
        return
    for ep, fp in jobs:
        push(ep, fp)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
