"""
supabase_admin.py — Control del proyecto Supabase vía Management API.

Permite consultar el estado del proyecto y REACTIVARLO (un-pause) sin entrar al
dashboard, p.ej. desde un botón del catálogo. Complementa el keep-alive
(tools/supabase_maintenance.py) que evita que se pause: esto es el plan B para
devolverlo a la actividad si llegara a pausarse.

IMPORTANTE — proyecto correcto (2026-06-03): la herramienta usa el proyecto cuyo
ref está en SUPABASE_URL del .env (`mfbtebngddjjuwfaelat`). El email de pausa de
Supabase hablaba de OTRO proyecto viejo (`dcnvdaaexyuhqvyrrkob`) que NO se usa.

REQUISITO: un Personal Access Token de Supabase (distinto de la service_role key):
  crear en https://supabase.com/dashboard/account/tokens y ponerlo en .env como
  SUPABASE_ACCESS_TOKEN. El project ref se deriva de SUPABASE_URL (o
  SUPABASE_PROJECT_REF si se quiere forzar).

CLI:
    python -m tools.supabase_admin --status     # estado (ACTIVE_HEALTHY / INACTIVE / ...)
    python -m tools.supabase_admin --restore    # reactiva si está pausado
    python -m tools.supabase_admin --ensure      # status + restore solo si hace falta
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_MGMT_BASE = "https://api.supabase.com/v1"


class SupabaseAdminNotConfigured(RuntimeError):
    """Falta SUPABASE_ACCESS_TOKEN o no se puede derivar el project ref."""


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass


def _project_ref() -> str:
    """Deriva el ref del proyecto desde SUPABASE_PROJECT_REF o SUPABASE_URL."""
    ref = os.getenv("SUPABASE_PROJECT_REF", "").strip()
    if ref:
        return ref
    url = os.getenv("SUPABASE_URL", "").strip()
    # https://<ref>.supabase.co
    if "supabase.co" in url:
        return url.split("//")[-1].split(".")[0]
    raise SupabaseAdminNotConfigured(
        "No se puede derivar el project ref (ni SUPABASE_PROJECT_REF ni SUPABASE_URL válidos)"
    )


def _access_token() -> str:
    tok = os.getenv("SUPABASE_ACCESS_TOKEN", "").strip()
    if not tok:
        raise SupabaseAdminNotConfigured(
            "Falta SUPABASE_ACCESS_TOKEN en .env. Crea un Personal Access Token en "
            "https://supabase.com/dashboard/account/tokens"
        )
    return tok


def _headers() -> dict:
    return {"Authorization": f"Bearer {_access_token()}", "Content-Type": "application/json"}


def get_status() -> dict:
    """Devuelve {ref, status, healthy, paused, error}. status p.ej. ACTIVE_HEALTHY,
    INACTIVE (pausado), COMING_UP, RESTORING..."""
    _load_env()
    import httpx
    try:
        ref = _project_ref()
    except SupabaseAdminNotConfigured as exc:
        return {"ref": None, "status": "UNKNOWN", "healthy": False, "paused": None,
                "error": str(exc)}
    try:
        r = httpx.get(f"{_MGMT_BASE}/projects/{ref}", headers=_headers(), timeout=20)
        if r.status_code == 200:
            data = r.json()
            status = data.get("status", "UNKNOWN")
            return {
                "ref": ref,
                "status": status,
                "healthy": status == "ACTIVE_HEALTHY",
                "paused": status in ("INACTIVE", "PAUSED", "PAUSING"),
                "error": None,
            }
        return {"ref": ref, "status": "UNKNOWN", "healthy": False, "paused": None,
                "error": f"Management API {r.status_code}: {r.text[:200]}"}
    except Exception as exc:
        return {"ref": ref, "status": "UNKNOWN", "healthy": False, "paused": None,
                "error": str(exc)[:200]}


def restore() -> dict:
    """Reactiva (un-pause) el proyecto vía Management API. Devuelve {ok, message}."""
    _load_env()
    import httpx
    try:
        ref = _project_ref()
        headers = _headers()
    except SupabaseAdminNotConfigured as exc:
        return {"ok": False, "message": str(exc)}
    try:
        r = httpx.post(f"{_MGMT_BASE}/projects/{ref}/restore", headers=headers,
                       json={}, timeout=30)
        if r.status_code in (200, 201, 202):
            return {"ok": True, "message": f"Reactivación solicitada para {ref} (puede tardar 1-2 min)"}
        return {"ok": False, "message": f"Management API {r.status_code}: {r.text[:200]}"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)[:200]}


def ensure_active() -> dict:
    """Comprueba estado y reactiva SOLO si está pausado. Idempotente."""
    st = get_status()
    if st.get("paused"):
        res = restore()
        return {"action": "restore", "status_before": st["status"], **res}
    return {"action": "none", "status": st.get("status"),
            "ok": st.get("healthy", False), "message": st.get("error") or "ya activo"}


def _cli() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if "--restore" in args:
        print(restore())
    elif "--ensure" in args:
        print(ensure_active())
    else:
        st = get_status()
        icon = "OK" if st["healthy"] else ("PAUSADO" if st["paused"] else "?")
        print(f"[{icon}] proyecto {st['ref']}: {st['status']}"
              + (f"  ERROR: {st['error']}" if st["error"] else ""))


if __name__ == "__main__":
    _cli()
