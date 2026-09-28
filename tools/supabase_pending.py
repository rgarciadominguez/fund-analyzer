"""Cola de sincronizaciones con Supabase APLAZADAS (Supabase restringido / caído).

Por qué (29-sep-2026): Supabase devolvía 402 "exceed_storage_size_quota" (proyecto restringido varios días).
El sync fallaba, el pipeline marcaba el análisis como FAIL crítico y el fondo se quedaba colgado en la cola
del portal, aunque el dashboard ya estaba publicado en el Worker (git) y el análisis local era bueno.
Ahora: si Supabase no responde, el sync se APLAZA (se anota aquí), el portal recibe lo que no depende de
Supabase, y el reintento es automático (cada hora desde consume_inputs_rafa, o `--retry` a mano).

Fichero: data/_pending_supabase_sync.json  {ISIN: {since, reason, attempts, last_error, pasos:[...]}}
CLI:  python -m tools.supabase_pending            # lista
      python -m tools.supabase_pending --retry    # reintenta si Supabase responde
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PENDING = ROOT / "data" / "_pending_supabase_sync.json"
PASOS = ("sync_to_supabase", "publish_storage", "funddash")


def _load() -> dict:
    try:
        return json.loads(PENDING.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(d: dict) -> None:
    PENDING.parent.mkdir(parents=True, exist_ok=True)
    PENDING.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")


def add(isin: str, reason: str, pasos=PASOS) -> None:
    d = _load()
    e = d.get(isin.upper()) or {"since": datetime.now(timezone.utc).isoformat(), "attempts": 0}
    e["reason"] = str(reason)[:200]
    e["pasos"] = sorted(set(e.get("pasos") or []) | set(pasos))
    d[isin.upper()] = e
    _save(d)


def remove(isin: str, paso: str | None = None) -> None:
    d = _load()
    e = d.get(isin.upper())
    if not e:
        return
    if paso:
        e["pasos"] = [p for p in e.get("pasos") or [] if p != paso]
        if e["pasos"]:
            d[isin.upper()] = e
            _save(d)
            return
    d.pop(isin.upper(), None)
    _save(d)


def pending() -> dict:
    return _load()


def retry(log=print, max_isins: int = 20) -> dict:
    """Reintenta los aplazados si Supabase responde. Devuelve {isin: ok}."""
    d = _load()
    if not d:
        return {}
    from tools.supabase_client import probe
    ok, why = probe()
    if not ok:
        log(f"[SUPABASE_PENDING] {len(d)} aplazado(s); Supabase sigue sin responder ({why}) — se reintenta más tarde")
        return {}
    out = {}
    for isin in list(d.keys())[:max_isins]:
        e = d[isin]
        e["attempts"] = int(e.get("attempts") or 0) + 1
        errores = []
        for paso in list(e.get("pasos") or PASOS):
            if paso == "sync_to_supabase":
                cmd = [sys.executable, "-m", "tools.sync_to_supabase", isin, "--quiet"]
            elif paso == "publish_storage":
                cmd = [sys.executable, "-m", "tools.publish_dashboard", "--isin", isin, "--no-git", "--wait", "0"]
            elif paso == "funddash":
                cmd = [sys.executable, "-m", "tools.funddash_sync", "--isin", isin]
            else:
                continue
            r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=1500)
            if r.returncode == 0:
                remove(isin, paso)
                log(f"[SUPABASE_PENDING] {isin} {paso}: OK")
            else:
                errores.append(f"{paso} rc={r.returncode}: {(r.stderr or r.stdout or '')[-160:].strip()}")
        d = _load()
        if isin in d:
            d[isin]["attempts"] = e["attempts"]
            d[isin]["last_error"] = " | ".join(errores)[:400]
            _save(d)
            log(f"[SUPABASE_PENDING] {isin}: pendiente ({'; '.join(errores)[:200]})")
        out[isin] = isin not in d
    return out


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if "--retry" in sys.argv:
        print(retry())
    else:
        print(json.dumps(pending(), ensure_ascii=False, indent=2) or "{}")
