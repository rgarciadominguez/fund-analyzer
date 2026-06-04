"""
supabase_maintenance.py — Keep-alive + health check diario de Supabase.

PROBLEMA (2026-06-03): Supabase pausa AUTOMÁTICAMENTE los proyectos del plan
gratuito que no ven "actividad suficiente" durante 7 días (email de aviso de
ant.wilson@supabase.com sobre el proyecto fund-dashboard). Si se pausa, el
catálogo y la API dejan de funcionar hasta restaurarlo a mano.

SOLUCIÓN: este módulo hace dos cosas y se programa para correr a diario:
  1. keepalive()    — una query trivial que registra ACTIVIDAD → resetea el
                      contador de 7 días → el proyecto nunca se auto-pausa.
  2. health_check() — verifica que todo está OK "a final del día": reachability,
                      nº de filas (funds, fund_groups), cobertura de brokers, y
                      detecta anomalías (0 filas, caída brusca, etc.).

Cada ejecución añade una línea a `data/supabase_health.jsonl` (auditoría).

CLI:
    python -m tools.supabase_maintenance               # keepalive + health (default)
    python -m tools.supabase_maintenance --keepalive   # solo keep-alive (rápido)
    python -m tools.supabase_maintenance --health      # solo health check
    python -m tools.supabase_maintenance --history 10  # últimas N entradas del log

Salida exit code: 0 si OK, 1 si hay alarmas (para que un scheduler pueda alertar).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEALTH_LOG = ROOT / "data" / "supabase_health.jsonl"

# Umbrales de anomalía: si las filas caen por debajo, algo va mal.
MIN_FUNDS = 1
MIN_FUND_GROUPS = 1


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# Proyectos Supabase EXTRA a mantener vivos además del principal (.env).
# P.ej. el proyecto del fund-dashboard (dcnvdaa...), una herramienta distinta que
# comparte cuenta. Config en data/supabase_keepalive_targets.json:
#   [{"name","ref","url","anon_key","table"}]
_TARGETS_PATH = ROOT / "data" / "supabase_keepalive_targets.json"


def _extra_targets() -> list[dict]:
    if not _TARGETS_PATH.exists():
        return []
    try:
        return json.loads(_TARGETS_PATH.read_text(encoding="utf-8")) or []
    except Exception:
        return []


def _ping_rest(url: str, anon_key: str, table: str = "funds") -> dict:
    """Ping REST directo a un proyecto Supabase (sin SDK). Sirve para proyectos
    que no son el principal del .env."""
    import httpx
    t0 = datetime.now()
    try:
        r = httpx.get(
            f"{url}/rest/v1/{table}?select=*&limit=1",
            headers={"apikey": anon_key, "Authorization": f"Bearer {anon_key}"},
            timeout=20,
        )
        latency = int((datetime.now() - t0).total_seconds() * 1000)
        ok = r.status_code in (200, 206)
        return {"ok": ok, "latency_ms": latency,
                "error": None if ok else f"HTTP {r.status_code}"}
    except Exception as exc:
        latency = int((datetime.now() - t0).total_seconds() * 1000)
        # getaddrinfo/DNS fail normalmente = proyecto PAUSADO.
        return {"ok": False, "latency_ms": latency,
                "error": f"{type(exc).__name__}: {str(exc)[:120]} (¿pausado?)"}


def keepalive() -> dict:
    """Query trivial que registra actividad en Supabase (anti auto-pause).

    Pinguea el proyecto principal (.env) Y los proyectos extra configurados
    (data/supabase_keepalive_targets.json), p.ej. el del fund-dashboard.

    Devuelve {"ok", "latency_ms", "error", "targets": {name: {...}}}.
    """
    t0 = datetime.now()
    result: dict
    try:
        from tools.supabase_client import get_client
        client = get_client()
        # SELECT mínimo: 1 fila de funds. Cuenta como actividad de DB + API.
        client.table("funds").select("isin").limit(1).execute()
        latency = int((datetime.now() - t0).total_seconds() * 1000)
        result = {"ok": True, "latency_ms": latency, "error": None}
    except Exception as exc:
        latency = int((datetime.now() - t0).total_seconds() * 1000)
        result = {"ok": False, "latency_ms": latency, "error": str(exc)[:300]}

    # Proyectos extra (fund-dashboard, etc.)
    targets: dict = {}
    for t in _extra_targets():
        name = t.get("name") or t.get("ref") or "extra"
        if t.get("url") and t.get("anon_key"):
            targets[name] = _ping_rest(t["url"], t["anon_key"], t.get("table") or "funds")
    if targets:
        result["targets"] = targets
    return result


def health_check() -> dict:
    """Comprueba integridad básica de Supabase. Devuelve dict con métricas + alarmas."""
    alarms: list[str] = []
    metrics: dict = {}
    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as exc:
        return {
            "reachable": False,
            "alarms": [f"Supabase inaccesible: {str(exc)[:200]}"],
            "metrics": {},
        }

    def _count(table: str) -> int | None:
        try:
            res = client.table(table).select("*", count="exact", head=True).execute()
            return getattr(res, "count", None)
        except Exception as exc:
            alarms.append(f"count({table}) falló: {str(exc)[:120]}")
            return None

    n_funds = _count("funds")
    n_groups = _count("fund_groups")
    metrics["funds"] = n_funds
    metrics["fund_groups"] = n_groups

    if n_funds is not None and n_funds < MIN_FUNDS:
        alarms.append(f"funds = {n_funds} (esperado >= {MIN_FUNDS})")
    if n_groups is not None and n_groups < MIN_FUND_GROUPS:
        alarms.append(f"fund_groups = {n_groups} (esperado >= {MIN_FUND_GROUPS})")

    # Cobertura de brokers (informativo, no alarma).
    try:
        rows = client.table("funds").select("isin,broker_disponible").limit(2000).execute().data or []
        con_brokers = sum(1 for r in rows if r.get("broker_disponible"))
        metrics["broker_coverage"] = f"{con_brokers}/{len(rows)}"
    except Exception:
        pass

    # Comparar con la última métrica registrada → detectar caída brusca (>20%).
    prev = _last_metrics()
    if prev and n_funds is not None and prev.get("funds"):
        try:
            if n_funds < int(prev["funds"]) * 0.8:
                alarms.append(f"Caída brusca de funds: {prev['funds']} → {n_funds}")
        except (TypeError, ValueError):
            pass

    return {"reachable": True, "alarms": alarms, "metrics": metrics}


def _last_metrics() -> dict | None:
    """Métricas de la última entrada del log (para comparar)."""
    if not HEALTH_LOG.exists():
        return None
    try:
        lines = HEALTH_LOG.read_text(encoding="utf-8").strip().splitlines()
        for line in reversed(lines):
            entry = json.loads(line)
            if entry.get("metrics"):
                return entry["metrics"]
    except Exception:
        pass
    return None


def _append_log(entry: dict) -> None:
    HEALTH_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEALTH_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run(do_keepalive: bool = True, do_health: bool = True) -> dict:
    """Ejecuta keep-alive y/o health check, registra en el log y devuelve el resumen."""
    entry: dict = {"timestamp": _now()}
    alarms: list[str] = []

    if do_keepalive:
        ka = keepalive()
        entry["keepalive"] = ka
        if not ka["ok"]:
            alarms.append(f"keepalive falló: {ka['error']}")

    if do_health:
        hc = health_check()
        entry["reachable"] = hc["reachable"]
        entry["metrics"] = hc.get("metrics", {})
        entry["health_alarms"] = hc.get("alarms", [])
        alarms.extend(hc.get("alarms", []))

    entry["alarms"] = alarms
    entry["status"] = "OK" if not alarms else "ALARM"
    _append_log(entry)
    return entry


def _cli() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    args = sys.argv[1:]

    if "--history" in args:
        try:
            n = int(args[args.index("--history") + 1])
        except (IndexError, ValueError):
            n = 10
        if not HEALTH_LOG.exists():
            print("(sin historial todavía)")
            return
        lines = HEALTH_LOG.read_text(encoding="utf-8").strip().splitlines()[-n:]
        for line in lines:
            e = json.loads(line)
            print(f"  {e['timestamp'][:19]}  {e['status']:5}  "
                  f"funds={e.get('metrics', {}).get('funds')}  "
                  f"alarms={e.get('alarms') or '-'}")
        return

    do_ka = "--health" not in args  # si pide solo --health, no keepalive
    do_hc = "--keepalive" not in args
    if "--keepalive" in args:
        do_ka, do_hc = True, False
    if "--health" in args:
        do_ka, do_hc = False, True

    entry = run(do_keepalive=do_ka, do_health=do_hc)
    icon = "✓" if entry["status"] == "OK" else "⚠"
    print(f"{icon} Supabase {entry['status']}")
    if entry.get("keepalive"):
        ka = entry["keepalive"]
        print(f"   keep-alive: {'OK' if ka['ok'] else 'FALLO'} ({ka['latency_ms']}ms)")
    if "metrics" in entry:
        print(f"   métricas: {entry['metrics']}")
    if entry["alarms"]:
        for a in entry["alarms"]:
            print(f"   ⚠ {a}")
    sys.exit(0 if entry["status"] == "OK" else 1)


if __name__ == "__main__":
    _cli()
