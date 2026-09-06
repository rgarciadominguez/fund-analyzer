"""retrofit_fund.py — Proceso COMPLETO de mejora multi-año de un fondo YA analizado (plan Rafa
2026-09-06): (1) sourcing de MÁS años/docs (skill ar-sourcing), (2) extraer TODOS los AR/SAR,
(3) reconstruir evolución de cartera + narrativa (analyst), (4) archivar docs + sync portal/Supabase.

Detecta 'hit your limits': si topa la cuota Claude Max en cualquier paso, sale con status LIMIT|... y
NO sigue (para el plan: si topas durante 1 → haz 2 y para). Cada fondo se commitea/pushea al terminar.

Uso: python -m tools.retrofit_fund <ISIN>
Salida (última línea): DONE|<ISIN>|<n_años> | LIMIT|<ISIN>|<paso> | ERROR|<ISIN>|<msg>
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUOTA = ("session limit", "hit your", "usage limit")
TOOLS = "Read,Write,Bash,Edit,Agent,Glob,Grep,WebSearch,WebFetch"


def _cowork(isin: str, prompt: str, logname: str) -> bool:
    """Corre una skill cowork. Devuelve True si topó la cuota (session limit)."""
    log = ROOT / "logs" / f"{logname}_{isin}.log"
    log.parent.mkdir(exist_ok=True)
    subprocess.call([sys.executable, "-m", "tools.claude_cowork", str(log), prompt,
                     "--model", "claude-opus-4-8", "--allowedTools", TOOLS])
    txt = log.read_text(encoding="utf-8", errors="replace").lower() if log.exists() else ""
    return any(m in txt for m in QUOTA)


def _hist_count(isin: str) -> int:
    try:
        d = json.loads((ROOT / "data" / "funds" / isin / "output.json").read_text(encoding="utf-8"))
        return len((d.get("posiciones", {}) or {}).get("historicas") or [])
    except Exception:
        return -1


def main(isin: str) -> None:
    isin = isin.upper()
    print(f"[retrofit] {isin}: hist antes = {_hist_count(isin)} años", flush=True)

    # 1) sourcing de más años
    if _cowork(isin, f"ar sourcing cowork {isin}", "skill_ar_sourcing"):
        print(f"LIMIT|{isin}|ar-sourcing", flush=True); return
    # 2) extraer todos los AR/SAR (incl. los nuevos)
    if _cowork(isin, f"extract pdfs cowork {isin}", "skill_extract_pdfs"):
        print(f"LIMIT|{isin}|extract", flush=True); return
    subprocess.call([sys.executable, "-m", "agents.orchestrator", "--isin", isin, "--consume-extracted"])

    # 3) analyst con los nuevos años (narrativa de evolución/consistencia). Borra la síntesis para
    # forzar re-run (GOTCHA --resume). Backup por si acaso.
    syn = ROOT / "data" / "funds" / isin / "analyst_synthesis_cowork.json"
    if syn.exists():
        try:
            syn.replace(syn.with_suffix(".json.bak_retrofit"))
        except Exception:
            pass
    if _cowork(isin, f"analyst cowork {isin}", "skill_analyst"):
        print(f"LIMIT|{isin}|analyst", flush=True); return
    subprocess.call([sys.executable, "-m", "agents.orchestrator", "--isin", isin, "--consume-all-cowork"])

    # 4) archivar docs + dashboard + sync portal/Supabase
    subprocess.call([sys.executable, "-m", "tools.archive_docs", "--isin", isin])
    os.environ["DASHBOARD_SKIP_ENRICH"] = "1"
    subprocess.call([sys.executable, "dashboard/generate_dashboard.py", isin])
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.portal_analyze_worker import push_meta
        push_meta(isin, dry=False, do_push=True)
        from tools.supabase_client import get_client
        from datetime import datetime, timezone
        c = get_client()
        g = c.table("funds").select("fund_group_id").eq("isin", isin).execute().data
        if g:
            c.table("fund_groups").update({"fecha_ultimo_analisis": datetime.now(timezone.utc).isoformat()}
                                          ).eq("fund_group_id", g[0]["fund_group_id"]).execute()
        from tools.sync_to_supabase import _refresh_portal_catalog
        _refresh_portal_catalog(lambda m: None, isin)
    except Exception as e:
        print(f"[retrofit] sync warn: {str(e)[:80]}", flush=True)

    # commit + push
    subprocess.call(["git", "add", f"dashboard/fund-{isin}.html", f"data/funds/{isin}/output.json",
                     "data/known_annual_reports.json", "data/known_manager_letters.json"])
    n = _hist_count(isin)
    subprocess.call(["git", "commit", "-q", "-m", f"retrofit multi-año {isin}: {n} años de evolución de cartera"])
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    subprocess.call(["git", "push", "origin", "v2-cowork"], env=env)
    print(f"DONE|{isin}|{n} años historicas", flush=True)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 2:
        print("uso: python -m tools.retrofit_fund <ISIN>"); sys.exit(1)
    main(sys.argv[1])
