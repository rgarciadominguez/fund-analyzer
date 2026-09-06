"""retrofit_fund.py — Proceso COMPLETO de mejora multi-año de fondos YA analizados (plan Rafa
2026-09-06): por cada fondo (1) sourcing SOLO de los huecos (gap embebido en el prompt, determinista),
(2) extraer TODOS los AR/SAR, (3) reconstruir evolución + narrativa (analyst), (4) archivar docs + sync.

AUTO-RESUME (mejora de ayer, ahora también aquí): si topa 'hit your limits' en cualquier skill, PARSEA
la hora de reset (Madrid), DUERME hasta reset+5min y CONTINÚA solo — no para. Además retoma desde el
paso donde se quedó (no repite el sourcing/extract ya hechos).

Uso: python -m tools.retrofit_fund <ISIN> [<ISIN> ...]   (los procesa 1 a 1, en orden)
Salida por fondo: DONE|<ISIN>|<n_años>  ·  ERROR|<ISIN>|<msg>
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time as _time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUOTA = ("session limit", "hit your", "usage limit")
TOOLS = "Read,Write,Bash,Edit,Agent,Glob,Grep,WebSearch,WebFetch"
MAX_WAITS = 8  # nº máx. de esperas de reset por paso antes de rendirse


def _log(isin: str, logname: str) -> Path:
    p = ROOT / "logs" / f"{logname}_{isin}.log"
    p.parent.mkdir(exist_ok=True)
    return p


def _read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _limit_in(txt: str) -> bool:
    low = txt.lower()
    return any(m in low for m in QUOTA)


def _reset_seconds(log_path: Path) -> float:
    """Segundos hasta reset+5min desde el mensaje 'resets 3am (Europe/Madrid)' del log. 3600 si no
    lo saca (fallback: 1h). La hora del mensaje es HORA DE MADRID."""
    try:
        from zoneinfo import ZoneInfo
        madrid = ZoneInfo("Europe/Madrid")
    except Exception:
        return 3600.0
    txt = _read(log_path)[-4000:]
    m = re.search(r"resets\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)", txt, re.I)
    if not m:
        return 3600.0
    h = int(m.group(1)); mi = int(m.group(2) or 0); ap = m.group(3).lower()
    if ap == "pm" and h != 12:
        h += 12
    if ap == "am" and h == 12:
        h = 0
    now = datetime.now(madrid)
    reset = now.replace(hour=h, minute=mi, second=0, microsecond=0)
    if reset <= now:
        reset += timedelta(days=1)
    reset += timedelta(minutes=5)
    return max(60.0, (reset - now).total_seconds())


def _cowork(isin: str, prompt: str, logname: str) -> bool:
    """Corre una skill cowork con AUTO-RESUME. Devuelve True si tras MAX_WAITS sigue topando (fallo)."""
    log = _log(isin, logname)
    for attempt in range(MAX_WAITS + 1):
        subprocess.call([sys.executable, "-m", "tools.claude_cowork", str(log), prompt,
                         "--model", "claude-opus-4-8", "--allowedTools", TOOLS])
        txt = _read(log)
        if not _limit_in(txt):
            return False  # OK
        wait = _reset_seconds(log)
        print(f"[retrofit] {isin}/{logname}: LÍMITE de cuota → durmiendo {wait/60:.0f} min "
              f"hasta el reset (Madrid) y CONTINÚO (intento {attempt + 1}/{MAX_WAITS})", flush=True)
        _time.sleep(min(wait, 6 * 3600))
    return True


def _step_done(isin: str, logname: str) -> bool:
    """El paso ya se completó si su log existe y NO acabó en límite de cuota."""
    log = _log(isin, logname)
    return log.exists() and not _limit_in(_read(log))


def _hist_count(isin: str) -> int:
    try:
        d = json.loads((ROOT / "data" / "funds" / isin / "output.json").read_text(encoding="utf-8"))
        return len((d.get("posiciones", {}) or {}).get("historicas") or [])
    except Exception:
        return -1


def _gap_prompt(isin: str) -> str:
    """Prompt de ar-sourcing con el GAP EXACTO embebido: la skill busca SOLO (tipo, año) que faltan."""
    try:
        from tools.doc_completeness import assess
        a = assess(isin)
        return (f"ar sourcing cowork {isin}. HUECOS a rellenar — busca SOLO estos (tipo, año), NO "
                f"re-busques años/tipos ya cubiertos: AR faltan={a.get('faltan_ar', [])}; "
                f"SAR faltan={a.get('faltan_sar', [])}; cartas faltan={a.get('faltan_carta', [])}. "
                f"Cobertura actual: {a.get('resumen', '')}.")
    except Exception:
        return f"ar sourcing cowork {isin}"


def retrofit_one(isin: str) -> None:
    isin = isin.upper()
    print(f"[retrofit] === {isin} === hist antes = {_hist_count(isin)} años", flush=True)

    # 1) sourcing SOLO de los huecos (skip si ya se completó en un run previo)
    if not _step_done(isin, "skill_ar_sourcing"):
        p = _gap_prompt(isin)
        print(f"[retrofit] {isin} gap: {p[:180]}", flush=True)
        if _cowork(isin, p, "skill_ar_sourcing"):
            print(f"ERROR|{isin}|ar-sourcing sin cuota tras {MAX_WAITS} esperas", flush=True); return
    else:
        print(f"[retrofit] {isin}: ar-sourcing ya hecho, salto", flush=True)

    # 2) extraer todos los AR/SAR (incl. nuevos)
    if _cowork(isin, f"extract pdfs cowork {isin}", "skill_extract_pdfs"):
        print(f"ERROR|{isin}|extract sin cuota", flush=True); return
    subprocess.call([sys.executable, "-m", "agents.orchestrator", "--isin", isin, "--consume-extracted"])

    # 3) analyst con los nuevos años (borra síntesis para forzar re-run)
    syn = ROOT / "data" / "funds" / isin / "analyst_synthesis_cowork.json"
    if syn.exists():
        try:
            syn.replace(syn.with_suffix(".json.bak_retrofit"))
        except Exception:
            pass
    if _cowork(isin, f"analyst cowork {isin}", "skill_analyst"):
        print(f"ERROR|{isin}|analyst sin cuota", flush=True); return
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
        from datetime import timezone
        c = get_client()
        g = c.table("funds").select("fund_group_id").eq("isin", isin).execute().data
        if g:
            c.table("fund_groups").update({"fecha_ultimo_analisis": datetime.now(timezone.utc).isoformat()}
                                          ).eq("fund_group_id", g[0]["fund_group_id"]).execute()
        from tools.sync_to_supabase import _refresh_portal_catalog
        _refresh_portal_catalog(lambda m: None, isin)
    except Exception as e:
        print(f"[retrofit] {isin} sync warn: {str(e)[:80]}", flush=True)

    subprocess.call(["git", "add", f"dashboard/fund-{isin}.html", f"data/funds/{isin}/output.json",
                     "data/known_annual_reports.json", "data/known_manager_letters.json"])
    n = _hist_count(isin)
    subprocess.call(["git", "commit", "-q", "-m", f"retrofit multi-año {isin}: {n} años de evolución de cartera"])
    subprocess.call(["git", "push", "origin", "v2-cowork"], env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
    print(f"DONE|{isin}|{n} años historicas", flush=True)


def main(isins: list[str]) -> None:
    for i, isin in enumerate(isins, 1):
        print(f"[retrofit] fondo {i}/{len(isins)}", flush=True)
        try:
            retrofit_one(isin)
        except Exception as e:
            print(f"ERROR|{isin.upper()}|{str(e)[:100]}", flush=True)
    print("[retrofit] PLAN COMPLETO", flush=True)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 2:
        print("uso: python -m tools.retrofit_fund <ISIN> [<ISIN> ...]"); sys.exit(1)
    main(sys.argv[1:])
