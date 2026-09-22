"""Publica el dashboard de un fondo en TODOS los destinos y VERIFICA que llegó — o falla en voz alta.

Por qué existe (2026-09-22): el dashboard de MontLake se regeneró en local pero (a) el commit
salió sin el código (un `git add` con una ruta inexistente abortó en silencio), (b) Supabase
Storage se quedó con la versión anterior y (c) nadie comprobó lo que servía el Worker. Tres
destinos, tres formas de quedarse a medias sin enterarse. Este es el ÚNICO camino para publicar
un dashboard (lo usa el .bat y cualquier regeneración manual):

  1. sello de build en el HTML (<!-- hf-build:<sha12> -->) → identifica la versión sin ambigüedad
  2. git: add EXPLÍCITO de los ficheros (falla si alguno no existe), commit y comprobación de que
     el commit CONTIENE el HTML; push con reintentos (tools.git_autopush)
  3. Supabase Storage: HTML + output.json (tools.upload_dashboards) y relectura para comprobar el sello
  4. Worker (Cloudflare): sondeo hasta que sirva el sello (deploy ≈1-3 min) — timeout configurable
  5. informe final: OK solo si los tres destinos sirven el MISMO sello; si no, exit 1 y motivo

CLI: python -m tools.publish_dashboard --isin IE000Z9YV312 [--no-git] [--no-storage] [--wait 600]
     python -m tools.publish_dashboard --verify IE000Z9YV312     (solo comprobar, sin publicar)
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKER = os.environ.get("FUND_WORKER_BASE", "https://fund-analyzer.rafagdominguez96.workers.dev")
BRANCH = "v2-cowork"
_STAMP = re.compile(r"<!-- hf-build:([0-9a-f]{12}) -->")


def _git(*args, timeout=180):
    p = subprocess.run(["git", *args], cwd=str(ROOT), capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout,
                       env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def stamp_html(html_path: Path) -> str:
    """Escribe/actualiza el sello de build (hash del contenido sin el sello). Devuelve el sello."""
    txt = html_path.read_text(encoding="utf-8")
    body = _STAMP.sub("", txt)
    sha = hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
    new = f"<!-- hf-build:{sha} -->\n" + body.lstrip("\n")
    if new != txt:
        html_path.write_text(new, encoding="utf-8")
    return sha


def stamp_of(text: str) -> str | None:
    m = _STAMP.search(text or "")
    return m.group(1) if m else None


def _fetch_worker(isin: str) -> str:
    import httpx
    r = httpx.get(f"{WORKER}/fund-{isin}", params={"_": int(time.time())},
                  headers={"Cache-Control": "no-cache"}, timeout=60, follow_redirects=True)
    return r.text if r.status_code == 200 else ""


def _fetch_storage(isin: str) -> str:
    import httpx
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.sync_to_supabase import BUCKET_NAME
    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    r = httpx.get(f"{url}/storage/v1/object/{BUCKET_NAME}/dashboards/fund-{isin}.html",
                  headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=60)
    return r.text if r.status_code == 200 else ""


def verify(isin: str, expected: str, wait: int = 0, log=print) -> dict:
    """Comprueba que Storage y Worker sirven el sello `expected`. Con `wait`, sondea el Worker."""
    res = {"esperado": expected}
    try:
        res["storage"] = stamp_of(_fetch_storage(isin))
    except Exception as e:  # noqa: BLE001
        res["storage"] = f"error: {str(e)[:60]}"
    t0 = time.time()
    while True:
        try:
            res["worker"] = stamp_of(_fetch_worker(isin))
        except Exception as e:  # noqa: BLE001
            res["worker"] = f"error: {str(e)[:60]}"
        if res["worker"] == expected or time.time() - t0 >= wait:
            break
        log(f"[publish] Worker aún sirve {res['worker']} (esperado {expected}) — reintento en 30 s")
        time.sleep(30)
    res["ok"] = res["storage"] == expected and res["worker"] == expected
    return res


def publish(isin: str, do_git: bool = True, do_storage: bool = True, wait: int = 600, log=print) -> int:
    isin = isin.upper()
    html = ROOT / "dashboard" / f"fund-{isin}.html"
    if not html.exists():
        log(f"[publish] ERROR: no existe {html}")
        return 1
    sello = stamp_html(html)
    log(f"[publish] {isin} sello {sello}")

    if do_git:
        files = [f"dashboard/fund-{isin}.html"]
        for extra in ("dashboard/_class_map.json",):
            if (ROOT / extra).exists():
                files.append(extra)
        rc, _, err = _git("add", "--", *files)          # explícito: falla si falta alguno
        if rc != 0:
            log(f"[publish] ERROR git add: {err[-200:]}")
            return 1
        rc, out, _ = _git("diff", "--cached", "--name-only")
        if out.strip():
            rc, _, err = _git("commit", "-q", "-m", f"auto: regen dashboard {isin} (build {sello})")
            if rc != 0:
                log(f"[publish] ERROR git commit: {err[-200:]}")
                return 1
            rc, out, _ = _git("show", "--stat", "--name-only", "--format=", "HEAD")
            if f"dashboard/fund-{isin}.html" not in out:      # el fallo del 21-sep, imposible de repetir
                log(f"[publish] ERROR: el commit NO contiene el HTML (stat: {out[:200]})")
                return 1
            log(f"[publish] commit OK ({out.count(chr(10)) + 1} fichero/s)")
        else:
            log("[publish] git: sin cambios que commitear")
        from tools.git_autopush import push
        if push(BRANCH) != 0:
            log("[publish] ERROR: push no realizado")
            return 1

    if do_storage:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        from tools.upload_dashboards import upload_one
        r = upload_one(get_client(), isin, log=log)
        if not r or not r.get("html"):        # upload_one devuelve {'html': bytes, 'json': bytes}
            log(f"[publish] ERROR: subida a Storage fallida ({r})")
            return 1

    res = verify(isin, sello, wait=wait if do_git else 0, log=log)
    log(f"[publish] verificación: storage={res['storage']} worker={res['worker']} esperado={sello} → "
        f"{'OK en todos los destinos' if res['ok'] else 'DESAJUSTE'}")
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin")
    ap.add_argument("--verify", metavar="ISIN", help="solo comprobar sello local vs Storage vs Worker")
    ap.add_argument("--no-git", action="store_true")
    ap.add_argument("--no-storage", action="store_true")
    ap.add_argument("--wait", type=int, default=600, help="segundos máx. esperando al deploy del Worker")
    a = ap.parse_args()
    if a.verify:
        isin = a.verify.upper()
        local = stamp_of((ROOT / "dashboard" / f"fund-{isin}.html").read_text(encoding="utf-8")) or "(sin sello)"
        r = verify(isin, local)
        print(f"local={local} storage={r['storage']} worker={r['worker']} → {'OK' if r['ok'] else 'DESAJUSTE'}")
        sys.exit(0 if r["ok"] else 1)
    if not a.isin:
        ap.error("--isin o --verify")
    sys.exit(publish(a.isin, do_git=not a.no_git, do_storage=not a.no_storage, wait=a.wait))
