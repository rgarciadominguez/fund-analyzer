"""Push robusto de los commits automáticos del dashboard (deploy Cloudflare = git push).

Por qué existe: el bat hacía UN `git push` con la salida descartada. Un fallo transitorio
(SSL/red) dejaba el dashboard sin desplegar, sin motivo en el log y sin reintento; y si el
siguiente run no cambiaba el dashboard, el commit pendiente se quedaba sin subir.

Qué hace: si la rama local va por delante de origin, hace push con reintentos y deja el
motivo del fallo en stdout (→ log del run). Idempotente: si no hay nada pendiente, sale 0.

Uso:  python -m tools.git_autopush [--branch v2-cowork] [--retries 4]
Exit: 0 = al día / push OK · 1 = push falló tras los reintentos
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _git(*args, timeout=180):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="never")
    p = subprocess.run(["git", *args], cwd=str(REPO), env=env, capture_output=True,
                       text=True, encoding="utf-8", errors="replace", timeout=timeout)
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def pending(branch: str) -> int:
    """Nº de commits locales que origin no tiene (-1 si no se puede saber)."""
    rc, out, _ = _git("rev-list", "--count", f"origin/{branch}..{branch}", timeout=60)
    try:
        return int(out) if rc == 0 else -1
    except ValueError:
        return -1


def push(branch: str = "v2-cowork", retries: int = 4) -> int:
    n = pending(branch)
    if n == 0:
        print("[git_autopush] al día con origin (nada que subir)")
        return 0
    print(f"[git_autopush] {n if n > 0 else '?'} commit(s) pendientes → push origin {branch}")
    err = ""
    for i in range(1, retries + 1):
        try:
            rc, _, err = _git("push", "origin", branch)
        except subprocess.TimeoutExpired:
            rc, err = 1, "timeout (180s)"
        if rc == 0:
            print(f"[git_autopush] [OK] push hecho (intento {i})")
            return 0
        print(f"[git_autopush] [WARN] intento {i}/{retries} falló: {err[-300:]}")
        if i < retries:
            time.sleep(min(15 * i, 60))
    print("[git_autopush] [ERROR] push NO realizado; lo reintentará el guardián / el próximo run")
    return 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", default="v2-cowork")
    ap.add_argument("--retries", type=int, default=4)
    a = ap.parse_args()
    sys.exit(push(a.branch, a.retries))
