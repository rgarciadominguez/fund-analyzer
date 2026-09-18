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


def _push_target() -> str:
    """Destino del push. El SERVIDOR no tiene credencial de git guardada (allí GCM no puede
    preguntar: 'Cannot prompt because user interactivity has been disabled', 17-sep-2026). Si en
    `.env` hay GITHUB_PUSH_TOKEN (fine-grained PAT con permiso Contents:write sobre el repo), se
    empuja por HTTPS con ese token. El token NUNCA se imprime (stderr de git se filtra)."""
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO / ".env")
    except Exception:
        pass
    tok = (os.environ.get("GITHUB_PUSH_TOKEN") or "").strip()
    if not tok:
        return "origin"
    rc, url, _ = _git("remote", "get-url", "origin", timeout=30)
    if rc != 0 or not url.startswith("https://"):
        return "origin"
    host_path = url.split("://", 1)[1].split("@")[-1]          # github.com/usuario/repo.git
    return f"https://x-access-token:{tok}@{host_path}"


def push(branch: str = "v2-cowork", retries: int = 4) -> int:
    n = pending(branch)
    if n == 0:
        print("[git_autopush] al día con origin (nada que subir)")
        return 0
    target = _push_target()
    via = "token de .env" if target != "origin" else "credencial del sistema"
    print(f"[git_autopush] {n if n > 0 else '?'} commit(s) pendientes -> push origin {branch} ({via})")
    err = ""
    for i in range(1, retries + 1):
        try:
            rc, _, err = _git("push", target, f"{branch}:{branch}")
            if target != "origin":
                err = err.replace(target, "<remote>")            # jamás filtrar el token
        except subprocess.TimeoutExpired:
            rc, err = 1, "timeout (180s)"
        if rc == 0:
            if target != "origin":
                # push por URL no actualiza origin/<branch>; alinearlo para que `pending` sea 0
                sha = _git("rev-parse", branch, timeout=30)[1]
                if sha:
                    _git("update-ref", f"refs/remotes/origin/{branch}", sha, timeout=30)
            print(f"[git_autopush] [OK] push hecho (intento {i})")
            return 0
        print(f"[git_autopush] [WARN] intento {i}/{retries} falló: {err[-300:]}")
        if i < retries:
            time.sleep(min(15 * i, 60))
    print("[git_autopush] [ERROR] push NO realizado; lo reintentará el guardián / el próximo run")
    return 1


if __name__ == "__main__":
    # El .bat redirige la salida a un log con la consola en cp1252: un carácter no codificable
    # tumbaba el proceso ANTES de hacer el push. Nunca debe fallar por imprimir.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", default="v2-cowork")
    ap.add_argument("--retries", type=int, default=4)
    a = ap.parse_args()
    sys.exit(push(a.branch, a.retries))
