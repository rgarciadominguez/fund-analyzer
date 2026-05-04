"""Pipeline robusto para UN fondo (orden serial obligatorio):
   1. extractor → 2. validar (si incompleto: retry 1x) → 3. analyst → 4. dashboard → 5. quality.

Pasos clave anti-fallo:
- Atomic writes en intl_data.json y output.json (ya en código).
- Retry con backoff en concept_extractor para errores transitorios Gemini.
- Timeout duro OPUS_AUDIT_MAX_SECONDS=300 en analyst (env var).
- Validador post-extractor: si los criterios de completitud no se cumplen,
  reintenta el extractor 1 vez antes de pasar al analyst.

Uso:
    python scripts/run_fund_pipeline_robust.py <ISIN> [--nombre N] [--gestora G]
    python scripts/run_fund_pipeline_robust.py <ISIN1> <ISIN2> ...

Cuando se pasan múltiples ISINs los procesa SECUENCIALMENTE (nunca en paralelo).
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

ENV_BASE = dict(os.environ)
ENV_BASE["PYTHONIOENCODING"] = "utf-8"

# Config por defecto: opus audit con timeout 5 min para que el analyst no se cuelgue
ENV_BASE.setdefault("OPUS_AUDIT_MAX_SECONDS", "300")


# ── Cleanup garantizado de subprocesses hijos ─────────────────────────
# Mantiene una lista global de procesos vivos. Al recibir SIGINT/SIGTERM
# o cualquier exit, mata todos los hijos. Garantiza que jamás quedan
# zombis del extractor/analyst sobreviviendo al runner.
_LIVE_CHILDREN: list[subprocess.Popen] = []


def _terminate_all_children():
    for p in list(_LIVE_CHILDREN):
        try:
            if p.poll() is None:  # aún vivo
                p.terminate()
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()
        except Exception:
            pass


atexit.register(_terminate_all_children)


def _signal_handler(signum, frame):
    print(f"\n[!] Señal {signum} recibida — terminando subprocesses hijos", flush=True)
    _terminate_all_children()
    sys.exit(128 + signum)


for _sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(_sig, _signal_handler)
    except (ValueError, OSError):
        pass  # algunas señales no disponibles en Windows


def _run(label: str, cmd: list[str], log_path: Path, env: dict | None = None) -> int:
    """Ejecuta un comando, escribe stdout/stderr al log, devuelve exit code.
    Registra el proceso en _LIVE_CHILDREN para garantizar cleanup en exit."""
    print(f"  {label}...", flush=True)
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=(env or ENV_BASE))
        _LIVE_CHILDREN.append(proc)
        try:
            rc = proc.wait()
        finally:
            try:
                _LIVE_CHILDREN.remove(proc)
            except ValueError:
                pass
    elapsed = time.time() - t0
    print(f"    exit={rc}  time={elapsed:.0f}s  log={log_path.name}", flush=True)
    # Adaptador para que las llamadas existentes que usaban .returncode sigan
    # funcionando — devolvemos un namespace compatible.
    class _R:
        def __init__(self, code): self.returncode = code
    result = _R(rc)
    return result.returncode


def _load_meta(isin: str) -> tuple[str, str]:
    """Lee nombre + gestora desde varias fuentes en cascada."""
    fund_dir = ROOT / "data" / "funds" / isin

    # 1) output.json previo (si existe pipeline ya corrido)
    p = fund_dir / "output.json"
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            n = d.get("nombre") or ""
            g = d.get("gestora") or ""
            if n and g:
                return n, g
        except Exception:
            pass

    # 2) manager_profile.json
    p = fund_dir / "manager_profile.json"
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            n = d.get("fund_name") or d.get("nombre") or ""
            g = d.get("gestora") or ""
            if n or g:
                return n, g
        except Exception:
            pass

    # 3) cssf_data.json (LU funds) — identity card
    p = fund_dir / "cssf_data.json"
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            ident = d.get("identity") or {}
            n = ident.get("fund_name") or ident.get("nombre") or ""
            g = ident.get("management_company") or ident.get("gestora") or ""
            if n or g:
                return n, g
        except Exception:
            pass

    # 4) intl_data.json existente
    p = fund_dir / "intl_data.json"
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            n = d.get("nombre") or ""
            g = d.get("gestora") or ""
            if n or g:
                return n, g
        except Exception:
            pass

    return "", ""


def run_pipeline(isin: str, nombre: str = "", gestora: str = "") -> dict:
    """Ejecuta pipeline completo con auto-retry. Devuelve diagnóstico final."""
    py = sys.executable
    fund_dir = ROOT / "data" / "funds" / isin
    fund_dir.mkdir(parents=True, exist_ok=True)

    if not nombre or not gestora:
        meta_n, meta_g = _load_meta(isin)
        nombre = nombre or meta_n
        gestora = gestora or meta_g

    print(f"\n==================== {isin} ({nombre} / {gestora}) ====================", flush=True)

    # Importar validator dinámicamente para usar la versión más reciente
    from agents.pipeline_validator import validate_pipeline

    extractor_cmd = [
        py, "-m", "agents.intl_extractor_v2",
        "--isin", isin, "--nombre", nombre, "--gestora", gestora,
    ]

    # 1) Extractor (1ª pasada)
    rc = _run("EXTRACTOR (pasada 1)", extractor_cmd, fund_dir / "extractor_run.log")

    # Validar resultado del extractor antes de seguir
    val = validate_pipeline(isin)
    if val["should_retry_extractor"]:
        print(f"    !!! Extractor pasada 1 incompleto. Razones:", flush=True)
        for r in val["reasons"]:
            print(f"        - {r}", flush=True)

        # ── Estrategia retry conservadora ──
        # Principio: la pasada 2 NUNCA debe dejar peor resultado que la 1.
        #   1. Backup intl_data.json de pasada 1 a .pass1.bak.
        #   2. Invalidación SELECTIVA de caches: SOLO los que tengan
        #      `value: null/[]/{}/empty` (los que NO devolvieron nada). Los
        #      caches con datos válidos se preservan.
        #   3. Re-correr extractor (la nueva pasada usa caches válidos +
        #      reintenta solo los vacíos).
        #   4. Comparar pass2 vs pass1 (heurística: más posiciones, más bytes,
        #      más conceptos rellenos). Quedarse con el mejor.
        intl_path = fund_dir / "intl_data.json"
        backup_path = fund_dir / "intl_data.pass1.bak"
        pass1_metrics = dict(val["metrics"])
        if intl_path.exists():
            backup_path.write_bytes(intl_path.read_bytes())
            print(f"    → Backup pasada 1 → {backup_path.name} ({intl_path.stat().st_size:,}b)", flush=True)

        # Invalidación SELECTIVA de caches: solo los vacíos (value=null/[]/{}).
        # Los caches con datos reales (top_holdings con holdings, etc.) NO se tocan.
        cache_dir = fund_dir / "cache" / "extractor_v3"
        invalidated = 0
        if cache_dir.exists():
            for cf in cache_dir.glob("*.json"):
                try:
                    cdata = json.loads(cf.read_text(encoding="utf-8"))
                except Exception:
                    continue
                # Considerar vacío: value es None, lista vacía, dict vacío, o
                # un dict con solo claves cuyos valores son todos None/empty.
                v = cdata.get("value") if isinstance(cdata, dict) else None
                is_empty = False
                if v is None:
                    is_empty = True
                elif isinstance(v, (list, tuple)) and len(v) == 0:
                    is_empty = True
                elif isinstance(v, dict):
                    if not v:
                        is_empty = True
                    else:
                        all_empty = True
                        for vv in v.values():
                            if vv not in (None, "", [], {}):
                                all_empty = False
                                break
                        is_empty = all_empty
                if is_empty:
                    cf.unlink()
                    invalidated += 1
        print(f"    → Invalidados {invalidated} caches vacíos (selectivo, los datos válidos se preservan)", flush=True)
        print("    → Reintentando extractor (pasada 2)", flush=True)
        rc = _run("EXTRACTOR (pasada 2)", extractor_cmd, fund_dir / "extractor_run.log")

        # Comparar pasada 1 vs 2: quedarse con el mejor
        val2 = validate_pipeline(isin)
        pass2_metrics = dict(val2["metrics"])

        def _score(m: dict) -> tuple:
            """Tupla comparable: (posiciones, bytes intl_data, históricas con datos)."""
            return (
                m.get("actuales") or 0,
                m.get("intl_data_bytes") or 0,
                m.get("historicas_count") or 0,
            )

        s1 = _score(pass1_metrics)
        s2 = _score(pass2_metrics)
        print(f"    → Comparativa: pass1={s1} vs pass2={s2}", flush=True)

        if s2 > s1:
            print(f"    → Pasada 2 mejor → mantenida", flush=True)
            val = val2
            try: backup_path.unlink()
            except Exception: pass
        elif s2 < s1:
            # Pasada 2 peor: restaurar pass1
            print(f"    !!! Pasada 2 PEOR que pasada 1 → restaurando pass1 desde backup", flush=True)
            if backup_path.exists():
                intl_path.write_bytes(backup_path.read_bytes())
                backup_path.unlink()
            val = validate_pipeline(isin)
        else:
            # s1 == s2 (igual de pobre): el cache del mapper probablemente está
            # mal mapeado y la invalidación selectiva del extractor no ayuda.
            # PASE 3: reset TOTAL del cache (mapper + extractor) y reintentar.
            print(f"    → Pasada 2 == Pasada 1. Si incompleto, reset TOTAL de caches", flush=True)
            val = val2
            if val["should_retry_extractor"]:
                mapper_dir = fund_dir / "cache" / "mapper"
                wiped = 0
                for d in (cache_dir, mapper_dir):
                    if d.exists():
                        for cf in d.glob("*.json"):
                            try:
                                cf.unlink()
                                wiped += 1
                            except Exception:
                                pass
                if intl_path.exists():
                    intl_path.unlink()
                print(f"    → Reset total: {wiped} caches borrados (mapper + extractor)", flush=True)
                print("    → Reintentando extractor (pasada 3 — sin cache)", flush=True)
                rc = _run("EXTRACTOR (pasada 3)", extractor_cmd, fund_dir / "extractor_run.log")
                val3 = validate_pipeline(isin)
                pass3_metrics = dict(val3["metrics"])
                s3 = _score(pass3_metrics)
                print(f"    → Comparativa: pass1={s1} vs pass3={s3}", flush=True)
                if s3 > s1:
                    print(f"    → Pasada 3 mejor que pasada 1 → mantenida", flush=True)
                    val = val3
                    try: backup_path.unlink()
                    except Exception: pass
                else:
                    print(f"    !!! Pasada 3 NO mejora pasada 1 → restaurando pass1", flush=True)
                    if backup_path.exists():
                        intl_path.write_bytes(backup_path.read_bytes())
                        backup_path.unlink()
                    val = validate_pipeline(isin)
            else:
                try: backup_path.unlink()
                except Exception: pass

        if val["should_retry_extractor"]:
            print(f"    !!! El mejor resultado tras los retries sigue incompleto. Continuando.", flush=True)
            for r in val["reasons"]:
                print(f"        - {r}", flush=True)

    print(f"    Estado extractor: pos={val['metrics']['actuales']} "
          f"intl_data={val['metrics']['intl_data_bytes']:,}b", flush=True)

    # 2) Analyst (con timeout Opus duro de 5 min)
    rc = _run("ANALYST", [py, "-m", "agents.analyst_agent", isin],
              fund_dir / "analyst_run.log")

    # 3) Dashboard
    rc = _run("DASHBOARD", [py, "dashboard/generate_dashboard.py", isin],
              fund_dir / "dashgen.log")

    # 4) Quality
    rc = _run("QUALITY", [py, "-m", "agents.dashboard_quality_agent", isin],
              fund_dir / "quality_run.log")

    # 5) Validación final
    val_final = validate_pipeline(isin)
    # Touch flag de finalización para detección robusta desde monitor externo.
    # Tocar SIEMPRE (incluso si validación falla), para no dejar al monitor
    # esperando indefinidamente.
    try:
        (fund_dir / ".pipeline_done").write_text(
            f"{val_final.get('valid', False)}\n", encoding="utf-8"
        )
    except Exception:
        pass
    quality_log = (fund_dir / "quality_run.log").read_text(encoding="utf-8", errors="ignore")
    n_fallos = quality_log.count("\n  • ")

    print(f"    === RESULTADO ===", flush=True)
    print(f"    valid={val_final['valid']}  pos={val_final['metrics']['actuales']}  "
          f"gestora=\"{val_final['metrics']['gestora']}\"  "
          f"AUM={val_final['metrics']['aum_meur']}  "
          f"fallos_quality={n_fallos}", flush=True)
    if not val_final["valid"]:
        print(f"    Razones:", flush=True)
        for r in val_final["reasons"]:
            print(f"      - {r}", flush=True)
    return val_final


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("isins", nargs="+", help="ISINs a procesar (en orden)")
    parser.add_argument("--nombre", default="", help="Nombre del fondo (solo si 1 ISIN)")
    parser.add_argument("--gestora", default="", help="Gestora (solo si 1 ISIN)")
    args = parser.parse_args()

    results = {}
    for isin in args.isins:
        n = args.nombre if len(args.isins) == 1 else ""
        g = args.gestora if len(args.isins) == 1 else ""
        results[isin] = run_pipeline(isin, n, g)

    print("\n" + "=" * 60, flush=True)
    print("RESUMEN GLOBAL", flush=True)
    print("=" * 60, flush=True)
    for isin, r in results.items():
        m = r["metrics"]
        status = "✅" if r["valid"] else "⚠️"
        print(f"{status} {isin}: pos={m['actuales']} gestora=\"{m['gestora']}\" "
              f"intl={m['intl_data_bytes']:,}b output={m['output_bytes']:,}b", flush=True)


if __name__ == "__main__":
    main()
