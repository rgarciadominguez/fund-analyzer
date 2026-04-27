"""
Quality-Loop-Only: ejecuta fixes sobre fondo ya procesado.

NO re-ejecuta CNMV/Letters/etc. Solo:
1. Patch directo nombre_match_latest_pdf (lee PDF, sin LLM)
2. Quality Loop con safeguards (max_iter, no-progress abort)
3. Re-genera HTML

Uso:
    python run_quality_only.py ES0175437039
    python run_quality_only.py ES0175437039 ES0140794001 ...

Salvaguardas:
- Lee output.json ANTES de cualquier LLM, hace backup .pre_quality.json
- max_iter cap (default 3)
- Aborta si una iteracion no reduce fallos
- Anti-invencion: las reglas resumen_returns_match_data e historia_kpis_calculables
  detectan invenciones en la siguiente iteracion
"""
import sys
import json
import re
import shutil
import subprocess
from pathlib import Path
from datetime import datetime

# Fix Windows console encoding
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# Load .env BEFORE importing agents (Gemini SDK needs GOOGLE_API_KEY at init)
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


def log(isin: str, level: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{isin}] [{level}] {msg}")


def patch_nombre_from_pdf(isin: str) -> bool:
    """Patch directo del nombre desde el ultimo PDF semestral CNMV.
    Devuelve True si lo cambio, False si no hizo falta."""
    import pdfplumber

    fund_dir = ROOT / "data" / "funds" / isin
    out_path = fund_dir / "output.json"
    if not out_path.exists():
        log(isin, "WARN", f"No existe {out_path}")
        return False

    pdf_dir = fund_dir / "raw" / "reports"
    pdfs = sorted(pdf_dir.glob(f"CNMV_{isin}_*_H*.pdf"))
    if not pdfs:
        log(isin, "WARN", "Sin PDFs CNMV")
        return False
    latest = pdfs[-1]

    try:
        with pdfplumber.open(latest) as pdf:
            text = pdf.pages[0].extract_text() or ""
    except Exception as exc:
        log(isin, "ERROR", f"Error leyendo PDF: {exc}")
        return False

    nombre_pdf = ""
    gestora_pdf = ""
    for line in text.split("\n")[:20]:
        line = line.strip()
        if not line:
            continue
        # Nombre: primera linea en mayusculas que no sea cabecera
        if not nombre_pdf and any(c.isalpha() for c in line) and line == line.upper() and len(line) > 5:
            if not any(s in line.upper() for s in ("INFORME", "Nº REGISTRO", "REGISTRO CNMV")):
                nombre_pdf = line
        # Gestora: capturar SOLO linea que empieza por "Gestora:" (NO "Grupo Gestora:")
        # Excluir lineas que empiezan con "Grupo"
        if not line.lower().startswith("grupo"):
            m = re.search(r"^Gestora:\s*([^|]+?)(?:\s+Depositario:|$)", line, re.I)
            if m and not gestora_pdf:
                gestora_pdf = m.group(1).strip()

    if not nombre_pdf:
        log(isin, "WARN", "No se pudo extraer nombre del PDF")
        return False

    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)

    nombre_old = data.get("nombre", "")
    gestora_old = data.get("gestora", "")
    changed = False

    # Comparar tokens significativos
    def tokens(s):
        return set(re.findall(r"[A-ZÁÉÍÓÚÑ]{4,}", s.upper()))

    GENERIC = {"FONDO", "FONDOS", "INVERSION", "EURO", "EUROS"}
    common = (tokens(nombre_old) - GENERIC) & (tokens(nombre_pdf) - GENERIC)
    n_pdf_sig = len(tokens(nombre_pdf) - GENERIC)
    if n_pdf_sig and len(common) / n_pdf_sig < 0.5:
        log(isin, "FIX", f"Nombre: '{nombre_old}' -> '{nombre_pdf}'")
        data["nombre"] = nombre_pdf
        changed = True

    if gestora_pdf and not gestora_old:
        log(isin, "FIX", f"Gestora: '' -> '{gestora_pdf}'")
        data["gestora"] = gestora_pdf
        changed = True
    elif gestora_pdf and gestora_pdf.upper() not in gestora_old.upper() and gestora_old.upper() not in gestora_pdf.upper():
        # Mismatch significativo
        log(isin, "FIX", f"Gestora: '{gestora_old}' -> '{gestora_pdf}'")
        data["gestora"] = gestora_pdf
        changed = True

    if changed:
        # Backup
        bak = fund_dir / "output.pre_nombre_fix.json"
        if not bak.exists():
            shutil.copy(out_path, bak)
        # Marcar paths editados manualmente para que analyst no los sobrescriba
        try:
            from tools.output_merger import mark_manual_edit
            mark_manual_edit(data, "nombre")
            if data.get("gestora"):
                mark_manual_edit(data, "gestora")
        except Exception:
            pass
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        # IMPORTANTE: tambien patchear cnmv_data.json (fuente de verdad para analyst)
        cnmv_path = fund_dir / "cnmv_data.json"
        if cnmv_path.exists():
            with open(cnmv_path, encoding="utf-8") as f:
                cd = json.load(f)
            cd_changed = False
            if cd.get("nombre") != data["nombre"]:
                cd["nombre"] = data["nombre"]
                cd_changed = True
            if data.get("gestora") and cd.get("gestora") != data["gestora"]:
                cd["gestora"] = data["gestora"]
                cd_changed = True
            if cd_changed:
                bak_cnmv = fund_dir / "cnmv_data.pre_nombre_fix.json"
                if not bak_cnmv.exists():
                    shutil.copy(cnmv_path, bak_cnmv)
                with open(cnmv_path, "w", encoding="utf-8") as f:
                    json.dump(cd, f, indent=2, ensure_ascii=False)
                log(isin, "OK", "cnmv_data.json tambien patcheado")

        log(isin, "OK", f"output.json patcheado (backup en {bak.name})")
    else:
        log(isin, "INFO", "Nombre/Gestora ya correctos")

    return changed


def run_quality_loop(isin: str, max_iter: int = 3) -> dict:
    """Ejecuta quality loop sobre fondo ya procesado.
    Re-corre solo agentes upstream necesarios + analyst, NO cnmv."""
    from agents.dashboard_quality_agent import DashboardQualityAgent

    fund_dir = ROOT / "data" / "funds" / isin
    out_path = fund_dir / "output.json"

    # Backup pre-loop
    bak = fund_dir / "output.pre_quality_loop.json"
    if not bak.exists() and out_path.exists():
        shutil.copy(out_path, bak)

    quality = DashboardQualityAgent(isin)
    report = quality.run()

    n_estructura = sum(1 for f in report.get("fallos", []) if f.get("fail_type") in ("estructura", "content"))
    n_total = len(report.get("fallos", []))
    log(isin, "QUALITY", f"Iter 0 — {n_total} fallos ({n_estructura} corregibles), {report.get('score_display')}")

    iteration = 0
    prev_total = n_total + 1  # asegurar entrada al loop

    # Cargar config para analyst
    cfg_path = fund_dir / "config.json"
    if cfg_path.exists():
        config = json.loads(cfg_path.read_text(encoding="utf-8"))
    else:
        config = {"objetivo": "1", "horizonte_historico": "1", "fuentes": "1", "clase_accion": "I EUR", "contexto_adicional": ""}

    # Cargar hints de output
    data = json.loads(out_path.read_text(encoding="utf-8"))
    fund_name_hint = data.get("nombre", "")
    gestora_hint = data.get("gestora", "")
    anio_creacion = (data.get("kpis") or {}).get("anio_creacion")
    gestores_hint = data.get("gestores", {}).get("equipo", []) if isinstance(data.get("gestores"), dict) else []

    while n_estructura > 0 and iteration < max_iter and n_total < prev_total:
        iteration += 1
        prev_total = n_total

        log(isin, "QUALITY", f"Iter {iteration}/{max_iter} — re-ejecutando agentes")

        # Agrupar por agente
        fallos = [f for f in report.get("fallos", []) if f.get("fail_type") in ("estructura", "content")]
        agentes = {}
        for f in fallos:
            ag = f.get("agente_responsable", "analyst_agent")
            agentes.setdefault(ag, []).append(f)

        log(isin, "QUALITY", f"Fallos por agente: " + ", ".join(f"{a}={len(fs)}" for a, fs in agentes.items()))

        # CNMV enrichment retry (sectores, serie_rentabilidad, mix normalize)
        # NO re-descarga, solo enriquece datos existentes (Gemini Flash batch)
        if "cnmv_agent" in agentes:
            try:
                from agents.cnmv_enrichment import CNMVEnricher
                log(isin, "RETRY", "cnmv_enrichment (sectores, rentabilidad, mix)")
                enricher = CNMVEnricher(isin, quality_feedback=agentes["cnmv_agent"])
                enricher.run()
                log(isin, "OK", "cnmv_enrichment completado")
            except Exception as exc:
                log(isin, "ERROR", f"cnmv_enrichment fallo: {str(exc)[:120]}")

        # CASCADA managers: profiler → deep_agent → google_snippets → sibling
        if "manager_deep_agent" in agentes:
            import asyncio

            def _has_real_managers() -> bool:
                prof_path = ROOT / "data" / "funds" / isin / "manager_profile.json"
                if not prof_path.exists():
                    return False
                try:
                    p = json.loads(prof_path.read_text(encoding="utf-8"))
                except Exception:
                    return False
                names = p.get("equipo_gestor") or p.get("equipo") or []
                real = [n for n in names if isinstance(n, str) and n.strip()
                        and not n.lower().startswith("equipo")]
                return len(real) > 0

            try:
                from agents.manager_profiler import ManagerProfiler
                log(isin, "RETRY", "Cascada 1/4 manager_profiler")
                manager = ManagerProfiler(isin, fund_name=fund_name_hint, gestora=gestora_hint, manager_names=gestores_hint or None)
                asyncio.run(manager.run())
                log(isin, "OK", f"manager_profiler done (real={_has_real_managers()})")
            except Exception as exc:
                log(isin, "ERROR", f"profiler fallo: {str(exc)[:120]}")

            if not _has_real_managers():
                try:
                    from agents.manager_deep_agent import ManagerDeepAgent
                    log(isin, "RETRY", "Cascada 2/4 manager_deep_agent")
                    deep = ManagerDeepAgent(isin=isin, fund_name=fund_name_hint, gestora=gestora_hint, manager_names=gestores_hint or None)
                    asyncio.run(deep.run())
                    log(isin, "OK", f"deep done (real={_has_real_managers()})")
                except Exception as exc:
                    log(isin, "ERROR", f"deep fallo: {str(exc)[:120]}")

            if not _has_real_managers():
                try:
                    from agents.manager_google_snippets import find_managers, save_to_manager_profile, sync_to_output
                    log(isin, "RETRY", "Cascada 3/4 google_snippets")
                    res = find_managers(isin, fund_name_hint, gestora_hint)
                    if res.get("managers"):
                        save_to_manager_profile(isin, res)
                        sync_to_output(isin, res["managers"], gestora_hint)
                        log(isin, "OK", f"snippets: {res['managers']}")
                    else:
                        log(isin, "INFO", "snippets: vacio")
                except Exception as exc:
                    log(isin, "ERROR", f"snippets fallo: {str(exc)[:120]}")

            if not _has_real_managers():
                try:
                    from tools.sibling_finder import propagate_gestores
                    log(isin, "RETRY", "Cascada 4/4 sibling_finder")
                    r = propagate_gestores(isin, dry_run=False)
                    log(isin, "OK", f"sibling: {r.get('status')} from {r.get('from','-')}")
                except Exception as exc:
                    log(isin, "ERROR", f"sibling fallo: {str(exc)[:120]}")

        # Analyst retry SIEMPRE con quality_feedback (rebuilt resumen/historia/etc)
        if "analyst_agent" in agentes or "cnmv_agent" in agentes:
            try:
                from agents.analyst_agent import AnalystAgent
                log(isin, "RETRY", "analyst_agent con quality_feedback")
                analyst = AnalystAgent(isin, config, quality_feedback=fallos)
                analyst.run()
                log(isin, "OK", "analyst_agent completado")
            except Exception as exc:
                log(isin, "ERROR", f"analyst fallo: {str(exc)[:120]}")

        # Re-evaluar
        report = quality.run()
        n_estructura = sum(1 for f in report.get("fallos", []) if f.get("fail_type") in ("estructura", "content"))
        n_total = len(report.get("fallos", []))
        log(isin, "QUALITY", f"Iter {iteration} — {n_total} fallos ({n_estructura} corregibles), antes {prev_total}")

        if n_total >= prev_total:
            log(isin, "WARN", f"Iter {iteration} no redujo fallos ({n_total} >= {prev_total}) — abortando loop")
            break

    # Re-generar HTML
    try:
        gen_path = ROOT / "dashboard" / "generate_dashboard.py"
        result = subprocess.run(
            [sys.executable, str(gen_path), isin],
            cwd=str(ROOT), capture_output=True, text=True, timeout=120,
            env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"},
        )
        if result.returncode == 0:
            log(isin, "OK", "dashboard HTML regenerado")
        else:
            log(isin, "WARN", f"generate_dashboard fallo: {result.stderr[:200]}")
    except Exception as exc:
        log(isin, "ERROR", f"No pudo regenerar dashboard: {exc}")

    return report


def process_fund(isin: str, max_iter: int = 3):
    log(isin, "START", f"Iniciando quality-only (max_iter={max_iter})")
    # Paso 1: patch nombre directo
    patch_nombre_from_pdf(isin)
    # Paso 2: quality loop con LLM retries
    final_report = run_quality_loop(isin, max_iter=max_iter)
    n_total = len(final_report.get("fallos", []))
    n_estructura = sum(1 for f in final_report.get("fallos", []) if f.get("fail_type") in ("estructura", "content"))
    aceptable = final_report.get("aceptable", False)
    log(isin, "DONE", f"{n_total} fallos finales ({n_estructura} corregibles), aceptable={aceptable}")
    return final_report


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python run_quality_only.py <ISIN> [<ISIN> ...]")
        sys.exit(1)
    for isin in sys.argv[1:]:
        try:
            process_fund(isin.strip().upper())
        except Exception as exc:
            log(isin, "FATAL", f"{exc}")
            import traceback
            traceback.print_exc()
        print()
