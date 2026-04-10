"""
Orchestrator — Coordinador del pipeline Fund Analyzer

CLI:
    python -m agents.orchestrator --isin ES0112231008
    python -m agents.orchestrator --isin ES0112231008 --auto   (usa defaults, sin preguntar)

Flujo:
    1. Preguntas clarificatorias (o --auto para usar defaults)
    2. Detecta ES vs INT por prefijo ISIN
    3. Ejecuta: cnmv/intl → letters → analyst
    4. Muestra resumen con rich
    5. Escribe progress.log durante toda la ejecución
"""
import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, SpinnerColumn
from rich.prompt import Prompt
from rich.table import Table

sys.path.insert(0, str(Path(__file__).parent.parent))

console = Console()
ROOT = Path(__file__).parent.parent

# ── Preguntas clarificatorias (del CLAUDE.md) ────────────────────────────────

CLARIFYING_QUESTIONS = [
    {
        "id": "objetivo",
        "pregunta": "Objetivo del analisis?",
        "opciones": [
            "1. Analisis completo (KPIs + cualitativo + cuantitativo + posiciones)",
            "2. Solo KPIs y datos cuantitativos (rapido)",
            "3. Solo analisis cualitativo",
            "4. Solo posiciones actuales y cartera",
            "5. Personalizado",
        ],
        "default": "1",
    },
    {
        "id": "horizonte_historico",
        "pregunta": "Cuantos anos de historico?",
        "opciones": [
            "1. Desde inicio del fondo",
            "2. Ultimos 5 anos",
            "3. Ultimos 3 anos",
            "4. Solo datos actuales",
        ],
        "default": "1",
    },
    {
        "id": "fuentes",
        "pregunta": "Fuentes a incluir?",
        "opciones": [
            "1. Todas (informes + cartas gestores)",
            "2. Solo informes oficiales",
            "3. Solo cartas trimestrales",
        ],
        "default": "1",
    },
    {
        "id": "clase_accion",
        "pregunta": "Clase de accion (fondos INT con multiples clases)?",
        "tipo": "texto_libre",
        "default": "I EUR",
    },
    {
        "id": "contexto_adicional",
        "pregunta": "Algo especifico a priorizar? (enter para omitir)",
        "tipo": "texto_libre",
        "default": "",
    },
]

DEFAULT_CONFIG = {q["id"]: q["default"] for q in CLARIFYING_QUESTIONS}

# Metadatos conocidos por ISIN para --auto (evita buscar en web si ya conocemos el fondo)
KNOWN_FUND_METADATA: dict[str, dict] = {
    "LU1694789451": {
        "nombre": "DNCA INVEST - ALPHA BONDS",
        "gestora": "DNCA Investments",
        "horizonte_historico": "4",  # solo último año disponible — sin PDFs automáticos
    },
    "ES0112231008": {"nombre": "Avantage Fund FI", "gestora": "Avantage Capital SGIIC"},
    "LU0840158819": {
        "nombre": "",
        "gestora": "",
        "horizonte_historico": "4",
    },
}


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(agent: str, level: str, msg: str, log_path: Path) -> None:
    line = f"[{_ts()}] [{agent}] [{level}] {msg}"
    console.log(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ── Configuración ─────────────────────────────────────────────────────────────

def get_config(isin: str, auto: bool) -> dict:
    """Retorna config: reutiliza config.json existente, pide si no existe y no --auto."""
    fund_dir = ROOT / "data" / "funds" / isin
    fund_dir.mkdir(parents=True, exist_ok=True)
    config_path = fund_dir / "config.json"

    # Reutilizar config existente
    if config_path.exists():
        try:
            existing = json.loads(config_path.read_text(encoding="utf-8"))
            if not auto:
                console.print(Panel(
                    f"[cyan]Config guardada encontrada para {isin}[/cyan]\n"
                    + "\n".join(f"  {k}: {v}" for k, v in existing.items()),
                    expand=False,
                ))
                ans = Prompt.ask("Usar la misma configuracion?", choices=["s", "n"], default="s")
                if ans.lower() == "s":
                    return existing
            else:
                return existing
        except Exception:
            pass

    # --auto → usar defaults + metadatos conocidos
    if auto:
        config = dict(DEFAULT_CONFIG)
        if isin in KNOWN_FUND_METADATA:
            config.update(KNOWN_FUND_METADATA[isin])
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        return config

    # Mostrar preguntas interactivas
    console.print(Panel(
        f"[bold cyan]FUND ANALYZER — Configuracion del analisis[/bold cyan]\n"
        f"ISIN: [green]{isin}[/green]",
        expand=False,
    ))
    console.print("\nAntes de iniciar, necesito algunas aclaraciones:\n")

    config = {}
    for i, q in enumerate(CLARIFYING_QUESTIONS, 1):
        console.print(f"[bold][{i}/{len(CLARIFYING_QUESTIONS)}][/bold] {q['pregunta']}")
        if q.get("tipo") == "texto_libre":
            val = Prompt.ask(f"  Valor", default=q["default"])
        else:
            for opt in q.get("opciones", []):
                console.print(f"  {opt}")
            val = Prompt.ask(f"  Seleccion", default=q["default"])
        config[q["id"]] = val
        console.print()

    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config


# ── Pipeline principal ────────────────────────────────────────────────────────

async def analyze_fund(isin: str, auto: bool = False) -> dict:
    """Pipeline completo para un ISIN."""
    isin = isin.strip().upper()
    start_time = time.time()

    log_path = ROOT / "progress.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n[{_ts()}] [ORCHESTRATOR] [START] Pipeline {isin}\n{'='*60}\n")

    def log(agent, level, msg):
        _log(agent, level, msg, log_path)

    # Obtener config
    config = get_config(isin, auto)
    log("ORCHESTRATOR", "OK", f"Config: {config}")

    prefix = isin[:2].upper()
    is_es = prefix == "ES"
    is_lu = prefix == "LU"

    fund_dir = ROOT / "data" / "funds" / isin
    results: dict = {}
    tokens_used = 0

    # ── Progress bar ─────────────────────────────────────────────────────────
    steps_es = [
        ("Agente CNMV", "Descargando datos CNMV"),
        ("Letters Agent", "Buscando cartas trimestrales"),
        ("Analyst Agent", "Sintetizando y llamando a Claude"),
        ("Readings Agent", "Buscando lecturas y análisis externos"),
        ("Meta Agent", "Revisión de calidad del pipeline"),
    ]
    steps_lu = [
        ("CSSF Agent", "Consultando regulador CSSF Luxembourg"),
        ("Intl Agent", "Descargando annual report"),
        ("Letters Agent", "Buscando cartas trimestrales"),
        ("Analyst Agent", "Sintetizando y llamando a Claude"),
        ("Readings Agent", "Buscando lecturas y análisis externos"),
        ("Meta Agent", "Revisión de calidad del pipeline"),
    ]
    steps = steps_lu if is_lu else steps_es

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        main_task = progress.add_task(f"Pipeline {isin}", total=len(steps))

        # ── Paso 0: CSSF Agent (solo para LU) ────────────────────────────────
        if is_lu:
            progress.update(main_task, description="Consultando regulador CSSF Luxembourg")
            log("ORCHESTRATOR", "START", "Paso 0: CSSF Agent")
            try:
                from agents.cssf_agent import CSSFAgent
                cssf = CSSFAgent(isin, config)
                results["cssf"] = await cssf.run()
                log("CSSF", "OK", "cssf_data.json generado")
            except Exception as exc:
                log("CSSF", "ERROR", f"Paso CSSF falló: {exc}")
            progress.advance(main_task)

        # ── Paso 1: Agente fuente ─────────────────────────────────────────────
        progress.update(main_task, description=steps[0][1] if not is_lu else steps[1][1])
        log("ORCHESTRATOR", "START", f"Paso 1: {'CNMV' if is_es else 'INTL'} Agent")

        try:
            if is_es:
                from agents.cnmv_agent import CNMVAgent
                agent = CNMVAgent(isin, config)
                results["cnmv"] = await agent.run()
                log("CNMV", "OK", f"cnmv_data.json generado")
            else:
                from agents.intl_agent import IntlAgent
                agent = IntlAgent(isin, config)
                results["intl"] = await agent.run()
                log("INTL", "OK", f"intl_data.json generado")
        except Exception as exc:
            log("ORCHESTRATOR", "ERROR", f"Paso 1 falló: {exc}")
            import traceback
            log("ORCHESTRATOR", "TRACE", traceback.format_exc()[:300])

        progress.advance(main_task)

        # ── Extract metadata from cnmv_data/intl_data for downstream agents ──
        fund_name_hint = ""
        gestora_hint = ""
        anio_creacion_hint = None
        gestores_hint: list[str] = []

        # Read from the just-generated source data
        for data_fname in ["cnmv_data.json", "intl_data.json", "cssf_data.json"]:
            data_path = fund_dir / data_fname
            if data_path.exists():
                try:
                    src = json.loads(data_path.read_text(encoding="utf-8"))
                    if not fund_name_hint:
                        fund_name_hint = src.get("nombre", "") or src.get("nombre_oficial", "")
                    if not gestora_hint:
                        gestora_hint = src.get("gestora", "") or src.get("gestora_oficial", "")
                    if not anio_creacion_hint:
                        anio_creacion_hint = (src.get("kpis") or {}).get("anio_creacion")
                except Exception:
                    pass

        log("ORCHESTRATOR", "INFO", f"Metadata: nombre={fund_name_hint[:40]}, gestora={gestora_hint[:30]}")

        # ── Paso 2: Sources Agent (descubrimiento de fuentes) ─────────────────
        progress.update(main_task, description="Sources Agent")
        log("ORCHESTRATOR", "START", "Paso 2: Sources Agent")

        try:
            from agents.sources_agent import SourcesAgent
            sources = SourcesAgent(
                isin, fund_name=fund_name_hint,
                gestora=gestora_hint, gestor_principal=gestores_hint[0] if gestores_hint else "",
            )
            results["sources"] = await sources.run()
            n_sources = len(results["sources"].get("sources", []))
            log("SOURCES", "OK", f"{n_sources} fuentes descubiertas")
        except Exception as exc:
            log("SOURCES", "ERROR", f"Sources falló: {exc}")
            results["sources"] = {}

        progress.advance(main_task)

        # ── Paso 3: Letters + Readings + Manager Deep (EN PARALELO) ──────────
        progress.update(main_task, description="Letters + Readings + Manager (paralelo)")
        log("ORCHESTRATOR", "START", "Paso 3: Letters + Readings + Manager (paralelo)")

        async def _run_letters():
            try:
                from agents.letters_agent import LettersAgent
                gestora_url = config.get("gestora_url", "")
                letters = LettersAgent(
                    isin, config, gestora_url=gestora_url,
                    fund_name=fund_name_hint,
                    gestora=gestora_hint,
                    anio_creacion=anio_creacion_hint,
                )
                return await letters.run()
            except Exception as exc:
                log("LETTERS", "ERROR", f"Letters falló: {exc}")
                return {}

        async def _run_readings():
            try:
                from agents.readings_agent import ReadingsAgent
                readings = ReadingsAgent(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, gestores=gestores_hint,
                )
                return await readings.run()
            except Exception as exc:
                log("READINGS", "ERROR", f"Readings falló: {exc}")
                return {}

        async def _run_manager_deep():
            try:
                from agents.manager_deep_agent import ManagerDeepAgent
                manager = ManagerDeepAgent(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, manager_names=gestores_hint or None,
                )
                return await manager.run()
            except Exception as exc:
                log("MANAGER", "ERROR", f"Manager Deep falló: {exc}")
                return {}

        letters_result, readings_result, manager_result = await asyncio.gather(
            _run_letters(), _run_readings(), _run_manager_deep()
        )
        results["letters"] = letters_result
        results["readings"] = readings_result
        results["manager"] = manager_result
        n_cartas = len(letters_result.get("cartas", []))
        n_lecturas = len(readings_result.get("lecturas", []))
        n_externos = len(readings_result.get("analisis_externos", []))
        log("ORCHESTRATOR", "OK",
            f"Letters: {n_cartas} | Readings: {n_lecturas} lect + {n_externos} ext | Manager: {'OK' if manager_result.get('nombre') else 'parcial'}")

        progress.advance(main_task)

        # ── Paso 3b: Letters Deep (segundo pase — necesita letters terminado) ─
        progress.update(main_task, description="Letters Deep Agent")
        log("ORCHESTRATOR", "START", "Paso 3b: Letters Deep Agent")

        try:
            from agents.letters_deep_agent import LettersDeepAgent
            letters_deep = LettersDeepAgent(isin, fund_name=fund_name_hint)
            results["letters_deep"] = await letters_deep.run()
            n_deep = results["letters_deep"].get("deep_extracted", 0)
            log("LETTERS_DEEP", "OK", f"{n_deep} cartas enriquecidas")
        except Exception as exc:
            log("LETTERS_DEEP", "ERROR", f"Letters Deep falló: {exc}")

        progress.advance(main_task)

        # ── Paso 4: Analyst Agent ─────────────────────────────────────────────
        progress.update(main_task, description="Analyst Agent (síntesis)")
        log("ORCHESTRATOR", "START", "Paso 4: Analyst Agent")

        try:
            from agents.analyst_agent import AnalystAgent
            analyst = AnalystAgent(isin, config)
            output = analyst.run()
            results["output"] = output
            log("ANALYST", "OK", "output.json generado")
        except Exception as exc:
            log("ANALYST", "ERROR", f"Paso 3 falló: {exc}")
            import traceback
            log("ANALYST", "TRACE", traceback.format_exc()[:500])
            output = {"isin": isin, "error": str(exc)}
            out_path = fund_dir / "output.json"
            out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

        progress.advance(main_task)

        # ── Paso 4: Validation Agent ──────────────────────────────────────────
        progress.update(main_task, description="Validation Agent")
        log("ORCHESTRATOR", "START", "Paso 4: Validation Agent")

        try:
            from agents.validation_agent import ValidationAgent
            validator = ValidationAgent(isin, fund_dir=fund_dir, config=config)
            results["validation"] = await validator.run()
            quality_score = results["validation"].get("quality_score", 0)
            log("VALIDATION", "OK", f"Validación completada — quality score: {quality_score}/100")
        except Exception as exc:
            log("VALIDATION", "ERROR", f"Paso 4 falló: {exc}")

        progress.advance(main_task)

        # ── Paso 5: Meta Agent ────────────────────────────────────────────────
        progress.update(main_task, description="Meta Agent (QA)")
        log("ORCHESTRATOR", "START", "Paso 5: Meta Agent")

        try:
            from agents.meta_agent import MetaAgent
            meta = MetaAgent(isin, fund_dir=fund_dir, config=config)
            results["meta"] = await meta.run()
            n_issues = len(results["meta"].get("issues", []))
            log("META", "OK", f"Meta-QA completado: {n_issues} issues detectados")
        except Exception as exc:
            log("META", "ERROR", f"Paso 5 falló: {exc}")

        progress.advance(main_task)

        # ── Paso 6: Quality Loop — Dashboard Quality Agent ───────────────────
        # Evalúa output.json contra patrón Avantage. Si no es aceptable,
        # re-ejecuta agentes upstream con feedback hasta max_iter veces.
        progress.update(main_task, description="Quality Loop (Dashboard)")
        log("ORCHESTRATOR", "START", "Paso 6: Dashboard Quality Loop")

        try:
            quality_report = await _run_quality_loop(
                isin, fund_dir, config,
                fund_name_hint=fund_name_hint,
                gestora_hint=gestora_hint,
                anio_creacion_hint=anio_creacion_hint,
                gestores_hint=gestores_hint,
                log=log,
                max_iter=5,
            )
            results["quality"] = quality_report
            score = quality_report.get("score", 0)
            aceptable = quality_report.get("aceptable", False)
            log("QUALITY", "OK", f"Score final: {score}/100 — {'ACEPTABLE' if aceptable else 'INSUFICIENTE'}")
            # Refrescar output después del loop
            out_path = fund_dir / "output.json"
            if out_path.exists():
                results["output"] = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception as exc:
            log("QUALITY", "ERROR", f"Quality loop falló: {exc}")
            import traceback
            log("QUALITY", "TRACE", traceback.format_exc()[:500])

    # ═══════════════════════════════════════════════════════════════════════
    # POST-PIPELINE: Verificación → Publicación → Feedback → Auto-mejora
    # ═══════════════════════════════════════════════════════════════════════

    elapsed = round(time.time() - start_time, 1)
    output  = results.get("output", {})
    meta_result = results.get("meta", {})

    completed_fields = _count_nonempty(output)
    null_fields      = _find_null_fields(output)
    xml_count   = len(list((fund_dir / "raw" / "xml").glob("*.xml"))) if (fund_dir / "raw" / "xml").exists() else 0
    pdf_count   = len(list((fund_dir / "raw").rglob("*.pdf")))
    carta_count = len(list((fund_dir / "raw" / "letters").glob("*.pdf"))) if (fund_dir / "raw" / "letters").exists() else 0

    # ── Resumen de pipeline ───────────────────────────────────────────────────
    table = Table(title=f"Pipeline completado — {isin}", show_header=True)
    table.add_column("Campo", style="cyan")
    table.add_column("Valor", style="green")
    table.add_row("Duracion total", f"{elapsed}s")
    table.add_row("Nombre fondo", output.get("nombre", "-"))
    table.add_row("Gestora", output.get("gestora", "-"))
    table.add_row("AUM actual", f"{output.get('kpis', {}).get('aum_actual_meur', '-')} M€")
    table.add_row("Participes", str(output.get("kpis", {}).get("num_participes", "-")))
    table.add_row("Campos completados", str(completed_fields))
    table.add_row("Campos null", str(len(null_fields)))
    table.add_row("XMLs descargados", str(xml_count))
    table.add_row("PDFs descargados", str(pdf_count))
    table.add_row("Cartas gestores", str(carta_count))
    console.print(table)

    if null_fields:
        console.print(f"[yellow]Campos null: {', '.join(null_fields[:15])}"
                      + (f" (+{len(null_fields)-15} más)" if len(null_fields) > 15 else ""))

    if meta_result.get("issues"):
        console.print(f"\n[bold yellow]! Meta-QA: {len(meta_result['issues'])} issues detectados[/bold yellow]")

    # ── PASO A: Verificar que el fondo está listo para el dashboard ───────────
    from agents.meta_agent import _fund_ready_for_dashboard
    dashboard_ready, blockers = _fund_ready_for_dashboard(output)

    if not dashboard_ready:
        console.print(Panel(
            "[bold red]FONDO NO LISTO PARA EL DASHBOARD[/bold red]\n"
            + "\n".join(f"  • {b}" for b in blockers)
            + "\n\n[yellow]El fondo NO se publicará hasta resolver los blockers.[/yellow]",
            title="Verificación de calidad",
            border_style="red",
        ))
        log("OUTPUT", "WARN", f"Fondo {isin} NO publicado: {'; '.join(blockers)}")
    else:
        console.print(Panel(
            "[bold green]Fondo listo para el dashboard[/bold green]\n"
            + f"  AUM: {output.get('kpis', {}).get('aum_actual_meur', '?')} M€  |  "
            + f"Mix activos: {len(output.get('cuantitativo', {}).get('mix_activos_historico', []))} años  |  "
            + f"Posiciones: {len((output.get('posiciones') or {}).get('actuales', []))}",
            title="Verificación de calidad",
            border_style="green",
        ))

        # ── PASO B: Git commit + push → actualizar Streamlit ─────────────────
        console.print("\n[bold cyan]Publicando en Streamlit...[/bold cyan]")
        git_ok = _git_commit_and_push(isin, output.get("nombre", isin))
        if git_ok:
            console.print("[green]OK Streamlit actualizado — cambios publicados en el repositorio[/green]")
            log("OUTPUT", "OK", "Git push completado — Streamlit Cloud redesplegará en ~1 min")
        else:
            console.print("[yellow]Git push falló — revisar conexión o conflictos[/yellow]")
            log("OUTPUT", "WARN", "Git push falló")

    # ── PASO C: Verificación del output (mostrar datos clave) ─────────────────
    _print_output_verification(output, meta_result)

    # ── PASO D: Recoger feedback del usuario + lanzar Improver ───────────────
    if not auto:
        console.print(Panel(
            "Revisa el fondo en el dashboard y vuelve con tu feedback.\n"
            "[cyan]Puedes escribir aquí tus observaciones[/cyan] (errores, datos incorrectos,\n"
            "mejoras visuales, fuentes que faltan, etc.) o pulsar Enter para saltar.",
            title="Feedback del análisis",
            border_style="green",
        ))
        await _collect_feedback_and_improve(isin, fund_dir)

    separator = "=" * 48
    summary = (
        f"\n{separator}\n"
        f"PIPELINE COMPLETADO — {isin}\n"
        f"Duracion total: {elapsed}s\n"
        f"Dashboard listo: {'SI' if dashboard_ready else 'NO — ' + '; '.join(blockers)}\n"
        f"Campos completados: {completed_fields}\n"
        f"Campos null: {', '.join(null_fields[:10])}\n"
        f"Ficheros: {xml_count} XML + {pdf_count} PDF + {carta_count} cartas\n"
        f"{separator}\n"
    )
    log("OUTPUT", "OK", summary)
    return output


# ── Quality Loop ─────────────────────────────────────────────────────────────

async def _run_quality_loop(
    isin: str,
    fund_dir: Path,
    config: dict,
    fund_name_hint: str,
    gestora_hint: str,
    anio_creacion_hint,
    gestores_hint: list,
    log,
    max_iter: int = 2,
) -> dict:
    """Loop iterativo: evalúa con DashboardQualityAgent → reagenta upstream agents
    en función de los fallos → re-ejecuta analyst → re-evalúa.
    Termina cuando aceptable=True o max_iter alcanzado.
    """
    from agents.dashboard_quality_agent import DashboardQualityAgent

    quality = DashboardQualityAgent(isin)
    report = quality.run()
    log("QUALITY", "INFO",
        f"Iteración 0 — score {report.get('score', 0)}/100, "
        f"{len(report.get('fallos', []))} fallos")

    iteration = 0
    while not report.get("aceptable", False) and iteration < max_iter:
        iteration += 1
        log("QUALITY", "INFO", f"Iteración {iteration}/{max_iter} — reagenting upstream agents")

        # Agrupar fallos por agente responsable
        fallos = report.get("fallos", [])
        fallos_por_agente: dict = {}
        for f in fallos:
            agente = f.get("agente_responsable", "analyst_agent")
            fallos_por_agente.setdefault(agente, []).append(f)

        log("QUALITY", "INFO",
            f"Fallos por agente: " + ", ".join(
                f"{a}={len(fs)}" for a, fs in fallos_por_agente.items()))

        # ── Re-ejecutar upstream agents según fallos ─────────────────────────
        # manager_deep_agent: filosofía/perfiles del gestor
        if "manager_deep_agent" in fallos_por_agente:
            try:
                from agents.manager_deep_agent import ManagerDeepAgent
                log("QUALITY", "RETRY", "Re-ejecutando manager_deep_agent")
                manager = ManagerDeepAgent(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, manager_names=gestores_hint or None,
                )
                await manager.run()
                log("QUALITY", "OK", "manager_deep_agent re-ejecutado")
            except Exception as exc:
                log("QUALITY", "ERROR", f"manager_deep retry falló: {exc}")

        # readings_agent: fuentes externas
        if "readings_agent" in fallos_por_agente:
            try:
                from agents.readings_agent import ReadingsAgent
                log("QUALITY", "RETRY", "Re-ejecutando readings_agent")
                readings = ReadingsAgent(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, gestores=gestores_hint,
                )
                await readings.run()
                log("QUALITY", "OK", "readings_agent re-ejecutado")
            except Exception as exc:
                log("QUALITY", "ERROR", f"readings retry falló: {exc}")

        # letters_agent: cartas trimestrales
        if "letters_agent" in fallos_por_agente:
            try:
                from agents.letters_agent import LettersAgent
                log("QUALITY", "RETRY", "Re-ejecutando letters_agent")
                letters = LettersAgent(
                    isin, config,
                    gestora_url=config.get("gestora_url", ""),
                    fund_name=fund_name_hint,
                    gestora=gestora_hint,
                    anio_creacion=anio_creacion_hint,
                )
                await letters.run()
                log("QUALITY", "OK", "letters_agent re-ejecutado")
            except Exception as exc:
                log("QUALITY", "ERROR", f"letters retry falló: {exc}")

        # cnmv_agent: solo en casos extremos (es muy costoso)
        # Lo dejamos fuera del loop por defecto — los datos cuantitativos
        # rara vez mejoran sin descargas nuevas.

        # ── Re-ejecutar analyst SIEMPRE con quality_feedback ─────────────────
        # Aunque los fallos sean de upstream, analyst debe re-sintetizar
        # con los nuevos datos + las correcciones específicas.
        try:
            from agents.analyst_agent import AnalystAgent
            log("QUALITY", "RETRY", "Re-ejecutando analyst_agent con quality_feedback")
            analyst = AnalystAgent(isin, config, quality_feedback=fallos)
            analyst.run()
            log("QUALITY", "OK", "analyst_agent re-ejecutado")
        except Exception as exc:
            log("QUALITY", "ERROR", f"analyst retry falló: {exc}")

        # ── Re-evaluar ───────────────────────────────────────────────────────
        report = quality.run()
        log("QUALITY", "INFO",
            f"Iteración {iteration} — nuevo score {report.get('score', 0)}/100, "
            f"{len(report.get('fallos', []))} fallos")

    # ── Re-generar dashboard HTML con el output final del loop ──────────────
    try:
        import subprocess
        gen_path = ROOT / "dashboard" / "generate_dashboard.py"
        result = subprocess.run(
            ["python", str(gen_path), isin],
            cwd=str(ROOT), capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            log("QUALITY", "OK", f"Dashboard HTML regenerado tras quality loop")
        else:
            log("QUALITY", "WARN", f"generate_dashboard falló: {result.stderr[:200]}")
    except Exception as exc:
        log("QUALITY", "WARN", f"No se pudo regenerar dashboard: {exc}")

    if report.get("aceptable", False):
        console.print(Panel(
            f"[bold green]Quality loop OK — score {report['score']}/100 (iteración {iteration})[/bold green]",
            border_style="green",
        ))
    else:
        fallos_summary = "\n".join(
            f"  • [{f.get('prioridad','?')}] {f.get('seccion','?')}: {f.get('problema','')[:80]}"
            for f in report.get("fallos", [])[:8]
        )
        console.print(Panel(
            f"[bold yellow]Quality loop terminó sin alcanzar 80 — score {report.get('score', 0)}/100[/bold yellow]\n"
            f"Tras {iteration} iteraciones quedan {len(report.get('fallos', []))} fallos:\n{fallos_summary}",
            title="Quality insuficiente",
            border_style="yellow",
        ))

    return report


# ── Git helpers ───────────────────────────────────────────────────────────────

def _git_commit_and_push(isin: str, nombre: str) -> bool:
    """Hace git add + commit + push de todos los cambios del fondo analizado."""
    import subprocess
    fund_data_path = ROOT / "data" / "funds" / isin

    try:
        # Stage datos del fondo + agentes modificados
        subprocess.run(
            ["git", "add",
             str(fund_data_path),
             str(ROOT / "agents"),
             str(ROOT / "dashboard" / "app.py"),
             str(ROOT / "data" / "improvements"),
             ],
            cwd=str(ROOT), check=True, capture_output=True,
        )
        # Check if there's anything to commit
        status = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=str(ROOT), capture_output=True,
        )
        if status.returncode == 0:
            # Nothing staged — check untracked
            log_fn = lambda m: None  # noqa
            return True  # already up to date

        msg = f"Análisis {isin} ({nombre}) + pipeline fixes\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
        subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=str(ROOT), check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=str(ROOT), check=True, capture_output=True,
        )
        return True
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else ""
        log("GIT", "ERROR", f"git error: {stderr[:200]}")
        return False


# ── Output verification display ───────────────────────────────────────────────

def _print_output_verification(output: dict, meta_result: dict):
    """Muestra tabla de verificación de los datos clave del output."""
    kpis  = output.get("kpis", {}) or {}
    cuant = output.get("cuantitativo", {}) or {}
    cual  = output.get("cualitativo", {}) or {}
    pos   = output.get("posiciones", {}) or {}
    consist = output.get("analisis_consistencia", {}) or {}

    def chk(v) -> str:
        return "[green]OK[/]" if v else "[red]FALTA[/]"

    mix_years = [m.get("periodo") for m in cuant.get("mix_activos_historico", [])]
    mix_sums  = []
    for m in cuant.get("mix_activos_historico", []):
        keys = ["renta_fija_pct", "rv_pct", "iic_pct", "liquidez_pct", "depositos_pct"]
        total = sum((m.get(k) or 0) for k in keys)
        if total > 5:
            mix_sums.append(f"{m.get('periodo','?')}={total:.0f}%")

    gestores_names = [g.get("nombre", "") for g in cual.get("gestores", []) if g.get("nombre")]
    aum_puntos = len(cuant.get("serie_aum", []))
    n_periodos = len(consist.get("periodos", []))
    n_pos = len(pos.get("actuales", []))
    n_hist_pos = len(pos.get("historicas", []))

    table = Table(title="Verificación del output", show_header=True, border_style="cyan")
    table.add_column("Campo", style="cyan", width=28)
    table.add_column("Valor / Estado", width=55)

    table.add_row("Nombre", output.get("nombre", "") or "[red]VACÍO[/]")
    table.add_row("Gestora", output.get("gestora", "") or "[red]VACÍO[/]")
    table.add_row("AUM actual (M€)", f"{kpis.get('aum_actual_meur', '?')}  {chk(kpis.get('aum_actual_meur'))}")
    table.add_row("Partícipes", f"{kpis.get('num_participes', '?')}  {chk(kpis.get('num_participes'))}")
    table.add_row("TER %", f"{kpis.get('ter_pct', '?')}  {chk(kpis.get('ter_pct'))}")
    table.add_row("Gestores", ", ".join(gestores_names) if gestores_names else "[red]FALTA[/]")
    table.add_row("Estrategia", chk(cual.get("estrategia")))
    table.add_row("Serie AUM", f"{aum_puntos} puntos  {chk(aum_puntos >= 3)}")
    table.add_row("Mix activos", f"{len(mix_years)} años: {', '.join(str(y) for y in mix_years[-5:])}  {chk(mix_years)}")
    table.add_row("Mix sumas", "  ".join(mix_sums[-6:]) if mix_sums else "[dim]n/a[/]")
    table.add_row("Posiciones actuales", f"{n_pos}  {chk(n_pos > 0)}")
    table.add_row("Posiciones históricas", f"{n_hist_pos} periodos  {chk(n_hist_pos > 0)}")
    table.add_row("Periodos consistencia", f"{n_periodos}  {chk(n_periodos >= 3)}")
    table.add_row("Cartas gestores", chk((output.get("fuentes") or {}).get("cartas_gestores")))
    console.print(table)

    # Issues bloqueantes destacados
    blockers_issues = [i for i in meta_result.get("issues", []) if "BLOQUEANTE" in i or "patron_conocido" in i]
    if blockers_issues:
        console.print("\n[bold yellow]Issues a resolver:[/bold yellow]")
        for i in blockers_issues:
            safe = i.encode("cp1252", errors="replace").decode("cp1252")
            console.print(f"  [yellow]•[/] {safe}")


# ── Feedback + Improver ───────────────────────────────────────────────────────

async def _collect_feedback_and_improve(isin: str, fund_dir: Path):
    """
    Recoge feedback del usuario en terminal y lo guarda.
    Luego lanza el ImproverAgent en modo propose para generar mejoras.
    """
    from rich.prompt import Prompt

    console.print("\n[bold]Tu feedback (Enter para saltar cada pregunta):[/bold]")

    questions = [
        ("datos_incorrectos", "¿Hay algún dato incorrecto o sospechoso?"),
        ("falta_info",        "¿Qué información falta o es insuficiente?"),
        ("errores_visuales",  "¿Algo no se muestra bien en el dashboard?"),
        ("mejoras",           "¿Qué cambiarías o mejorarías?"),
        ("fuentes",           "¿Alguna fuente de datos que deberíamos añadir?"),
    ]

    respuestas: dict = {}
    for q_id, q_text in questions:
        try:
            resp = Prompt.ask(f"[cyan]{q_text}[/cyan]", default="")
            if resp.strip():
                respuestas[q_id] = resp.strip()
        except (KeyboardInterrupt, EOFError):
            break

    if respuestas:
        fb = {
            "isin":       isin,
            "timestamp":  datetime.now().isoformat(),
            "fuente":     "usuario_post_pipeline",
            "respuestas": respuestas,
            "issues":     list(respuestas.values()),  # para que improver los lea
        }
        fb_path = fund_dir / "feedback.json"
        existing = []
        if fb_path.exists():
            try:
                existing = json.loads(fb_path.read_text(encoding="utf-8"))
                if isinstance(existing, dict):
                    existing = [existing]
            except Exception:
                existing = []
        existing.append(fb)
        fb_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        console.print("[green]Feedback guardado.[/green]")
        log("FEEDBACK", "OK", f"Feedback guardado con {len(respuestas)} respuestas")

        # Lanzar Improver con el nuevo feedback
        console.print("\n[cyan]Analizando feedback con ImproverAgent...[/cyan]")
        try:
            from agents.improver_agent import ImproverAgent
            improver = ImproverAgent(mode="propose")
            report = await improver.run()
            proposals = report.get("proposals", [])
            if proposals:
                console.print(f"[green]ImproverAgent: {len(proposals)} propuestas de mejora generadas[/green]")
                for p in proposals:
                    conf = p.get("confianza", "?")
                    safe = str(p.get("propuesta", ""))[:100].encode("cp1252", errors="replace").decode("cp1252")
                    console.print(f"  [dim]{p['agent']} (confianza {conf}%):[/dim] {safe}")
                console.print(
                    f"\n[dim]Para aplicar automáticamente los patches de alta confianza:[/dim]\n"
                    f"  python -m agents.improver_agent --apply"
                )
            else:
                console.print("[dim]No se generaron propuestas nuevas.[/dim]")
        except Exception as exc:
            log("IMPROVER", "WARN", f"Improver post-feedback falló: {exc}")
    else:
        # Sin feedback — lanzar improver igualmente en modo silencioso
        try:
            from agents.improver_agent import ImproverAgent
            improver = ImproverAgent(mode="propose")
            await improver.run()
        except Exception:
            pass


def _count_nonempty(obj) -> int:
    """Cuenta valores no-nulos recursivamente."""
    if obj is None or obj == "" or obj == [] or obj == {}:
        return 0
    if isinstance(obj, dict):
        return sum(_count_nonempty(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(_count_nonempty(i) for i in obj) + 1
    return 1


def _find_null_fields(obj, path="") -> list[str]:
    """Encuentra campos null/vacíos en el nivel superior."""
    nulls = []
    if not isinstance(obj, dict):
        return nulls
    for k, v in obj.items():
        p = f"{path}.{k}" if path else k
        if v is None or v == "" or v == [] or v == {}:
            nulls.append(p)
        elif isinstance(v, dict):
            nulls.extend(_find_null_fields(v, p))
    return nulls


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fund Analyzer Orchestrator")
    parser.add_argument("--isin", required=True, help="ISIN del fondo (ej. ES0112231008)")
    parser.add_argument("--auto", action="store_true",
                        help="Usar valores por defecto sin preguntar")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    asyncio.run(analyze_fund(args.isin, auto=args.auto))


if __name__ == "__main__":
    main()
