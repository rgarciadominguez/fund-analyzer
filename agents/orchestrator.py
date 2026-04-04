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

        # ── Paso 2: Letters Agent ─────────────────────────────────────────────
        letters_step_idx = 2 if is_lu else 1
        progress.update(main_task, description=steps[letters_step_idx][1])
        log("ORCHESTRATOR", "START", "Paso 2: Letters Agent")

        try:
            from agents.letters_agent import LettersAgent

            # URL de la gestora según tipo
            gestora_url = ""
            if is_es:
                gestora_url = config.get("gestora_url", "")
            else:
                gestora_url = config.get("gestora_url", "")

            # Pasar nombre, gestora y anio_creacion para DDG search
            output_so_far = results.get("output") or {}
            kpis_so_far = output_so_far.get("kpis", {}) if isinstance(output_so_far, dict) else {}
            # Intentar leer output parcial si analyst no ha corrido aún
            partial_out_path = fund_dir / "output.json"
            if partial_out_path.exists():
                try:
                    partial = json.loads(partial_out_path.read_text(encoding="utf-8"))
                    fund_name_hint = partial.get("nombre", "")
                    gestora_hint   = partial.get("gestora", "")
                    anio_creacion_hint = (partial.get("kpis") or {}).get("anio_creacion")
                except Exception:
                    fund_name_hint = gestora_hint = ""
                    anio_creacion_hint = None
            else:
                fund_name_hint = gestora_hint = ""
                anio_creacion_hint = None

            letters = LettersAgent(
                isin, config, gestora_url=gestora_url,
                fund_name=fund_name_hint,
                gestora=gestora_hint,
                anio_creacion=anio_creacion_hint,
            )
            results["letters"] = await letters.run()
            n_cartas = len(results["letters"].get("cartas", []))
            log("LETTERS", "OK", f"{n_cartas} cartas procesadas")
        except Exception as exc:
            log("LETTERS", "ERROR", f"Paso 2 falló: {exc}")

        progress.advance(main_task)

        # ── Paso 3: Analyst Agent ─────────────────────────────────────────────
        analyst_step_idx = 3 if is_lu else 2
        progress.update(main_task, description=steps[analyst_step_idx][1])
        log("ORCHESTRATOR", "START", "Paso 3: Analyst Agent")

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
            # Crear output mínimo
            output = {"isin": isin, "error": str(exc)}
            out_path = fund_dir / "output.json"
            out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

        progress.advance(main_task)

        # ── Paso 4: Readings Agent ────────────────────────────────────────────
        readings_step_idx = 4 if is_lu else 3
        progress.update(main_task, description=steps[readings_step_idx][1])
        log("ORCHESTRATOR", "START", "Paso 4: Readings Agent")

        try:
            from agents.readings_agent import ReadingsAgent
            out_for_readings = results.get("output", {})
            if isinstance(out_for_readings, dict):
                r_nombre  = out_for_readings.get("nombre", "")
                r_gestora = out_for_readings.get("gestora", "")
                r_gestores = [g.get("nombre", "") for g in
                              out_for_readings.get("cualitativo", {}).get("gestores", []) if g.get("nombre")]
            else:
                r_nombre = r_gestora = ""
                r_gestores = []
            readings = ReadingsAgent(isin, fund_name=r_nombre, gestora=r_gestora, gestores=r_gestores)
            results["readings"] = await readings.run()
            log("READINGS", "OK",
                f"Lecturas: {len(results['readings'].get('lecturas', []))} | "
                f"Externos: {len(results['readings'].get('analisis_externos', []))}")
        except Exception as exc:
            log("READINGS", "ERROR", f"Paso 4 falló: {exc}")

        progress.advance(main_task)

        # ── Paso 5: Meta Agent ────────────────────────────────────────────────
        meta_step_idx = 5 if is_lu else 4
        progress.update(main_task, description=steps[meta_step_idx][1])
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

    # ── Resumen final ─────────────────────────────────────────────────────────
    elapsed = round(time.time() - start_time, 1)
    output = results.get("output", {})

    # Contar campos
    completed_fields = _count_nonempty(output)
    null_fields = _find_null_fields(output)

    # Contar ficheros
    xml_count = len(list((fund_dir / "raw" / "xml").glob("*.xml"))) if (fund_dir / "raw" / "xml").exists() else 0
    pdf_count = len(list((fund_dir / "raw").rglob("*.pdf")))
    carta_count = len(list((fund_dir / "raw" / "letters").glob("*.pdf"))) if (fund_dir / "raw" / "letters").exists() else 0

    # Tabla de resumen con rich
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

    # Meta-QA issues
    meta_result = results.get("meta", {})
    if meta_result.get("issues"):
        console.print(f"\n[bold yellow]⚠ Meta-QA: {len(meta_result['issues'])} issues detectados[/bold yellow]")

    separator = "=" * 48
    summary = (
        f"\n{separator}\n"
        f"PIPELINE COMPLETADO — {isin}\n"
        f"Duracion total: {elapsed}s\n"
        f"Campos completados: {completed_fields}\n"
        f"Campos null: {', '.join(null_fields[:10])}\n"
        f"Ficheros: {xml_count} XML + {pdf_count} PDF + {carta_count} cartas\n"
        f"{separator}\n"
    )
    log("OUTPUT", "OK", summary)

    return output


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
