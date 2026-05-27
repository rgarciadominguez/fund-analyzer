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
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Fix Windows console encoding (charmap can't encode → and other unicode) ──
# Without this, Quality Loop crashes when printing tracebacks/messages with
# arrows or accented chars on Windows default cp1252 console.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

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


# ── F6: fund_group shared cache (manager_profile, intl_discovery, readings) ─
# Para clases hermanas (mismo nombre_base + gestora), evitar re-correr Opus
# en manager_profiler/discovery/readings. Cache local en data/fund_groups_cache/.
# Desactivable con DISABLE_FUND_GROUP_CACHE=1.

import uuid as _uuid_mod
import re as _re_mod
import shutil as _shutil_mod

FUND_GROUP_CACHE_DIR = ROOT / "data" / "fund_groups_cache"
FUND_GROUP_CACHE_TTL_DAYS = 30
FUND_GROUP_CACHE_FILES = (
    "manager_profile.json",
    "intl_discovery_data.json",
    "readings_data.json",
    # F6 extension: intl_data.json (output del extractor INT, también caro en LLM).
    # ES no lo genera — para fondos ES esta entrada simplemente no copia nada.
    "intl_data.json",
)


def _fg_normalize_nombre_base(nombre: str) -> str:
    """Replica simplificada de tools.import_taxonomy.normalize_nombre_base
    para el cache. Mantenido local para evitar import-cycle con tools/."""
    if not nombre:
        return ""
    s = str(nombre).strip()
    s = _re_mod.sub(r"\((acc|dist|inc|usd|eur|chf|gbp|jpy|hedged|hedge)\)", " ", s, flags=_re_mod.I)
    s = _re_mod.sub(r"\b(EUR|USD|CHF|GBP|JPY|CAD)\s+(HEDGED|HEDGE)\b", " ", s, flags=_re_mod.I)
    s = _re_mod.sub(r"\s+", " ", s).strip()
    noise = {"EUR","USD","CHF","GBP","JPY","CAD","AUD","SEK","NOK","DKK","HKD","SGD",
             "ACC","DIST","ACUMULACION","ACUMULACIÓN","DISTRIBUCION","DISTRIBUCIÓN","INC",
             "HEDGED","HEDGE","UNHEDGED","FI","FIL","FCP","SICAV","PLC","LTD","SA",
             "RETAIL","INSTITUTIONAL","INST","CORPORATE","CLASE","CLASS","CL"}
    cls_letter = _re_mod.compile(r"^[A-Z]{1,3}\d?$")
    tokens = s.split(" ")
    if len(tokens) >= 2 and _re_mod.match(r"^(CLASE|CLASS|CL)\b", tokens[-2], _re_mod.I):
        tokens = tokens[:-2]
    elif tokens and _re_mod.match(r"^(CLASE|CLASS|CL)\b", tokens[-1], _re_mod.I):
        tokens = tokens[:-1]
    changed = True
    while changed and tokens:
        changed = False
        last = tokens[-1].upper().rstrip(",.;:")
        if last in noise:
            tokens.pop(); changed = True; continue
        if cls_letter.match(last) and len(tokens) > 1:
            tokens.pop(); changed = True; continue
    return _re_mod.sub(r"\s+", " ", " ".join(tokens)).strip()


def _fg_compute_id(nombre_base: str, gestora: str) -> str:
    """uuid5(NAMESPACE_OID, 'gestora::nombre_base') — clave del cache F6.
    Distinta del fund_group_id de Supabase (que usa namespace custom)."""
    key = f"{(gestora or '').strip().lower()}::{(nombre_base or '').strip().lower()}"
    return str(_uuid_mod.uuid5(_uuid_mod.NAMESPACE_OID, key))


def _fg_resolve_identity(fund_dir: Path, fallback_isin: str) -> dict:
    """Lee nombre_oficial + gestora_oficial desde regulator file o cnmv_data
    según haya. Si no encuentra nada usable, devuelve {}."""
    candidates = [
        "cssf_data.json", "cbi_data.json", "amf_data.json",
        "bundesanzeiger_data.json", "cnmv_data.json",
    ]
    for fname in candidates:
        fp = fund_dir / fname
        if not fp.exists():
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        identity = data.get("identity") or data
        nombre = identity.get("nombre_oficial") or data.get("nombre") or ""
        gestora = identity.get("gestora_oficial") or data.get("gestora") or ""
        if nombre and gestora:
            return {"nombre": nombre, "gestora": gestora}
    return {}


def _fg_apply_cache(isin: str, fund_dir: Path, identity: dict, log) -> set:
    """Si existe cache válido (<30d) para este fund_group_id, copia los 3 JSONs
    desde el ISIN pointer al fund_dir actual. Devuelve set de archivos copiados.
    DISABLE_FUND_GROUP_CACHE=1 lo desactiva."""
    if os.environ.get("DISABLE_FUND_GROUP_CACHE") == "1":
        return set()
    if not identity.get("nombre") or not identity.get("gestora"):
        return set()
    nombre_base = _fg_normalize_nombre_base(identity["nombre"])
    fund_group_id = _fg_compute_id(nombre_base, identity["gestora"])
    cache_file = FUND_GROUP_CACHE_DIR / f"{fund_group_id}.json"
    if not cache_file.exists():
        return set()
    age_days = (time.time() - cache_file.stat().st_mtime) / 86400.0
    if age_days > FUND_GROUP_CACHE_TTL_DAYS:
        log("FUND_GROUP_CACHE", "INFO", f"cache expirado ({age_days:.1f}d > {FUND_GROUP_CACHE_TTL_DAYS}d)")
        return set()
    try:
        meta = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception as e:
        log("FUND_GROUP_CACHE", "WARN", f"cache ilegible: {e}")
        return set()
    pointer_isin = meta.get("pointer_isin")
    if not pointer_isin or pointer_isin == isin:
        return set()
    pointer_dir = ROOT / "data" / "funds" / pointer_isin
    if not pointer_dir.exists():
        log("FUND_GROUP_CACHE", "WARN", f"pointer dir no existe: {pointer_isin}")
        return set()
    copied = set()
    fund_dir.mkdir(parents=True, exist_ok=True)
    for fname in FUND_GROUP_CACHE_FILES:
        src = pointer_dir / fname
        if not src.exists():
            continue
        dst = fund_dir / fname
        try:
            _shutil_mod.copy2(src, dst)
            copied.add(fname)
        except Exception as e:
            log("FUND_GROUP_CACHE", "WARN", f"copy {fname}: {e}")
    if copied:
        log("FUND_GROUP_CACHE", "OK",
            f"hit fund_group={fund_group_id[:8]}.. pointer={pointer_isin} "
            f"copiados={sorted(copied)} (age={age_days:.1f}d)")
    return copied


def _fg_save_cache(isin: str, identity: dict, log) -> None:
    """Tras run exitoso, escribir cache pointer→isin para que clases hermanas
    de futuros runs lo reutilicen."""
    if os.environ.get("DISABLE_FUND_GROUP_CACHE") == "1":
        return
    if not identity.get("nombre") or not identity.get("gestora"):
        return
    nombre_base = _fg_normalize_nombre_base(identity["nombre"])
    fund_group_id = _fg_compute_id(nombre_base, identity["gestora"])
    FUND_GROUP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = FUND_GROUP_CACHE_DIR / f"{fund_group_id}.json"
    payload = {
        "fund_group_id": fund_group_id,
        "nombre_base": nombre_base,
        "gestora": identity["gestora"],
        "pointer_isin": isin,
        "saved_at": datetime.now().isoformat(),
    }
    try:
        cache_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log("FUND_GROUP_CACHE", "OK", f"saved pointer {isin} para fund_group={fund_group_id[:8]}..")
    except Exception as e:
        log("FUND_GROUP_CACHE", "WARN", f"save failed: {e}")


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

async def analyze_fund(isin: str, auto: bool = False, prep_only: bool = False) -> dict:
    """Pipeline completo para un ISIN.

    Si prep_only=True (Fase 1 cowork-skill, 2026-05-04): ejecuta toda la prep
    determinista (CNMV/INT, manager, letters, readings, sources) y SE DETIENE
    antes del Paso 4 (analyst_agent). Luego invoca bundle_exporter para que la
    skill `analyst-cowork` consuma los inputs desde Claude Max sin coste API.
    """
    isin = isin.strip().upper()
    start_time = time.time()

    # Cost-Opt Fase 2 (2026-05-02): publicar ISIN en env para que componentes
    # como tools/gemini_wrapper.py extract_fast (extractor INT) puedan
    # asociar su coste al fondo correcto en cost_log.jsonl.
    os.environ["CURRENT_FUND_ISIN"] = isin

    log_path = ROOT / "progress.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n[{_ts()}] [ORCHESTRATOR] [START] Pipeline {isin}\n{'='*60}\n")

    def log(agent, level, msg):
        _log(agent, level, msg, log_path)

    # Refactor L2 P1 (2026-05-05): in prep-only mode, wipe stale pending_*.json
    # before agents start emitting tasks. Manifests are overwritten per run,
    # not concatenated, to avoid stale entries from a previous failed prep.
    if prep_only:
        from tools.api_mode import is_cowork_mode
        if is_cowork_mode():
            from tools.pending_manifest import clean_pending_manifests
            fund_dir_for_clean = ROOT / "data" / "funds" / isin
            fund_dir_for_clean.mkdir(parents=True, exist_ok=True)
            removed = clean_pending_manifests(fund_dir_for_clean)
            if removed:
                log("ORCHESTRATOR", "INFO",
                    f"--prep-only cowork: limpiados {len(removed)} manifests stale: {removed}")

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

        # ── Paso 0: Regulator router (Fase E — soporta LU/IE/FR/DE) ─────────
        # Antes solo CSSF para LU. Ahora regulator_router enruta automaticamente
        # por prefijo ISIN a CSSFAgent (LU), CBIAgent (IE), AMFAgent (FR),
        # BundesanzeigerAgent (DE). GB/otros prefijos: regulator None,
        # discovery hace todo el trabajo de identity + docs.
        if not is_es:
            progress.update(main_task, description="Consultando regulador (router)")
            log("ORCHESTRATOR", "START", "Paso 0: Regulator router")
            try:
                from agents.regulator_router import run_regulator
                reg_out = await run_regulator(isin, config)
                reg_name = (reg_out.get("regulator") or "NONE").lower()
                if reg_name and reg_name != "none":
                    # Persistir en {regulador}_data.json para el discovery (linea 273+)
                    reg_file = fund_dir / f"{reg_name}_data.json"
                    reg_file.write_text(
                        json.dumps(reg_out, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    results["regulator"] = reg_out
                    log("REGULATOR", "OK", f"{reg_name}_data.json generado (identity={bool(reg_out.get('identity'))})")
                else:
                    log("REGULATOR", "INFO",
                        f"Sin regulador para prefijo {prefix}, discovery hara identity + docs")
                    results["regulator"] = reg_out
            except Exception as exc:
                log("REGULATOR", "ERROR", f"Paso router fallo: {exc}")
                import traceback
                log("REGULATOR", "TRACE", traceback.format_exc()[:300])
            progress.advance(main_task)

        # ── F6: fund_group shared cache lookup ───────────────────────────────
        # Tras regulator (INT) o pre-CNMV (ES), si conocemos nombre+gestora
        # vía identity, comprobar si otro ISIN del mismo fund_group ya tiene
        # manager_profile / intl_discovery / readings cacheados (<30d).
        # discovery skipea solo cuando intl_discovery_data.json ya tiene >=3 docs.
        # manager/readings: respetamos el cache hit más abajo con flags.
        fg_cache_hits: set = set()
        try:
            _fg_identity = _fg_resolve_identity(fund_dir, isin)
            if _fg_identity:
                fg_cache_hits = _fg_apply_cache(isin, fund_dir, _fg_identity, log)
        except Exception as exc:
            log("FUND_GROUP_CACHE", "WARN", f"lookup falló (no bloquea pipeline): {exc}")

        # ── Paso 1a (INT): Discovery v2 — descargar PDFs de web gestora ─────
        if not is_es:
            progress.update(main_task, description="Discovery v2: buscando documentos del fondo")
            # Skip discovery si ya tiene docs suficientes (evita sobrescribir
            # un discovery bueno con uno peor por falta de identity en re-run)
            import json as _json
            existing_disc = fund_dir / "intl_discovery_data.json"
            skip_discovery = False
            if existing_disc.exists():
                try:
                    ed = _json.loads(existing_disc.read_text(encoding="utf-8"))
                    n_existing = len(ed.get("documents", []))
                    if n_existing >= 3:
                        log("DISCOVERY", "SKIP", f"Ya tiene {n_existing} docs, reutilizando")
                        skip_discovery = True
                except Exception:
                    pass

            if skip_discovery:
                pass
            else:
                log("ORCHESTRATOR", "START", "Paso 1a: Discovery v2 (web gestora + wayback)")
                try:
                    from agents.intl_discovery_agent import IntlDiscoveryAgent
                    identity = {}
                    gap = {}
                    for reg_file in ("cssf_data.json", "cbi_data.json", "amf_data.json", "bundesanzeiger_data.json"):
                        reg_path = fund_dir / reg_file
                        if reg_path.exists():
                            reg_data = _json.loads(reg_path.read_text(encoding="utf-8"))
                            identity = reg_data.get("identity", {})
                            break
                    from agents.regulator_router import compute_gap
                    gap = compute_gap({"identity": identity}, config)
                    disc_agent = IntlDiscoveryAgent(
                        isin=isin, identity=identity, gap=gap,
                        web_search_fn=None, config=config,
                    )
                    results["discovery"] = await disc_agent.run()
                    n_docs = len(results["discovery"].get("documents", []))
                    log("DISCOVERY", "OK", f"{n_docs} docs descubiertos")
                except Exception as exc:
                    log("DISCOVERY", "ERROR", f"Discovery falló: {exc}")
                    import traceback
                    log("DISCOVERY", "TRACE", traceback.format_exc()[:300])
            progress.advance(main_task)

        # ── Paso 1b: Agente extractor ─────────────────────────────────────────
        progress.update(main_task, description=steps[0][1] if not is_lu else steps[1][1])
        log("ORCHESTRATOR", "START", f"Paso 1b: {'CNMV' if is_es else 'INTL Extractor v3'}")

        try:
            if is_es:
                from agents.cnmv_agent import CNMVAgent
                agent = CNMVAgent(isin, config)
                results["cnmv"] = await agent.run()
                log("CNMV", "OK", f"cnmv_data.json generado")
            else:
                # F6 extension: si fund_group cache copió intl_data.json, reutilizar.
                if "intl_data.json" in fg_cache_hits:
                    try:
                        results["intl"] = json.loads(
                            (fund_dir / "intl_data.json").read_text(encoding="utf-8")
                        )
                        log("INTL", "OK", "reutilizado de fund_group cache (skip extractor)")
                    except Exception as exc:
                        log("INTL", "WARN", f"cache F6 ilegible, corriendo extractor: {exc}")
                        results["intl"] = None
                else:
                    results["intl"] = None
                if results.get("intl") is None:
                    from agents.intl_extractor_v2 import IntlExtractor
                    agent = IntlExtractor(isin, config)
                    results["intl"] = await agent.run()
                    log("INTL", "OK", f"intl_data.json generado (extractor v3 concept-first)")
        except Exception as exc:
            log("ORCHESTRATOR", "ERROR", f"Paso 1b falló: {exc}")
            import traceback
            log("ORCHESTRATOR", "TRACE", traceback.format_exc()[:300])

        # ── Refactor L2 fix (2026-05-05): emit ES qualitative tasks ──
        # cnmv_agent has the function _parse_seccion_cualitativo to emit
        # tasks but it's orphan (never called). And fondos like HOROS
        # publish only on their own web (raw/discovery/_web_*.pdf), not
        # CNMV. Centralize the emission here so every ES fund — with or
        # without CNMV semestrales — gets a populated pending_extraction.
        if is_es:
            try:
                from tools.api_mode import is_cowork_mode
                from tools.pending_manifest import emit_es_qualitative_tasks
                if is_cowork_mode():
                    n = emit_es_qualitative_tasks(fund_dir, isin)
                    log("CNMV_QUAL_EMIT", "INFO",
                        f"emitidas {n} tasks cualitativas a pending_extraction.json")
            except Exception as exc:
                log("CNMV_QUAL_EMIT", "WARN", f"no se pudo emitir tasks: {exc}")

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
                    # INT: gestores del extractor v3 (cualitativo.gestores[])
                    if not gestores_hint:
                        for g in (src.get("cualitativo") or {}).get("gestores", []):
                            if isinstance(g, dict) and g.get("nombre"):
                                gestores_hint.append(g["nombre"])
                except Exception:
                    pass

        log("ORCHESTRATOR", "INFO",
            f"Metadata: nombre={fund_name_hint[:40]}, gestora={gestora_hint[:30]}, "
            f"gestores={gestores_hint[:3]}")

        # ── Guard Fix 1 Fase J+ (2026-04-28): abortar si CNMV/regulator vacío ─
        # Síntoma: ISIN inválido o no registrado en regulador → sin nombre/AUM/PDFs.
        # Continuar consume LLM para nada y produce output basura (cross-fund leak,
        # readings random). Mejor abortar con mensaje claro.
        cnmv_data_path = fund_dir / "cnmv_data.json"
        intl_data_path = fund_dir / "intl_data.json"
        primary_data_path = cnmv_data_path if is_es else intl_data_path

        is_empty = False
        if primary_data_path.exists():
            try:
                primary = json.loads(primary_data_path.read_text(encoding="utf-8"))
                aum_val = (primary.get("kpis") or {}).get("aum_actual_meur")
                pos_count = len((primary.get("posiciones") or {}).get("actuales", []))
                xml_count_check = len(list((fund_dir / "raw" / "xml").glob("*.*"))) if (fund_dir / "raw" / "xml").exists() else 0
                pdf_count_check = len(list((fund_dir / "raw").rglob("*.pdf")))
                # Vacío total: sin nombre, sin AUM, sin posiciones, sin docs
                if (not fund_name_hint and aum_val is None and pos_count == 0
                        and xml_count_check == 0 and pdf_count_check == 0):
                    is_empty = True
            except Exception:
                pass
        else:
            # Ni siquiera el JSON principal — claramente ISIN inválido
            is_empty = True

        if is_empty:
            log("ORCHESTRATOR", "ERROR",
                f"ABORT: CNMV/regulator devolvió datos VACÍOS para {isin}. "
                f"Probable ISIN inválido o no registrado. No continuamos pipeline para "
                f"evitar coste LLM y output basura. Verifica el ISIN.")
            console.print(Panel(
                f"[bold red]PIPELINE ABORTADO — ISIN {isin}[/bold red]\n"
                f"El extractor primario ({'CNMV' if is_es else 'regulator router'}) NO\n"
                f"devolvió datos del fondo (sin nombre, AUM, posiciones, ni documentos).\n"
                f"Probable causa: ISIN inválido o no registrado en el regulador.\n"
                f"Verifica el ISIN y vuelve a ejecutar.",
                title="Aborto preventivo",
                border_style="red",
            ))
            return {"isin": isin, "error": "datos_primarios_vacios", "aborted": True}

        # ── Paso 2: Sources Agent (descubrimiento de fuentes) ─────────────────
        progress.update(main_task, description="Sources Agent")
        log("ORCHESTRATOR", "START", "Paso 2: Sources Agent")

        try:
            from agents.sources_agent import SourcesAgent
            sources = SourcesAgent(
                isin, fund_name=fund_name_hint,
                gestora=gestora_hint, gestores=gestores_hint,
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
                from agents.letters_collector import LettersCollector
                letters = LettersCollector(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint,
                    anio_creacion=anio_creacion_hint,
                )
                return await letters.run()
            except Exception as exc:
                log("LETTERS", "ERROR", f"Letters falló: {exc}")
                return {}

        async def _run_readings():
            try:
                from agents.readings_collector import ReadingsCollector
                readings = ReadingsCollector(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, gestores=gestores_hint,
                )
                return await readings.run()
            except Exception as exc:
                log("READINGS", "ERROR", f"Readings falló: {exc}")
                return {}

        async def _run_manager_deep():
            # F6: si el cache F6 copió manager_profile.json, no re-correr profiler
            if "manager_profile.json" in fg_cache_hits:
                mp_path = fund_dir / "manager_profile.json"
                try:
                    cached = json.loads(mp_path.read_text(encoding="utf-8"))
                    log("MANAGER", "OK", "reutilizado de fund_group cache (skip profiler)")
                    return cached
                except Exception as exc:
                    log("MANAGER", "WARN", f"cache F6 ilegible, corriendo profiler: {exc}")
            try:
                from agents.manager_profiler import ManagerProfiler
                manager = ManagerProfiler(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, manager_names=gestores_hint or None,
                )
                return await manager.run()
            except Exception as exc:
                log("MANAGER", "ERROR", f"Manager Deep falló: {exc}")
                return {}

        async def _run_gestora_resources():
            """M3 v2 Fase M (2026-04-30): extrae cartas/KIID/folleto/videos
            de la web gestora vía Serper. Output: gestora_resources.json.

            Deriva el dominio de gestora_hint usando KNOWN_GESTORA_DOMAINS de
            sources_agent. Si no hay match, skip silencioso (no rompe pipeline).
            """
            try:
                from agents.sources_agent import KNOWN_GESTORA_DOMAINS
                from agents.gestora_resources_extractor import GestoraResourcesExtractor

                # Deriva domain por keyword match (primer token gestora normalizado)
                gestora_lower = (gestora_hint or "").lower()
                domain = None
                for key, dom in KNOWN_GESTORA_DOMAINS.items():
                    if key in gestora_lower:
                        domain = dom
                        break
                if not domain:
                    log("GESTORA_RES", "WARN",
                        f"Sin domain conocido para gestora={gestora_hint!r} — skip M3")
                    return {}

                extractor = GestoraResourcesExtractor(
                    isin=isin,
                    gestora_domain=domain,
                    fund_name=fund_name_hint,
                    gestora=gestora_hint,
                )
                return await extractor.run()
            except Exception as exc:
                log("GESTORA_RES", "ERROR", f"M3 falló: {exc}")
                return {}

        # K20 Fase K (2026-04-29): readings ahora corre DESPUÉS del manager_profiler
        # para usar nombres de gestores curados (Opus lead/co) en sus queries.
        # Antes corría paralelo y readings recibía gestores_hint (a menudo vacío
        # para ES). Ahora paralelo letters + manager, luego readings con curated.
        letters_result, manager_result, gestora_res_result = await asyncio.gather(
            _run_letters(), _run_manager_deep(), _run_gestora_resources()
        )
        results["letters"] = letters_result
        results["manager"] = manager_result
        results["gestora_resources"] = gestora_res_result
        if gestora_res_result and gestora_res_result.get("total", 0) > 0:
            log("GESTORA_RES", "OK",
                f"{gestora_res_result['total']} recursos gestora "
                f"(tipos={gestora_res_result.get('por_tipo', {})})")

        # Extraer gestores curados de manager_profiler para readings
        curated_gestores = manager_result.get("equipo_gestor") or []
        if not curated_gestores:
            # Fallback a gestores_hint original si manager_profiler vacío
            curated_gestores = list(gestores_hint or [])
        log("ORCHESTRATOR", "INFO",
            f"Gestores curados para readings: {curated_gestores}")

        # readings con gestores curados (puede usar entrevistas, análisis por gestor)
        # F6: si el cache F6 copió readings_data.json, reutilizar sin re-correr
        readings_result = None
        if "readings_data.json" in fg_cache_hits:
            try:
                readings_result = json.loads(
                    (fund_dir / "readings_data.json").read_text(encoding="utf-8")
                )
                log("READINGS", "OK", "reutilizado de fund_group cache (skip collector)")
            except Exception as exc:
                log("READINGS", "WARN", f"cache F6 ilegible, corriendo collector: {exc}")
                readings_result = None
        if readings_result is None:
            try:
                from agents.readings_collector import ReadingsCollector
                readings = ReadingsCollector(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, gestores=curated_gestores,
                )
                readings_result = await readings.run()
            except Exception as exc:
                log("READINGS", "ERROR", f"Readings falló: {exc}")
                readings_result = {}
        results["readings"] = readings_result

        n_cartas = len(letters_result.get("cartas", []))
        n_lecturas = len(readings_result.get("lecturas", []))
        n_externos = len(readings_result.get("analisis_externos", []))
        log("ORCHESTRATOR", "OK",
            f"Letters: {n_cartas} | Readings: {n_lecturas} lect + {n_externos} ext | Manager: {'OK' if manager_result.get('nombre') else 'parcial'}")

        # ── Paso 3a Fix Loop (2026-04-29): manager_deep_agent enrich auto ───
        # Tras manager_profiler (que identifica lead/co vía Opus en Fase J), invocar
        # manager_deep_agent para enriquecer esos lead/co con full-text articles
        # (articulos_completos) — el bucle iterativo Fix 3 Fase H que solo se
        # ejecutaba en quality_loop retry. Sin esto, analyst recibe equipo_gestor
        # pero articulos_completos vacío → perfiles esqueleto.
        try:
            curated_names = manager_result.get("equipo_gestor") or []
            curated_names = [n for n in curated_names if isinstance(n, str) and n.strip()
                             and not n.lower().startswith("equipo")]
            if curated_names:
                from agents.manager_deep_agent import ManagerDeepAgent
                log("ORCHESTRATOR", "START", f"Paso 3a: manager_deep enrich (lead/co={curated_names})")
                deep = ManagerDeepAgent(
                    isin=isin, fund_name=fund_name_hint, gestora=gestora_hint,
                    manager_names=curated_names,  # curados por profiler+Opus
                )
                deep_result = await deep.run()
                results["manager_deep"] = deep_result
                arts = deep_result.get("articulos_completos", {}) or {}
                total_arts = sum(len(a) for a in arts.values())
                log("MANAGER_DEEP", "OK",
                    f"Enriquecido: {len(arts)} gestores × ~{total_arts // max(len(arts),1)} arts cada = {total_arts} full-text")
            else:
                log("ORCHESTRATOR", "INFO", "Paso 3a skip: sin nombres curados de profiler")
        except Exception as exc:
            log("MANAGER_DEEP", "ERROR", f"Paso 3a (enrich) falló: {exc}")

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

        # ── Cut --prep-only (Fase 1 cowork-skill, 2026-05-04) ────────────────
        # Hasta aquí la prep determinista está completa. Si el usuario pidió
        # --prep-only, exportamos el bundle y nos detenemos ANTES del analyst.
        # La skill `analyst-cowork` (en Cowork/Claude Code bajo Max) consumirá
        # `data/funds/{ISIN}/bundle/` sin coste API.
        if prep_only:
            log("ORCHESTRATOR", "INFO", "--prep-only: deteniendo antes del Paso 4 (analyst)")
            try:
                from agents.bundle_exporter import run as _bundle_run
                manifest = _bundle_run(isin)
                log("BUNDLE", "OK",
                    f"bundle exportado en data/funds/{isin}/bundle/ "
                    f"({sum(m['size_bytes'] for m in manifest['files'].values())/1024:.1f} KB total)")
            except Exception as exc:
                log("BUNDLE", "ERROR", f"bundle_exporter falló: {exc}")
                import traceback
                log("BUNDLE", "TRACE", traceback.format_exc()[:500])
                raise
            try:
                from agents.bundle_validator import validate as _bundle_validate
                vresult = _bundle_validate(isin)
                if vresult["valid"]:
                    log("BUNDLE", "OK",
                        f"bundle_validator: VALID (warnings={len(vresult.get('warnings') or [])})")
                else:
                    log("BUNDLE", "ERROR",
                        f"bundle_validator: INVALID — {vresult.get('errors')}")
            except Exception as exc:
                log("BUNDLE", "WARN", f"bundle_validator no ejecutado: {exc}")
            console.print(Panel(
                f"[bold green]Prep completa para {isin}[/bold green]\n"
                f"Bundle: data/funds/{isin}/bundle/\n\n"
                f"Próximo paso (Cowork / Claude Code bajo Max):\n"
                f'  Di: "analyst cowork {isin}"\n\n'
                f"Tras la skill, integra el resultado:\n"
                f"  python -m agents.orchestrator --isin {isin} --consume-cowork",
                border_style="green",
                title="--prep-only",
            ))
            return {"isin": isin, "prep_only": True, "bundle_manifest": manifest}

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
                # O9 Cost-Opt (2026-05-02): bajado de 5 a 1 (default).
                # Override con env QUALITY_LOOP_MAX_ITER si necesitas más.
                # Iter 2+ rara vez aporta valor — análisis Mayo 2026 mostró
                # 70% de iter 2 generan output rejected by guard (sin valor)
                # pero gastan ~$0.10 cada vez. Para fondos donde realmente hay
                # fallos críticos recuperables, usar QUALITY_LOOP_MAX_ITER=3.
                max_iter=int(os.environ.get("QUALITY_LOOP_MAX_ITER", "1")),
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

    # ── Paso 7: publication_calendar (Bug 7, 2026-04-27) ──────────────────────
    # Detecta cadencia de informes/cartas → next_expected_date para futuros crons.
    try:
        from tools.publication_calendar import update_output_with_calendar
        cal_ok = update_output_with_calendar(isin)
        if cal_ok:
            log("CALENDAR", "OK", "publication_calendar actualizado en output.json")
            # Refrescar output con el nuevo calendar
            out_path = fund_dir / "output.json"
            if out_path.exists():
                results["output"] = json.loads(out_path.read_text(encoding="utf-8"))
        else:
            log("CALENDAR", "INFO", "publication_calendar no generado (sin docs históricos)")
    except Exception as exc:
        log("CALENDAR", "ERROR", f"publication_calendar falló: {exc}")

    # ── Paso 8: _meta block (Bug E Fase G 2026-04-28) ─────────────────────────
    # Persiste metadata del pipeline para auditoría: versión, modelos, sources_attempted
    try:
        elapsed_meta = round(time.time() - start_time, 1)
        out_path_meta = fund_dir / "output.json"
        if out_path_meta.exists():
            output_meta = json.loads(out_path_meta.read_text(encoding="utf-8"))
            # sources_attempted: chequear qué ficheros se generaron
            sources_attempted = {}
            file_checks = {
                "cnmv_xml": (fund_dir / "raw" / "xml", "ok ({} XMLs)" if (fund_dir / "raw" / "xml").exists() else "no_results"),
                "cnmv_pdf": (fund_dir / "raw" / "reports", "ok ({} PDFs)"),
                "letters_data": (fund_dir / "letters_data.json", "ok ({} cartas)"),
                "readings_data": (fund_dir / "readings_data.json", "ok"),
                "manager_profile": (fund_dir / "manager_profile.json", "ok"),
                "intl_data": (fund_dir / "intl_data.json", "ok"),
            }
            for key, (path, fmt) in file_checks.items():
                if path.exists():
                    if path.is_dir():
                        n = len(list(path.glob("*.*")))
                        sources_attempted[key] = fmt.format(n) if "{}" in fmt else fmt
                    else:
                        try:
                            jd = json.loads(path.read_text(encoding="utf-8"))
                            n = len(jd.get("cartas", []) or jd.get("analisis", []) or [])
                            sources_attempted[key] = fmt.format(n) if "{}" in fmt else fmt
                        except Exception:
                            sources_attempted[key] = fmt if "{}" not in fmt else fmt.format(0)
                else:
                    sources_attempted[key] = "no_results"

            # Pro sources attempted (Bug B)
            try:
                readings_path = fund_dir / "readings_data.json"
                if readings_path.exists():
                    rd = json.loads(readings_path.read_text(encoding="utf-8"))
                    pro_attempted = rd.get("_pro_sources_attempted", []) or []
                    discarded_cf = rd.get("_discarded_cross_fund", []) or []
                    sources_attempted["pro_sources_queried"] = len(pro_attempted)
                    sources_attempted["readings_discarded_cross_fund"] = len(discarded_cf)
            except Exception:
                pass

            # Anti-invención warnings count
            ai_warnings = ((output_meta.get("analyst_synthesis") or {})
                           .get("gestores", {}).get("_anti_invencion_warnings") or [])
            ai_count = len(ai_warnings) if isinstance(ai_warnings, list) else 0

            output_meta["_meta"] = {
                "pipeline_version": "fase_g_2026-04-28",
                "fecha_ejecucion": datetime.now().isoformat(),
                "duracion_total_seg": elapsed_meta,
                "sources_attempted": sources_attempted,
                "anti_invencion_warnings_count": ai_count,
                "isin": isin,
                "tipo": output_meta.get("tipo", ""),
            }
            out_path_meta.write_text(json.dumps(output_meta, ensure_ascii=False, indent=2), encoding="utf-8")
            log("META_BLOCK", "OK", f"_meta persistido: {len(sources_attempted)} sources, {ai_count} anti-invención warnings")
            results["output"] = output_meta
    except Exception as exc:
        log("META_BLOCK", "ERROR", f"_meta block falló: {exc}")

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

    # F6: si el run es exitoso (dashboard_ready), persistir cache pointer
    # para que clases hermanas del mismo fund_group reusen estos outputs.
    if dashboard_ready:
        try:
            _fg_identity_final = _fg_resolve_identity(fund_dir, isin)
            # Fallback: si los regulator/cnmv files no tienen nombre+gestora,
            # tomar de output.json (que ya ha pasado el merge_prep).
            if not _fg_identity_final.get("nombre") or not _fg_identity_final.get("gestora"):
                _fg_identity_final = {
                    "nombre": output.get("nombre") or "",
                    "gestora": output.get("gestora") or "",
                }
            _fg_save_cache(isin, _fg_identity_final, log)
        except Exception as exc:
            log("FUND_GROUP_CACHE", "WARN", f"save final falló (no bloquea): {exc}")

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
    según los fallos → re-ejecuta analyst → re-evalúa.

    Termina cuando:
      - fallos == 0
      - max_iter alcanzado
      - una iteración no reduce el número de fallos (no estamos avanzando)
    """
    from agents.dashboard_quality_agent import DashboardQualityAgent

    quality = DashboardQualityAgent(isin)
    report = quality.run()
    n_fallos_estructura = report.get("fallos_estructura", len(report.get("fallos", [])))
    n_fallos_total = len(report.get("fallos", []))
    aceptable = report.get("aceptable", n_fallos_estructura == 0)
    log("QUALITY", "INFO",
        f"Iteración 0 — {n_fallos_total} fallos ({n_fallos_estructura} estructura/content, "
        f"{report.get('fallos_scarcity', 0)} scarcity) — {report.get('score_display', '')}")

    iteration = 0
    # K3 Fase K (2026-04-29): backup pre-loop para anti-regresión
    output_path_for_backup = ROOT / "data" / "funds" / isin / "output.json"
    iter_backups: list[Path] = []
    while not aceptable and n_fallos_estructura > 0 and iteration < max_iter:
        iteration += 1
        prev_fallos_estructura = n_fallos_estructura

        # K3 backup pre-iter: snapshot del output antes de reagentar/regenerar
        if output_path_for_backup.exists():
            iter_backup = output_path_for_backup.parent / f"output.iter_{iteration-1}.json"
            try:
                iter_backup.write_text(output_path_for_backup.read_text(encoding="utf-8"), encoding="utf-8")
                iter_backups.append(iter_backup)
            except Exception:
                pass

        log("QUALITY", "INFO", f"Iteración {iteration}/{max_iter} — reagenting upstream agents (solo estructura/content)")

        # Agrupar fallos corregibles (estructura + content) por agente responsable
        fallos = [f for f in report.get("fallos", []) if f.get("fail_type") in ("estructura", "content")]
        fallos_por_agente: dict = {}
        for f in fallos:
            agente = f.get("agente_responsable", "analyst_agent")
            fallos_por_agente.setdefault(agente, []).append(f)

        log("QUALITY", "INFO",
            f"Fallos por agente: " + ", ".join(
                f"{a}={len(fs)}" for a, fs in fallos_por_agente.items()))

        # ── Auto-corrección nombre del fondo (sin LLM, cero coste) ───────────
        # Si quality detecta nombre_match_latest_pdf, llamar al patcher que
        # lee el último PDF semestral y corrige output + cnmv_data.
        # Marca _manual_edits para que no se sobrescriba en futuros runs.
        nombre_fallos = [f for f in report.get("fallos", [])
                         if f.get("regla_id") == "nombre_match_latest_pdf"]
        if nombre_fallos:
            try:
                import sys as _sys
                _sys.path.insert(0, str(ROOT))
                from run_quality_only import patch_nombre_from_pdf
                log("QUALITY", "AUTOFIX", "Detectado nombre_match_latest_pdf — llamando patch_nombre_from_pdf")
                changed = patch_nombre_from_pdf(isin)
                log("QUALITY", "OK", f"patch_nombre_from_pdf done (changed={changed})")
            except Exception as exc:
                log("QUALITY", "ERROR", f"patch_nombre_from_pdf falló: {exc}")

        # ── Re-ejecutar upstream agents según fallos ─────────────────────────
        # CASCADA gestores: manager_profiler → manager_deep_agent →
        # google_snippets → sibling_finder. Cada paso solo se ejecuta si el
        # anterior no encontró nombres reales.
        if "manager_deep_agent" in fallos_por_agente:
            def _has_real_managers() -> bool:
                """Verifica si manager_profile.json tiene nombres reales (no vacío, no genéricos)."""
                prof_path = ROOT / "data" / "funds" / isin / "manager_profile.json"
                if not prof_path.exists():
                    return False
                try:
                    p = json.loads(prof_path.read_text(encoding="utf-8"))
                except Exception:
                    return False
                # Acepta cualquier nombre que no empiece por 'Equipo' (team aggregate)
                names = p.get("equipo_gestor") or p.get("equipo") or []
                real = [n for n in names if isinstance(n, str) and n.strip()
                        and not n.lower().startswith("equipo")]
                return len(real) > 0

            # Paso 1: manager_profiler (rápido, búsqueda simple)
            try:
                from agents.manager_profiler import ManagerProfiler
                log("QUALITY", "RETRY", "Cascada gestores 1/4 — manager_profiler")
                manager = ManagerProfiler(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, manager_names=gestores_hint or None,
                )
                await manager.run()
                log("QUALITY", "OK", f"manager_profiler done (real={_has_real_managers()})")
            except Exception as exc:
                log("QUALITY", "ERROR", f"manager_profiler falló: {exc}")

            # Paso 2: manager_deep_agent (búsquedas Google, Citywire, web gestora)
            if not _has_real_managers():
                try:
                    from agents.manager_deep_agent import ManagerDeepAgent
                    log("QUALITY", "RETRY", "Cascada gestores 2/4 — manager_deep_agent")
                    deep = ManagerDeepAgent(
                        isin=isin, fund_name=fund_name_hint, gestora=gestora_hint,
                        manager_names=gestores_hint or None,
                    )
                    await deep.run()
                    log("QUALITY", "OK", f"manager_deep_agent done (real={_has_real_managers()})")
                except Exception as exc:
                    log("QUALITY", "ERROR", f"manager_deep_agent falló: {exc}")

            # Paso 3: google_snippets (extrae nombres de snippets Morningstar/FT/Finect)
            if not _has_real_managers():
                try:
                    from agents.manager_google_snippets import find_managers, save_to_manager_profile, sync_to_output
                    log("QUALITY", "RETRY", "Cascada gestores 3/4 — google_snippets")
                    result = find_managers(isin, fund_name_hint, gestora_hint)
                    if result.get("managers"):
                        save_to_manager_profile(isin, result)
                        sync_to_output(isin, result["managers"], gestora_hint)
                        log("QUALITY", "OK", f"google_snippets: {result['managers']}")
                    else:
                        log("QUALITY", "INFO", "google_snippets: sin nombres")
                except Exception as exc:
                    log("QUALITY", "ERROR", f"google_snippets falló: {exc}")

            # Paso 4: sibling_finder (copia de fondo hermano de la misma gestora con name_root match)
            if not _has_real_managers():
                try:
                    from tools.sibling_finder import propagate_gestores
                    log("QUALITY", "RETRY", "Cascada gestores 4/4 — sibling_finder")
                    r = propagate_gestores(isin, dry_run=False)
                    log("QUALITY", "OK", f"sibling_finder: {r.get('status')} from {r.get('from','-')}")
                except Exception as exc:
                    log("QUALITY", "ERROR", f"sibling_finder falló: {exc}")

        # readings_agent: fuentes externas
        if "readings_agent" in fallos_por_agente:
            try:
                from agents.readings_collector import ReadingsCollector
                log("QUALITY", "RETRY", "Re-ejecutando readings_collector")
                readings = ReadingsCollector(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint, gestores=gestores_hint,
                )
                await readings.run()
                log("QUALITY", "OK", "readings_collector re-ejecutado")
            except Exception as exc:
                log("QUALITY", "ERROR", f"readings retry falló: {exc}")

        # letters_agent: cartas trimestrales
        if "letters_agent" in fallos_por_agente:
            try:
                from agents.letters_collector import LettersCollector
                log("QUALITY", "RETRY", "Re-ejecutando letters_collector")
                letters = LettersCollector(
                    isin, fund_name=fund_name_hint,
                    gestora=gestora_hint,
                    anio_creacion=anio_creacion_hint,
                )
                await letters.run()
                log("QUALITY", "OK", "letters_collector re-ejecutado")
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
        n_fallos_estructura = report.get("fallos_estructura", len(report.get("fallos", [])))
        n_fallos_total = len(report.get("fallos", []))
        aceptable = report.get("aceptable", n_fallos_estructura == 0)
        log("QUALITY", "INFO",
            f"Iteración {iteration} — {n_fallos_total} fallos ({n_fallos_estructura} estructura/content, "
            f"{report.get('fallos_scarcity', 0)} scarcity) — antes: {prev_fallos_estructura} estructura")

        # Abortar si no estamos reduciendo fallos de estructura/content
        # K3 Fase K (2026-04-29): si la iter NO mejoró O empeoró → restaurar
        # backup de iter anterior para no quedarnos con output peor.
        if n_fallos_estructura >= prev_fallos_estructura:
            log("QUALITY", "WARN",
                f"Iteración {iteration} no redujo fallos estructura ({n_fallos_estructura} >= {prev_fallos_estructura}) — abortando loop")
            # Restaurar backup pre-iter (si existe)
            if iter_backups:
                last_backup = iter_backups[-1]
                if last_backup.exists():
                    try:
                        output_path_for_backup.write_text(last_backup.read_text(encoding="utf-8"), encoding="utf-8")
                        log("QUALITY", "ROLLBACK", f"Restaurado output desde {last_backup.name} (iter {iteration} no mejoró)")
                    except Exception as exc:
                        log("QUALITY", "WARN", f"Rollback falló: {exc}")
            break

    # K3 cleanup: borrar backups iter al finalizar (ya no son útiles)
    for b in iter_backups:
        try:
            b.unlink()
        except Exception:
            pass

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

    final_fallos = len(report.get("fallos", []))
    final_estructura = report.get("fallos_estructura", 0)
    final_scarcity = report.get("fallos_scarcity", 0)
    final_aceptable = report.get("aceptable", False)
    score_display = report.get("score_display", f"{final_fallos} fallos")

    if final_aceptable:
        scarcity_note = f" ({final_scarcity} pendientes de datos)" if final_scarcity > 0 else ""
        console.print(Panel(
            f"[bold green]Quality loop ACEPTABLE — {score_display} (iteración {iteration})[/bold green]\n"
            f"Todos los fallos de estructura/content resueltos.{scarcity_note}",
            border_style="green",
        ))
    else:
        fallos_summary = "\n".join(
            f"  • [{f.get('seccion','?')}] [{f.get('fail_type','?')}] {f.get('problema','')[:80]}"
            for f in report.get("fallos", [])[:8]
        )
        console.print(Panel(
            f"[bold yellow]Quality loop terminó con {final_fallos} fallos restantes "
            f"({final_estructura} estructura, {final_scarcity} scarcity)[/bold yellow]\n"
            f"{score_display}\n"
            f"Tras {iteration} iteraciones quedan:\n{fallos_summary}",
            title="Quality con fallos pendientes",
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
        # Use console directly — log fn no está en scope aquí
        console.print(f"[red][GIT] error: {stderr[:300]}[/red]")
        # Fallback retry: si el push falló por SSL, reintentar sin verificación
        # (workaround mientras el CA bundle Git local está mal — abr 2026)
        if "SSL" in stderr or "TLS" in stderr or "trust anchors" in stderr:
            try:
                subprocess.run(
                    ["git", "-c", "http.sslverify=false", "push", "origin", "main"],
                    cwd=str(ROOT), check=True, capture_output=True,
                )
                console.print("[yellow][GIT] push completado tras retry sin SSL verify[/yellow]")
                return True
            except subprocess.CalledProcessError as exc2:
                stderr2 = exc2.stderr.decode("utf-8", errors="replace") if exc2.stderr else ""
                console.print(f"[red][GIT] retry tambien fallo: {stderr2[:200]}[/red]")
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


# ── Cowork-skill integration (Fase 1, 2026-05-04) ─────────────────────────────

def _consume_cowork_analyst(isin: str, fund_dir: Path, log) -> dict:
    """Integrate analyst_synthesis_cowork.json (produced by the skill) into output.json.

    - Verifies sha256 of inputs vs the hashes the skill recorded (drift detection).
    - Overwrites analyst_synthesis.* with the skill's output.
    - Marks the 8 paths as _manual_edits to preserve them across future Python runs.
    - Records the run in _meta.cowork_runs[].

    Returns a dict with drift list and integration summary.
    """
    import hashlib
    from tools.output_merger import save_output, mark_manual_edit

    cowork_json = fund_dir / "analyst_synthesis_cowork.json"
    if not cowork_json.exists():
        raise FileNotFoundError(
            f"{cowork_json} no existe. Ejecuta la skill `analyst-cowork` primero."
        )

    cowork_data = json.loads(cowork_json.read_text(encoding="utf-8"))
    cowork_meta = cowork_data.get("_meta", {}) or {}

    # Drift detection: compare input hashes the skill saw vs current files
    expected_hashes = cowork_meta.get("input_files_hash", {}) or {}
    drift: list[str] = []
    for fname, expected in expected_hashes.items():
        path = fund_dir / fname
        if not path.exists():
            drift.append(f"{fname}: no existe ahora")
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        exp_norm = expected.replace("sha256:", "") if isinstance(expected, str) else ""
        if exp_norm and exp_norm.lower() != actual.lower():
            drift.append(f"{fname}: cambió desde la skill")
    if drift:
        log("COWORK", "WARN", "Drift en inputs detectado:")
        for d in drift:
            log("COWORK", "WARN", f"  - {d}")
        log("COWORK", "WARN",
            "El analyst de Cowork puede estar desactualizado vs los datos crudos actuales.")

    # Load existing output.json (or seed)
    output_path = fund_dir / "output.json"
    if output_path.exists():
        output_data = json.loads(output_path.read_text(encoding="utf-8"))
    else:
        output_data = {"isin": isin}

    # Replace analyst_synthesis with the skill's
    if "analyst_synthesis" not in cowork_data:
        raise ValueError("analyst_synthesis_cowork.json no contiene 'analyst_synthesis'")
    output_data["analyst_synthesis"] = cowork_data["analyst_synthesis"]

    # Protect each section as _manual_edits so future Python runs don't overwrite
    sections = list(cowork_data["analyst_synthesis"].keys())
    for sec in sections:
        mark_manual_edit(output_data, f"analyst_synthesis.{sec}")

    # Record metadata
    output_data.setdefault("_meta", {}).setdefault("cowork_runs", []).append({
        "ts": cowork_meta.get("generated"),
        "main_model": cowork_meta.get("main_model"),
        "audit_model": cowork_meta.get("audit_model"),
        "anti_invencion_flagged_count": len(cowork_meta.get("anti_invencion_flagged", []) or []),
        "audit_iterations": cowork_meta.get("audit_iterations", 0),
        "input_drift": drift,
        "skill_version": cowork_meta.get("skill_version"),
        "sections_generated": cowork_meta.get("sections_generated") or sections,
    })

    save_output(isin, output_data)
    log("COWORK", "OK",
        f"Analyst de Cowork integrado ({len(sections)} secciones, drift={len(drift)})")
    if cowork_meta.get("anti_invencion_flagged"):
        log("COWORK", "WARN",
            f"Anti-invención: {len(cowork_meta['anti_invencion_flagged'])} flags residuales")
    return {"drift": drift, "sections": sections, "meta": cowork_meta}


async def consume_cowork_pipeline(isin: str, log_path: Path) -> dict:
    """Standalone pipeline for `--consume-cowork`: integrate skill output then
    run validation + meta + quality + calendar + dashboard regeneration.

    Skips ALL upstream agents (no CNMV, no manager, no letters, no readings).
    Reads hints from the existing output.json instead of from upstream JSONs.
    """
    isin = isin.strip().upper()
    os.environ["CURRENT_FUND_ISIN"] = isin
    fund_dir = ROOT / "data" / "funds" / isin

    def log(agent, level, msg):
        _log(agent, level, msg, log_path)

    log("ORCHESTRATOR", "START", f"--consume-cowork pipeline {isin}")

    # 1. Integrate the cowork skill output
    integration = _consume_cowork_analyst(isin, fund_dir, log)

    # 2. Re-load output.json (now has analyst_synthesis from cowork)
    output_path = fund_dir / "output.json"
    output_data = json.loads(output_path.read_text(encoding="utf-8"))
    fund_name_hint = output_data.get("nombre", "") or ""
    gestora_hint = output_data.get("gestora", "") or ""
    anio_creacion_hint = (output_data.get("kpis") or {}).get("anio_creacion")

    # Hints for downstream from manager_profile if present
    gestores_hint: list[str] = []
    mp_path = fund_dir / "manager_profile.json"
    if mp_path.exists():
        try:
            mp = json.loads(mp_path.read_text(encoding="utf-8"))
            gestores_hint = list(mp.get("equipo_gestor") or [])[:3]
        except Exception:
            pass

    # Reuse config if present, else minimal
    config_path = fund_dir / "config.json"
    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        config = {"objetivo": "1", "horizonte_historico": "1",
                  "fuentes": "1", "clase_accion": "todas", "contexto_adicional": ""}

    # 3. Validation
    try:
        from agents.validation_agent import ValidationAgent
        validator = ValidationAgent(isin, fund_dir=fund_dir, config=config)
        await validator.run()
        log("VALIDATION", "OK", "Validation completada (post-cowork)")
    except Exception as exc:
        log("VALIDATION", "ERROR", f"Validation falló: {exc}")

    # 4. Meta
    try:
        from agents.meta_agent import MetaAgent
        meta = MetaAgent(isin, fund_dir=fund_dir, config=config)
        await meta.run()
        log("META", "OK", "Meta completado (post-cowork)")
    except Exception as exc:
        log("META", "ERROR", f"Meta falló: {exc}")

    # 5. Quality loop (same env knob as the normal flow)
    try:
        quality_report = await _run_quality_loop(
            isin, fund_dir, config,
            fund_name_hint=fund_name_hint,
            gestora_hint=gestora_hint,
            anio_creacion_hint=anio_creacion_hint,
            gestores_hint=gestores_hint,
            log=log,
            max_iter=int(os.environ.get("QUALITY_LOOP_MAX_ITER", "1")),
        )
        score = quality_report.get("score", 0)
        log("QUALITY", "OK", f"Quality loop: score={score}/100 (post-cowork)")
    except Exception as exc:
        log("QUALITY", "ERROR", f"Quality loop falló: {exc}")

    # 6. publication_calendar
    try:
        from tools.publication_calendar import update_output_with_calendar
        if update_output_with_calendar(isin):
            log("CALENDAR", "OK", "publication_calendar actualizado")
    except Exception as exc:
        log("CALENDAR", "WARN", f"publication_calendar: {exc}")

    # 7. Regenerate dashboard
    try:
        import subprocess
        gen_path = ROOT / "dashboard" / "generate_dashboard.py"
        result = subprocess.run(
            ["python", str(gen_path), isin],
            cwd=str(ROOT), capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            log("DASHBOARD", "OK", f"dashboard/fund-{isin}.html regenerado")
        else:
            log("DASHBOARD", "WARN", f"generate_dashboard rc={result.returncode}: {result.stderr[:200]}")
    except Exception as exc:
        log("DASHBOARD", "ERROR", f"generate_dashboard falló: {exc}")

    console.print(Panel(
        f"[bold green]Consume-cowork OK para {isin}[/bold green]\n"
        f"Secciones integradas: {len(integration['sections'])}\n"
        f"Input drift: {len(integration['drift'])} ficheros\n"
        f"Dashboard: dashboard/fund-{isin}.html",
        border_style="green",
        title="--consume-cowork",
    ))
    return {"isin": isin, "consume_cowork": True, **integration}


# ══════════════════════════════════════════════════════════════════════════════
# Refactor L2 (2026-05-05) — consumes for the 3 cowork prep skills
# ══════════════════════════════════════════════════════════════════════════════

def _merge_prep_into_output(isin: str, fund_dir: Path, log) -> dict:
    """Merge top-level fields from prep file (cnmv_data.json o intl_data.json)
    into output.json, respetando _manual_edits.

    Razón: en flujos legacy (analyze_fund, consume_cowork_pipeline) este merge
    lo hacía AnalystAgent.run() capa 2 (consolidación deterministic, NO LLM).
    En cowork flow eliminamos el quality_loop+analyst legacy (Fix 1), por lo
    que el merge debe hacerse explícitamente al inicio de consume_all_cowork.
    Sin esto, output.json queda sin KPIs, posiciones, nombre, gestora, etc.

    Idempotente: re-ejecutar con el mismo input produce el mismo output.

    Returns: dict con {merged: bool, fields: list[str], source: str|None}
    """
    from tools.output_merger import (
        load_output, save_output, merge_preserving_manual_edits,
    )

    # Campos TOP-LEVEL que se copian del prep al output. Lista deliberadamente
    # exhaustiva para cubrir tanto fondos ES (cnmv_data) como INT (intl_data).
    # Si un campo no existe en el source, simplemente se ignora.
    TOP_LEVEL_FROM_PREP = (
        # Identidad
        "nombre", "gestora", "isin", "tipo", "ultima_actualizacion",
        # Cuantitativo / KPIs / posiciones
        "kpis", "cuantitativo", "posiciones",
        # Cualitativo
        "cualitativo", "comision_exito", "hechos_relevantes",
        "anio_creacion", "analisis_consistencia",
        # Fuentes (informes, xmls, urls)
        "fuentes",
        # Flags internos del prep
        "serie_vl_corrupta",
        # INT específicos
        "_int_clases", "_int_gestores", "_int_cualitativo",
        "_int_estrategia", "_int_filosofia", "_int_tipo_activos",
        "_int_historia",
        "economia_fondo", "clases",
        "asset_allocation", "geographic_allocation",
        "sector_allocation", "performance",
    )

    # 1. Cargar source: cnmv_data.json para ES, intl_data.json para INT
    cnmv_path = fund_dir / "cnmv_data.json"
    intl_path = fund_dir / "intl_data.json"
    src = None
    src_name = None
    if cnmv_path.exists():
        try:
            src = json.loads(cnmv_path.read_text(encoding="utf-8"))
            src_name = "cnmv_data.json"
        except Exception as exc:
            log("MERGE_PREP", "WARN", f"cnmv_data.json no parseable: {exc}")
    if src is None and intl_path.exists():
        try:
            src = json.loads(intl_path.read_text(encoding="utf-8"))
            src_name = "intl_data.json"
        except Exception as exc:
            log("MERGE_PREP", "WARN", f"intl_data.json no parseable: {exc}")
    if src is None:
        log("MERGE_PREP", "WARN",
            "Ni cnmv_data.json ni intl_data.json existen. Skip merge.")
        return {"merged": False, "fields": [], "source": None}

    # 2. Cargar output existente (vacío si no existe)
    existing = load_output(isin) or {}
    if not existing:
        existing = {"isin": isin}

    # 3. Construir 'new' = existing actualizado con campos top-level del source
    new = dict(existing)
    fields_copied = []
    for field in TOP_LEVEL_FROM_PREP:
        if field not in src:
            continue
        v = src[field]
        if v is None:
            continue
        new[field] = v
        fields_copied.append(field)

    # Asegurar isin (por si el source tenía otro o no lo tenía)
    new["isin"] = isin

    # 4. Merge respetando _manual_edits del existing
    merged = merge_preserving_manual_edits(existing, new)

    # 5. Guardar via output_merger (atomic write)
    try:
        save_output(isin, merged)
        log("MERGE_PREP", "OK",
            f"output.json mergeado desde {src_name} "
            f"({len(fields_copied)} campos): "
            f"{', '.join(fields_copied[:8])}"
            f"{'...' if len(fields_copied) > 8 else ''}")
    except Exception as exc:
        log("MERGE_PREP", "ERROR", f"save_output falló: {exc}")
        return {"merged": False, "fields": fields_copied, "error": str(exc)}

    return {"merged": True, "fields": fields_copied, "source": src_name}


def _consume_extracted(isin: str, fund_dir: Path, log) -> dict:
    """Integrate outputs of extract-pdfs-cowork into cnmv_data.json or intl_data.json.

    Reads `data/funds/{ISIN}/extracted/{task_id}.json` (one per task in
    pending_extraction.json). Each output has shape
        {"task_id", "agent", "data": {...}, "anti_invencion_notes": [...]}
    where `data` follows the schema declared in the original task.

    Routing:
    - agent="cnmv_agent"      → merge data into cnmv_data.cualitativo (per period
                                 if periodic, else top-level)
    - agent="cnmv_enrichment" → merge data.positions_with_sector into
                                 cnmv_data.posiciones.actuales matching by name
    - agent="intl_extractor_v2" → merge data into intl_data.json (kpis,
                                 posiciones, cualitativo, clases, economia_fondo)

    Idempotent: re-running with the same outputs is a no-op.
    """
    from tools.output_merger import save_output, mark_manual_edit

    extracted_dir = fund_dir / "extracted"
    if not extracted_dir.exists():
        log("CONSUME", "WARN",
            f"No hay {extracted_dir} — skill extract-pdfs-cowork no se ha ejecutado")
        return {"isin": isin, "n_integrated": 0, "n_failed": 0}

    task_files = sorted(extracted_dir.glob("*.json"))
    # Filter completion manifest (not a task)
    task_files = [p for p in task_files if p.name != "extraction_complete.json"]
    if not task_files:
        log("CONSUME", "WARN", f"{extracted_dir} vacío (0 tasks)")
        return {"isin": isin, "n_integrated": 0, "n_failed": 0}

    # Load both data files (only the relevant one is touched per task)
    cnmv_path = fund_dir / "cnmv_data.json"
    intl_path = fund_dir / "intl_data.json"

    def _load(p: Path) -> dict | None:
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save(p: Path, d: dict) -> None:
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

    cnmv_data = _load(cnmv_path)
    intl_data = _load(intl_path)
    n_ok = 0
    n_fail = 0
    integrated_paths: list[str] = []

    for tf in task_files:
        try:
            task = json.loads(tf.read_text(encoding="utf-8"))
        except Exception as exc:
            log("CONSUME", "WARN", f"task {tf.name}: parse fallido: {exc}")
            n_fail += 1
            continue

        agent_name = task.get("agent", "")
        data = task.get("data") or {}
        if not isinstance(data, dict):
            log("CONSUME", "WARN", f"task {tf.name}: 'data' no es dict, skip")
            n_fail += 1
            continue

        if agent_name == "cnmv_agent" and cnmv_data is not None:
            # Fix-3: estructurar cualitativo en campos canónicos planos +
            # histórico por periodo. El render del dashboard y la skill
            # analyst-cowork esperan cualitativo.{contexto_mercado,
            # decisiones_tomadas, tesis_gestora, perspectivas} (strings).
            # Antes: metíamos `data` bajo cualitativo[task_id] y nadie lo leía.
            import re as _re
            cual = cnmv_data.setdefault("cualitativo", {})
            tid = task.get("task_id") or task.get("id") or tf.stem

            # Periodo desde task_id (formato esperado: "..._YYYY_HX" o "..._YYYY")
            m = _re.search(r"(\d{4})(?:[_-]?H[12])?", tid)
            periodo = m.group(0) if m else "current"

            # Histórico por periodo (preserva todos)
            histo = cual.setdefault("_historico", {})
            histo[periodo] = data

            # Colapsar a campos planos canónicos: walk de más antiguo a más
            # reciente; cada periodo overrides los campos que tiene. Resultado:
            # el más reciente gana para fields que tiene; periodos previos
            # rellenan los huecos (order-independent).
            # Filtrar a periodos parseables (YYYY o YYYY_HX) antes del sort para
            # evitar bug "None" > "2025" lexicográfico.
            valid_periods = [p for p in histo.keys()
                              if _re.match(r"^\d{4}([_-]?H[12])?$", str(p))]
            if not valid_periods:
                valid_periods = list(histo.keys())
            valid_periods.sort()  # ascendente: oldest first
            for per in valid_periods:
                snap = histo.get(per)
                if not isinstance(snap, dict):
                    continue
                for canonical_field in ("contexto_mercado", "decisiones_tomadas",
                                          "tesis_gestora", "perspectivas"):
                    v = snap.get(canonical_field)
                    if v:
                        cual[canonical_field] = v

            integrated_paths.append(f"cnmv_data.cualitativo.{tid}")
            n_ok += 1

        elif agent_name == "cnmv_enrichment" and cnmv_data is not None:
            # Merge sector classifications into cnmv_data.posiciones.actuales
            classified = data.get("positions_with_sector") or []
            if not isinstance(classified, list):
                log("CONSUME", "WARN", f"task {tf.name}: positions_with_sector no es lista")
                n_fail += 1
                continue
            actuales = (cnmv_data.get("posiciones") or {}).get("actuales") or []
            by_name = {(p.get("nombre") or "").strip().lower(): p for p in actuales if isinstance(p, dict)}
            for entry in classified:
                if not isinstance(entry, dict):
                    continue
                name = (entry.get("nombre") or "").strip().lower()
                sector = (entry.get("sector") or "").strip()
                if name and sector and name in by_name:
                    by_name[name]["sector"] = sector
            integrated_paths.append(f"cnmv_data.posiciones.actuales (sectores)")
            n_ok += 1

        elif agent_name == "intl_extractor_v2" and intl_data is not None:
            # Fix-5.5: merge granular. Antes hacíamos un assign indiscriminado
            # que sobrescribía posiciones cada vez. Ahora:
            # - kpis: merge campo a campo, last-write-wins (factsheets más
            #   recientes ganan sobre AR antiguos)
            # - posiciones: la lista más larga gana (factsheet típicamente
            #   solo trae top 10, AR trae todas)
            # - allocations / performance: last-write-wins
            # - cualitativo / commentary: update merge

            new_kpis = data.get("kpis") or {}
            if isinstance(new_kpis, dict) and new_kpis:
                existing_kpis = intl_data.setdefault("kpis", {})
                for k, v in new_kpis.items():
                    if v is None or v == "":
                        continue
                    existing_kpis[k] = v

            new_pos = (data.get("posiciones_actuales")
                        or (data.get("posiciones") if isinstance(data.get("posiciones"), list)
                             else (data.get("posiciones") or {}).get("actuales") if isinstance(data.get("posiciones"), dict)
                             else None))
            if isinstance(new_pos, list) and new_pos:
                existing_pos_root = intl_data.setdefault("posiciones", {})
                if isinstance(existing_pos_root, list):
                    # legacy: era lista directa
                    existing_pos_root = {"actuales": existing_pos_root}
                    intl_data["posiciones"] = existing_pos_root
                existing_pos = existing_pos_root.get("actuales") or []

                # Fix-5.5-bis (2026-05-06): priorizar por TIPO de task, no por
                # longitud de lista. Riesgo previo: AR corporate (paraguas, 100
                # holdings) gana sobre factsheet del sub-fondo (10 holdings
                # reales). Ahora: factsheet > annual_subfund > otros.
                tid = (task.get("task_id") or task.get("id") or "").lower()
                _PRIO = {  # mayor número = mayor prioridad
                    "factsheet": 100,
                    "annual_subfund": 60,
                    "semi_annual_subfund": 60,
                    "web_fund_page": 90,  # live data
                    "manager_letter": 30,
                    "kid": 20,
                    "prospectus": 20,
                    "generic_pdf": 10,
                }
                new_prio = 50  # default si no detecta tipo
                for ttype, pri in _PRIO.items():
                    if ttype in tid:
                        new_prio = pri
                        break
                # Detectar prio del existente (guardado en _meta si lo hay)
                existing_prio = intl_data.get("_posiciones_prio", 0)
                if not existing_pos or new_prio >= existing_prio:
                    existing_pos_root["actuales"] = new_pos
                    intl_data["_posiciones_prio"] = new_prio

            for key in ("asset_allocation", "geographic_allocation",
                          "sector_allocation", "performance"):
                v = data.get(key)
                if v:
                    intl_data[key] = v

            cual = data.get("cualitativo") or {}
            if isinstance(cual, dict) and cual:
                intl_data.setdefault("cualitativo", {}).update(cual)

            com = data.get("comentario_gestor")
            if com:
                intl_data.setdefault("cualitativo", {})["comentario_factsheet"] = com

            # KID-only fields
            for key in ("perfil_riesgo_srri", "objetivo_inversion",
                          "politica_inversion", "comisiones"):
                v = data.get(key)
                if v:
                    intl_data[key] = v

            # Schema fields que solo aparecen en AR sub-fund
            for key in ("_int_clases", "_int_gestores", "_int_cualitativo",
                          "economia_fondo", "clases"):
                v = data.get(key)
                if v:
                    intl_data[key] = v

            integrated_paths.append(f"intl_data.{tf.stem}")
            n_ok += 1
        else:
            log("CONSUME", "WARN",
                f"task {tf.name}: agent='{agent_name}' no reconocido o data file ausente")
            n_fail += 1

    # Persist updated data files
    if cnmv_data is not None and any(p.startswith("cnmv_data") for p in integrated_paths):
        _save(cnmv_path, cnmv_data)
        log("CONSUME", "OK", f"cnmv_data.json actualizado")
    if intl_data is not None and any(p.startswith("intl_data") for p in integrated_paths):
        _save(intl_path, intl_data)
        log("CONSUME", "OK", f"intl_data.json actualizado")

    # Mark integrated paths as manual edits in output.json so future runs preserve them
    output_path = fund_dir / "output.json"
    if output_path.exists():
        try:
            output_data = json.loads(output_path.read_text(encoding="utf-8"))
            for p in integrated_paths:
                # Translate cnmv_data.X -> top-level cualitativo.X for the manual edit guard
                if p.startswith("cnmv_data.cualitativo."):
                    mark_manual_edit(output_data, p.replace("cnmv_data.", ""))
            save_output(isin, output_data)
        except Exception as exc:
            log("CONSUME", "WARN", f"no se pudo marcar manual_edits: {exc}")

    log("CONSUME_EXTRACTED", "OK", f"integradas {n_ok} tasks, {n_fail} fallaron")
    return {"isin": isin, "n_integrated": n_ok, "n_failed": n_fail,
            "paths": integrated_paths}


def _consume_manager_deep(isin: str, fund_dir: Path, log) -> dict:
    """Integrate outputs of manager-deep-cowork into manager_profile.json.

    Reads `data/funds/{ISIN}/manager_deep_complete.json` (or per-task files
    under `manager_deep_outputs/`). The skill writes:
    - identify_lead_co result: {"equipo_gestor": [...], "equipo_roles": {...},
      "_opus_lead_confidence": "..."}
    - extract_articles result: {"articulos_completos": {gestor: [...]}}

    Merge strategy mirrors the K1 fix in CLAUDE.md: the skill's data is added
    to manager_profile.json WITHOUT overwriting fields the profiler already
    set. Only adds missing keys.
    """
    profile_path = fund_dir / "manager_profile.json"
    if not profile_path.exists():
        log("CONSUME_MGR_DEEP", "WARN", "manager_profile.json no existe — saltando")
        return {"isin": isin, "n_integrated": 0}

    try:
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception as exc:
        log("CONSUME_MGR_DEEP", "ERROR", f"no se pudo cargar profile: {exc}")
        return {"isin": isin, "n_integrated": 0, "error": str(exc)}

    n = 0

    # Try aggregated complete file first
    complete_path = fund_dir / "manager_deep_complete.json"
    outputs_dir = fund_dir / "manager_deep_outputs"

    payloads: list[dict] = []
    if complete_path.exists():
        try:
            payloads.append(json.loads(complete_path.read_text(encoding="utf-8")))
        except Exception as exc:
            log("CONSUME_MGR_DEEP", "WARN", f"complete parse: {exc}")
    if outputs_dir.exists():
        for tf in sorted(outputs_dir.glob("*.json")):
            try:
                payloads.append(json.loads(tf.read_text(encoding="utf-8")))
            except Exception:
                pass

    if not payloads:
        log("CONSUME_MGR_DEEP", "WARN",
            f"No hay outputs (ni {complete_path.name} ni {outputs_dir.name}/) — skill no ejecutada")
        return {"isin": isin, "n_integrated": 0}

    # Backup pre-merge (K1 anti-regression)
    backup = fund_dir / "manager_profile.backup_pre_deep.json"
    try:
        backup.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    for payload in payloads:
        if not isinstance(payload, dict):
            continue

        # identify_lead_co outputs
        if payload.get("type") == "identify_lead_co" or payload.get("task_id", "").startswith("identify"):
            data = payload.get("data") or payload
            new_team = data.get("equipo_gestor") or []
            new_roles = data.get("equipo_roles") or {}
            confidence = data.get("_opus_lead_confidence") or data.get("confidence")
            # Only set if profile doesn't already have them populated
            if new_team and not profile.get("equipo_gestor"):
                profile["equipo_gestor"] = new_team
                n += 1
            if new_roles and not profile.get("equipo_roles"):
                profile["equipo_roles"] = new_roles
                n += 1
            if confidence and not profile.get("_opus_lead_confidence"):
                profile["_opus_lead_confidence"] = confidence

        # extract_articles outputs
        if payload.get("type") == "extract_articles" or "articulos_completos" in (payload.get("data") or {}):
            data = payload.get("data") or payload
            articulos = data.get("articulos_completos") or {}
            if articulos:
                existing = profile.setdefault("articulos_completos", {})
                for gestor, arts in articulos.items():
                    if not isinstance(arts, list):
                        continue
                    existing.setdefault(gestor, [])
                    existing[gestor].extend(a for a in arts if isinstance(a, dict))
                n += len(articulos)

        # Optional metadata: _known_public_undersourced (K10 conditional)
        kpu = (payload.get("data") or {}).get("_known_public_undersourced") or []
        if kpu and "_known_public_undersourced" not in profile:
            profile["_known_public_undersourced"] = kpu

    profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    log("CONSUME_MGR_DEEP", "OK", f"manager_profile.json mergeado ({n} campos añadidos)")

    # Mirror equipo_gestor + equipo_roles into output.json under gestores
    # Fix-4: además, propagar `profile.equipo[]` (perfiles RICOS con cargo,
    # biografia_inicial, trayectoria, fuentes...) a `output.gestores.perfiles[]`
    # en el formato que el dashboard renderer y analyst-cowork esperan.
    # Antes: profile.equipo tenía los 8 perfiles ricos pero nunca llegaban a
    # output.gestores.perfiles (quedaba en 0).
    output_path = fund_dir / "output.json"
    if output_path.exists():
        try:
            from tools.output_merger import save_output, mark_manual_edit
            output_data = json.loads(output_path.read_text(encoding="utf-8"))
            gestores = output_data.setdefault("gestores", {})

            equipo_gestor = profile.get("equipo_gestor") or []
            if equipo_gestor:
                gestores["equipo"] = equipo_gestor
                mark_manual_edit(output_data, "gestores.equipo")

            # Propagar perfiles ricos
            equipo_rich = profile.get("equipo") or []
            if equipo_rich and isinstance(equipo_rich, list):
                lead_set = set((equipo_gestor or [])[:1])
                co_set = set((equipo_gestor or [])[1:])
                perfiles_out = []
                for entry in equipo_rich:
                    if not isinstance(entry, dict):
                        continue
                    nombre = (entry.get("nombre") or "").strip()
                    if not nombre:
                        continue
                    bio_init = entry.get("biografia_inicial") or entry.get("biografia") or ""
                    trayectoria = (entry.get("trayectoria")
                                    or entry.get("background")
                                    or bio_init)
                    p = {
                        "nombre": nombre,
                        "cargo": entry.get("cargo") or "",
                        "biografia": bio_init,
                        "trayectoria": trayectoria,
                        "filosofia": (entry.get("filosofia")
                                       or entry.get("filosofia_inversion") or ""),
                        "decisiones_clave": entry.get("decisiones_clave") or [],
                        "cv_bullets": entry.get("cv_bullets") or [],
                        "is_lead": nombre in lead_set,
                        "is_co": nombre in co_set,
                        "fuentes": entry.get("fuentes") or [],
                    }
                    perfiles_out.append(p)
                if perfiles_out:
                    gestores["perfiles"] = perfiles_out
                    mark_manual_edit(output_data, "gestores.perfiles")
                    # Fix-4-bis (2026-05-06): el dashboard renderer lee de
                    # analyst_synthesis.gestores.perfiles[] (línea 1363 de
                    # generate_dashboard.py), NO de output.gestores.perfiles[].
                    # Propagamos a ambos paths para que los perfiles ricos
                    # aparezcan tanto en accessor como en dashboard.
                    asyn = output_data.setdefault("analyst_synthesis", {})
                    asyn_gest = asyn.setdefault("gestores", {})
                    # Solo escribir si la skill analyst-cowork no escribió
                    # perfiles propios (no sobrescribir su trabajo).
                    if not asyn_gest.get("perfiles"):
                        asyn_gest["perfiles"] = perfiles_out
                        mark_manual_edit(output_data, "analyst_synthesis.gestores.perfiles")

            if profile.get("articulos_completos"):
                output_data.setdefault("_meta", {})["articulos_completos_per_gestor"] = {
                    g: len(arts) for g, arts in profile["articulos_completos"].items()
                }
            save_output(isin, output_data)
        except Exception as exc:
            log("CONSUME_MGR_DEEP", "WARN", f"mirror a output.json falló: {exc}")

    return {"isin": isin, "n_integrated": n}


def _consume_letters_extract(isin: str, fund_dir: Path, log) -> dict:
    """Integrate K15 outputs of letters-extract-cowork into letters_data.cartas[].

    The skill writes K15 fields back to each carta entry: tesis_gestora,
    decisiones_tomadas, contexto_mercado, citas_textuales, posiciones_destacadas,
    outlook. Looks for them in `letters_data.cartas` (in-place edit by skill)
    or in a separate `letters_extract_complete.json`.
    """
    letters_path = fund_dir / "letters_data.json"
    if not letters_path.exists():
        log("CONSUME_LETTERS", "WARN", "letters_data.json no existe — saltando")
        return {"isin": isin, "n_integrated": 0}

    try:
        letters_data = json.loads(letters_path.read_text(encoding="utf-8"))
    except Exception as exc:
        log("CONSUME_LETTERS", "ERROR", f"parse letters_data.json: {exc}")
        return {"isin": isin, "n_integrated": 0, "error": str(exc)}

    cartas = letters_data.get("cartas") or []
    if not cartas:
        log("CONSUME_LETTERS", "WARN", "letters_data.json no tiene cartas")
        return {"isin": isin, "n_integrated": 0}

    K15_FIELDS = (
        "tesis_gestora", "decisiones_tomadas", "contexto_mercado",
        "citas_textuales", "posiciones_destacadas", "outlook",
    )

    # If skill wrote to a separate complete file, merge that into cartas first
    complete_path = fund_dir / "letters_extract_complete.json"
    if complete_path.exists():
        try:
            complete = json.loads(complete_path.read_text(encoding="utf-8"))
            ext_cartas = complete.get("cartas") if isinstance(complete, dict) else complete
            if isinstance(ext_cartas, list):
                # Index by archivo or url for matching
                idx_existing = {}
                for i, c in enumerate(cartas):
                    if not isinstance(c, dict):
                        continue
                    key = c.get("archivo") or c.get("url")
                    if key:
                        idx_existing[key] = i
                for ec in ext_cartas:
                    if not isinstance(ec, dict):
                        continue
                    key = ec.get("archivo") or ec.get("url")
                    if not key or key not in idx_existing:
                        continue
                    target = cartas[idx_existing[key]]
                    for f in K15_FIELDS:
                        if ec.get(f) and not target.get(f):
                            target[f] = ec[f]
        except Exception as exc:
            log("CONSUME_LETTERS", "WARN", f"merge complete fallo: {exc}")

    # Count cartas with K15 populated
    n_with_k15 = sum(
        1 for c in cartas
        if isinstance(c, dict) and any(c.get(f) for f in K15_FIELDS)
    )

    letters_path.write_text(
        json.dumps(letters_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log("CONSUME_LETTERS", "OK",
        f"letters_data.json: {n_with_k15}/{len(cartas)} cartas con K15")

    return {"isin": isin, "n_integrated": n_with_k15, "n_total": len(cartas)}


async def _consume_html_fallback(isin: str, fund_dir: Path, log) -> dict:
    """B6 (2026-05-19): tras consume-extracted, si intl_data.json sigue sin
    AUM ni posiciones (típico cuando la gestora INT no publica PDFs descargables),
    dispara _fallback_html_extract para extraer cuanti desde páginas HTML
    harvested. Solo aplica a tipo INT.

    Coste: ~1 call Gemini Flash = $0.02-0.05.
    No-op si ya hay datos o si no hay URLs harvested.
    """
    intl_path = fund_dir / "intl_data.json"
    if not intl_path.exists():
        return {"skipped": True, "reason": "no intl_data.json (fondo ES o pipeline INT no corrió)"}
    try:
        intl = json.loads(intl_path.read_text(encoding="utf-8"))
    except Exception as exc:
        log("HTML_FALLBACK", "WARN", f"intl_data.json no parseable: {exc}")
        return {"skipped": True, "reason": "intl_data.json corrupto"}

    if intl.get("tipo") != "INT":
        return {"skipped": True, "reason": "no es fondo INT"}

    kpis = intl.get("kpis") or {}
    posiciones = (intl.get("posiciones") or {}).get("actuales") or []
    needs = (kpis.get("aum_actual_meur") is None) and (len(posiciones) == 0)
    if not needs:
        return {"skipped": True, "reason": "ya hay AUM o posiciones tras consume-extracted"}

    # Disparar fallback HTML
    from agents.intl_extractor_v2 import IntlExtractor
    extractor = IntlExtractor(
        isin=isin,
        config={
            "nombre": intl.get("nombre", ""),
            "gestora": intl.get("gestora", ""),
        },
    )
    log("HTML_FALLBACK", "START", f"disparando fallback HTML para {isin}")
    try:
        triggered = await extractor._fallback_html_extract(intl)
    except Exception as exc:
        log("HTML_FALLBACK", "ERROR", f"_fallback_html_extract crashed: {exc}")
        return {"triggered": False, "error": str(exc)}

    if not triggered:
        log("HTML_FALLBACK", "OK", "sin cambios (sin URLs útiles o sin datos extraíbles)")
        return {"triggered": False}

    # Guardar intl_data.json con los nuevos campos
    intl_path.write_text(
        json.dumps(intl, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Re-mergear a output.json para que el resto del pipeline lo vea
    try:
        _merge_prep_into_output(isin, fund_dir, log)
    except Exception as exc:
        log("HTML_FALLBACK", "WARN", f"merge tras fallback falló: {exc}")

    new_aum = intl.get("kpis", {}).get("aum_actual_meur")
    new_posic = len((intl.get("posiciones") or {}).get("actuales", []))
    log("HTML_FALLBACK", "OK",
        f"fallback aplicado: AUM={new_aum}, posiciones={new_posic}")
    return {
        "triggered": True,
        "aum_actual_meur": new_aum,
        "n_posiciones": new_posic,
    }


async def consume_all_cowork_pipeline(isin: str, log_path: Path) -> dict:
    """Orchestrate the 4 cowork consumes in order, then run validation +
    meta + quality + dashboard. Used by the bat after all skills complete.
    """
    isin = isin.strip().upper()
    os.environ["CURRENT_FUND_ISIN"] = isin
    fund_dir = ROOT / "data" / "funds" / isin

    def log(agent, level, msg):
        _log(agent, level, msg, log_path)

    log("ORCHESTRATOR", "START", f"--consume-all-cowork pipeline {isin}")

    summary: dict[str, Any] = {"isin": isin}

    # CRÍTICO Fix-MERGE-PREP (2026-05-06): mergear cnmv_data/intl_data → output.json
    # ANTES de cualquier consume. Sin esto, output.json no tendría kpis,
    # posiciones, nombre, gestora, cualitativo, etc. (el merge lo hacía el
    # quality_loop+analyst legacy que eliminamos en Fix 1, por lo que hay que
    # hacerlo explícitamente aquí). Idempotente. Cero coste API.
    try:
        summary["merge_prep"] = _merge_prep_into_output(isin, fund_dir, log)
    except Exception as exc:
        log("MERGE_PREP", "ERROR", f"falló inesperadamente: {exc}")
        summary["merge_prep"] = {"error": str(exc)}

    # 1. Consume extract-pdfs-cowork outputs
    try:
        summary["extracted"] = _consume_extracted(isin, fund_dir, log)
    except Exception as exc:
        log("CONSUME_EXTRACTED", "ERROR", f"falló: {exc}")
        summary["extracted"] = {"error": str(exc)}

    # 1b. Fallback HTML (B6 2026-05-19): si tras consume-extracted aún falta
    # AUM/posiciones (típico INT sin PDFs públicos), extraer desde páginas HTML
    # harvested usando Gemini Flash. Coste ~$0.02-0.05.
    try:
        summary["html_fallback"] = await _consume_html_fallback(isin, fund_dir, log)
    except Exception as exc:
        log("HTML_FALLBACK", "ERROR", f"falló: {exc}")
        summary["html_fallback"] = {"error": str(exc)}

    # 2. Consume manager-deep-cowork
    try:
        summary["manager_deep"] = _consume_manager_deep(isin, fund_dir, log)
    except Exception as exc:
        log("CONSUME_MGR_DEEP", "ERROR", f"falló: {exc}")
        summary["manager_deep"] = {"error": str(exc)}

    # 3. Consume letters-extract-cowork
    try:
        summary["letters_extract"] = _consume_letters_extract(isin, fund_dir, log)
    except Exception as exc:
        log("CONSUME_LETTERS", "ERROR", f"falló: {exc}")
        summary["letters_extract"] = {"error": str(exc)}

    # 4. Consume analyst-cowork (existing)
    try:
        summary["analyst"] = _consume_cowork_analyst(isin, fund_dir, log)
    except Exception as exc:
        log("COWORK", "ERROR", f"analyst consume falló: {exc}")
        summary["analyst"] = {"error": str(exc)}

    # 4.5. Name recovery (T2.5 2026-05-27): si el campo `nombre` quedó como el
    # ISIN o vacío (típico cuando CBI/AMF no resolvió la identity card), pero
    # el analyst_synthesis ya menciona el nombre real → recuperarlo con
    # regex local + fallback Haiku. Marca `_manual_edits["nombre"]` para que
    # próximas regeneraciones no lo sobrescriban.
    try:
        output_path_nr = fund_dir / "output.json"
        if output_path_nr.exists():
            from tools.name_recovery import recover_name_if_needed
            output_data_nr = json.loads(output_path_nr.read_text(encoding="utf-8"))
            result = recover_name_if_needed(output_data_nr, isin, log_fn=log)
            summary["name_recovery"] = result
            if result.get("applied"):
                output_path_nr.write_text(
                    json.dumps(output_data_nr, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
    except Exception as exc:
        log("NAME_RECOVERY", "ERROR", f"falló inesperadamente: {exc}")
        summary["name_recovery"] = {"applied": False, "error": str(exc)}

    # 5. Validation + meta + quality + calendar + dashboard (same as consume_cowork_pipeline)
    config_path = fund_dir / "config.json"
    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        config = {"objetivo": "1", "horizonte_historico": "1",
                  "fuentes": "1", "clase_accion": "todas", "contexto_adicional": ""}

    output_path = fund_dir / "output.json"
    fund_name_hint = ""
    gestora_hint = ""
    anio_creacion_hint = None
    gestores_hint: list[str] = []
    if output_path.exists():
        try:
            output_data = json.loads(output_path.read_text(encoding="utf-8"))
            fund_name_hint = output_data.get("nombre", "") or ""
            gestora_hint = output_data.get("gestora", "") or ""
            anio_creacion_hint = (output_data.get("kpis") or {}).get("anio_creacion")
            mp_path = fund_dir / "manager_profile.json"
            if mp_path.exists():
                mp = json.loads(mp_path.read_text(encoding="utf-8"))
                gestores_hint = list(mp.get("equipo_gestor") or [])[:3]
        except Exception:
            pass

    try:
        from agents.validation_agent import ValidationAgent
        validator = ValidationAgent(isin, fund_dir=fund_dir, config=config)
        await validator.run()
        log("VALIDATION", "OK", "Validation post-consume-all-cowork")
    except Exception as exc:
        log("VALIDATION", "ERROR", f"falló: {exc}")

    try:
        from agents.meta_agent import MetaAgent
        meta = MetaAgent(isin, fund_dir=fund_dir, config=config)
        await meta.run()
        log("META", "OK", "Meta post-consume-all-cowork")
    except Exception as exc:
        log("META", "ERROR", f"falló: {exc}")

    # NOTE (Fix-1): NO quality_loop en consume_all_cowork_pipeline.
    # El quality_loop existe para iterar el analyst_agent legacy (Gemini/Sonnet).
    # En modo cowork la calidad la garantiza la skill analyst-cowork con su
    # propio audit_pass; re-disparar el legacy aquí provoca llamadas a Gemini
    # que pueden estar bloqueadas (sin créditos) y escribir
    # `analyst_synthesis.{section} = {"error": "Gemini no generó resultado"}`,
    # corrompiendo output.json. Para el flujo cowork basta con dashboard_quality
    # (ya generado por meta y reflejado en quality_report.json).
    try:
        from agents.dashboard_quality_agent import DashboardQualityAgent
        dq = DashboardQualityAgent(isin)
        dq_report = dq.run()
        if isinstance(dq_report, dict):
            score = dq_report.get("score", 0)
            n_fallos = len(dq_report.get("fallos") or [])
            log("QUALITY", "OK",
                f"Dashboard quality (sin loop): score={score}/103, fallos={n_fallos}")
            try:
                (fund_dir / "quality_report.json").write_text(
                    json.dumps(dq_report, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass
    except Exception as exc:
        log("QUALITY", "WARN", f"Dashboard quality (single pass) falló: {exc}")

    try:
        from tools.publication_calendar import update_output_with_calendar
        if update_output_with_calendar(isin):
            log("CALENDAR", "OK", "publication_calendar actualizado")
    except Exception as exc:
        log("CALENDAR", "WARN", f"calendar: {exc}")

    try:
        import subprocess
        gen_path = ROOT / "dashboard" / "generate_dashboard.py"
        result = subprocess.run(
            ["python", str(gen_path), isin],
            cwd=str(ROOT), capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            log("DASHBOARD", "OK", f"dashboard/fund-{isin}.html regenerado")
        else:
            log("DASHBOARD", "WARN",
                f"generate_dashboard rc={result.returncode}: {result.stderr[:200]}")
    except Exception as exc:
        log("DASHBOARD", "ERROR", f"falló: {exc}")

    console.print(Panel(
        f"[bold green]Consume-all-cowork OK para {isin}[/bold green]\n"
        f"extract-pdfs: {summary.get('extracted', {}).get('n_integrated', '?')} tasks\n"
        f"manager-deep: {summary.get('manager_deep', {}).get('n_integrated', '?')} campos\n"
        f"letters-extract: {summary.get('letters_extract', {}).get('n_integrated', '?')} cartas con K15\n"
        f"analyst: {len((summary.get('analyst') or {}).get('sections', []))} secciones\n"
        f"Dashboard: dashboard/fund-{isin}.html",
        title="--consume-all-cowork",
        border_style="green",
    ))
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Orquestador del pipeline fund-analyzer (refactor cowork v2)"
    )
    parser.add_argument("--isin", required=True, help="ISIN del fondo (ej. ES0146309002)")
    parser.add_argument("--auto", action="store_true",
                        help="No preguntar parámetros (usar config.json o defaults)")

    parser.add_argument("--prep-only", action="store_true",
                        help="Fase 1 cowork: ejecuta solo prep determinista (CNMV bulkdata + "
                             "PDFs + Serper) y emite los manifests pending_*.json. NO llama "
                             "LLM en agentes refactorizados.")

    parser.add_argument("--api-fallback", action="store_true",
                        help="Activa modo legacy: agentes refactorizados (cnmv_agent cualitativo, "
                             "cnmv_enrichment sectores, intl_extractor_v2, manager_profiler, "
                             "manager_deep_agent, letters_deep_agent) vuelven a llamar Gemini "
                             "directamente. SIN este flag (default), modo cowork: emiten "
                             "manifests para que las skills procesen bajo Claude Max.")

    # Consume flags (run after the cowork skills finish)
    parser.add_argument("--consume-cowork", action="store_true",
                        help="Integra analyst_synthesis_cowork.json en output.json + valida + "
                             "meta + quality + dashboard. Asume que las skills ya corrieron.")
    parser.add_argument("--consume-extracted", action="store_true",
                        help="Integra outputs de extract-pdfs-cowork (extracted/*.json) en "
                             "cnmv_data.json o intl_data.json.")
    parser.add_argument("--consume-manager-deep", action="store_true",
                        help="Integra outputs de manager-deep-cowork en manager_profile.json.")
    parser.add_argument("--consume-letters-extract", action="store_true",
                        help="Integra K15 de letters-extract-cowork en letters_data.cartas[].")
    parser.add_argument("--consume-all-cowork", action="store_true",
                        help="Encadena los 4 consumes (extract, manager-deep, letters-extract, "
                             "analyst) + validation + meta + quality + dashboard. Lo usa el bat "
                             "después de las 4 skills.")

    args = parser.parse_args()

    # Set FUND_ANALYZER_MODE BEFORE any agent import (lazy import order matters
    # for the `is_cowork_mode()` gating in agents/*.py).
    if args.api_fallback:
        os.environ["FUND_ANALYZER_MODE"] = "api"
    # else: leave default (cowork)

    # Mutual exclusion: --prep-only and --consume-* are exclusive
    consume_flags = [
        args.consume_cowork, args.consume_extracted, args.consume_manager_deep,
        args.consume_letters_extract, args.consume_all_cowork,
    ]
    n_consume = sum(1 for f in consume_flags if f)

    if args.prep_only and any(consume_flags):
        console.print("[red]ERROR: --prep-only y los flags --consume-* son mutuamente exclusivos.[/red]")
        sys.exit(1)
    if n_consume > 1:
        console.print("[red]ERROR: usa solo UN flag --consume-* a la vez (o --consume-all-cowork).[/red]")
        sys.exit(1)

    # Route to the right pipeline
    if args.consume_all_cowork:
        log_path = ROOT / "progress.log"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n[{_ts()}] [ORCHESTRATOR] [START] consume-all-cowork {args.isin}\n{'='*60}\n")
        asyncio.run(consume_all_cowork_pipeline(args.isin, log_path))
    elif args.consume_cowork:
        log_path = ROOT / "progress.log"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n[{_ts()}] [ORCHESTRATOR] [START] consume-cowork {args.isin}\n{'='*60}\n")
        asyncio.run(consume_cowork_pipeline(args.isin, log_path))
    elif args.consume_extracted:
        log_path = ROOT / "progress.log"
        fund_dir = ROOT / "data" / "funds" / args.isin.strip().upper()
        def _log_simple(a, l, m): _log(a, l, m, log_path)
        _consume_extracted(args.isin.strip().upper(), fund_dir, _log_simple)
    elif args.consume_manager_deep:
        log_path = ROOT / "progress.log"
        fund_dir = ROOT / "data" / "funds" / args.isin.strip().upper()
        def _log_simple(a, l, m): _log(a, l, m, log_path)
        _consume_manager_deep(args.isin.strip().upper(), fund_dir, _log_simple)
    elif args.consume_letters_extract:
        log_path = ROOT / "progress.log"
        fund_dir = ROOT / "data" / "funds" / args.isin.strip().upper()
        def _log_simple(a, l, m): _log(a, l, m, log_path)
        _consume_letters_extract(args.isin.strip().upper(), fund_dir, _log_simple)
    else:
        asyncio.run(analyze_fund(args.isin, auto=args.auto, prep_only=args.prep_only))


if __name__ == "__main__":
    main()
