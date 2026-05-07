"""pending_manifest — append-during-prep, overwrite-between-runs helpers
for the cowork skill manifests.

Asymmetry by design (per FASE 2 plan, 2026-05-05):
- pending_extraction.json     — written by cnmv_agent + cnmv_enrichment + intl_extractor_v2
- pending_manager_deep.json   — written by manager_profiler + manager_deep_agent
- letters: NO pending file. The skill `letters-extract-cowork` reads
  `letters_data.cartas[]` and processes each carta lacking K15 fields.
  Implicit manifest based on schema, not on a separate file.

Within a single prep-only run, multiple agents may add tasks to the same
manifest. Between runs the manifest is overwritten — the orchestrator
calls `clean_pending_manifests(fund_dir)` at the start of `--prep-only`
to wipe old state.

Lifecycle:
1. orchestrator.--prep-only starts → calls clean_pending_manifests()
2. each agent adds tasks via append_extraction_task() / append_manager_deep_task()
3. orchestrator finishes prep → manifests sit on disk for the cowork skills
4. user runs `claude -p "extract pdfs cowork {ISIN}"` etc.
5. skills consume manifests, write outputs to extracted/ and update files
6. orchestrator.--consume-* integrates outputs and (optionally) deletes manifests
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Filenames consumed by the cowork skills. Update here if the contract
# in BUNDLE_CONTRACT.md ever moves them.
EXTRACTION_MANIFEST = "pending_extraction.json"
MANAGER_DEEP_MANIFEST = "pending_manager_deep.json"

ALL_MANIFESTS = (EXTRACTION_MANIFEST, MANAGER_DEEP_MANIFEST)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean_pending_manifests(fund_dir: Path) -> list[str]:
    """Remove all pending_*.json from fund_dir. Called by orchestrator at the
    start of --prep-only to ensure a fresh manifest per run.

    Returns the list of paths actually removed (informational).
    """
    removed: list[str] = []
    for fname in ALL_MANIFESTS:
        path = fund_dir / fname
        if path.exists():
            try:
                path.unlink()
                removed.append(fname)
            except OSError:
                pass
    return removed


def _load_or_init(path: Path, isin: str, tipo: str = "") -> dict:
    """Load an existing manifest or initialize a fresh one with metadata."""
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("tasks"), list):
                return data
        except Exception:
            pass
    return {
        "isin": isin,
        "tipo": tipo,
        "generated_at": _now_iso(),
        "tasks": [],
    }


def _save(path: Path, manifest: dict) -> None:
    manifest["updated_at"] = _now_iso()
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def append_extraction_task(
    fund_dir: Path,
    isin: str,
    *,
    task_id: str,
    agent: str,
    pdf_path: str,
    schema: dict,
    context: str = "",
    two_stage: bool = False,
    tipo: str = "",
    extra: dict | None = None,
) -> None:
    """Add a PDF extraction task to pending_extraction.json.

    Idempotent on task_id: if a task with the same id already exists, it is
    replaced (last write wins, useful when the same agent re-emits during
    the same prep-only run for whatever reason).
    """
    manifest_path = fund_dir / EXTRACTION_MANIFEST
    manifest = _load_or_init(manifest_path, isin, tipo)
    if tipo and not manifest.get("tipo"):
        manifest["tipo"] = tipo

    task: dict[str, Any] = {
        "id": task_id,
        "agent": agent,
        "pdf_path": pdf_path,
        "schema": schema,
    }
    if context:
        task["context"] = context
    if two_stage:
        task["two_stage"] = True
    if extra:
        task.update(extra)

    # Replace existing task with the same id (idempotency)
    existing = [t for t in manifest["tasks"] if t.get("id") != task_id]
    existing.append(task)
    manifest["tasks"] = existing
    _save(manifest_path, manifest)


def append_manager_deep_task(
    fund_dir: Path,
    isin: str,
    *,
    task_type: str,  # "identify_lead_co" | "extract_articles"
    fund_name: str = "",
    gestora: str = "",
    candidate_names: list[str] | None = None,
    candidate_urls: list[dict] | None = None,
    context: str = "",
) -> None:
    """Add a manager-deep task. Two task_type values are recognized:

    - "identify_lead_co": skill should identify the canonical lead/co
      manager + initial bio. Replaces manager_profiler._enrich_with_opus.
    - "extract_articles": skill should fetch/filter URLs about each
      identified manager and populate articulos_completos[gestor].
    """
    manifest_path = fund_dir / MANAGER_DEEP_MANIFEST
    manifest = _load_or_init(manifest_path, isin)
    if fund_name and not manifest.get("fund_name"):
        manifest["fund_name"] = fund_name
    if gestora and not manifest.get("gestora"):
        manifest["gestora"] = gestora

    task: dict[str, Any] = {
        "id": task_type,
        "type": task_type,
    }
    if candidate_names:
        task["candidate_names"] = candidate_names
    if candidate_urls:
        task["candidate_urls"] = candidate_urls
    if context:
        task["context"] = context

    existing = [t for t in manifest["tasks"] if t.get("id") != task_type]
    existing.append(task)
    manifest["tasks"] = existing
    _save(manifest_path, manifest)


# ── ES qualitative emitter (Refactor L2 fix, 2026-05-05) ──────────────────
# Bug observado en run HOROS: cnmv_agent ya guarda seccion_9_texto_completo
# en cnmv_data.json, pero la función _parse_seccion_cualitativo (que era la
# que emitía la task) quedó huérfana en el refactor. Y los fondos ES que
# publican semestrales solo en su web (HOROS, AzValor) tienen PDFs en
# raw/discovery/_web_*.pdf que nadie procesaba.
#
# Este helper resuelve ambos casos. El orchestrator lo llama una vez tras
# cnmv_agent.run() en modo --prep-only.

# Campos cualitativos canónicos extraídos de PDFs CNMV semestrales o
# informes web de la gestora. Schema usado por extract-pdfs-cowork.
ES_CUALITATIVO_SCHEMA = {
    "contexto_mercado": (
        "string. Resumen 150-250 palabras del entorno macro y mercado "
        "durante el periodo según la visión de la gestora."
    ),
    "decisiones_tomadas": (
        "string. Resumen 100-200 palabras de las decisiones de inversión "
        "(compras, ventas, aumentos/reducciones), con nombres de activos."
    ),
    "tesis_gestora": (
        "string. Tesis o filosofía expresada en este periodo, si la hay."
    ),
    "perspectivas": (
        "string. Outlook o perspectivas de mercado expresadas, si las hay."
    ),
}

ES_CUALITATIVO_CONTEXT = (
    "Informe semestral/trimestral del fondo ES. Extrae los 4 campos del "
    "schema usando SOLO texto literal del documento (sin inventar). Si un "
    "campo no aparece, devuelve null o vacío. NO escribir 'no disponible' "
    "o equivalentes."
)


def emit_es_qualitative_tasks(
    fund_dir: Path,
    isin: str,
) -> int:
    """Scan fund_dir and emit tasks to pending_extraction.json for every
    qualitative source available:

    1. cnmv_data.json with seccion_9_texto_completo per period → emit task
       with `extra.text` (no PDF path needed; the skill works on the text).
    2. raw/discovery/_web_*.pdf → emit one task per PDF (pdf_path set).
    3. raw/reports/*.pdf → emit one task per PDF (pdf_path set).

    Returns total tasks emitted. Idempotent (replaces existing task with
    same id).
    """
    n_emitted = 0

    # 1) Section 9 text from cnmv_data.json
    # cnmv_agent stores it in two possible places:
    #  (a) top-level: cnmv["seccion_9_texto_completo"]
    #  (b) flat dict: cnmv["cualitativo"]["seccion_9_texto_completo"]  ← REAL
    #  (c) per-period (legacy/hypothetical): cnmv["cualitativo"][year]["..."]
    cnmv_path = fund_dir / "cnmv_data.json"
    if cnmv_path.exists():
        try:
            cnmv = json.loads(cnmv_path.read_text(encoding="utf-8"))
            periodo = cnmv.get("_periodo_pdf", "") or ""
            cual = cnmv.get("cualitativo") or {}

            # (a) Top-level
            sec9_top = cnmv.get("seccion_9_texto_completo", "") or ""
            if sec9_top.strip():
                year_label = periodo or "current"
                append_extraction_task(
                    fund_dir, isin,
                    task_id=f"cnmv_cualitativo_{year_label}",
                    agent="cnmv_agent",
                    pdf_path="(text already segmented in extra.text)",
                    schema=ES_CUALITATIVO_SCHEMA,
                    context=ES_CUALITATIVO_CONTEXT + (
                        f" Periodo: {periodo}." if periodo else ""
                    ),
                    extra={"text": sec9_top[:7000]},
                    tipo="ES",
                )
                n_emitted += 1

            # (b) Flat under cualitativo (current cnmv_agent build)
            if isinstance(cual, dict):
                sec9_flat = cual.get("seccion_9_texto_completo") or ""
                if isinstance(sec9_flat, str) and sec9_flat.strip():
                    sec10 = cual.get("seccion_10_perspectivas_texto") or ""
                    extra = {"text": sec9_flat[:7000]}
                    if isinstance(sec10, str) and sec10.strip():
                        extra["text_perspectivas"] = sec10[:3000]
                    append_extraction_task(
                        fund_dir, isin,
                        task_id="cnmv_cualitativo_seccion9",
                        agent="cnmv_agent",
                        pdf_path="(text already segmented in extra.text)",
                        schema=ES_CUALITATIVO_SCHEMA,
                        context=(
                            f"{ES_CUALITATIVO_CONTEXT} Texto pre-segmentado de "
                            f"sección 9 (visión, decisiones, inversiones) y "
                            f"opcional sección 10 (perspectivas) del último "
                            f"informe semestral CNMV."
                        ),
                        extra=extra,
                        tipo="ES",
                    )
                    n_emitted += 1

            # (c) Per-period (defensive: only when value IS a dict, not a string)
            if isinstance(cual, dict):
                for year, payload in cual.items():
                    if not isinstance(payload, dict):
                        continue  # skip flat string fields like "seccion_9_texto_completo"
                    text = payload.get("seccion_9_texto_completo") or ""
                    if not text or not text.strip():
                        continue
                    append_extraction_task(
                        fund_dir, isin,
                        task_id=f"cnmv_cualitativo_hist_{year}",
                        agent="cnmv_agent",
                        pdf_path="(text already segmented in extra.text)",
                        schema=ES_CUALITATIVO_SCHEMA,
                        context=f"{ES_CUALITATIVO_CONTEXT} Periodo: {year}.",
                        extra={"text": text[:7000]},
                        tipo="ES",
                    )
                    n_emitted += 1
        except Exception:
            pass

    # 2) PDFs in raw/discovery/_web_*.pdf (gestora's own publications)
    discovery_dir = fund_dir / "raw" / "discovery"
    if discovery_dir.exists():
        for pdf in sorted(discovery_dir.glob("_web_*.pdf")):
            # Skip if PDF clearly not qualitative (e.g. KIID, factsheets)
            name_lower = pdf.name.lower()
            if any(skip in name_lower for skip in ("kiid", "_dfi", "factsheet", "ficha")):
                continue
            stem = pdf.stem.replace("_web_", "")[:40].replace(".", "_").replace(" ", "_")
            append_extraction_task(
                fund_dir, isin,
                task_id=f"web_qualitativo_{stem}",
                agent="cnmv_agent",
                pdf_path=str(pdf.relative_to(fund_dir.parent.parent.parent)).replace("\\", "/"),
                schema=ES_CUALITATIVO_SCHEMA,
                context=(
                    f"{ES_CUALITATIVO_CONTEXT} Documento de la gestora "
                    f"(no CNMV). Archivo: {pdf.name}."
                ),
                tipo="ES",
            )
            n_emitted += 1

    # 3) PDFs in raw/reports/*.pdf (CNMV semestrales propios, si los hay)
    reports_dir = fund_dir / "raw" / "reports"
    if reports_dir.exists():
        for pdf in sorted(reports_dir.glob("*.pdf")):
            stem = pdf.stem[:40].replace(".", "_").replace(" ", "_")
            append_extraction_task(
                fund_dir, isin,
                task_id=f"reports_qualitativo_{stem}",
                agent="cnmv_agent",
                pdf_path=str(pdf.relative_to(fund_dir.parent.parent.parent)).replace("\\", "/"),
                schema=ES_CUALITATIVO_SCHEMA,
                context=(
                    f"{ES_CUALITATIVO_CONTEXT} Informe semestral CNMV. "
                    f"Archivo: {pdf.name}."
                ),
                tipo="ES",
            )
            n_emitted += 1

    return n_emitted
