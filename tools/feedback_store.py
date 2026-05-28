"""Human-in-the-loop feedback storage (T3.1, 2026-05-28).

Persiste feedback del usuario en:
  - data/funds/<ISIN>/human_feedback.json  (local, append-only por fondo)
  - data/feedback_global.jsonl              (global, append-only por item)

SCHEMA — `human_feedback.json` (per fondo)
==========================================

{
  "isin": "IE00BDR0JY05",
  "fund_name_at_creation": "Ashoka WhiteOak India Opp",
  "feedbacks": [
    {
      "id": "fb_<ts>_<rand>",
      "created_at": "ISO8601 UTC",
      "raw_text": "<texto libre del usuario>",
      "raw_urls": ["<urls extra>"],
      "structured_items": [
        {
          "target_path": "nombre" | "kpis.aum_actual_meur" | ... | null,
          "target_section": "resumen" | "historia" | "gestores" | ... | null,
          "action": "set" | "add" | "replace" | "revisar" | "consultar_fuente",
          "value": "<valor sugerido si se conoce>" | null,
          "confidence": "high" | "medium" | "low",
          "source_urls": ["..."],
          "rationale": "<por qué este item>"
        }
      ],
      "estado": "pending" | "applied" | "partially_resolved" | "resolved",
      "applied_at": "ISO8601 | null",
      "resolved_items": [0, 2],   ← indices de structured_items
      "run_id_applied": "ISIN_yyyymmdd_hhmmss | null"
    }
  ]
}

SCHEMA — `feedback_global.jsonl` (append-only)
=============================================

Una línea por structured_item:
{"ts","feedback_id","isin","fund_name","gestora","isin_prefix","target_path",
 "target_section","action","value_summary","confidence","source_urls","resolved":bool}

Eventos especiales (deleted, item_resolved) en líneas separadas con campo "event".

API pública:
  - list_feedback(isin)
  - get_pending(isin)
  - append_feedback(isin, raw_text, raw_urls, structured_items, fund_name='')
  - delete_feedback(isin, feedback_id)  # solo pending
  - mark_applied(isin, feedback_id, run_id)
  - mark_items_resolved(isin, feedback_id, resolved_item_idxs)
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
GLOBAL_LOG = ROOT / "data" / "feedback_global.jsonl"


# ════════════════════════════════════════════════════════════════════
# Helpers internos
# ════════════════════════════════════════════════════════════════════


def _gen_feedback_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    rand = uuid.uuid4().hex[:4]
    return f"fb_{ts}_{rand}"


def _feedback_path(isin: str) -> Path:
    return FUNDS_DIR / isin / "human_feedback.json"


def _load(isin: str) -> dict:
    """Carga el archivo de feedback. Si no existe → schema vacío en memoria."""
    p = _feedback_path(isin)
    if not p.exists():
        return {"isin": isin, "feedbacks": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        # Defensive: garantizar estructura mínima
        if not isinstance(data, dict):
            return {"isin": isin, "feedbacks": []}
        data.setdefault("isin", isin)
        data.setdefault("feedbacks", [])
        return data
    except Exception:
        return {"isin": isin, "feedbacks": []}


def _save(isin: str, data: dict) -> None:
    p = _feedback_path(isin)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(p)


def _summarize_value(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v[:200]
    return str(v)[:200]


def _get_gestora_from_output(isin: str) -> str:
    try:
        op = FUNDS_DIR / isin / "output.json"
        if not op.exists():
            return ""
        d = json.loads(op.read_text(encoding="utf-8"))
        return (d.get("gestora") or "").strip()
    except Exception:
        return ""


def _append_global_line(entry: dict) -> None:
    GLOBAL_LOG.parent.mkdir(parents=True, exist_ok=True)
    with GLOBAL_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _append_global_for_feedback(isin: str, fund_name: str, fb: dict) -> None:
    """Append una línea por structured_item al log global."""
    isin_prefix = isin[:2].upper()
    ts = fb.get("created_at")
    gestora = _get_gestora_from_output(isin)
    for item in fb.get("structured_items", []):
        entry = {
            "ts": ts,
            "feedback_id": fb.get("id"),
            "isin": isin,
            "fund_name": fund_name,
            "gestora": gestora,
            "isin_prefix": isin_prefix,
            "target_path": item.get("target_path"),
            "target_section": item.get("target_section"),
            "action": item.get("action"),
            "value_summary": _summarize_value(item.get("value")),
            "confidence": item.get("confidence"),
            "source_urls": item.get("source_urls") or [],
            "resolved": False,
        }
        _append_global_line(entry)


# ════════════════════════════════════════════════════════════════════
# API pública — lectura
# ════════════════════════════════════════════════════════════════════


def list_feedback(isin: str) -> list[dict]:
    """Devuelve lista completa de feedbacks del fondo (vacía si no existe)."""
    return list(_load(isin).get("feedbacks", []))


def get_pending(isin: str) -> list[dict]:
    """Solo feedbacks pending (no applied/resolved)."""
    return [f for f in list_feedback(isin) if f.get("estado") == "pending"]


def get_feedback_by_id(isin: str, feedback_id: str) -> dict | None:
    for fb in list_feedback(isin):
        if fb.get("id") == feedback_id:
            return fb
    return None


# ════════════════════════════════════════════════════════════════════
# API pública — escritura
# ════════════════════════════════════════════════════════════════════


def append_feedback(
    isin: str,
    raw_text: str,
    raw_urls: Iterable[str] = (),
    structured_items: Iterable[dict] = (),
    fund_name: str = "",
) -> dict:
    """Guarda un nuevo feedback estructurado.

    Devuelve el dict del feedback creado (con `id`, `created_at`, `estado=pending`).
    También añade entradas al log global, una por structured_item.
    """
    if not isin or not isin.strip():
        raise ValueError("isin requerido")
    isin = isin.strip().upper()

    data = _load(isin)
    if not data.get("fund_name_at_creation") and fund_name:
        data["fund_name_at_creation"] = fund_name

    fb = {
        "id": _gen_feedback_id(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "raw_text": str(raw_text or "").strip(),
        "raw_urls": list(raw_urls or []),
        "structured_items": list(structured_items or []),
        "estado": "pending",
        "applied_at": None,
        "resolved_items": [],
        "run_id_applied": None,
    }
    data.setdefault("feedbacks", []).append(fb)
    _save(isin, data)

    # Log global (best-effort, no aborta si falla)
    try:
        _append_global_for_feedback(isin, fund_name, fb)
    except Exception:
        pass

    return fb


def delete_feedback(isin: str, feedback_id: str) -> bool:
    """Borra un feedback pending. Devuelve False si no existe o no es pending
    (los applied/resolved permanecen — son histórico)."""
    data = _load(isin)
    feedbacks = data.get("feedbacks") or []
    for i, fb in enumerate(feedbacks):
        if fb.get("id") != feedback_id:
            continue
        if fb.get("estado") != "pending":
            return False
        feedbacks.pop(i)
        _save(isin, data)
        try:
            _append_global_line({
                "ts": datetime.now(timezone.utc).isoformat(),
                "event": "deleted",
                "feedback_id": feedback_id,
                "isin": isin,
            })
        except Exception:
            pass
        return True
    return False


def mark_applied(isin: str, feedback_id: str, run_id: str) -> bool:
    """Marca un feedback como `applied` (orchestrator lo llama al arrancar
    el re-run que va a procesarlo). Devuelve False si no existe."""
    data = _load(isin)
    for fb in data.get("feedbacks", []):
        if fb.get("id") != feedback_id:
            continue
        fb["estado"] = "applied"
        fb["applied_at"] = datetime.now(timezone.utc).isoformat()
        fb["run_id_applied"] = run_id
        _save(isin, data)
        return True
    return False


def mark_items_resolved(
    isin: str, feedback_id: str, resolved_item_idxs: Iterable[int]
) -> dict | None:
    """Marca items concretos como resolved. Si TODOS los items quedan
    resolved → estado='resolved'. Si solo algunos → 'partially_resolved'.
    Devuelve el feedback actualizado o None si no existe.
    """
    data = _load(isin)
    new_idxs = set(int(i) for i in (resolved_item_idxs or []))
    for fb in data.get("feedbacks", []):
        if fb.get("id") != feedback_id:
            continue
        items = fb.get("structured_items") or []
        prev = set(fb.get("resolved_items") or [])
        merged = sorted(prev | new_idxs)
        fb["resolved_items"] = merged
        if items and len(merged) >= len(items):
            fb["estado"] = "resolved"
        elif merged:
            fb["estado"] = "partially_resolved"
        _save(isin, data)
        # Eventos globales: una línea por item nuevo resolved
        for idx in (new_idxs - prev):
            if 0 <= idx < len(items):
                item = items[idx]
                try:
                    _append_global_line({
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "event": "item_resolved",
                        "feedback_id": feedback_id,
                        "isin": isin,
                        "item_idx": idx,
                        "target_path": item.get("target_path"),
                        "target_section": item.get("target_section"),
                    })
                except Exception:
                    pass
        return fb
    return None


def set_item_results(
    isin: str, feedback_id: str, per_item_results: list[dict],
) -> bool:
    """T3.X (2026-05-28): persiste el resultado de APLICAR cada item del
    feedback. `per_item_results` es lista de
    {idx, applied: bool, reason: str, resolved: bool|None, verify_reason: str|None}.

    El widget del dashboard lee este campo para mostrar al usuario por qué
    un item no se aplicó (o no se resolvió tras aplicarlo).
    """
    data = _load(isin)
    for fb in data.get("feedbacks", []):
        if fb.get("id") != feedback_id:
            continue
        fb["item_results"] = list(per_item_results)
        _save(isin, data)
        return True
    return False


def update_verify_results(
    isin: str, feedback_id: str, verify_per_idx: dict,
) -> bool:
    """Actualiza el resultado del VERIFY (resolved + verify_reason) por idx
    sobre `item_results` previamente seteado. `verify_per_idx` es
    {idx: (resolved: bool, verify_reason: str)}.

    Si no existe item_results todavía, lo crea con apply=None.
    """
    data = _load(isin)
    for fb in data.get("feedbacks", []):
        if fb.get("id") != feedback_id:
            continue
        items_count = len(fb.get("structured_items") or [])
        existing = fb.get("item_results") or []
        # Map por idx
        by_idx = {r.get("idx"): r for r in existing if isinstance(r, dict)}
        for idx, (resolved, reason) in (verify_per_idx or {}).items():
            r = by_idx.get(idx) or {
                "idx": idx, "applied": None, "reason": "",
            }
            r["resolved"] = bool(resolved)
            r["verify_reason"] = str(reason or "")
            by_idx[idx] = r
        # Ordenar por idx y rellenar huecos
        merged: list[dict] = []
        for i in range(items_count):
            merged.append(by_idx.get(i) or {
                "idx": i, "applied": None, "reason": "",
                "resolved": None, "verify_reason": None,
            })
        fb["item_results"] = merged
        _save(isin, data)
        return True
    return False


__all__ = [
    "list_feedback", "get_pending", "get_feedback_by_id",
    "append_feedback", "delete_feedback",
    "mark_applied", "mark_items_resolved",
    "set_item_results", "update_verify_results",
]
