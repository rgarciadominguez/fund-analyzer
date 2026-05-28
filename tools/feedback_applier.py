"""Apply pending human feedback to output.json + supporting files (T3.6).

Llamado por `agents.orchestrator.consume_all_cowork_pipeline` cuando el bat
le pasa `--apply-feedback`. Lee feedbacks pending de
`data/funds/<ISIN>/human_feedback.json` y aplica las acciones estructuradas.

Acciones soportadas:
  - set          → output_data[path] = value + mark_manual_edit(path)
  - add          → output_data[path].append(value) si no existe
  - replace      → output_data[path] = value (lista completa)
  - consultar_fuente → URL → fuentes_adicionales (intl_discovery + analyst hints)
  - revisar      → output_data["_feedback_revisar"][target_section/path] +=
                   {raw_text, rationale, source_urls} (lo lee el analyst en T3.7)

Tras el run, `verify_resolved_after_run` compara valores antes/después por
item y marca items resolved en human_feedback.json.

Helper paths soportados:
  - top-level: "nombre", "gestora", "tipo"
  - kpis.*: "kpis.aum_actual_meur", "kpis.ter_pct", "kpis.num_participes", ...
  - listas: "gestores.equipo", "posiciones.actuales"
  - paths analyst se escriben como hints en _feedback_revisar
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

from tools import feedback_store as fs

LogFn = Callable[[str, str, str], None]  # (agent, level, msg)


# ════════════════════════════════════════════════════════════════════
# Path navigation
# ════════════════════════════════════════════════════════════════════


def _get_nested(data: dict, path: str):
    """Navega un path tipo 'kpis.aum_actual_meur'. Devuelve None si no existe."""
    if not path:
        return None
    parts = path.split(".")
    cur = data
    for p in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur


def _set_nested(data: dict, path: str, value) -> None:
    """Escribe un valor en un path nested, creando dicts intermedios."""
    parts = path.split(".")
    cur = data
    for p in parts[:-1]:
        if not isinstance(cur.get(p), dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


# ════════════════════════════════════════════════════════════════════
# Aplicación de items
# ════════════════════════════════════════════════════════════════════


def _apply_set(output_data: dict, path: str, value, log: Optional[LogFn]) -> bool:
    if not path or value is None:
        return False
    prev = _get_nested(output_data, path)
    if prev == value:
        return False
    _set_nested(output_data, path, value)
    # Marcar como manual edit
    try:
        from tools.output_merger import mark_manual_edit
        mark_manual_edit(output_data, path)
    except Exception:
        edits = output_data.setdefault("_manual_edits", [])
        if isinstance(edits, list) and path not in edits:
            edits.append(path)
    if log:
        log("FEEDBACK", "OK", f"set {path}: {prev!r} → {value!r}")
    return True


def _apply_add(output_data: dict, path: str, value, log: Optional[LogFn]) -> bool:
    """Añade `value` a lista en path. Crea la lista si no existe."""
    if not path or value is None:
        return False
    parts = path.split(".")
    cur = output_data
    for p in parts[:-1]:
        if not isinstance(cur.get(p), dict):
            cur[p] = {}
        cur = cur[p]
    last = parts[-1]
    if not isinstance(cur.get(last), list):
        cur[last] = []
    # Si value es lista, extender. Si no, append (si no duplica).
    target = cur[last]
    added = 0
    if isinstance(value, list):
        for v in value:
            if v not in target:
                target.append(v)
                added += 1
    else:
        if value not in target:
            target.append(value)
            added = 1
    if added:
        try:
            from tools.output_merger import mark_manual_edit
            mark_manual_edit(output_data, path)
        except Exception:
            edits = output_data.setdefault("_manual_edits", [])
            if isinstance(edits, list) and path not in edits:
                edits.append(path)
        if log:
            log("FEEDBACK", "OK", f"add {path}: +{added} elementos (valor={value!r})")
    return added > 0


def _apply_replace(output_data: dict, path: str, value, log: Optional[LogFn]) -> bool:
    """Reemplaza lista entera en path."""
    if not path or value is None:
        return False
    if not isinstance(value, list):
        value = [value]
    _set_nested(output_data, path, value)
    try:
        from tools.output_merger import mark_manual_edit
        mark_manual_edit(output_data, path)
    except Exception:
        edits = output_data.setdefault("_manual_edits", [])
        if isinstance(edits, list) and path not in edits:
            edits.append(path)
    if log:
        log("FEEDBACK", "OK", f"replace {path}: lista completa ({len(value)} items)")
    return True


def _apply_consultar_fuente(
    output_data: dict, source_urls: list[str], log: Optional[LogFn]
) -> bool:
    """Añade URLs a `fuentes_adicionales` (lista en output.json) para que
    discovery / analyst las consideren."""
    if not source_urls:
        return False
    bucket = output_data.setdefault("fuentes_adicionales", [])
    if not isinstance(bucket, list):
        bucket = []
        output_data["fuentes_adicionales"] = bucket
    added = 0
    for u in source_urls:
        if u and u not in bucket:
            bucket.append(u)
            added += 1
    if added:
        try:
            from tools.output_merger import mark_manual_edit
            mark_manual_edit(output_data, "fuentes_adicionales")
        except Exception:
            pass
        if log:
            log("FEEDBACK", "OK", f"consultar_fuente: +{added} URLs en fuentes_adicionales")
    return added > 0


def _apply_revisar(
    output_data: dict,
    item: dict,
    raw_text: str,
    log: Optional[LogFn],
) -> bool:
    """Anota el item revisar en `_feedback_revisar` para que analyst (T3.7) lea
    como instrucción prioritaria al regenerar la sección."""
    revisar_bucket = output_data.setdefault("_feedback_revisar", [])
    if not isinstance(revisar_bucket, list):
        revisar_bucket = []
        output_data["_feedback_revisar"] = revisar_bucket
    entry = {
        "target_path": item.get("target_path"),
        "target_section": item.get("target_section"),
        "rationale": item.get("rationale", ""),
        "source_urls": list(item.get("source_urls") or []),
        "raw_text_hint": raw_text[:500] if raw_text else "",
    }
    revisar_bucket.append(entry)
    if log:
        target = item.get("target_section") or item.get("target_path") or "(global)"
        log("FEEDBACK", "OK", f"revisar registrado para {target}: {item.get('rationale','')[:80]}")
    return True


# ════════════════════════════════════════════════════════════════════
# Public API
# ════════════════════════════════════════════════════════════════════


def apply_pending_feedback(
    isin: str,
    fund_dir: Path,
    run_id: str = "",
    log_fn: Optional[LogFn] = None,
) -> dict:
    """Aplica todos los feedbacks pending al output.json del fondo.

    Marca cada feedback aplicado con mark_applied(run_id).
    Devuelve resumen: {n_feedbacks, n_items_applied, snapshots_pre}.

    `snapshots_pre` mapea (feedback_id, item_idx) → valor previo del target.
    Se usa por verify_resolved_after_run para detectar qué se resolvió.
    """
    fund_dir = Path(fund_dir)
    output_path = fund_dir / "output.json"
    if not output_path.exists():
        if log_fn:
            log_fn("FEEDBACK", "WARN", f"no existe output.json para {isin}")
        return {"applied": False, "reason": "no_output", "n_items_applied": 0}

    pending = fs.get_pending(isin)
    if not pending:
        if log_fn:
            log_fn("FEEDBACK", "INFO", f"no hay feedback pending para {isin}")
        return {"applied": False, "reason": "no_pending", "n_items_applied": 0}

    output_data = json.loads(output_path.read_text(encoding="utf-8"))

    snapshots_pre: dict = {}  # (feedback_id, item_idx) → pre_value
    n_items_applied = 0
    n_items_failed = 0

    if log_fn:
        log_fn("FEEDBACK", "INFO",
               f"aplicando {len(pending)} feedback(s) pending a {isin}…")

    for fb in pending:
        fb_id = fb.get("id")
        raw_text = fb.get("raw_text") or ""
        items = fb.get("structured_items") or []
        for idx, item in enumerate(items):
            action = (item.get("action") or "").lower()
            path = item.get("target_path")
            value = item.get("value")
            # Snapshot del valor previo del target (para verify_resolved)
            if path:
                snapshots_pre[(fb_id, idx)] = _get_nested(output_data, path)
            else:
                snapshots_pre[(fb_id, idx)] = None
            # Aplicar
            success = False
            if action == "set":
                success = _apply_set(output_data, path, value, log_fn)
            elif action == "add":
                success = _apply_add(output_data, path, value, log_fn)
            elif action == "replace":
                success = _apply_replace(output_data, path, value, log_fn)
            elif action == "consultar_fuente":
                success = _apply_consultar_fuente(
                    output_data, item.get("source_urls") or [], log_fn,
                )
            elif action == "revisar":
                success = _apply_revisar(output_data, item, raw_text, log_fn)
            else:
                if log_fn:
                    log_fn("FEEDBACK", "WARN", f"action desconocida: {action!r}")
            if success:
                n_items_applied += 1
            else:
                n_items_failed += 1
        # Marcar feedback como applied
        try:
            fs.mark_applied(isin, fb_id, run_id)
        except Exception as e:
            if log_fn:
                log_fn("FEEDBACK", "WARN", f"mark_applied({fb_id}) falló: {e}")

    # Guardar output.json modificado (atómico)
    tmp = output_path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(output_path)

    if log_fn:
        log_fn("FEEDBACK", "OK",
               f"{n_items_applied} items aplicados, {n_items_failed} sin cambio "
               f"({len(pending)} feedback(s))")

    return {
        "applied": True,
        "n_feedbacks": len(pending),
        "n_items_applied": n_items_applied,
        "n_items_failed": n_items_failed,
        "snapshots_pre": snapshots_pre,
    }


def verify_resolved_after_run(
    isin: str,
    fund_dir: Path,
    snapshots_pre: dict,
    log_fn: Optional[LogFn] = None,
) -> dict:
    """Tras el re-run completo, compara valor de cada item con snapshot pre.
    Si el valor del target_path cambió respecto al pre → item resolved.

    Items sin target_path (revisar, consultar_fuente) se consideran resolved
    si tras el run el output cambió de forma medible (heurística simple: el
    `ultima_actualizacion` cambió, lo cual siempre debería ocurrir tras un
    re-run normal).
    """
    fund_dir = Path(fund_dir)
    output_path = fund_dir / "output.json"
    if not output_path.exists():
        return {"verified": False, "reason": "no_output"}
    output_data = json.loads(output_path.read_text(encoding="utf-8"))

    # Agrupar resoluciones por feedback_id
    resolved_per_fb: dict[str, list[int]] = {}
    for (fb_id, idx), pre_value in snapshots_pre.items():
        # Encontrar el item para saber su target
        fb = fs.get_feedback_by_id(isin, fb_id)
        if not fb:
            continue
        items = fb.get("structured_items") or []
        if not (0 <= idx < len(items)):
            continue
        item = items[idx]
        path = item.get("target_path")
        action = item.get("action")
        post_value = _get_nested(output_data, path) if path else None
        is_resolved = False
        if action in ("set", "add", "replace"):
            # Resolved si post != pre
            is_resolved = (post_value != pre_value)
        elif action == "consultar_fuente":
            # Resolved si la URL aparece en fuentes_adicionales
            urls = item.get("source_urls") or []
            bucket = output_data.get("fuentes_adicionales") or []
            is_resolved = any(u in bucket for u in urls)
        elif action == "revisar":
            # Resolved si el analyst regeneró la sección (target_section cambió
            # de longitud >5% o tiene contenido nuevo). Heurística: assume
            # resolved tras un re-run completo.
            target_section = item.get("target_section")
            if target_section:
                synth = output_data.get("analyst_synthesis") or {}
                sec_obj = synth.get(target_section) or {}
                texto = sec_obj.get("texto") or ""
                is_resolved = len(texto) > 100  # tiene contenido
            else:
                is_resolved = True  # global revisar, assume processed
        if is_resolved:
            resolved_per_fb.setdefault(fb_id, []).append(idx)

    # Aplicar mark_items_resolved + T3.12 auto-apply URLs útiles a registry
    total_resolved = 0
    useful_urls_for_registry: list[str] = []
    for fb_id, idxs in resolved_per_fb.items():
        try:
            fs.mark_items_resolved(isin, fb_id, idxs)
            total_resolved += len(idxs)
        except Exception as e:
            if log_fn:
                log_fn("FEEDBACK", "WARN", f"mark_items_resolved({fb_id}) falló: {e}")
        # T3.12: recopilar URLs útiles (de items resolved con source_urls)
        fb = fs.get_feedback_by_id(isin, fb_id)
        if fb:
            for idx in idxs:
                items = fb.get("structured_items") or []
                if 0 <= idx < len(items):
                    urls = items[idx].get("source_urls") or []
                    useful_urls_for_registry.extend(urls)

    # T3.12 (2026-05-28): tras resolver feedback con URLs útiles, auto-añadir
    # los hosts a gestoras_registry[gestora].html_fallback_useful_domains
    # para que futuros runs de la MISMA gestora prioricen esos dominios.
    if useful_urls_for_registry:
        try:
            from urllib.parse import urlparse
            useful_hosts = list({
                urlparse(u).netloc.lower()
                for u in useful_urls_for_registry
                if u
            })
            useful_hosts = [h for h in useful_hosts if h]
            # Necesitamos saber la gestora
            gestora = (output_data.get("gestora") or "").strip()
            if useful_hosts and gestora and gestora.upper() != isin.upper():
                from agents.discovery_v2 import persist_html_fallback_to_registry
                persist_html_fallback_to_registry(
                    isin=isin, gestora=gestora, useful_domains=useful_hosts,
                )
                if log_fn:
                    log_fn("FEEDBACK", "OK",
                           f"T3.12: +{len(useful_hosts)} hosts útiles a registry para «{gestora}»: "
                           f"{', '.join(useful_hosts[:3])}{'…' if len(useful_hosts) > 3 else ''}")
        except Exception as e:
            if log_fn:
                log_fn("FEEDBACK", "WARN", f"T3.12 persist_to_registry falló: {e}")

    if log_fn:
        log_fn("FEEDBACK", "INFO",
               f"verify: {total_resolved} item(s) marcados resolved")
    return {
        "verified": True,
        "n_resolved": total_resolved,
        "feedbacks_touched": list(resolved_per_fb.keys()),
        "useful_hosts_registered": len(useful_urls_for_registry),
    }


__all__ = ["apply_pending_feedback", "verify_resolved_after_run"]
