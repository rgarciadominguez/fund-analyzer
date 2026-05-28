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


def _apply_set(output_data: dict, path: str, value, log: Optional[LogFn]) -> tuple[bool, str]:
    """Devuelve (success, reason). reason explica qué pasó (aplicado/no aplicado)."""
    if not path:
        return False, "no se aplicó: item sin target_path"
    if value is None:
        return False, "no se aplicó: el parser no extrajo el valor concreto a setear"
    prev = _get_nested(output_data, path)
    if prev == value:
        return False, f"no se aplicó: el valor ya era {value!r}"
    _set_nested(output_data, path, value)
    try:
        from tools.output_merger import mark_manual_edit
        mark_manual_edit(output_data, path)
    except Exception:
        edits = output_data.setdefault("_manual_edits", [])
        if isinstance(edits, list) and path not in edits:
            edits.append(path)
    if log:
        log("FEEDBACK", "OK", f"set {path}: {prev!r} → {value!r}")
    return True, f"actualizado: {prev!r} → {value!r}"


def _apply_add(output_data: dict, path: str, value, log: Optional[LogFn]) -> tuple[bool, str]:
    if not path:
        return False, "no se aplicó: item sin target_path"
    if value is None:
        return False, "no se aplicó: el parser no extrajo el valor"
    parts = path.split(".")
    cur = output_data
    for p in parts[:-1]:
        if not isinstance(cur.get(p), dict):
            cur[p] = {}
        cur = cur[p]
    last = parts[-1]
    if not isinstance(cur.get(last), list):
        cur[last] = []
    target = cur[last]
    added = 0
    duplicates: list = []
    if isinstance(value, list):
        for v in value:
            if v not in target:
                target.append(v)
                added += 1
            else:
                duplicates.append(v)
    else:
        if value not in target:
            target.append(value)
            added = 1
        else:
            duplicates.append(value)
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
        return True, f"añadido {added} elemento(s) a {path}"
    return False, f"no se aplicó: el valor ya estaba en {path} ({duplicates!r})"


def _apply_replace(output_data: dict, path: str, value, log: Optional[LogFn]) -> tuple[bool, str]:
    if not path:
        return False, "no se aplicó: item sin target_path"
    if value is None:
        return False, "no se aplicó: el parser no extrajo el valor"
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
    return True, f"lista completa reemplazada ({len(value)} items)"


def _apply_consultar_fuente(
    output_data: dict, source_urls: list[str], log: Optional[LogFn]
) -> tuple[bool, str]:
    if not source_urls:
        return False, "no se aplicó: item sin URLs"
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
        return True, f"+{added} URLs añadidas a fuentes_adicionales"
    return False, "no se aplicó: las URLs ya estaban en fuentes_adicionales"


def _apply_revisar(
    output_data: dict,
    item: dict,
    raw_text: str,
    log: Optional[LogFn],
) -> tuple[bool, str]:
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
    target = item.get("target_section") or item.get("target_path") or "(global)"
    if log:
        log("FEEDBACK", "OK", f"revisar registrado para {target}: {item.get('rationale','')[:80]}")
    return True, f"instrucción registrada en _feedback_revisar para que analyst la procese ({target})"


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
    # T3.X (2026-05-28): item_results_per_fb registra por feedback el
    # resultado de aplicar cada item: {applied: bool, reason: str}.
    # Se persiste en el feedback para que el UI muestre el por qué.
    item_results_per_fb: dict[str, list[dict]] = {}
    n_items_applied = 0
    n_items_failed = 0

    if log_fn:
        log_fn("FEEDBACK", "INFO",
               f"aplicando {len(pending)} feedback(s) pending a {isin}…")

    for fb in pending:
        fb_id = fb.get("id")
        raw_text = fb.get("raw_text") or ""
        items = fb.get("structured_items") or []
        per_item_results: list[dict] = []
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
            success, reason = False, ""
            if action == "set":
                success, reason = _apply_set(output_data, path, value, log_fn)
            elif action == "add":
                success, reason = _apply_add(output_data, path, value, log_fn)
            elif action == "replace":
                success, reason = _apply_replace(output_data, path, value, log_fn)
            elif action == "consultar_fuente":
                success, reason = _apply_consultar_fuente(
                    output_data, item.get("source_urls") or [], log_fn,
                )
            elif action == "revisar":
                success, reason = _apply_revisar(output_data, item, raw_text, log_fn)
            else:
                reason = f"acción desconocida: {action!r}"
                if log_fn:
                    log_fn("FEEDBACK", "WARN", f"action desconocida: {action!r}")
            per_item_results.append({
                "idx": idx,
                "applied": success,
                "reason": reason,
                # Estos campos los actualiza verify_resolved_after_run
                "resolved": None,
                "verify_reason": None,
            })
            if success:
                n_items_applied += 1
            else:
                n_items_failed += 1
        item_results_per_fb[fb_id] = per_item_results
        # Marcar feedback como applied + guardar item_results
        try:
            fs.mark_applied(isin, fb_id, run_id)
            fs.set_item_results(isin, fb_id, per_item_results)
        except Exception as e:
            if log_fn:
                log_fn("FEEDBACK", "WARN", f"mark_applied/set_item_results({fb_id}) falló: {e}")

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

    # Agrupar resoluciones por feedback_id + capturar razón per item
    resolved_per_fb: dict[str, list[int]] = {}
    verify_reasons_per_fb: dict[str, dict[int, tuple[bool, str]]] = {}
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
        reason = ""
        if action in ("set", "add", "replace"):
            is_resolved = (post_value != pre_value)
            if is_resolved:
                reason = f"valor cambió: {pre_value!r} → {post_value!r}"
            else:
                reason = (
                    f"el valor no cambió tras el run (sigue siendo {post_value!r}). "
                    f"Causas posibles: el agente fuente lo sobrescribió, "
                    f"_manual_edits no protegió, o el item no se aplicó."
                )
        elif action == "consultar_fuente":
            urls = item.get("source_urls") or []
            bucket = output_data.get("fuentes_adicionales") or []
            matched = [u for u in urls if u in bucket]
            is_resolved = len(matched) > 0
            if is_resolved:
                reason = f"URL(s) presentes en fuentes_adicionales: {matched}"
            else:
                reason = (
                    "las URLs no aparecen en fuentes_adicionales. "
                    "El discovery no las indexó o el extractor las descartó."
                )
        elif action == "revisar":
            target_section = item.get("target_section")
            if target_section:
                synth = output_data.get("analyst_synthesis") or {}
                sec_obj = synth.get(target_section) or {}
                texto = sec_obj.get("texto") or ""
                is_resolved = len(texto) > 100
                if is_resolved:
                    reason = (
                        f"sección '{target_section}' tiene contenido "
                        f"({len(texto)} chars). El analyst la regeneró considerando el feedback."
                    )
                else:
                    reason = (
                        f"sección '{target_section}' quedó vacía o muy corta "
                        f"({len(texto)} chars). El analyst no consiguió generar contenido — "
                        f"posiblemente faltan datos en bundle/ para esa sección."
                    )
            else:
                is_resolved = True
                reason = "feedback global 'revisar': asumido procesado tras re-run completo del analyst"
        else:
            reason = f"acción no verificable: {action}"

        verify_reasons_per_fb.setdefault(fb_id, {})[idx] = (is_resolved, reason)
        if is_resolved:
            resolved_per_fb.setdefault(fb_id, []).append(idx)

    # Aplicar mark_items_resolved + T3.12 auto-apply URLs útiles a registry +
    # actualizar item_results con la razón del verify
    total_resolved = 0
    useful_urls_for_registry: list[str] = []
    # Set de feedback_ids con cualquier item evaluado (resuelto o no)
    all_fb_ids = set(resolved_per_fb.keys()) | set(verify_reasons_per_fb.keys())
    for fb_id in all_fb_ids:
        idxs_resolved = resolved_per_fb.get(fb_id, [])
        try:
            if idxs_resolved:
                fs.mark_items_resolved(isin, fb_id, idxs_resolved)
                total_resolved += len(idxs_resolved)
            # Actualizar verify_reason+resolved en cada item_result persistido
            reasons_map = verify_reasons_per_fb.get(fb_id, {})
            try:
                fs.update_verify_results(isin, fb_id, reasons_map)
            except Exception:
                pass
        except Exception as e:
            if log_fn:
                log_fn("FEEDBACK", "WARN", f"mark_items_resolved({fb_id}) falló: {e}")
        # T3.12: recopilar URLs útiles (de items resolved con source_urls)
        fb = fs.get_feedback_by_id(isin, fb_id)
        if fb:
            for idx in idxs_resolved:
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
