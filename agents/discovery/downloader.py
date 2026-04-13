"""
Downloader: descarga + valida + indexa contenido.

Usado por ambos tracks. Deduplica via SharedState.attempted_downloads.
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx

from agents.discovery.state import DiscoveredDoc, SharedState
from agents.discovery.validator import (
    detect_language,
    guess_fecha_publicacion,
    validate_file,
)


def _safe_filename(url: str, doc_type: str, periodo: str) -> str:
    base = url.rsplit("/", 1)[-1].split("?", 1)[0]
    if not base or "." not in base:
        ext = "pdf"
        if url.lower().endswith(".html"):
            ext = "html"
        elif url.lower().endswith(".xml"):
            ext = "xml"
        base = f"{doc_type}_{periodo or 'latest'}.{ext}"
    safe = re.sub(r"[^\w\-_.]", "_", base)[:120]
    return safe


async def download_and_register(
    state: SharedState,
    c: httpx.AsyncClient,
    url: str,
    doc_type: str,
    periodo: str,
    source: str,
    source_detail: str = "",
) -> DiscoveredDoc | None:
    """
    Descarga el archivo, valida, indexa y lo registra en state.downloaded_docs.
    Devuelve el DiscoveredDoc si es válido, None si falla.
    """
    # Dedup
    if url in state.attempted_downloads:
        return state.already_downloaded(url)
    await state.mark_download_attempted(url)

    if not state.budget.try_http():
        return None

    target_dir = state.fund_dir / "raw" / "discovery"
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _safe_filename(url, doc_type, periodo)
    target = target_dir / filename

    try:
        r = await c.get(url, timeout=120, follow_redirects=True)
        if r.status_code != 200:
            return None
        ct = (r.headers.get("content-type") or "").lower()
        if not any(k in ct for k in ("pdf", "html", "xml", "octet-stream")):
            # probablemente HTML de error
            if not r.content.startswith(b"%PDF") and b"<html" not in r.content[:200].lower():
                return None
        target.write_bytes(r.content)
    except Exception:
        return None

    # Validar
    fund_name = state.identity.get("nombre_oficial", "")
    is_valid, contains = validate_file(target, state.isin, fund_name)
    if not is_valid:
        try:
            target.unlink()
        except Exception:
            pass
        return None

    # Metadata
    fecha = guess_fecha_publicacion(target)
    lang = detect_language(target)
    content_type = "html" if target.suffix.lower() in (".html", ".htm") else (
        "xml" if target.suffix.lower() == ".xml" else "pdf"
    )

    doc = DiscoveredDoc(
        doc_type=doc_type,
        periodo=periodo or fecha[:4],
        url=url,
        local_path=str(target),
        source=source,
        source_detail=source_detail,
        content_type=content_type,
        size_bytes=target.stat().st_size,
        fecha_publicacion=fecha,
        validated=True,
        contains=contains,
        lang=lang,
    )
    await state.add_doc(doc)
    return doc
