"""
aportados.py — Ingesta de material PROFESIONAL aportado por Rafa (docs de gestoras
gated, análisis externos) al pulsar "actualizar" en el portal. Se trata como fuente
PRIORITARIA (material curado > discovery automático) para mejorar el output final.

Flujo:
  Portal (al actualizar) → sube ficheros a Storage `uploads/{ISIN}/` y añade al item de la
  cola:  "docs_aportados": ["url1.pdf", ...],  "analisis_externos": [{url, nota}, ...]
  Worker → aportados.ingest(isin, docs_urls, analisis_externos):
     - descarga los PDFs a data/funds/{ISIN}/raw/aportados/
     - escribe manifiesto data/funds/{ISIN}/aportados.json
     - register_for_extraction(): añade los PDFs a pending_extraction.json (flag
       aportado=alta prioridad) para que la skill extract-pdfs los procese
     - los análisis externos se inyectan como readings prioritarios (readings_data.json)

El analyst da MÁS peso a lo aportado (contexto marca "APORTADO POR EL ASESOR — fuente
prioritaria y fiable"). No sustituye al pipeline: lo complementa.

CLI (test):
    python -m tools.aportados --isin LU1623762843 --doc file:///C:/ruta/informe.pdf
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _fund_dir(isin: str) -> Path:
    return ROOT / "data" / "funds" / isin.upper()


def _slug(name: str) -> str:
    name = (name or "doc.pdf").encode("ascii", "ignore").decode()
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return re.sub(r"_+", "_", name).strip("_") or "doc.pdf"


def _download(url: str, dest: Path) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fund-analyzer"})
        data = urllib.request.urlopen(req, timeout=90).read()
        if not data:
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return True
    except Exception:
        return False


def ingest(isin: str, docs_urls: list[str] | None = None,
           analisis_externos: list[dict] | None = None, log=print) -> dict:
    """Descarga los docs aportados, escribe el manifiesto y registra para extracción +
    readings. Devuelve resumen. Idempotente."""
    isin = isin.upper()
    fd = _fund_dir(isin)
    apo_dir = fd / "raw" / "aportados"
    docs_urls = docs_urls or []
    analisis_externos = analisis_externos or []

    manifest = {"isin": isin, "actualizado": datetime.now(timezone.utc).isoformat(),
                "docs": [], "analisis_externos": analisis_externos}
    for url in docs_urls:
        name = _slug(Path(url.split("?")[0]).name) or "aportado.pdf"
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
        dest = apo_dir / name
        if _download(url, dest):
            manifest["docs"].append({"nombre": name, "url": url,
                                     "local_path": str(dest), "bytes": dest.stat().st_size})
            log(f"[APORTADO] descargado {name} ({dest.stat().st_size} bytes)")
        else:
            log(f"[APORTADO] no se pudo descargar {url[:60]}")

    if not manifest["docs"] and not analisis_externos:
        return {"ok": True, "n_docs": 0, "n_externos": 0}

    (fd).mkdir(parents=True, exist_ok=True)
    (fd / "aportados.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    register_for_extraction(isin, manifest, log=log)
    inject_readings(isin, analisis_externos, log=log)
    return {"ok": True, "n_docs": len(manifest["docs"]), "n_externos": len(analisis_externos)}


def register_for_extraction(isin: str, manifest: dict, log=print) -> int:
    """Añade los PDFs aportados como tasks en pending_extraction.json, marcados como fuente
    prioritaria fiable (el extractor y el analyst les dan más peso)."""
    isin = isin.upper()
    pe_path = _fund_dir(isin) / "pending_extraction.json"
    pe = {}
    if pe_path.exists():
        try:
            pe = json.loads(pe_path.read_text(encoding="utf-8"))
        except Exception:
            pe = {}
    pe.setdefault("isin", isin)
    tasks = pe.setdefault("tasks", [])
    existing_paths = {t.get("pdf_path") for t in tasks if isinstance(t, dict)}
    n = 0
    for doc in manifest.get("docs", []):
        lp = doc["local_path"]
        if lp in existing_paths:
            continue
        tasks.append({
            "id": f"aportado_{_slug(doc['nombre'])}",
            "agent": "intl_extractor_v2",
            "pdf_path": lp,
            "schema": (
                "{'periodo': 'YYYY-MM — fecha de los DATOS del documento (no la de publicación), "
                "con mes: es un snapshot y debe SUMAR un punto a la evolución, no pisar el del AR', "
                "'texto_cualitativo': 'string — resumen del documento', "
                "'criterios_inversion': {'spread_objetivo': 'p.ej. +300 pb sobre tasa libre de riesgo', "
                "'calidad_crediticia_minima': 'p.ej. BBB- / solo investment grade', "
                "'tamano_minimo_emisor': 'p.ej. capitalización mínima $5bn', "
                "'otros_limites': 'list — duración, concentración, geografía, divisa…'}, "
                "'estructura_gestion': {'management_company': 'ManCo / plataforma legal (p.ej. Waystone/MontLake)', "
                "'investment_manager': 'gestor de inversión REAL que toma las decisiones (p.ej. Fortune)', "
                "'roles': 'quién hace qué: regulatorio/legal/administración vs gestión de cartera', "
                "'por_que': 'razón del modelo (plataforma UCITS para gestoras boutique, etc.)'}, "
                "'vision_gestores': {'decisiones_clave': 'list', 'cambios_cartera': 'list', "
                "'cambios_estrategia': 'list', 'outlook': 'string — visión a futuro'}, "
                "'sector_allocation': 'list [{sector, peso_pct}] del snapshot', "
                "'geographic_allocation': 'list [{region, peso_pct}] del snapshot', "
                "'asset_allocation': 'dict del snapshot', "
                "'sector_allocation_history': 'list [{periodo:YYYY-MM, sectores:{sector:peso}}] SOLO si el doc trae gráficos de EVOLUCIÓN por fechas', "
                "'geographic_allocation_history': 'list [{periodo:YYYY-MM, zonas:{region:peso}}] idem', "
                "'posiciones': 'list si es cartera/AR', 'datos_clave': 'dict'}"
            ),
            "context": (f"DOCUMENTO APORTADO POR EL ASESOR para el fondo {isin} — fuente "
                        "PRIORITARIA, curada y fiable (material profesional de la gestora o "
                        "análisis externo de calidad). Dale MÁS peso que a las fuentes "
                        "automáticas al sintetizar. CAPTURA LITERALMENTE los criterios de "
                        "inversión (spread objetivo, rating mínimo, tamaño mínimo de emisor, "
                        "límites), la estructura de gestión (ManCo/plataforma vs gestor real) y la "
                        "visión de los gestores (decisiones, cambios, outlook). Si hay gráficos de "
                        "evolución (sector/geografía/tipo de activo por fechas), extrae CADA fecha "
                        "como un punto de la serie — son muy valiosos para el análisis."),
            "aportado": True,
            "two_stage": True,
        })
        n += 1
    pe["updated_at"] = datetime.now(timezone.utc).isoformat()
    pe_path.write_text(json.dumps(pe, ensure_ascii=False, indent=2), encoding="utf-8")
    if n:
        log(f"[APORTADO] {n} docs añadidos a la cola de extracción (prioritarios)")
    return n


def register_from_folder(isin: str, log=print) -> int:
    """RECONCILE: escanea raw/aportados/*.pdf y registra en pending_extraction.json los que falten.
    Blindaje: garantiza que un doc aportado SIEMPRE entra a extracción, aunque el prep regenerara
    el manifiesto (pisando el task) o el análisis se lanzara sin pasar por ingest(). Idempotente."""
    isin = isin.upper()
    apo_dir = _fund_dir(isin) / "raw" / "aportados"
    if not apo_dir.exists():
        return 0
    docs = []
    for p in sorted(apo_dir.glob("*.pdf")):
        try:
            docs.append({"nombre": p.name, "local_path": str(p), "bytes": p.stat().st_size})
        except Exception:
            continue
    if not docs:
        return 0
    return register_for_extraction(isin, {"docs": docs}, log=log)


def inject_readings(isin: str, analisis_externos: list[dict], log=print) -> int:
    """Inyecta los análisis externos aportados como readings PRIORITARIOS."""
    if not analisis_externos:
        return 0
    isin = isin.upper()
    rp = _fund_dir(isin) / "readings_data.json"
    R = {}
    if rp.exists():
        try:
            R = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            R = {}
    lst = R.setdefault("analisis_completos", [])
    known = {(r.get("url") or "") for r in lst if isinstance(r, dict)}
    n = 0
    for a in analisis_externos:
        url = a.get("url") or ""
        if url and url in known:
            continue
        lst.append({
            "url": url, "fuente": a.get("fuente") or "Aportado por el asesor",
            "titulo": a.get("nota") or "Análisis externo aportado",
            "texto": a.get("texto") or a.get("nota") or "",
            "aportado": True, "prioridad": "alta",
        })
        n += 1
    R["updated_at"] = datetime.now(timezone.utc).isoformat()
    rp.write_text(json.dumps(R, ensure_ascii=False, indent=2), encoding="utf-8")
    if n:
        log(f"[APORTADO] {n} análisis externos inyectados como readings prioritarios")
    return n


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    ap.add_argument("--doc", action="append", default=[], help="URL/file:// de un PDF aportado")
    ap.add_argument("--externo", action="append", default=[], help="URL de un análisis externo")
    ap.add_argument("--reconcile", action="store_true",
                    help="registra en pending_extraction los PDFs de raw/aportados/ que falten (blindaje)")
    a = ap.parse_args()
    if a.reconcile:
        n = register_from_folder(a.isin)
        print(json.dumps({"ok": True, "reconciled": n}, ensure_ascii=False))
        return
    ext = [{"url": u} for u in a.externo]
    r = ingest(a.isin, docs_urls=a.doc, analisis_externos=ext)
    print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()
