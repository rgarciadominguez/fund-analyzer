"""novedades.py — Búsqueda de novedades de un fondo en su fecha de seguimiento (Rafa 30-sep-2026).

Rafa: "cuando toque revisión anual debe buscar todo desde el último año con info hasta ahora (SR, AR, cartas,
análisis externos, entrevistas, noticias…) y con eso me avisa de que hay nuevos docs para revisarlos a mano y
se abre la opción de actualizar el análisis con esa información nueva".

  buscar(isin, motivo)  — lanza la skill novedades-cowork (Opus) → data/funds/{ISIN}/novedades/novedades.json
                          y lo envía al portal (POST admin/fondos/seguimiento): pantalla "Seguimiento de
                          fondos", columna "Nuevos docs" + aviso. Si no hay nada nuevo, también lo dice.
  integrar(isin)        — al lanzar el update anual desde el portal: los docs encontrados entran como material
                          aportado (tools.aportados: PDFs a extracción prioritaria, URLs como lecturas), así el
                          update no vuelve a buscar lo que ya está encontrado.

Lo dispara tools.seguimiento_fondos.disparar() cuando llega la fecha de la agenda (solo Top/Bueno), en
segundo plano y solo con la cola de análisis parada.
CLI:  buscar ISIN [motivo] · integrar ISIN · ver ISIN
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODEL = os.environ.get("MODEL_SOURCING", "claude-opus-5")


def _log(m: str) -> None:
    print(f"[NOVEDADES] {m}", flush=True)


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def _desde(isin: str) -> str:
    fd = ROOT / "data" / "funds" / isin
    out = _load(fd / "output.json", {}) or {}
    cand = [str(out.get("ultima_actualizacion") or "")[:10]]
    prev = _load(fd / "novedades" / "novedades.json", {}) or {}
    if prev.get("buscado"):
        cand.append(str(prev["buscado"])[:10])
    cand = [c for c in cand if c]
    return min(cand) if cand else "2000-01-01"


def buscar(isin: str, motivo: str = "update_anual") -> dict:
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    out = _load(fd / "output.json", {}) or {}
    nd = fd / "novedades"
    nd.mkdir(parents=True, exist_ok=True)
    (nd / "_encargo.json").write_text(json.dumps({"isin": isin, "nombre": out.get("nombre"), "gestora": out.get("gestora"),
                                                  "desde": _desde(isin), "motivo": motivo}, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    logf = ROOT / "logs" / f"skill_novedades_{isin}.log"
    antes = (nd / "novedades.json").stat().st_mtime if (nd / "novedades.json").exists() else 0
    rc = subprocess.call([sys.executable, "-m", "tools.claude_cowork", str(logf), f"novedades cowork {isin}",
                          "--model", MODEL, "--allowedTools", "Read,Write,Bash,Edit,WebSearch,WebFetch,Glob,Grep"],
                         cwd=str(ROOT))
    man = _load(nd / "novedades.json", {}) or {}
    nuevo = (nd / "novedades.json").exists() and (nd / "novedades.json").stat().st_mtime > antes
    if not nuevo:
        _log(f"{isin}: la búsqueda no dejó manifiesto (rc={rc}); ver {logf.name}")
        return {"ok": False, "rc": rc}
    avisar_portal(isin, man, motivo)
    return {"ok": True, "n": len(man.get("docs") or []), "manifiesto": man}


def avisar_portal(isin: str, man: dict, motivo: str) -> None:
    try:
        from tools.revision import _api
        out = _load(ROOT / "data" / "funds" / isin / "output.json", {}) or {}
        r = _api("POST", "admin/fondos/seguimiento", {
            "isin": isin, "nombre": out.get("nombre") or isin, "motivo": motivo,
            "buscado": man.get("buscado") or datetime.now().isoformat(timespec="minutes"),
            "resumen": man.get("resumen_rafa") or "",
            "docs": [{k: d.get(k) for k in ("tipo", "titulo", "fecha", "url", "resumen")} for d in man.get("docs") or []],
            "no_encontrado": man.get("no_encontrado") or []})
        _log(f"{isin}: enviado al portal ({len(man.get('docs') or [])} docs nuevos) → {str(r)[:100]}")
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude avisar al portal: {str(e)[:120]}")


def integrar(isin: str) -> int:
    """Los docs de novedades entran al update anual como material aportado (fuente prioritaria)."""
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    man = _load(fd / "novedades" / "novedades.json", {}) or {}
    docs = man.get("docs") or []
    if not docs or man.get("_integrado"):
        return 0
    pdfs, externos = [], []
    for d in docs:
        a = d.get("archivo")
        if a and (fd / a).exists():
            pdfs.append((fd / a).resolve().as_uri())
        elif d.get("url"):
            externos.append({"url": d["url"], "nota": f"{d.get('tipo')}: {d.get('titulo')} ({d.get('fecha')}). {d.get('resumen') or ''}"})
    previo = _load(fd / "aportados.json", {}) or {}
    try:
        from tools.aportados import ingest
        ingest(isin, pdfs, externos)
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude integrar las novedades: {str(e)[:120]}")
        return 0
    # ingest reescribe el manifiesto: se conservan los aportados anteriores de Rafa
    nuevo = _load(fd / "aportados.json", {}) or {}
    if previo.get("docs") or previo.get("analisis_externos"):
        vistos = {d.get("nombre") for d in nuevo.get("docs") or []}
        nuevo["docs"] = [d for d in previo.get("docs") or [] if d.get("nombre") not in vistos] + (nuevo.get("docs") or [])
        urls = {x.get("url") for x in nuevo.get("analisis_externos") or []}
        nuevo["analisis_externos"] = [x for x in previo.get("analisis_externos") or [] if x.get("url") not in urls] + \
                                     (nuevo.get("analisis_externos") or [])
        (fd / "aportados.json").write_text(json.dumps(nuevo, ensure_ascii=False, indent=2), encoding="utf-8")
    man["_integrado"] = datetime.now().isoformat(timespec="seconds")
    (fd / "novedades" / "novedades.json").write_text(json.dumps(man, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"{isin}: {len(pdfs)} PDFs y {len(externos)} enlaces de novedades integrados en el update")
    return len(pdfs) + len(externos)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(2)
    acc, isin = sys.argv[1], sys.argv[2].upper()
    if acc == "buscar":
        r = buscar(isin, sys.argv[3] if len(sys.argv) > 3 else "update_anual")
        sys.exit(0 if r.get("ok") else 1)
    if acc == "integrar":
        print(integrar(isin)); sys.exit(0)
    if acc == "ver":
        print(json.dumps(_load(ROOT / "data" / "funds" / isin / "novedades" / "novedades.json", {}), ensure_ascii=False, indent=2)); sys.exit(0)
    print(__doc__); sys.exit(2)
