"""Publica en output.json lo que los DOCS APORTADOS traen y que no es una serie numérica:

  1. `graficos_documento` — las páginas con gráficos de valor (sobre todo de EVOLUCIÓN: yield y
     duración históricos, IG vs no-IG, estructura de deuda, AUM y flujos, atribución por año…).
     Una presentación profesional las trae ya hechas por el gestor; re-dibujarlas exigiría leer
     valores de barras sin cifras (frágil e inventable). Se INCRUSTAN tal cual: el extractor
     (que ya ve el PDF) elige las páginas y escribe su lectura; aquí se renderizan a imagen en
     `dashboard/doc-charts/{ISIN}/` (el Worker sirve ./dashboard como estáticos) y se referencian.
  2. `clases_documento` — la tabla completa de clases del documento (código, ISIN, divisa,
     cubierta, reparto, comisión de gestión y de ÉXITO, mínimo, activa).

Idempotente y determinista (sin LLM). Lo llama el consume final antes de generar el dashboard.
CLI: python -m tools.aportado_publish --isin IE000Z9YV312
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHARTS_DIR = ROOT / "dashboard" / "doc-charts"
MAX_PAGES_PER_DOC = 16
RESOLUTION = 110          # dpi: legible a ancho completo, ~120-200 KB por página en JPEG
SECCIONES = {"cartera", "rentabilidad", "riesgo", "patrimonio", "estrategia"}


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "doc"


def _pdf_for(isin: str, extract: dict) -> Path | None:
    """PDF del extract. `pdf_path` puede venir absoluto de OTRA máquina (Surface vs servidor):
    se resuelve por nombre dentro de raw/aportados del fondo."""
    name = Path(str(extract.get("pdf_path") or "").replace("\\", "/")).name
    p = ROOT / "data" / "funds" / isin / "raw" / "aportados" / name
    return p if name and p.exists() else None


def _render(pdf: Path, pages: list[int], out_dir: Path, prefix: str, log) -> dict[int, str]:
    import pdfplumber
    out_dir.mkdir(parents=True, exist_ok=True)
    done: dict[int, str] = {}
    with pdfplumber.open(str(pdf)) as doc:
        n = len(doc.pages)
        for pg in pages:
            if not (1 <= pg <= n):
                log(f"[APORTADO-PUB] página {pg} fuera de rango (1-{n}) → ignorada")
                continue
            dest = out_dir / f"{prefix}-p{pg:02d}.jpg"
            if not dest.exists():
                img = doc.pages[pg - 1].to_image(resolution=RESOLUTION).original.convert("RGB")
                img.save(str(dest), "JPEG", quality=82, optimize=True)
            done[pg] = dest.name
    return done


def apply(isin: str, log=print) -> dict:
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    op = fd / "output.json"
    if not op.exists():
        return {"changed": False}
    from tools.aportados import current_extracts
    graficos: list[dict] = []
    clases: list[dict] = []
    keep: set[str] = set()
    for ep in current_extracts(isin):
        try:
            ex = json.loads(ep.read_text(encoding="utf-8"))
        except Exception:
            continue
        data = ex.get("data") or {}
        if not isinstance(data, dict):
            continue
        doc_name = Path(str(ex.get("pdf_path") or ep.name).replace("\\", "/")).name
        # ── clases ──
        for c in data.get("clases_documento") or []:
            if isinstance(c, dict) and (c.get("isin") or c.get("codigo")):
                row = dict(c)
                row["isin"] = (row.get("isin") or "").upper().strip() or None
                row["fuente"] = doc_name
                clases.append(row)
        # ── gráficos ──
        gl = [g for g in (data.get("graficos_documento") or [])
              if isinstance(g, dict) and str(g.get("pagina") or "").isdigit()]
        if not gl:
            continue
        pdf = _pdf_for(isin, ex)
        if not pdf:
            log(f"[APORTADO-PUB] PDF no encontrado para {doc_name} → gráficos sin publicar")
            continue
        # los de evolución primero; tope de páginas por doc
        gl.sort(key=lambda g: (0 if g.get("tipo") == "evolucion" else 1, int(g["pagina"])))
        gl = gl[:MAX_PAGES_PER_DOC]
        prefix = _slug(Path(doc_name).stem)
        rendered = _render(pdf, sorted({int(g["pagina"]) for g in gl}), CHARTS_DIR / isin, prefix, log)
        for g in gl:
            fn = rendered.get(int(g["pagina"]))
            if not fn:
                continue
            keep.add(fn)
            sec = (g.get("seccion") or "").lower()
            graficos.append({
                "img": f"doc-charts/{isin}/{fn}",
                "pagina": int(g["pagina"]),
                "titulo": g.get("titulo") or "",
                "tipo": g.get("tipo") or "",
                "seccion": sec if sec in SECCIONES else "cartera",
                "que_muestra": g.get("que_muestra") or "",
                "lectura": g.get("lectura") or "",
                "documento": doc_name,
                "periodo": data.get("periodo") or "",
            })
    # imágenes huérfanas (doc retirado / páginas que ya no se eligen)
    d = CHARTS_DIR / isin
    if graficos and d.exists():       # sin gráficos en esta pasada no se toca nada (vacío no borra)
        for f in d.glob("*.jpg"):
            if f.name not in keep:
                try:
                    f.unlink()
                except Exception:
                    pass
    out = json.loads(op.read_text(encoding="utf-8"))
    before = (out.get("graficos_documento"), out.get("clases_documento"))
    # Vacío NO borra: si esta pasada no trae nada (extract aún sin re-hacer), se conserva lo previo.
    if graficos:
        out["graficos_documento"] = sorted(graficos, key=lambda g: (g["seccion"], g["pagina"]))
    if clases:
        out["clases_documento"] = clases
    changed = before != (out.get("graficos_documento"), out.get("clases_documento"))
    if changed:
        op.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[APORTADO-PUB] {len(graficos)} gráficos del documento + {len(clases)} clases publicados")
    return {"changed": changed, "graficos": len(graficos), "clases": len(clases)}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    print(apply(ap.parse_args().isin))
