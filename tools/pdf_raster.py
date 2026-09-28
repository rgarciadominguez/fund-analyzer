"""Rasteriza páginas de un PDF a PNG con el primer motor disponible, sin depender de DLL del sistema.

Uso:  python -m tools.pdf_raster "<pdf>" <primera_pagina> <ultima_pagina> [dir_salida] [dpi]
      (páginas 0-based, última inclusive). Imprime una ruta PNG por línea. Código de salida 3 si
      ningún motor funciona (entonces el extractor sigue por texto y lo anota).

Por qué (28-sep-2026): en el servidor PyMuPDF no carga (faltan las DLL de Visual C++) y no hay
poppler, y la skill extract-pdfs se paró a preguntar qué instalar en vez de extraer. pypdfium2
lleva su propio pdfium y funciona sin nada del sistema; PyMuPDF se usa si está sano.
"""
from __future__ import annotations

import sys
from pathlib import Path


def render(pdf: str, first: int, last: int, out_dir: str | None = None, dpi: int = 200) -> list[str]:
    pdf_path = Path(pdf)
    out = Path(out_dir) if out_dir else pdf_path.parent / "_raster"
    out.mkdir(parents=True, exist_ok=True)
    stem = pdf_path.stem[:40]
    pages = range(int(first), int(last) + 1)
    errors = []
    # 1) PyMuPDF si carga
    try:
        import fitz  # type: ignore
        doc = fitz.open(str(pdf_path))
        outs = []
        for p in pages:
            if p >= len(doc):
                break
            dest = out / f"{stem}_p{p}.png"
            doc[p].get_pixmap(dpi=dpi).save(str(dest))
            outs.append(str(dest))
        if outs:
            return outs
    except Exception as e:  # noqa: BLE001
        errors.append(f"fitz: {str(e)[:80]}")
    # 2) pypdfium2 (binario propio, sin DLL del sistema)
    try:
        import pypdfium2 as pdfium  # type: ignore
        doc = pdfium.PdfDocument(str(pdf_path))
        outs = []
        for p in pages:
            if p >= len(doc):
                break
            dest = out / f"{stem}_p{p}.png"
            doc[p].render(scale=dpi / 72).to_pil().save(str(dest))
            outs.append(str(dest))
        if outs:
            return outs
    except Exception as e:  # noqa: BLE001
        errors.append(f"pypdfium2: {str(e)[:80]}")
    raise RuntimeError("sin motor de rasterizado: " + " | ".join(errors))


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 4:
        print(__doc__)
        raise SystemExit(2)
    try:
        for path in render(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]),
                           sys.argv[4] if len(sys.argv) > 4 else None,
                           int(sys.argv[5]) if len(sys.argv) > 5 else 200):
            print(path)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR {e}")
        raise SystemExit(3)
