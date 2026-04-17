"""
Ingesta manual de PDFs recibidos por email/otros canales.

Uso conversacional:
  Usuario: "tengo docs nuevos para LU1694789378"
  Asistente: copia los PDFs a raw/manual/, llama a ingest_files()
             y obtiene el dict con lo que se añadió / rechazó.

No tiene CLI — se importa y se llama.
"""
from __future__ import annotations

import json
import re
import shutil
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from rich.console import Console

from agents.discovery import kb as kb_mod
from agents.discovery.gestora_crawler import (
    classify_link, detect_factsheet_month, factsheet_subtype,
)
from agents.discovery.state import DiscoveredDoc
from agents.discovery.validator import (
    _extract_text_for_validation, classify_content, validate_file,
    detect_isins_in_doc, detect_language, guess_fecha_publicacion,
)

console = Console()


def ingest_files(
    isin: str,
    file_paths: list[str | Path],
    rerun_analyst: bool = False,
    trust_isin: bool = True,
) -> dict:
    """
    Valida + clasifica + registra una lista de PDFs para un fondo.

    Returns:
      {
        "added":    [ {doc_type, periodo, local_path, ...}, ... ],
        "rejected": [ {file, reason}, ... ],
        "total_docs_now": N,
      }
    """
    root = Path(__file__).parent.parent
    fund_dir = root / "data" / "funds" / isin.upper()
    if not fund_dir.exists():
        raise ValueError(f"Fund dir inexistente: {fund_dir}")

    manual_dir = fund_dir / "raw" / "manual"
    processed_dir = manual_dir / "processed"
    manual_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Cargar discovery JSON actual
    disc_path = fund_dir / "intl_discovery_data.json"
    if disc_path.exists():
        disc = json.loads(disc_path.read_text(encoding="utf-8"))
    else:
        disc = {"isin": isin.upper(), "documents": [], "still_missing": []}

    # Cargar identity para validación
    nombre = ""
    sicav = ""
    ident_path = fund_dir / "regulator_data.json"
    if ident_path.exists():
        try:
            idt = json.loads(ident_path.read_text(encoding="utf-8")).get("identity", {})
            nombre = idt.get("nombre_oficial", "")
            sicav = idt.get("sicav_paraguas", "")
        except Exception:
            pass

    kb = kb_mod.load_kb(fund_dir, isin)

    added: list[dict] = []
    rejected: list[dict] = []

    # Copiar los inputs a manual_dir (si no están ya ahí)
    staged_files: list[Path] = []
    for fp in file_paths:
        src = Path(fp)
        if not src.exists():
            rejected.append({"file": str(src), "reason": "file_not_found"})
            continue
        dst = manual_dir / src.name
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
        staged_files.append(dst)

    for path in staged_files:
        try:
            # 1. Validar. Si trust_isin=True (default para manual), solo exigimos
            # que el PDF sea parseable — el usuario garantiza que es del fondo.
            is_valid, contains = validate_file(path, isin, fund_name=nombre, sicav_paraguas=sicav)
            if not is_valid:
                if not trust_isin:
                    rejected.append({"file": path.name, "reason": "isin_not_found_or_unparseable"})
                    continue
                # trust: comprobar al menos que tiene texto extraíble
                try:
                    text = _extract_text_for_validation(path)
                    if not text.strip():
                        rejected.append({"file": path.name, "reason": "unparseable_empty_text"})
                        continue
                    # Re-derivar contains del texto ya extraído
                    from agents.discovery.validator import CONTENT_PATTERNS
                    contains = {k for pat, k in CONTENT_PATTERNS if pat.search(text)}
                except Exception as exc:
                    rejected.append({"file": path.name, "reason": f"parse_error:{exc}"})
                    continue

            # 2. Clasificar doc_type (URL→content→filename)
            actual_type = classify_link(text="", href=path.name) or ""
            if not actual_type and contains:
                text = _extract_text_for_validation(path)
                _, dominant = classify_content(text)
                actual_type = dominant or ""
            if not actual_type:
                actual_type = "unknown_pdf"

            # 3. Fecha / periodo
            fecha = guess_fecha_publicacion(path)
            lang = detect_language(path)
            month = ""
            fiscal_year = ""
            subtype = ""
            if actual_type == "factsheet":
                fiscal_year, month = detect_factsheet_month(path.name)
                subtype = factsheet_subtype(month)
            # Periodo: año del filename / fecha_pub
            periodo = ""
            m = re.search(r"(20\d{2})", path.name)
            if m:
                periodo = m.group(1)
            elif fecha:
                periodo = fecha[:4]
            if subtype in ("eoy", "mid_year") and fiscal_year and month:
                periodo = f"{fiscal_year}-{month}"

            isins_inside = detect_isins_in_doc(path)
            is_umbrella = (
                actual_type in ("annual_report", "semi_annual_report")
                and len(isins_inside) >= 3
            )

            # Dedup: si ya hay doc con mismo type+periodo validado, skip.
            # Para latest-only (kid/prospectus): reemplazar si es más reciente.
            LATEST_ONLY = {"kid", "prospectus"}
            if actual_type in LATEST_ONLY:
                existing_idx = next(
                    (i for i, d in enumerate(disc.get("documents", []))
                     if d["doc_type"] == actual_type), None,
                )
                if existing_idx is not None:
                    old = disc["documents"][existing_idx]
                    new_key = (periodo or "") + (fecha or "")
                    old_key = (old.get("periodo") or "") + (old.get("fecha_publicacion") or "")
                    if new_key <= old_key:
                        rejected.append({"file": path.name,
                                         "reason": f"already_have_newer {actual_type}@{old.get('periodo')}"})
                        continue
                    # El nuevo es más reciente: se sustituirá al añadir
                    disc["documents"].pop(existing_idx)
            else:
                dup = any(
                    d["doc_type"] == actual_type and d["periodo"] == periodo
                    for d in disc.get("documents", [])
                )
                if dup:
                    rejected.append({"file": path.name, "reason": f"already_have {actual_type}@{periodo}"})
                    continue

            doc = DiscoveredDoc(
                doc_type=actual_type,
                periodo=periodo,
                url=f"manual://{path.name}",
                local_path=str(path.resolve()),
                source="manual",
                source_detail="user-provided",
                content_type="pdf",
                size_bytes=path.stat().st_size,
                fecha_publicacion=fecha or "",
                validated=True,
                contains=contains or set(),
                lang=lang or "",
                isins_inside=isins_inside or set(),
                is_umbrella=is_umbrella,
                subtype=subtype,
            )

            # Serializar para JSON
            dd = asdict(doc)
            dd["contains"] = sorted(list(doc.contains))
            dd["isins_inside"] = sorted(list(doc.isins_inside))
            disc.setdefault("documents", []).append(dd)

            kb_mod.remember(kb, doc.doc_type, doc.periodo, doc.url)
            added.append(dd)

            # Mover a processed/ ANTES del log (por si el log falla en Windows)
            final = processed_dir / path.name
            if path.resolve() != final.resolve():
                shutil.move(str(path), str(final))

            try:
                console.log(f"[bold green]manual ingest[/bold green] {actual_type}@{periodo} <- {path.name}")
            except Exception:
                pass

        except Exception as exc:
            rejected.append({"file": path.name, "reason": f"exception:{exc}"})

    # Recalcular still_missing basándonos en lo que hay ahora (reusa regulator gap si existe)
    # (simple: si teníamos un missing que ahora cubrimos, quitarlo)
    new_missing = []
    for m in disc.get("still_missing", []):
        if not any(d["doc_type"] == m["doc_type"] and m["periodo"] in d["periodo"]
                   for d in disc["documents"]):
            new_missing.append(m)
    disc["still_missing"] = new_missing
    disc["ultima_actualizacion"] = datetime.now().isoformat(timespec="seconds")

    disc_path.write_text(json.dumps(disc, ensure_ascii=False, indent=2), encoding="utf-8")
    kb_mod.save_kb(fund_dir, kb)

    result = {
        "added": added,
        "rejected": rejected,
        "total_docs_now": len(disc["documents"]),
        "still_missing_now": len(disc["still_missing"]),
    }

    if rerun_analyst and added:
        console.log("[yellow]rerun_analyst=True → ejecuta agents/analyst_agent.py manualmente[/yellow]")

    return result
