"""bundle_exporter — Export the deterministic prep outputs as a self-contained bundle.

Implements BUNDLE_CONTRACT.md v1.0.0. The bundle is the stable boundary between
the prep (CNMV/INT pipeline) and any analyst consumer (legacy Python via API,
the analyst-cowork skill via Claude Max, or future consumers).

Layout:
    data/funds/{ISIN}/bundle/
        fund_data.json        (cnmv_data.json or intl_data.json, copied as-is)
        manager_profile.json  (copied as-is, with fallback to backup_pre_deep)
        letters_data.json     (copied as-is)
        readings.json         (synthesized from readings_data.json + legacy files)
        sources.json          (synthesized from fund_data.fuentes + manager + letters + ...)
        bundle_manifest.json  (sha256 + stats + schema_version)

Idempotent: re-running with the same inputs produces a bundle with identical
hashes (modulo timestamps in manifest).

CLI:
    python -m agents.bundle_exporter ES0112231008
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = "1.0.0"
EXPORTER_VERSION = "1.0.0"

# Files that must end up in bundle/
BUNDLE_FILES = [
    "fund_data.json",
    "manager_profile.json",
    "letters_data.json",
    "readings.json",
    "sources.json",
]


class BundleExportError(Exception):
    """Raised when the bundle cannot be assembled (missing required upstream)."""


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _sha256(path: Path) -> str:
    """Return sha256:<hex> of file contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _read_json(path: Path) -> dict | list | None:
    """Read JSON or return None if missing / unparseable."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data: Any) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False),
        encoding="utf-8",
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -----------------------------------------------------------------------------
# Synthesis: readings.json
# -----------------------------------------------------------------------------

def _normalize_reading(item: dict, source_label: str = "") -> dict | None:
    """Map a reading item from any legacy/current schema into the contract shape.

    Required: titulo, fuente, url. Returns None if any of those is missing.
    """
    if not isinstance(item, dict):
        return None
    titulo = (item.get("titulo") or item.get("title") or "").strip()
    url = (item.get("url") or item.get("link") or "").strip()
    if not titulo or not url:
        return None
    fuente = (
        item.get("fuente") or item.get("source") or source_label or "unknown"
    )
    out = {
        "titulo": titulo,
        "fuente": fuente,
        "url": url,
        "fecha": item.get("fecha") or item.get("date"),
        "tipo": item.get("tipo") or item.get("type") or item.get("source_type"),
        "resumen": item.get("resumen") or item.get("summary") or "",
        "puntos_clave": item.get("puntos_clave") or item.get("bullets") or [],
        "citas_textuales": item.get("citas_textuales") or item.get("citas") or item.get("quotes") or [],
    }
    if "palabras_estimadas" in item:
        out["palabras_estimadas"] = item["palabras_estimadas"]
    # Optional pass-through of richer fields for downstream consumers
    for opt in ("opinion_sobre_fondo", "datos_mencionados", "autor", "quality"):
        if item.get(opt):
            out[opt] = item[opt]
    return out


def _synthesize_readings(fund_dir: Path, isin: str) -> dict:
    """Build readings.json from readings_data.json + legacy lecturas/analisis_externos.

    Dedup by URL (case-insensitive, query-string preserved).
    """
    seen_urls: set[str] = set()
    readings: list[dict] = []

    # 1. Canonical (Fase I/L): readings_data.json with analisis_completos / otros_readings
    rd = _read_json(fund_dir / "readings_data.json")
    if isinstance(rd, dict):
        for key in ("analisis_completos", "otros_readings", "analisis", "lecturas"):
            for it in (rd.get(key) or []):
                norm = _normalize_reading(it)
                if norm and norm["url"].lower() not in seen_urls:
                    seen_urls.add(norm["url"].lower())
                    readings.append(norm)

    # 2. Legacy ES files (predate readings_data.json unification)
    for fname in ("analisis_externos.json", "lecturas.json"):
        legacy = _read_json(fund_dir / fname)
        if isinstance(legacy, list):
            items = legacy
        elif isinstance(legacy, dict):
            items = (legacy.get("analisis") or legacy.get("lecturas")
                     or legacy.get("items") or [])
        else:
            items = []
        for it in items:
            norm = _normalize_reading(it, source_label="legacy")
            if norm and norm["url"].lower() not in seen_urls:
                seen_urls.add(norm["url"].lower())
                readings.append(norm)

    return {
        "isin": isin,
        "generado": _now_iso(),
        "readings": readings,
    }


# -----------------------------------------------------------------------------
# Synthesis: sources.json
# -----------------------------------------------------------------------------

def _classify_source_type(url: str, hint: str = "") -> str:
    """Best-effort classification for the `tipo` field of sources.documentos."""
    u = (url or "").lower()
    h = (hint or "").lower()
    if "kiid" in h or "kid" in h or "/kid/" in u:
        return "KIID"
    if "prospect" in h or "folleto" in h or "prospect" in u:
        return "Prospectus"
    if "annual" in h or "annualreport" in h or "/annual" in u:
        return "Annual Report"
    if "semestral" in h or "semiannual" in h:
        return "Semi-Annual Report"
    if "factsheet" in h or "fact-sheet" in u or "factsheet" in u:
        return "Factsheet"
    if "carta" in h or "letter" in h or "/cartas/" in u:
        return "Carta trimestral"
    if "anexo" in h or "anexos.cnmv" in u or "cnmv.es" in u:
        return "Anexo CNMV"
    if "wayback" in u:
        return "Archive"
    return "Document"


def _classify_origin(url: str) -> str:
    """Classify the origin domain into one of the canonical buckets."""
    u = (url or "").lower()
    if "cnmv.es" in u:
        return "CNMV"
    if "cssf.lu" in u or "funds.cssf" in u:
        return "CSSF"
    if "centralbank.ie" in u:
        return "CBI"
    if "amf-france.org" in u:
        return "AMF"
    if "bundesanzeiger.de" in u:
        return "Bundesanzeiger"
    if "web.archive.org" in u or "wayback" in u:
        return "Wayback"
    if "morningstar" in u:
        return "Morningstar"
    if "citywire" in u:
        return "Citywire"
    if "fundssociety" in u or "funds-society" in u:
        return "Funds Society"
    return "Gestora"


def _synthesize_sources(fund_dir: Path, isin: str,
                        fund_data: dict, manager_profile: dict,
                        letters_data: dict, readings: dict) -> dict:
    """Build sources.json catalog from all upstream files."""
    seen_urls: set[str] = set()
    documentos: list[dict] = []

    def _add_doc(url: str, titulo: str = "", fecha: str | None = None,
                 tipo_hint: str = "", origen: str | None = None) -> None:
        if not url or not isinstance(url, str):
            return
        url = url.strip()
        if not url or url.lower() in seen_urls:
            return
        seen_urls.add(url.lower())
        documentos.append({
            "tipo": _classify_source_type(url, tipo_hint),
            "titulo": titulo or "(untitled)",
            "url": url,
            "fecha": fecha,
            "fuente_origen": origen or _classify_origin(url),
        })

    # 1. From fund_data.fuentes
    fuentes = (fund_data or {}).get("fuentes") or {}
    for it in (fuentes.get("informes_descargados") or []):
        if isinstance(it, dict):
            _add_doc(it.get("url") or "", it.get("titulo") or it.get("nombre") or "",
                     it.get("fecha"), tipo_hint=it.get("tipo") or "")
        elif isinstance(it, str):
            _add_doc(it)
    for it in (fuentes.get("cartas_gestores") or []):
        if isinstance(it, dict):
            _add_doc(it.get("url") or "", it.get("titulo") or "",
                     it.get("fecha"), tipo_hint="carta")
        elif isinstance(it, str):
            _add_doc(it, tipo_hint="carta")
    for it in (fuentes.get("xmls_cnmv") or []):
        url = it if isinstance(it, str) else (it.get("url") if isinstance(it, dict) else "")
        _add_doc(url, "XML CNMV", tipo_hint="anexo")

    # 2. From letters_data.cartas
    for c in (letters_data or {}).get("cartas", []) or []:
        if isinstance(c, dict):
            _add_doc(c.get("url_fuente") or c.get("url") or "",
                     c.get("titulo") or "",
                     c.get("fecha_inferida") or c.get("fecha"),
                     tipo_hint=c.get("tipo") or "carta")

    # 3. From readings.readings (already normalized)
    for r in (readings or {}).get("readings", []) or []:
        _add_doc(r.get("url") or "", r.get("titulo") or "",
                 r.get("fecha"), tipo_hint=r.get("tipo") or "")

    # 4. From manager_profile.articulos_completos[*][*]
    arts = (manager_profile or {}).get("articulos_completos") or {}
    if isinstance(arts, dict):
        for gestor, items in arts.items():
            for it in (items or []):
                if isinstance(it, dict):
                    _add_doc(it.get("fuente_url") or it.get("url") or "",
                             it.get("titulo") or "",
                             it.get("fecha"), tipo_hint="article")

    # 5. From intl_discovery_data.documents (INT only)
    disc = _read_json(fund_dir / "intl_discovery_data.json")
    if isinstance(disc, dict):
        for d in disc.get("documents") or []:
            if isinstance(d, dict):
                _add_doc(d.get("url") or "", d.get("title") or d.get("titulo") or "",
                         d.get("fecha") or d.get("date"),
                         tipo_hint=d.get("type") or d.get("tipo") or "")

    # 6. From gestora_resources.recursos (M3 v2)
    gres = _read_json(fund_dir / "gestora_resources.json")
    if isinstance(gres, dict):
        for r in gres.get("recursos") or []:
            if isinstance(r, dict):
                _add_doc(r.get("url") or "", r.get("titulo") or r.get("nombre") or "",
                         r.get("fecha"), tipo_hint=r.get("tipo") or "")

    # fuentes_consultadas: aggregate URLs we tried (success or not)
    consultadas: list[dict] = []
    consultadas_seen: set[str] = set()

    def _add_consulta(url: str, tipo: str, exito: bool) -> None:
        if not url:
            return
        key = (url.lower(), tipo, exito)
        if key in consultadas_seen:
            return
        consultadas_seen.add(key)
        consultadas.append({"url": url, "tipo": tipo, "exito": exito})

    for url in (fuentes.get("urls_consultadas") or []):
        _add_consulta(url, "web", True)
    for f in (manager_profile or {}).get("fuentes_consultadas") or []:
        if isinstance(f, dict):
            _add_consulta(f.get("url") or "", f.get("tipo") or "web",
                          bool(f.get("exito", True)))
        elif isinstance(f, str):
            _add_consulta(f, "web", True)
    for f in (letters_data or {}).get("fuentes_consultadas") or []:
        if isinstance(f, dict):
            _add_consulta(f.get("url") or "", f.get("tipo") or "web",
                          bool(f.get("exito", True)))
        elif isinstance(f, str):
            _add_consulta(f, "web", True)

    return {
        "isin": isin,
        "generado": _now_iso(),
        "documentos": documentos,
        "fuentes_consultadas": consultadas,
    }


# -----------------------------------------------------------------------------
# Stats for the manifest
# -----------------------------------------------------------------------------

def _kpis_completeness_pct(fund_data: dict) -> float:
    """Percentage of KPIs that are not None / not empty."""
    kpis = (fund_data or {}).get("kpis") or {}
    if not kpis:
        return 0.0
    total = len(kpis)
    filled = sum(1 for v in kpis.values()
                 if v is not None and v != "" and v != [])
    return round(filled / total * 100, 1) if total else 0.0


def _build_stats(fund_data: dict, manager_profile: dict,
                 letters_data: dict, readings: dict) -> dict:
    return {
        "num_letters": len((letters_data or {}).get("cartas") or []),
        "num_readings": len((readings or {}).get("readings") or []),
        "num_managers_identified": len(
            (manager_profile or {}).get("equipo_gestor")
            or (manager_profile or {}).get("equipo")
            or []
        ),
        "num_positions": len(
            ((fund_data or {}).get("posiciones") or {}).get("actuales") or []
        ),
        "kpis_completeness_pct": _kpis_completeness_pct(fund_data),
    }


# -----------------------------------------------------------------------------
# Manifest builder
# -----------------------------------------------------------------------------

def _build_manifest(bundle_dir: Path, isin: str, fund_data: dict,
                    source_paths: dict[str, str | list[str]],
                    stats: dict) -> dict:
    files_meta: dict[str, dict] = {}
    for fname in BUNDLE_FILES:
        fpath = bundle_dir / fname
        files_meta[fname] = {
            "size_bytes": fpath.stat().st_size,
            "sha256": _sha256(fpath),
        }
        sp = source_paths.get(fname)
        if isinstance(sp, list):
            files_meta[fname]["source_paths"] = sp
        elif sp:
            files_meta[fname]["source_path"] = sp

    return {
        "schema_version": SCHEMA_VERSION,
        "isin": isin,
        "fund_name": (fund_data or {}).get("nombre")
                      or (fund_data or {}).get("nombre_oficial") or "",
        "tipo": (fund_data or {}).get("tipo") or "",
        "generated_at": _now_iso(),
        "exporter_version": EXPORTER_VERSION,
        "files": files_meta,
        "stats": stats,
    }


# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------

def run(isin: str) -> dict:
    """Assemble the bundle for `isin` and return the manifest dict.

    Raises BundleExportError if mandatory upstream files are missing.
    """
    fund_dir = ROOT / "data" / "funds" / isin
    if not fund_dir.exists():
        raise BundleExportError(f"fund directory does not exist: {fund_dir}")

    bundle_dir = fund_dir / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    source_paths: dict[str, str | list[str]] = {}

    # 1. fund_data.json — copy cnmv_data or intl_data
    cnmv = fund_dir / "cnmv_data.json"
    intl = fund_dir / "intl_data.json"
    if cnmv.exists():
        shutil.copy2(cnmv, bundle_dir / "fund_data.json")
        source_paths["fund_data.json"] = str(cnmv.relative_to(ROOT)).replace("\\", "/")
    elif intl.exists():
        shutil.copy2(intl, bundle_dir / "fund_data.json")
        source_paths["fund_data.json"] = str(intl.relative_to(ROOT)).replace("\\", "/")
    else:
        raise BundleExportError(
            f"neither cnmv_data.json nor intl_data.json found in {fund_dir}"
        )

    # 2. manager_profile.json — copy with backup fallback.
    # P2 (2026-05-19): si no existe ni el main ni el backup, escribir uno
    # mínimo vacío en lugar de abortar todo el pipeline. Casos típicos:
    # manager_profiler crasheó silenciosamente (timeout web, exception async)
    # o fondo INT sin gestores en ninguna fuente.
    mp_main = fund_dir / "manager_profile.json"
    mp_backup = fund_dir / "manager_profile.backup_pre_deep.json"
    if mp_main.exists():
        shutil.copy2(mp_main, bundle_dir / "manager_profile.json")
        source_paths["manager_profile.json"] = str(mp_main.relative_to(ROOT)).replace("\\", "/")
    elif mp_backup.exists():
        shutil.copy2(mp_backup, bundle_dir / "manager_profile.json")
        source_paths["manager_profile.json"] = str(mp_backup.relative_to(ROOT)).replace("\\", "/")
    else:
        # Crear el archivo mínimo y escribirlo en disco + bundle
        from datetime import datetime as _dt
        fallback = {
            "isin": isin,
            "fund_name": "",
            "gestora": "",
            "generated": _dt.now().isoformat(),
            "equipo": [],
            "equipo_gestor": [],
            "equipo_roles": {},
            "fuentes_web": [],
            "_error": "manager_profile.json missing — created by bundle_exporter fallback",
        }
        try:
            mp_main.write_text(
                json.dumps(fallback, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            shutil.copy2(mp_main, bundle_dir / "manager_profile.json")
            source_paths["manager_profile.json"] = "bundle_exporter_fallback"
        except Exception as exc:
            raise BundleExportError(
                f"manager_profile.json missing AND fallback write failed in {fund_dir}: {exc}"
            )

    # 3. letters_data.json — copy
    letters_src = fund_dir / "letters_data.json"
    if not letters_src.exists():
        raise BundleExportError(f"letters_data.json not found in {fund_dir}")
    shutil.copy2(letters_src, bundle_dir / "letters_data.json")
    source_paths["letters_data.json"] = str(letters_src.relative_to(ROOT)).replace("\\", "/")

    # 4. readings.json — synthesize
    readings = _synthesize_readings(fund_dir, isin)
    _write_json(bundle_dir / "readings.json", readings)
    sp_list: list[str] = []
    for cand in ("readings_data.json", "analisis_externos.json", "lecturas.json"):
        if (fund_dir / cand).exists():
            sp_list.append(str((fund_dir / cand).relative_to(ROOT)).replace("\\", "/"))
    source_paths["readings.json"] = sp_list or "synthesized"

    # 5. sources.json — synthesize
    fund_data = _read_json(bundle_dir / "fund_data.json") or {}
    manager_profile = _read_json(bundle_dir / "manager_profile.json") or {}
    letters_data = _read_json(bundle_dir / "letters_data.json") or {}
    sources = _synthesize_sources(fund_dir, isin, fund_data, manager_profile,
                                   letters_data, readings)
    _write_json(bundle_dir / "sources.json", sources)
    source_paths["sources.json"] = "synthesized"

    # 5b. T3.7 (2026-05-28): human_feedback.json — copia los items APPLIED
    # (con sus rationales) para que la skill analyst-cowork los lea y los
    # respete como instrucción prioritaria al regenerar las secciones.
    hf_src = fund_dir / "human_feedback.json"
    if hf_src.exists():
        try:
            hf_data = _read_json(hf_src) or {}
            # Filtrar a items relevantes para el analyst:
            # - feedbacks applied (no resolved aún, no pending)
            # - items con target_section o action=revisar (afectan al narrative)
            relevant_items: list[dict] = []
            for fb in hf_data.get("feedbacks", []):
                if fb.get("estado") not in ("applied", "partially_resolved"):
                    continue
                for idx, item in enumerate(fb.get("structured_items", [])):
                    # Skip items ya resolved
                    if idx in (fb.get("resolved_items") or []):
                        continue
                    # Solo interesa lo que afecta secciones narrativas o es revisar
                    if item.get("target_section") or item.get("action") == "revisar":
                        relevant_items.append({
                            "feedback_id": fb.get("id"),
                            "item_idx": idx,
                            "raw_text_hint": (fb.get("raw_text") or "")[:300],
                            **item,
                        })
            bundle_hf = {
                "isin": isin,
                "n_relevant_items": len(relevant_items),
                "items": relevant_items,
            }
            _write_json(bundle_dir / "human_feedback.json", bundle_hf)
            source_paths["human_feedback.json"] = str(hf_src.relative_to(ROOT)).replace("\\", "/")
        except Exception:
            # Best-effort: si falla, no abortar el bundle
            pass

    # 6. manifest with hashes + stats
    stats = _build_stats(fund_data, manager_profile, letters_data, readings)
    manifest = _build_manifest(bundle_dir, isin, fund_data, source_paths, stats)
    _write_json(bundle_dir / "bundle_manifest.json", manifest)

    return manifest


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def _print_summary(manifest: dict) -> None:
    print(f"\nBundle ready for {manifest['isin']} ({manifest.get('fund_name', '')[:60]})")
    print(f"  schema_version: {manifest['schema_version']}")
    print(f"  tipo: {manifest['tipo']}")
    print(f"  generated_at: {manifest['generated_at']}")
    print(f"\n  Files:")
    total = 0
    for fname, meta in manifest["files"].items():
        kb = meta["size_bytes"] / 1024
        total += meta["size_bytes"]
        print(f"    {fname:<25} {kb:>8.1f} KB   {meta['sha256'][:23]}...")
    print(f"  Total: {total/1024:.1f} KB")
    print(f"\n  Stats:")
    for k, v in (manifest.get("stats") or {}).items():
        print(f"    {k}: {v}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a fund bundle (BUNDLE_CONTRACT v1.0.0)")
    parser.add_argument("isin", help="ISIN of the fund (must already have prep outputs)")
    args = parser.parse_args()
    try:
        manifest = run(args.isin)
        _print_summary(manifest)
        return 0
    except BundleExportError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"UNEXPECTED ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    # Force UTF-8 on Windows consoles (CLAUDE.md gotcha)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(main())
