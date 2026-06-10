"""doc_archive.py — Archivo DURABLE de los PDFs fuente (AR/SAR/...) que sourcing
descarga, para no re-buscarlos cada año y poder re-analizar con histórico+nuevo.

Diseño (Rafa, 2026-06-10):
- CONTENT-ADDRESSED: la clave es sha256 del fichero → `docs/{sha}.pdf`. El AR
  umbrella de Robeco (20MB compartido por 4 sub-fondos) se sube UNA sola vez.
- Backend ABSTRAÍDO (env DOC_ARCHIVE_BACKEND, default 'supabase') → migrable a
  Cloudflare R2 / S3 más adelante con coste algo mayor, sin tocar el resto.
- Empezamos en Supabase Storage (ya cableado, gratis 1GB). Cuando se llene, se
  migra a R2 moviendo los blobs + cambiando la env (las claves sha no cambian).
- Índice local `data/.doc_archive_index.json`: {f"{isin}/{filename}": sha} → en
  re-run sabemos el sha antes de descargar y recuperamos del archivo (sin fetch).

API:
  archive_file(local_path, isin, filename) -> sha|None   # sube (idempotente) + indexa
  retrieve(isin, filename, dest) -> bool                  # baja del archivo si está
  is_archived(isin, filename) -> bool
"""
from __future__ import annotations

import hashlib
import json
import os
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT / "data" / ".doc_archive_index.json"
BUCKET = os.environ.get("DOC_ARCHIVE_BUCKET", "funds-data")
PREFIX = "docs"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_index() -> dict:
    try:
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_index(idx: dict) -> None:
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Backend: Supabase Storage (REST) ─────────────────────────────────────────
def _backend() -> str:
    return os.environ.get("DOC_ARCHIVE_BACKEND", "supabase").lower()


def _supa_creds():
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not base or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY no en env")
    return base, key


def _supa_url(sha: str) -> str:
    base, _ = _supa_creds()
    return f"{base}/storage/v1/object/{BUCKET}/{PREFIX}/{sha}.pdf"


def _blob_exists(sha: str) -> bool:
    if _backend() != "supabase":
        raise NotImplementedError(f"backend {_backend()} no implementado aún")
    try:
        _, key = _supa_creds()
        req = urllib.request.Request(_supa_url(sha), method="HEAD",
                                     headers={"Authorization": f"Bearer {key}", "apikey": key})
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except Exception:
        return False


def _blob_upload(sha: str, data: bytes) -> bool:
    if _backend() != "supabase":
        raise NotImplementedError(f"backend {_backend()} no implementado aún")
    try:
        _, key = _supa_creds()
        req = urllib.request.Request(
            _supa_url(sha), data=data, method="PUT",
            headers={"Authorization": f"Bearer {key}", "apikey": key,
                     "Content-Type": "application/pdf", "x-upsert": "true"})
        urllib.request.urlopen(req, timeout=180)
        return True
    except Exception as e:
        print(f"[DOC_ARCHIVE] error subiendo {sha[:12]}: {e}")
        return False


def _blob_download(sha: str) -> bytes | None:
    if _backend() != "supabase":
        raise NotImplementedError(f"backend {_backend()} no implementado aún")
    try:
        _, key = _supa_creds()
        req = urllib.request.Request(_supa_url(sha), method="GET",
                                     headers={"Authorization": f"Bearer {key}", "apikey": key})
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.read()
    except Exception:
        return None


# ── API pública ───────────────────────────────────────────────────────────────
def archive_file(local_path, isin: str, filename: str = "") -> str | None:
    """Sube el fichero (idempotente, content-addressed) e indexa. Devuelve sha."""
    p = Path(local_path)
    if not p.exists() or p.stat().st_size < 1024:
        return None
    sha = _sha256(p)
    if not _blob_exists(sha):
        if not _blob_upload(sha, p.read_bytes()):
            return None
    idx = _load_index()
    idx[f"{isin}/{filename or p.name}"] = sha
    _save_index(idx)
    return sha


def is_archived(isin: str, filename: str) -> bool:
    sha = _load_index().get(f"{isin}/{filename}")
    return bool(sha) and _blob_exists(sha)


def retrieve(isin: str, filename: str, dest) -> bool:
    """Recupera del archivo a dest si está. True si lo escribió."""
    sha = _load_index().get(f"{isin}/{filename}")
    if not sha:
        return False
    data = _blob_download(sha)
    if not data or b"%%EOF" not in data[-8192:]:
        return False
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return True


def _backfill() -> int:
    """Archiva todos los AR/SAR ya descargados en raw/discovery (idempotente,
    dedup por content → los umbrella compartidos se suben 1 vez)."""
    funds = ROOT / "data" / "funds"
    n = up = 0
    for d in sorted(funds.iterdir()):
        if not d.is_dir() or "." in d.name:
            continue
        rd = d / "raw" / "discovery"
        if not rd.is_dir():
            continue
        for pdf in sorted(rd.glob("*.pdf")):
            nm = pdf.name.lower()
            if not (nm.startswith("annual_report_") or nm.startswith("semi_annual")):
                continue
            n += 1
            sha = archive_file(pdf, d.name, pdf.name)
            if sha:
                up += 1
                print(f"  {d.name}/{pdf.name} -> {sha[:12]}")
    print(f"\nBackfill: {up}/{n} AR/SAR archivados (dedup por sha).")
    return 0


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Archivo durable de docs fuente")
    ap.add_argument("--backfill", action="store_true", help="Archivar todos los AR/SAR de raw/discovery")
    args = ap.parse_args()
    if args.backfill:
        try:
            from dotenv import load_dotenv; load_dotenv()
        except Exception:
            pass
        raise SystemExit(_backfill())
    ap.print_help()
