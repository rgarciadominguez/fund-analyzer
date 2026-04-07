"""
Letters Deep Extraction Agent — Second-pass enrichment of letters_data.json

Runs AFTER letters_agent. Takes the cartas already found and performs a deeper
extraction pass using Claude API to pull out structured investment data:
returns, exposures, entries/exits, macro views, key quotes, executive summaries.

For each carta extracts:
  - Rentabilidad del periodo y benchmark
  - Exposicion renta variable inicio/fin
  - Numero de posiciones
  - Entradas y salidas con justificacion
  - Vision macro (max 5 puntos concretos)
  - Tesis de inversion destacadas
  - Citas textuales del gestor
  - Resumen ejecutivo (4-5 lineas)

Input:
  - data/funds/{ISIN}/letters_data.json  (cartas[] with basic fields)
  - Raw PDFs in data/funds/{ISIN}/raw/letters/
  - HTML letters via url_fuente

Output:
  - Enriched data/funds/{ISIN}/letters_data.json  (deep fields merged per carta)
  - data/funds/{ISIN}/letters_deep_log.json       (extraction stats)
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.claude_extractor import extract_structured_data
from tools.http_client import get_with_headers
from tools.pdf_extractor import extract_page_range, get_pdf_metadata

console = Console()

# ── Constants ────────────────────────────────────────────────────────────────

# Max characters of source text sent to Claude per carta (cost control)
MAX_TEXT_CHARS = 5000

# Concurrency limit for Claude API calls
CLAUDE_SEM_SIZE = 2

# Fields whose presence signals that deep extraction was already done.
# If ANY of these is non-null and non-empty the carta is skipped.
DEEP_MARKER_FIELDS = ("vision_macro", "resumen_ejecutivo", "entradas", "salidas")

# Headers for HTML fetching
HTML_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ── Deep extraction schema ───────────────────────────────────────────────────

DEEP_SCHEMA = {
    "rentabilidad_periodo_pct": (
        "rentabilidad del fondo en el periodo (numero, ej: 10.1). "
        "null si no se menciona"
    ),
    "rentabilidad_benchmark_pct": (
        "rentabilidad del benchmark si se menciona (numero o null)"
    ),
    "benchmark_nombre": "nombre del benchmark citado (string o null)",
    "exposicion_rv_inicio_pct": (
        "porcentaje de exposicion a renta variable al inicio del periodo "
        "(numero o null)"
    ),
    "exposicion_rv_fin_pct": (
        "porcentaje de exposicion a renta variable al final del periodo "
        "(numero o null)"
    ),
    "num_posiciones": "numero de posiciones en cartera (entero o null)",
    "entradas": [
        {
            "empresa": "nombre de la empresa o activo que entro en cartera",
            "justificacion": "razon de la entrada en cartera",
        }
    ],
    "salidas": [
        {
            "empresa": "nombre de la empresa o activo que salio de cartera",
            "justificacion": "razon de la salida de cartera",
        }
    ],
    "vision_macro": [
        "punto concreto de vision macroeconomica (max 5 puntos, frases cortas)"
    ],
    "tesis_destacadas": [
        "tesis de inversion principal articulada por el gestor"
    ],
    "citas_textuales": [
        "frase textual del gestor entre comillas, max 15 palabras"
    ],
    "resumen_ejecutivo": (
        "4-5 lineas de resumen ejecutivo con lo mas relevante de la carta. "
        "Incluir: rentabilidad, movimientos clave, perspectivas."
    ),
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ts() -> str:
    """Formatted timestamp for logging."""
    return datetime.now().strftime("%H:%M:%S")


def _has_api_key() -> bool:
    """Check whether ANTHROPIC_API_KEY is available."""
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def _has_value(value) -> bool:
    """Return True when a value is non-null and non-empty."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, list) and len(value) == 0:
        return False
    return True


def _to_float(value) -> float | None:
    """Coerce a value to float, handling string percentages like '3.5%'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().rstrip("%").strip().replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _to_int(value) -> int | None:
    """Coerce a value to int."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


# ── Main agent class ─────────────────────────────────────────────────────────

class LettersDeepAgent:
    """
    Deep extraction agent for fund manager letters.

    Reads letters_data.json produced by LettersAgent, obtains the full text
    of each carta (from PDF or HTML), and uses Claude API to extract a richer
    set of structured fields.

    Idempotent: skips cartas that already have deep fields.

    Usage::

        agent = LettersDeepAgent("ES0112231008", fund_name="Avantage Fund FI")
        result = await agent.run()
    """

    def __init__(self, isin: str, fund_name: str = ""):
        self.isin = isin.strip().upper()
        self.fund_name = fund_name

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.letters_path = self.fund_dir / "letters_data.json"
        self.raw_letters_dir = self.fund_dir / "raw" / "letters"
        self._log_path = root / "progress.log"

        # Runtime stats
        self._stats = {
            "total": 0,
            "skipped_already_done": 0,
            "skipped_no_text": 0,
            "extracted_ok": 0,
            "extracted_error": 0,
        }

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        """Write to console and append to progress.log."""
        line = f"[{_ts()}] [LETTERS_DEEP] [{level}] {msg}"
        # Safe for Windows cp1252 terminals
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> dict:
        """
        Main entry point.

        1. Load letters_data.json
        2. For each carta, skip if already deep-extracted, else get text and
           call Claude to extract deep fields, then merge into carta.
        3. Save enriched letters_data.json
        4. Save extraction log to letters_deep_log.json
        5. Return the enriched letters_data dict.
        """
        self._log("START", f"LettersDeepAgent -- {self.isin} ({self.fund_name})")

        # ── Guard: API key required ──────────────────────────────────────────
        if not _has_api_key():
            self._log(
                "WARN",
                "ANTHROPIC_API_KEY no configurada -- extraccion profunda omitida",
            )
            console.print(
                Panel(
                    "[yellow]ANTHROPIC_API_KEY no encontrada.\n"
                    "La extraccion profunda requiere Claude API.\n"
                    "Anade la key al fichero .env y re-ejecuta.",
                    title="Letters Deep Agent",
                )
            )
            return self._empty_result("ANTHROPIC_API_KEY not set")

        # ── 1. Load letters_data.json ────────────────────────────────────────
        if not self.letters_path.exists():
            self._log("WARN", f"No existe {self.letters_path}")
            return self._empty_result("letters_data.json not found")

        try:
            letters_data = json.loads(
                self.letters_path.read_text(encoding="utf-8")
            )
        except (json.JSONDecodeError, OSError) as exc:
            self._log("ERROR", f"Error leyendo letters_data.json: {exc}")
            return self._empty_result(f"read error: {exc}")

        cartas = letters_data.get("cartas", [])
        if not cartas:
            self._log("INFO", "Sin cartas en letters_data.json -- nada que enriquecer")
            return letters_data

        self._stats["total"] = len(cartas)
        self._log("INFO", f"{len(cartas)} cartas a procesar")

        # Resolve fund_name from other data files if not provided
        if not self.fund_name:
            self.fund_name = self._resolve_fund_name()

        # Index raw PDFs for matching
        raw_pdfs = self._index_raw_pdfs()
        if raw_pdfs:
            self._log("INFO", f"{len(raw_pdfs)} PDFs en raw/letters/")

        # ── 2. Process cartas with concurrency limit ─────────────────────────
        semaphore = asyncio.Semaphore(CLAUDE_SEM_SIZE)
        enriched_cartas: list[dict] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=console,
        ) as progress:
            task = progress.add_task("Deep extraction", total=len(cartas))

            async def _process_with_sem(idx: int, carta: dict) -> dict:
                async with semaphore:
                    result = await self._process_carta(idx, carta, raw_pdfs)
                    progress.advance(task)
                    return result

            tasks = [
                _process_with_sem(i, c) for i, c in enumerate(cartas)
            ]
            enriched_cartas = await asyncio.gather(*tasks)

        # ── 3. Save enriched letters_data.json ───────────────────────────────
        letters_data["cartas"] = enriched_cartas
        letters_data["deep_extraction"] = {
            "ultima_actualizacion": datetime.now().isoformat(),
            "stats": dict(self._stats),
        }
        self._save_json(self.letters_path, letters_data)

        # ── 4. Save extraction log ───────────────────────────────────────────
        log_data = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "timestamp": datetime.now().isoformat(),
            "stats": dict(self._stats),
            "cartas": [
                {
                    "periodo": c.get("periodo", "?"),
                    "deep_ok": bool(c.get("deep_extraction_ok")),
                    "error": c.get("deep_extraction_error"),
                }
                for c in enriched_cartas
            ],
        }
        self._save_json(self.fund_dir / "letters_deep_log.json", log_data)

        self._log(
            "DONE",
            f"Completo: {self._stats['extracted_ok']} OK, "
            f"{self._stats['skipped_already_done']} skip (done), "
            f"{self._stats['skipped_no_text']} skip (no text), "
            f"{self._stats['extracted_error']} errores",
        )
        return letters_data

    # ── Per-carta processing ─────────────────────────────────────────────────

    async def _process_carta(
        self, idx: int, carta: dict, raw_pdfs: dict[str, Path]
    ) -> dict:
        """
        Process one carta: check skip, get text, extract, merge.

        Returns the carta dict with deep fields merged (or unchanged if skipped).
        """
        periodo = carta.get("periodo", f"#{idx}")

        # Already deep-extracted?
        if self._is_already_deep(carta):
            self._log("SKIP", f"Carta {periodo}: already deep-extracted")
            self._stats["skipped_already_done"] += 1
            return carta

        # Obtain full text
        text = await self._get_carta_text(carta, raw_pdfs)
        if not text or len(text.strip()) < 80:
            self._log("SKIP", f"Carta {periodo}: no text ({len(text or '')} chars)")
            self._stats["skipped_no_text"] += 1
            return carta

        # Truncate for Claude
        text_trimmed = self._trim_text(text, MAX_TEXT_CHARS)

        # Call Claude
        try:
            deep_fields = await self._extract_deep(text_trimmed, carta)
        except Exception as exc:
            self._log("ERROR", f"Carta {periodo}: Claude error -- {exc}")
            self._stats["extracted_error"] += 1
            enriched = dict(carta)
            enriched["deep_extraction_ok"] = False
            enriched["deep_extraction_error"] = str(exc)[:200]
            return enriched

        # Merge (never overwrite existing)
        merged = self._merge_deep_fields(carta, deep_fields)
        merged["deep_extraction_ok"] = True
        merged["deep_extraction_ts"] = datetime.now().isoformat()

        n_ent = len(deep_fields.get("entradas", []))
        n_sal = len(deep_fields.get("salidas", []))
        rent = deep_fields.get("rentabilidad_periodo_pct")
        self._log(
            "OK",
            f"Carta {periodo}: deep extraction OK "
            f"(rent={rent}%, {n_ent} entradas, {n_sal} salidas)",
        )
        self._stats["extracted_ok"] += 1
        return merged

    def _is_already_deep(self, carta: dict) -> bool:
        """True if any deep marker field is present and non-empty."""
        for field in DEEP_MARKER_FIELDS:
            val = carta.get(field)
            if val is None:
                continue
            if isinstance(val, list) and len(val) > 0:
                return True
            if isinstance(val, str) and val.strip():
                return True
        return False

    # ── Text acquisition ─────────────────────────────────────────────────────

    async def _get_carta_text(
        self, carta: dict, raw_pdfs: dict[str, Path]
    ) -> str:
        """
        Obtain the full text of a carta from (in priority order):
          1. Local PDF in raw/letters/  (matched via archivo field or period)
          2. Remote HTML via url_fuente
          3. Inline text fields already present in the carta (fallback)
        """
        # Source 1: local PDF
        text = self._text_from_pdf(carta, raw_pdfs)
        if text and len(text.strip()) >= 80:
            return text

        # Source 2: HTML from url_fuente
        text = await self._text_from_html(carta)
        if text and len(text.strip()) >= 80:
            return text

        # Source 3: reconstruct from inline fields
        return self._text_from_inline(carta)

    # ── PDF text extraction ──────────────────────────────────────────────────

    def _text_from_pdf(self, carta: dict, raw_pdfs: dict[str, Path]) -> str:
        """Extract text from a local PDF referenced by the carta."""
        pdf_path = self._match_pdf(carta, raw_pdfs)
        if not pdf_path:
            return ""

        try:
            meta = get_pdf_metadata(str(pdf_path))
            num_pages = meta.get("num_pages", 0)
            if num_pages == 0:
                return ""
            # Letters are short (1-15 pages); cap to avoid giant mis-matches
            max_pages = min(num_pages, 15)
            text = extract_page_range(str(pdf_path), 0, max_pages)
            self._log(
                "DEBUG",
                f"PDF: {pdf_path.name} ({num_pages}p, {len(text)} chars)",
            )
            return text
        except Exception as exc:
            self._log("WARN", f"PDF extraction error {pdf_path.name}: {exc}")
            return ""

    def _match_pdf(self, carta: dict, raw_pdfs: dict[str, Path]) -> Path | None:
        """Match a carta to a raw PDF by archivo name, URL filename, or period."""
        if not raw_pdfs:
            return None

        # By archivo field
        archivo = carta.get("archivo", "")
        if archivo:
            stem = Path(archivo).stem.lower()
            if stem in raw_pdfs:
                return raw_pdfs[stem]
            # Try exact filename match
            candidate = self.raw_letters_dir / Path(archivo).name
            if candidate.exists():
                return candidate

        # By URL filename
        url = carta.get("url_fuente", carta.get("url", ""))
        if url:
            url_stem = Path(url.split("?")[0].split("#")[0]).stem.lower()
            if url_stem in raw_pdfs:
                return raw_pdfs[url_stem]

        # By period substring in PDF filename
        periodo = carta.get("periodo", "")
        if periodo:
            periodo_norm = periodo.lower().replace(" ", "").replace("-", "")
            for stem, path in raw_pdfs.items():
                stem_norm = stem.replace(" ", "").replace("-", "").replace("_", "")
                if periodo_norm in stem_norm or stem_norm in periodo_norm:
                    return path

        return None

    # ── HTML text extraction ─────────────────────────────────────────────────

    async def _text_from_html(self, carta: dict) -> str:
        """Fetch url_fuente and extract article text via BeautifulSoup."""
        url = carta.get("url_fuente", "")
        if not url or not url.startswith("http"):
            return ""
        # Skip PDF URLs (should go through _text_from_pdf)
        if url.lower().endswith(".pdf"):
            return ""

        try:
            html = await get_with_headers(url, HTML_HEADERS)
        except Exception as exc:
            self._log("DEBUG", f"HTML fetch failed {url[:60]}: {exc}")
            return ""

        return self._parse_html_article(html, url)

    def _parse_html_article(self, html: str, url: str = "") -> str:
        """
        Parse HTML and extract clean article text.
        Strips nav, header, footer, script, style elements.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove non-content tags
        for tag_name in (
            "script", "style", "nav", "header", "footer",
            "aside", "iframe", "noscript", "form",
        ):
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # Find main content container
        article = (
            soup.find("article")
            or soup.find(
                "div",
                class_=re.compile(
                    r"content|article|post|entry|body|main|text",
                    re.IGNORECASE,
                ),
            )
            or soup.find("main")
        )
        target = article if article else (soup.body if soup.body else soup)

        # Structured paragraph extraction
        paragraphs: list[str] = []
        for el in target.find_all(["p", "h1", "h2", "h3", "h4", "li", "td"]):
            text = el.get_text(separator=" ", strip=True)
            if text and len(text) > 15:
                paragraphs.append(text)

        if len(paragraphs) >= 3:
            result = "\n\n".join(paragraphs)
        else:
            result = target.get_text(separator="\n", strip=True)
            result = re.sub(r"\n{3,}", "\n\n", result)

        if result:
            self._log("DEBUG", f"HTML parsed: {url[:60]} ({len(result)} chars)")
        return result

    # ── Inline text fallback ─────────────────────────────────────────────────

    def _text_from_inline(self, carta: dict) -> str:
        """
        Last resort: reconstruct text from the inline fields already present
        in the carta (resumen_mercado, tesis_inversion, etc.).
        """
        parts: list[str] = []

        if carta.get("titulo"):
            parts.append(f"TITULO: {carta['titulo']}")
        if carta.get("periodo"):
            parts.append(f"PERIODO: {carta['periodo']}")

        for field in (
            "tesis_inversion", "resumen_mercado", "perspectivas",
            "decisiones_cartera", "texto_completo", "contenido",
        ):
            value = carta.get(field)
            if isinstance(value, str) and len(value.strip()) > 20:
                parts.append(
                    f"\n{field.upper().replace('_', ' ')}:\n{value}"
                )
            elif isinstance(value, list) and value:
                items = "\n".join(f"- {it}" for it in value if it)
                if items:
                    parts.append(
                        f"\n{field.upper().replace('_', ' ')}:\n{items}"
                    )

        # Posiciones comentadas
        posiciones = carta.get("posiciones_comentadas")
        if isinstance(posiciones, list) and posiciones:
            pos_lines: list[str] = []
            for pos in posiciones:
                if isinstance(pos, dict):
                    nombre = pos.get("nombre", pos.get("empresa", "?"))
                    racional = pos.get("racional", pos.get("justificacion", ""))
                    pos_lines.append(f"- {nombre}: {racional}")
                elif isinstance(pos, str):
                    pos_lines.append(f"- {pos}")
            if pos_lines:
                parts.append("\nPOSICIONES COMENTADAS:\n" + "\n".join(pos_lines))

        return "\n".join(parts)

    # ── Claude deep extraction ───────────────────────────────────────────────

    async def _extract_deep(self, text: str, carta: dict) -> dict:
        """
        Call Claude API to extract deep structured fields from carta text.

        Runs the synchronous extract_structured_data in an executor to avoid
        blocking the event loop.

        Returns:
            Cleaned dict of deep fields.

        Raises:
            Exception on Claude API failure.
        """
        periodo = carta.get("periodo", carta.get("fecha", ""))
        context = (
            f"Carta {periodo} del fondo {self.fund_name} ({self.isin}). "
            f"Extraer TODOS los datos numericos y cualitativos. "
            f"Para entradas y salidas incluir TODAS las mencionadas. "
            f"Las citas textuales deben ser literales del gestor (max 15 palabras). "
            f"Vision macro: max 5 puntos concretos. "
            f"Resumen ejecutivo: 4-5 lineas cubriendo rentabilidad, movimientos "
            f"clave y perspectivas."
        )

        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(
            None, extract_structured_data, text, DEEP_SCHEMA, context,
        )

        if not isinstance(raw, dict):
            raise ValueError(f"Claude returned non-dict: {type(raw)}")

        return self._clean_deep_result(raw)

    def _clean_deep_result(self, raw: dict) -> dict:
        """Validate types and sanitise the raw Claude response."""
        cleaned: dict = {}

        # Numeric float fields
        for fld in (
            "rentabilidad_periodo_pct", "rentabilidad_benchmark_pct",
            "exposicion_rv_inicio_pct", "exposicion_rv_fin_pct",
        ):
            cleaned[fld] = _to_float(raw.get(fld))

        # Integer
        cleaned["num_posiciones"] = _to_int(raw.get("num_posiciones"))

        # String
        bm = raw.get("benchmark_nombre")
        cleaned["benchmark_nombre"] = str(bm).strip() if bm else None

        # Lists of dicts: entradas, salidas
        for fld in ("entradas", "salidas"):
            raw_list = raw.get(fld, [])
            if not isinstance(raw_list, list):
                cleaned[fld] = []
                continue
            items: list[dict] = []
            for item in raw_list:
                if isinstance(item, dict) and item.get("empresa"):
                    items.append({
                        "empresa": str(item["empresa"]).strip(),
                        "justificacion": (
                            str(item.get("justificacion", "")).strip() or None
                        ),
                    })
            cleaned[fld] = items

        # Lists of strings
        for fld in ("vision_macro", "tesis_destacadas", "citas_textuales"):
            raw_list = raw.get(fld, [])
            if not isinstance(raw_list, list):
                cleaned[fld] = []
                continue
            cleaned[fld] = [
                str(s).strip() for s in raw_list if s and str(s).strip()
            ]

        # Resumen ejecutivo
        res = raw.get("resumen_ejecutivo")
        cleaned["resumen_ejecutivo"] = (
            str(res).strip() if res and str(res).strip() else None
        )

        return cleaned

    # ── Merging ──────────────────────────────────────────────────────────────

    def _merge_deep_fields(self, carta: dict, deep: dict) -> dict:
        """
        Merge deep extraction fields into the carta dict.
        NEVER overwrites existing non-null, non-empty fields -- only adds
        new ones.
        """
        merged = dict(carta)
        for key, value in deep.items():
            existing = merged.get(key)
            if _has_value(existing):
                continue
            if _has_value(value):
                merged[key] = value
        return merged

    # ── Text trimming ────────────────────────────────────────────────────────

    @staticmethod
    def _trim_text(text: str, max_chars: int) -> str:
        """
        Trim to max_chars preserving beginning and end (the intro and
        conclusion of manager letters carry the most value).
        """
        if len(text) <= max_chars:
            return text

        sep = "\n\n[...texto truncado por longitud...]\n\n"
        head_size = int(max_chars * 0.45)
        tail_size = int(max_chars * 0.45)

        head = text[:head_size]
        tail = text[-tail_size:]

        # Cut at paragraph boundaries when possible
        head_cut = head.rfind("\n\n")
        if head_cut > head_size * 0.5:
            head = head[:head_cut]

        tail_cut = tail.find("\n\n")
        if tail_cut != -1 and tail_cut < tail_size * 0.5:
            tail = tail[tail_cut:]

        return head + sep + tail

    # ── Utilities ────────────────────────────────────────────────────────────

    def _index_raw_pdfs(self) -> dict[str, Path]:
        """Index PDFs in raw/letters/ by lowercase stem."""
        pdfs: dict[str, Path] = {}
        if self.raw_letters_dir.exists():
            for f in self.raw_letters_dir.glob("*.pdf"):
                pdfs[f.stem.lower()] = f
        return pdfs

    def _resolve_fund_name(self) -> str:
        """Try to obtain fund name from other data files."""
        for filename in (
            "output.json", "cnmv_data.json", "intl_data.json", "cssf_data.json",
        ):
            path = self.fund_dir / filename
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    name = data.get("nombre", data.get("nombre_oficial", ""))
                    if name:
                        return name
                except (json.JSONDecodeError, OSError):
                    pass
        return self.isin

    def _save_json(self, path: Path, data: dict) -> None:
        """Write dict to JSON with UTF-8 encoding."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        self._log("OK", f"Guardado: {path}")

    def _empty_result(self, reason: str) -> dict:
        """Return a minimal result when processing cannot proceed."""
        return {
            "isin": self.isin,
            "ultima_actualizacion": datetime.now().isoformat(),
            "cartas": [],
            "deep_extraction": {
                "ultima_actualizacion": datetime.now().isoformat(),
                "stats": dict(self._stats),
                "error": reason,
            },
            "fuentes": {"cartas_gestores": [], "urls_consultadas": []},
        }


# ── Standalone CLI ───────────────────────────────────────────────────────────

async def _main():
    """CLI entry point for standalone execution."""
    import argparse
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser(
        description="Letters Deep Agent -- deep extraction of fund manager letters",
    )
    parser.add_argument("--isin", required=True, help="Fund ISIN code")
    parser.add_argument("--fund-name", default="", help="Fund name for context")
    parser.add_argument(
        "--force", action="store_true",
        help="Force re-extraction even if deep fields already present",
    )
    args = parser.parse_args()

    isin = args.isin.strip().upper()
    fund_dir = Path(__file__).parent.parent / "data" / "funds" / isin

    # Pre-flight check
    letters_path = fund_dir / "letters_data.json"
    if not letters_path.exists():
        console.print(
            f"[red]No existe {letters_path}\n"
            f"Ejecuta primero letters_agent para descargar las cartas."
        )
        sys.exit(1)

    console.print(
        Panel(
            f"[bold]ISIN: {isin}\n"
            f"Fund name: {args.fund_name or '(auto-detect)'}\n"
            f"Letters data: {letters_path}\n"
            f"Raw PDFs dir: {fund_dir / 'raw' / 'letters'}",
            title="Letters Deep Agent",
        )
    )

    agent = LettersDeepAgent(isin=isin, fund_name=args.fund_name)
    result = await agent.run()

    # Summary
    stats = result.get("deep_extraction", {}).get("stats", {})
    cartas = result.get("cartas", [])

    console.print()
    console.print(
        Panel(
            f"[bold green]Extraccion profunda completada[/]\n\n"
            f"Total cartas:         {stats.get('total', 0)}\n"
            f"Extracted OK:         {stats.get('extracted_ok', 0)}\n"
            f"Skipped (done):       {stats.get('skipped_already_done', 0)}\n"
            f"Skipped (no text):    {stats.get('skipped_no_text', 0)}\n"
            f"Errors:               {stats.get('extracted_error', 0)}\n"
            f"Output: {letters_path}",
            title="Resultado",
        )
    )

    for carta in cartas:
        periodo = carta.get("periodo", "?")
        ok = carta.get("deep_extraction_ok", False)
        status = "[green]OK" if ok else (
            f"[red]{carta.get('deep_extraction_error', 'skip')}"
        )
        rent = carta.get("rentabilidad_periodo_pct")
        rent_str = f" | rent={rent}%" if rent is not None else ""
        n_ent = len(carta.get("entradas", []))
        n_sal = len(carta.get("salidas", []))
        moves = f" | {n_ent} entradas, {n_sal} salidas" if ok else ""
        console.print(f"  {periodo}: {status}{rent_str}{moves}")


if __name__ == "__main__":
    asyncio.run(_main())
