"""Publication calendar — detecta cadencia de informes/cartas de cada fondo.

Lee:
- data/funds/{ISIN}/raw/reports/*.pdf  (CNMV semestrales / annual reports INT)
- data/funds/{ISIN}/letters_data.json   (cartas trimestrales)
- data/funds/{ISIN}/raw/xml/            (XMLs CNMV mensuales/trimestrales)

Calcula:
- frequency: annual | semiannual | quarterly | monthly
- publication_months_typical: lista de meses (1-12) en los que típicamente aparece
- last_known_date: fecha del documento más reciente
- next_expected_date: estimada con la cadencia detectada
- confidence: high (≥3 puntos coherentes) / medium (2 puntos) / low (1 punto)

Uso:
    python -m tools.publication_calendar --isin ES0159259011 [--update-output]
    python -m tools.publication_calendar --all
"""
import json
import re
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
FUNDS_DIR = ROOT / "data" / "funds"


# ── Parsers de fecha ─────────────────────────────────────────────────────────

_RE_PDF_YEAR_HALF = re.compile(r"_(\d{4})[_-]?H?([12])\.pdf$", re.I)
_RE_PDF_YEAR_QUARTER = re.compile(r"_(\d{4})[_-]?Q([1-4])\.pdf$", re.I)
_RE_PDF_YEAR_MONTH = re.compile(r"_(\d{4})[_-]?(\d{2})\.pdf$", re.I)
_RE_PDF_ANNUAL = re.compile(r"(?:annual|anual|jahres)[_-]?(\d{4})", re.I)
_RE_PERIODO_QUARTER = re.compile(r"^(\d{4})[_-]?Q([1-4])$", re.I)
_RE_PERIODO_HALF = re.compile(r"^(\d{4})[_-]?[SH]([12])$", re.I)
_RE_PERIODO_YEAR = re.compile(r"^(\d{4})$")
_RE_PERIODO_MONTH = re.compile(r"^(\d{4})[_-]?(\d{2})$")


def _date_from_filename(fname: str) -> date | None:
    """Extrae fecha aproximada del nombre del fichero PDF."""
    m = _RE_PDF_YEAR_HALF.search(fname)
    if m:
        year, half = int(m.group(1)), int(m.group(2))
        # H1 → 30 jun, H2 → 31 dic (convención CNMV)
        return date(year, 6, 30) if half == 1 else date(year, 12, 31)
    m = _RE_PDF_YEAR_QUARTER.search(fname)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        # Q1 → 31 mar, Q2 → 30 jun, Q3 → 30 sep, Q4 → 31 dic
        end_month = q * 3
        return date(year, end_month, 28)
    m = _RE_PDF_YEAR_MONTH.search(fname)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12:
            return date(year, month, 28)
    m = _RE_PDF_ANNUAL.search(fname)
    if m:
        return date(int(m.group(1)), 12, 31)
    return None


def _date_from_periodo(periodo: str) -> date | None:
    """Convierte '2025-Q4', '2024-S2', '2023', '2025-04' a date."""
    if not periodo:
        return None
    m = _RE_PERIODO_QUARTER.match(periodo)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        return date(year, q * 3, 28)
    m = _RE_PERIODO_HALF.match(periodo)
    if m:
        year, half = int(m.group(1)), int(m.group(2))
        return date(year, 6, 30) if half == 1 else date(year, 12, 31)
    m = _RE_PERIODO_MONTH.match(periodo)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 28)
    m = _RE_PERIODO_YEAR.match(periodo)
    if m:
        return date(int(m.group(1)), 12, 31)
    return None


# ── Detección de cadencia ────────────────────────────────────────────────────

def _detect_frequency(dates: list[date]) -> tuple[str, list[int], str]:
    """Detecta frequency, months_typical, confidence dado un listado de fechas.

    Returns: (frequency, months_typical, confidence)
    """
    if not dates:
        return "unknown", [], "low"
    dates_sorted = sorted(set(dates))
    if len(dates_sorted) == 1:
        return "unknown", [dates_sorted[0].month], "low"

    # Calcular deltas en días
    deltas = [(dates_sorted[i+1] - dates_sorted[i]).days for i in range(len(dates_sorted)-1)]
    median_delta = sorted(deltas)[len(deltas)//2]

    if median_delta <= 35:
        freq = "monthly"
    elif median_delta <= 100:
        freq = "quarterly"
    elif median_delta <= 200:
        freq = "semiannual"
    else:
        freq = "annual"

    # Meses típicos (mode)
    months = [d.month for d in dates_sorted]
    month_counter = Counter(months)
    typical_months = sorted([m for m, _ in month_counter.most_common(4)])

    # Confidence
    if len(dates_sorted) >= 3:
        # Consistencia: ≥80% de los meses son los típicos
        in_typical = sum(1 for m in months if m in typical_months)
        confidence = "high" if in_typical / len(months) >= 0.8 else "medium"
    elif len(dates_sorted) == 2:
        confidence = "medium"
    else:
        confidence = "low"

    return freq, typical_months, confidence


def _next_expected(last_date: date, frequency: str, typical_months: list[int]) -> date:
    """Estima próxima fecha esperada."""
    delta_days = {
        "monthly": 30, "quarterly": 91, "semiannual": 183, "annual": 365,
    }.get(frequency, 365)
    candidate = last_date + timedelta(days=delta_days)
    # Si tenemos meses típicos, ajustar al próximo mes típico tras candidate
    if typical_months:
        for offset in range(0, 13):
            check = candidate + timedelta(days=offset * 30)
            if check.month in typical_months:
                return check.replace(day=28)
    return candidate


# ── Builders por categoría ───────────────────────────────────────────────────

_RE_PDF_FULL_DATE = re.compile(r"(20\d{2})[_-]?(\d{2})[_-]?(\d{2})", re.I)
_RE_PDF_ANNUAL_REPORT_KEYWORD = re.compile(
    r"(?:annual\s*report|rapport[_\s-]*financier[_\s-]*annuel|rapport[_\s-]*annuel|"
    r"rechenschaft|jahres(?:bericht)?|annual[_\s-]*account|memoria[_\s-]*anual|"
    r"financial[_\s-]*statement|investment[_\s-]*report)",
    re.I,
)


def _date_from_filename_int(fname: str) -> date | None:
    """H1 Fase H_INT (2026-05-01): parser extra para nombres INT no-CNMV.

    Captura formatos típicos en raw/discovery/ INT:
    - REYL_ISP_Annual_Report_2024_low.pdf  → 2024-12-31
    - _web_Lettre_ISR_Avril_2019_France_FR.pdf → 2019 (annual)
    - 250404_xxx.pdf → 2025-04-04 (YYMMDD prefix)
    - dncas-2025-outlook.pdf → 2025

    Estrategia:
    1. Si ya match un parser CNMV existente → usar ese
    2. Si es PDF reconocible como annual report (keyword) + tiene año → usar 31-12 de ese año
    3. Si tiene fecha completa YYYYMMDD o YYMMDD → usar
    4. Si solo tiene año + es PDF largo (>1MB) → asumir AR anual
    """
    # Parser CNMV existente primero
    cnmv_d = _date_from_filename(fname)
    if cnmv_d:
        return cnmv_d
    # Annual report con keyword + año
    has_kw = bool(_RE_PDF_ANNUAL_REPORT_KEYWORD.search(fname))
    if has_kw:
        m = re.search(r"(20\d{2})", fname)
        if m:
            return date(int(m.group(1)), 12, 31)
    # Fecha completa YYMMDD prefix
    m = re.match(r"_?web_(\d{2})(\d{2})(\d{2})_", fname)
    if m:
        yy, mm, dd = m.groups()
        try:
            year = 2000 + int(yy)
            return date(year, int(mm), int(dd))
        except ValueError:
            pass
    return None


def _build_for_reports(fund_dir: Path) -> dict | None:
    """Detecta calendar para informes en raw/reports/ (CNMV) y raw/discovery/ (INT).

    H1 Fase H_INT (2026-05-01): añadida búsqueda en raw/discovery/ con parser
    extendido para reconocer annual reports INT (REYL_*, _web_*, etc.). Antes
    solo miraba raw/reports/ → ningún fondo INT detectaba annual_report.
    """
    all_dates = []
    sources_used = []

    # 1. raw/reports/ (CNMV semestrales H1/H2)
    reports_dir = fund_dir / "raw" / "reports"
    if reports_dir.exists():
        pdfs = sorted(reports_dir.glob("*.pdf"))
        dates = [d for f in pdfs if (d := _date_from_filename(f.name))]
        if dates:
            all_dates.extend(dates)
            sources_used.append(f"{len(dates)} PDFs raw/reports")

    # 2. raw/discovery/ (INT annual reports + factsheets descubiertos por discovery_v2)
    # H1: solo cuenta los que matchean keyword "annual report" o tienen fecha clara,
    # para evitar que factsheets/cartas/KIID inflen el cálculo de cadencia.
    disc_dir = fund_dir / "raw" / "discovery"
    if disc_dir.exists():
        for f in sorted(disc_dir.glob("*.pdf")):
            d = _date_from_filename_int(f.name)
            # Solo añadir si el PDF parece annual report (no factsheet/KIID)
            if d and (_RE_PDF_ANNUAL_REPORT_KEYWORD.search(f.name)
                      or "annual" in f.name.lower()):
                all_dates.append(d)
        n_disc = sum(1 for f in disc_dir.glob("*.pdf")
                     if _RE_PDF_ANNUAL_REPORT_KEYWORD.search(f.name)
                     or "annual" in f.name.lower())
        if n_disc:
            sources_used.append(f"{n_disc} AR raw/discovery")

    if not all_dates:
        return None
    freq, months, conf = _detect_frequency(all_dates)
    last = max(all_dates)
    return {
        "frequency": freq,
        "publication_months_typical": months,
        "last_known_date": last.isoformat(),
        "next_expected_date": _next_expected(last, freq, months).isoformat(),
        "source": " + ".join(sources_used),
        "confidence": conf,
    }


def _build_for_letters(fund_dir: Path) -> dict | None:
    """Detecta calendar para cartas trimestrales (letters_data.json)."""
    letters_path = fund_dir / "letters_data.json"
    if not letters_path.exists():
        return None
    try:
        d = json.loads(letters_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    cartas = d.get("cartas", []) or []
    dates = []
    for c in cartas:
        periodo = c.get("periodo") or c.get("fecha_inferida") or ""
        dt = _date_from_periodo(periodo)
        if dt:
            dates.append(dt)
    if not dates:
        return None
    freq, months, conf = _detect_frequency(dates)
    last = max(dates)
    return {
        "frequency": freq,
        "publication_months_typical": months,
        "last_known_date": last.isoformat(),
        "next_expected_date": _next_expected(last, freq, months).isoformat(),
        "source": f"{len(dates)} cartas en letters_data.json",
        "confidence": conf,
    }


# ── API pública ──────────────────────────────────────────────────────────────

def build_publication_calendar(isin: str) -> dict:
    """Construye el calendar completo para un fondo."""
    fund_dir = FUNDS_DIR / isin
    if not fund_dir.exists():
        return {}
    cal = {}
    reports = _build_for_reports(fund_dir)
    if reports:
        # Etiqueta según frecuencia detectada
        key = {
            "annual": "annual_report",
            "semiannual": "semiannual_report",
            "quarterly": "quarterly_report",
            "monthly": "monthly_report",
        }.get(reports["frequency"], "report")
        cal[key] = reports
    letters = _build_for_letters(fund_dir)
    if letters:
        cal["quarterly_letters"] = letters
    return cal


def update_output_with_calendar(isin: str) -> bool:
    """Calcula calendar y lo escribe en output.json (top-level publication_calendar)."""
    output_path = FUNDS_DIR / isin / "output.json"
    if not output_path.exists():
        return False
    try:
        data = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    cal = build_publication_calendar(isin)
    if not cal:
        return False
    data["publication_calendar"] = cal
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def find_funds_due_for_update(window_days: int = 14) -> list[dict]:
    """Recorre todos los fondos y devuelve los que tienen next_expected_date
    dentro de la ventana ±window_days desde hoy."""
    today = date.today()
    out = []
    for fund_dir in FUNDS_DIR.iterdir():
        if not fund_dir.is_dir():
            continue
        output_path = fund_dir / "output.json"
        if not output_path.exists():
            continue
        try:
            data = json.loads(output_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        cal = data.get("publication_calendar") or {}
        for key, info in cal.items():
            if not isinstance(info, dict):
                continue
            next_expected = info.get("next_expected_date")
            if not next_expected:
                continue
            try:
                nd = date.fromisoformat(next_expected)
            except Exception:
                continue
            delta = (nd - today).days
            if -window_days <= delta <= window_days:
                out.append({
                    "isin": fund_dir.name,
                    "nombre": data.get("nombre", ""),
                    "doc_type": key,
                    "next_expected": next_expected,
                    "days_to_expected": delta,
                    "source": info.get("source", ""),
                })
    return sorted(out, key=lambda x: x["days_to_expected"])


# ── CLI ──────────────────────────────────────────────────────────────────────

def _main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return
    if args[0] == "--all":
        # Build calendar para todos los fondos
        updated = 0
        for fund_dir in sorted(FUNDS_DIR.iterdir()):
            if not fund_dir.is_dir():
                continue
            isin = fund_dir.name
            if update_output_with_calendar(isin):
                updated += 1
                print(f"  [OK] {isin}")
        print(f"\nActualizados: {updated} fondos.")
    elif args[0] == "--due":
        window = int(args[1]) if len(args) > 1 else 14
        due = find_funds_due_for_update(window)
        if not due:
            print(f"Ningún fondo con publicación esperada en ±{window} días.")
            return
        print(f"=== Fondos con publicación esperada en ±{window} días ===\n")
        for item in due:
            print(f"  [{item['days_to_expected']:+4d}d] {item['isin']} ({item['nombre'][:50]}) — {item['doc_type']}: {item['next_expected']}")
    else:
        # Un ISIN concreto
        isin = args[0]
        cal = build_publication_calendar(isin)
        print(json.dumps(cal, ensure_ascii=False, indent=2))
        if "--update-output" in args:
            ok = update_output_with_calendar(isin)
            print(f"\noutput.json actualizado: {ok}")


if __name__ == "__main__":
    _main()
