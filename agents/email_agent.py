"""
Email request agent — genera borrador formal para pedir AR/SAR históricos
cuando discovery no los encuentra online.

Trigger: ≥50% de los AR esperados faltantes tras discovery.
Output:  data/funds/{ISIN}/email_request.eml  (abrible en Outlook/Apple Mail)
         data/funds/{ISIN}/email_request.txt  (copy-paste)

El módulo NO envía el email: deja el borrador preparado para que el usuario
lo revise y pulse "Enviar" desde su cliente.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()

FROM_NAME = "Rafa"
FROM_EMAIL = "rafagdominguez96@gmail.com"

# % mínimo de AR históricos que deben faltar para generar el email
THRESHOLD_MISSING_PCT = 0.50


def _years_expected(inception: str) -> list[int]:
    """Años de AR esperados: desde inception hasta el año anterior al actual."""
    m = re.search(r"(19|20)\d{2}", inception or "")
    if not m:
        return []
    start = int(m.group(0))
    now = datetime.now().year
    return list(range(start, now))  # AR del año N se publica en N+1


def _missing_buckets(missing: list[dict]) -> tuple[list[int], list[int], list[int]]:
    """
    Devuelve (years_AR, years_SAR, years_letters) faltantes.
    Filtra años todavía no publicables:
      - AR/SAR del año actual y posteriores (AR del año N se publica en N+1 mid-year).
      - Letters del año actual+1 en adelante.
    """
    now = datetime.now().year
    ar, sar, letters = [], [], []
    for m in missing:
        dt = m.get("doc_type") if isinstance(m, dict) else None
        per = m.get("periodo") if isinstance(m, dict) else ""
        y = re.search(r"(19|20)\d{2}", str(per))
        if not y:
            continue
        y = int(y.group(0))
        if dt == "annual_report" and y < now:
            ar.append(y)
        elif dt == "semi_annual_report" and y < now:
            sar.append(y)
        elif dt == "quarterly_letter" and y <= now:
            letters.append(y)
    return sorted(set(ar)), sorted(set(sar)), sorted(set(letters))


def _should_draft(state) -> tuple[bool, list[int], list[int], list[int]]:
    """Decide si procede + devuelve (ar_miss, sar_miss, letters_miss)."""
    missing = [{"doc_type": dt, "periodo": p} for dt, p in state.missing_doc_targets()]
    ar, sar, letters = _missing_buckets(missing)
    expected = _years_expected(state.identity.get("fecha_autorizacion", ""))
    if expected:
        pct = len([y for y in ar if y in expected]) / max(1, len(expected))
        return pct >= THRESHOLD_MISSING_PCT, ar, sar, letters
    # Fallback sin inception: disparar si faltan ≥3 AR o ≥3 SAR
    return (len(ar) >= 3 or len(sar) >= 3), ar, sar, letters


def _resolve_contact(state) -> str:
    """Cascada: KB regulators → scrape /contact del dominio gestora → fallback."""
    gestora = state.identity.get("gestora_oficial", "")
    # 1. KB per-gestora
    reg_kb = Path(__file__).parent.parent / "data" / "regulators_knowledge.json"
    if reg_kb.exists():
        try:
            kb = json.loads(reg_kb.read_text(encoding="utf-8"))
            for prefix, info in kb.items():
                contacts = info.get("gestora_contacts", {})
                if gestora and gestora.lower() in (k.lower() for k in contacts):
                    return next(v for k, v in contacts.items() if k.lower() == gestora.lower())
        except Exception:
            pass

    # 2. Scrape /contact en dominios ya descubiertos → coleccionar y rankear
    candidates: list[str] = []
    for host in list(state.discovered_gestora_domains):
        host_url = host.rstrip("/") if host.startswith("http") else f"https://{host.split('/')[0]}"
        for path in ("/contact", "/contactez-nous", "/kontakt", "/contacto",
                     "/en/contact", "/about/contact", "/contact-us", "/en/contact-us"):
            candidates.extend(_scrape_emails(host_url + path))
    if candidates:
        candidates.sort(key=lambda e: _rank_email(e), reverse=True)
        return candidates[0]

    # 3. Fallback genérico
    for host in domains:
        bare = host.split("/")[0].replace("www.", "")
        if "." in bare:
            return f"investor.relations@{bare}"
    return ""


# Prefijos de email rankeados por "lo que queremos que reciba esto primero"
_EMAIL_PREFIX_RANK = [
    ("investor-relations", 100), ("investorrelations", 100), ("investor_relations", 100),
    ("ir@", 95),
    ("clientservices", 80), ("client-services", 80), ("client_services", 80),
    ("clientservice", 75), ("fundservices", 75),
    ("info", 40), ("contact", 35),
    ("hello", 20), ("office", 20),
]

def _rank_email(addr: str) -> int:
    a = addr.lower()
    for prefix, score in _EMAIL_PREFIX_RANK:
        if a.startswith(prefix):
            return score
    return 10


def _scrape_emails(url: str) -> list[str]:
    """Colecta todos los emails de una página. Sin rank (lo hace el caller)."""
    try:
        with httpx.Client(follow_redirects=True, timeout=8, headers={"User-Agent": "Mozilla/5.0"}) as sc:
            r = sc.get(url)
            if r.status_code != 200:
                return []
            found: list[str] = []
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("mailto:"):
                    addr = href[7:].split("?")[0].strip()
                    if "@" in addr:
                        found.append(addr)
            for m in re.finditer(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text):
                found.append(m.group(0))
            # Dedup preservando orden
            seen = set()
            out = []
            for e in found:
                el = e.lower()
                if el not in seen:
                    seen.add(el)
                    out.append(e)
            return out
    except Exception:
        return []


def _template_en(fund: str, isin: str, gestora: str,
                 ar: list[int], sar: list[int], letters: list[int]) -> tuple[str, str]:
    lines = []
    if ar:
        lines.append(f"  • Annual Report: {', '.join(str(y) for y in ar)}")
    if sar:
        lines.append(f"  • Semi-Annual Report: {', '.join(str(y) for y in sar)}")
    if letters:
        lines.append(f"  • Annual / Quarterly Letters to investors: {', '.join(str(y) for y in letters)}")
    missing_block = "\n".join(lines)
    subject = f"Request for historical reports — {fund} ({isin})"
    body = f"""Dear Sir or Madam,

I am conducting a long-term analysis of {fund} ({isin}), managed by
{gestora}. I have been able to locate the most recent reports on your
website — thank you for keeping them available — but I cannot find
the following historical documents, which do not appear to be
publicly accessible:

{missing_block}

Would it be possible to send them through this channel, or to advise
where I could obtain them? They are needed for a historical
consistency analysis of the fund's thesis and investment process —
strictly personal/internal use.

Many thanks in advance.

Kind regards,
{FROM_NAME}
"""
    return subject, body


def _template_es(fund: str, isin: str, gestora: str,
                 ar: list[int], sar: list[int], letters: list[int]) -> tuple[str, str]:
    lines = []
    if ar:
        lines.append(f"  • Informe Anual: {', '.join(str(y) for y in ar)}")
    if sar:
        lines.append(f"  • Informe Semestral: {', '.join(str(y) for y in sar)}")
    if letters:
        lines.append(f"  • Cartas anuales / trimestrales a inversores: {', '.join(str(y) for y in letters)}")
    missing_block = "\n".join(lines)
    subject = f"Solicitud de informes históricos — {fund} ({isin})"
    body = f"""Estimados Sres.,

Estoy realizando un análisis de largo plazo del fondo {fund}
({isin}), gestionado por {gestora}. He podido localizar los Informes
Anuales y Semestrales más recientes en su web — gracias por
mantenerlos accesibles —, pero no encuentro publicados los siguientes
informes históricos:

{missing_block}

¿Sería posible que me los hicieran llegar por este canal o indicarme
dónde obtenerlos? Los necesito para un análisis de consistencia
histórica de la tesis y proceso del fondo — uso estrictamente
personal/interno.

Muchas gracias de antemano.

Un saludo,
{FROM_NAME}
"""
    return subject, body


def _write_eml(path: Path, to_addr: str, subject: str, body: str) -> None:
    date_hdr = format_datetime(datetime.now(timezone.utc))
    # Encode subject header if contains non-ASCII
    try:
        subject.encode("ascii")
        subject_hdr = subject
    except UnicodeEncodeError:
        from email.header import Header
        subject_hdr = Header(subject, "utf-8").encode()
    eml = (
        f"From: {FROM_NAME} <{FROM_EMAIL}>\r\n"
        f"To: {to_addr}\r\n"
        f"Subject: {subject_hdr}\r\n"
        f"Date: {date_hdr}\r\n"
        f"MIME-Version: 1.0\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Transfer-Encoding: 8bit\r\n"
        f"\r\n"
        f"{body}"
    )
    path.write_text(eml, encoding="utf-8")


def maybe_draft_request(state) -> dict | None:
    """
    Entry point. Llamado por discovery al final.
    Devuelve dict con info del borrador o None si no procede.
    """
    should, ar_miss, sar_miss, letters_miss = _should_draft(state)
    if not should:
        return None

    fund = state.identity.get("nombre_oficial") or state.isin
    gestora = state.identity.get("gestora_oficial") or "the management company"

    to_addr = _resolve_contact(state)
    if not to_addr:
        to_addr = "TODO@verify-manually.example"
        console.log("[yellow]email_agent: no contact resolved — usa placeholder[/yellow]")

    lang = "es" if state.isin.upper().startswith("ES") else "en"
    if lang == "es":
        subject, body = _template_es(fund, state.isin, gestora, ar_miss, sar_miss, letters_miss)
    else:
        subject, body = _template_en(fund, state.isin, gestora, ar_miss, sar_miss, letters_miss)

    eml_path = state.fund_dir / "email_request.eml"
    txt_path = state.fund_dir / "email_request.txt"
    _write_eml(eml_path, to_addr, subject, body)
    txt_path.write_text(
        f"To: {to_addr}\nSubject: {subject}\n\n{body}",
        encoding="utf-8",
    )

    console.log(f"[bold magenta]email drafted[/bold magenta]: {eml_path.name} "
                f"-> {to_addr}  (AR: {len(ar_miss)}, SAR: {len(sar_miss)}, letters: {len(letters_miss)})")
    return {
        "to": to_addr,
        "subject": subject,
        "missing_ar": ar_miss,
        "missing_sar": sar_miss,
        "missing_letters": letters_miss,
        "path": str(eml_path),
    }
