"""
cnmv_meta.py
Resuelve metadatos de un fondo desde la CNMV dado su ISIN.
Diseño: best-effort. Nunca bloquea el extractor principal.
"""
import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}

def resolve_isin(isin: str) -> dict:
    """
    Dado un ISIN devuelve dict con NIF y metadatos.
    Si algo falla devuelve campos en None. NUNCA lanza excepciones.
    """
    result = {
        "isin": isin, "nif": None, "nombre": None,
        "registro_cnmv": None, "gestora": None,
        "depositario": None, "fecha_creacion": None, "url_cnmv": None,
    }
    print(f"[cnmv_meta] Buscando {isin} en CNMV...")
    try:
        url = f"https://www.cnmv.es/portal/consultas/iic/fondo?isin={isin}&lang=es"
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        nif = _extract_nif(r.url, r.text)
        if nif:
            result["nif"] = nif
            result["url_cnmv"] = f"https://www.cnmv.es/portal/consultas/iic/fondo?nif={nif}&lang=es"
            print(f"[cnmv_meta] NIF: {nif}")
            _enrich(result, r.text)
        else:
            print("[cnmv_meta] NIF no encontrado — continuando sin él")
    except Exception as e:
        print(f"[cnmv_meta] Sin acceso a CNMV ({e}) — continuando")
    return result

def _extract_nif(url: str, html: str) -> str | None:
    for text in [url, html]:
        m = re.search(r"nif=([A-Z]\d{7,9})", text, re.IGNORECASE)
        if m:
            return m.group(1).upper()
    return None

def _enrich(result: dict, html: str):
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        if not result["nombre"]:
            t = soup.find("title")
            if t:
                parts = t.text.split(" - ")
                if len(parts) >= 3:
                    result["nombre"] = parts[2].strip()
        if not result["registro_cnmv"]:
            m = re.search(r"[Rr]egistro[:\s]+(\d{3,5})", text)
            if m:
                result["registro_cnmv"] = int(m.group(1))
        if not result["gestora"]:
            m = re.search(r"Gestora:\s*([^\n\r,]{5,80})", text)
            if m:
                result["gestora"] = m.group(1).strip()
        if not result["depositario"]:
            m = re.search(r"Depositario:\s*([^\n\r,]{5,80})", text)
            if m:
                result["depositario"] = m.group(1).strip()
        if not result["fecha_creacion"]:
            m = re.search(r"constituci[oó]n[:\s]+(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
            if m:
                d, mo, y = m.group(1).split("/")
                result["fecha_creacion"] = f"{y}-{mo}-{d}"
    except Exception:
        pass

def get_gestora_web(nif: str) -> str | None:
    if not nif:
        return None
    try:
        url = f"https://www.cnmv.es/portal/consultas/iic/fondo?nif={nif}&lang=es"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "cnmv.es" not in href and "google" not in href:
                return href.rstrip("/")
    except Exception:
        pass
    return None
