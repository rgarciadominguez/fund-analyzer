"""
cnmv_meta.py
Resuelve ISIN → NIF, nombre, gestora y URL gestora desde CNMV.
Sin JavaScript, sin Chrome. Usa requests + BeautifulSoup sobre la página pública.
"""

import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FundAnalyzer/1.0)",
    "Accept-Language": "es-ES,es;q=0.9"
}

def resolve_isin(isin: str) -> dict:
    """
    Dado un ISIN, devuelve:
    {
        "isin": "ES0112231008",
        "nif": "V87077459",
        "nombre": "AVANTAGE FUND, FI",
        "registro_cnmv": 4791,
        "gestora": "RENTA 4 GESTORA, S.G.I.I.C., S.A.",
        "depositario": "RENTA 4 BANCO, S.A.",
        "fecha_creacion": "2014-07-31",
        "url_cnmv": "https://www.cnmv.es/portal/consultas/iic/fondo?nif=V87077459"
    }
    """
    print(f"[cnmv_meta] Resolviendo ISIN {isin}...")

    # Intentar primero búsqueda directa por ISIN
    url = f"https://www.cnmv.es/portal/consultas/iic/fondo?isin={isin}&lang=es"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        r.raise_for_status()
        
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Extraer NIF de la URL final o de la página
        final_url = r.url
        nif_match = re.search(r"nif=([A-Z]\d+)", final_url)
        nif = nif_match.group(1) if nif_match else None
        
        # Intentar extraer datos básicos de la página
        result = {
            "isin": isin,
            "nif": nif,
            "nombre": None,
            "registro_cnmv": None,
            "gestora": None,
            "depositario": None,
            "fecha_creacion": None,
            "url_cnmv": f"https://www.cnmv.es/portal/consultas/iic/fondo?nif={nif}" if nif else None
        }
        
        # Buscar en texto de la página datos del fondo
        page_text = soup.get_text()
        
        # Registro CNMV
        reg_match = re.search(r"Nº\s*Registro\s*CNMV[:\s]*(\d+)", page_text, re.IGNORECASE)
        if not reg_match:
            reg_match = re.search(r"número\s*(\d+)\b", page_text, re.IGNORECASE)
        if reg_match:
            result["registro_cnmv"] = int(reg_match.group(1))
        
        print(f"[cnmv_meta] NIF: {nif}")
        return result

    except Exception as e:
        print(f"[cnmv_meta] Error resolviendo ISIN: {e}")
        return {
            "isin": isin,
            "nif": None,
            "nombre": None,
            "registro_cnmv": None,
            "gestora": None,
            "depositario": None,
            "fecha_creacion": None,
            "url_cnmv": None
        }


def get_gestora_url(nif: str) -> str | None:
    """
    Intenta extraer la URL de la web de la gestora/asesor desde el folleto CNMV.
    """
    # Buscar en hechos relevantes o datos generales
    urls_a_probar = [
        f"https://www.cnmv.es/portal/consultas/iic/fondo?nif={nif}&vista=0&lang=es",
    ]
    for url in urls_a_probar:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            # Buscar enlaces externos que no sean de la CNMV
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "cnmv.es" not in href:
                    return href
        except Exception:
            continue
    return None
