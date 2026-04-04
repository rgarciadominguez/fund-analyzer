"""
cartas.py
Extrae información cualitativa de las cartas semestrales de la gestora.
Usa web scraping para descubrir URLs de cartas + Claude API para extraer datos estructurados.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from anthropic import Anthropic

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FundAnalyzer/1.0)"}

client = Anthropic()

EXTRACTION_PROMPT = """Eres un analista de fondos de inversión. Tu tarea es extraer información estructurada de una carta semestral de un gestor de fondos.

Extrae SOLO lo siguiente en formato JSON estricto (sin markdown, sin texto extra):
{
  "periodo": "1H2024 o 2H2024 (semestre al que corresponde la carta)",
  "rv_pct": número o null (% renta variable mencionado explícitamente),
  "rf_pct": número o null (% renta fija mencionado explícitamente),
  "liquidez_pct": número o null (% liquidez/efectivo mencionado explícitamente),
  "geografia_esp_pct": número o null (% exposición España si se menciona),
  "rentabilidad_periodo_pct": número o null (rentabilidad del semestre del fondo),
  "hito_principal": "string corto (máx 120 chars) con el hito/decisión más importante del periodo",
  "vision_mercado": "string (máx 300 chars) con la visión del gestor sobre el mercado",
  "cambios_cartera": ["lista de cambios relevantes en cartera: entradas y salidas"],
  "empresas_mencionadas": ["lista de empresas mencionadas con contexto positivo"],
  "tono": "positivo|negativo|neutro"
}

Si un campo no aparece en el texto, pon null. No inventes datos.

CARTA A ANALIZAR:
"""


def find_cartas_url(gestora_web: str) -> str | None:
    """
    Dado el dominio de la gestora, encuentra la URL de la sección de cartas semestrales.
    """
    patrones = [
        f"{gestora_web}/category/cartas-semestrales/",
        f"{gestora_web}/cartas-semestrales/",
        f"{gestora_web}/informes/",
        f"{gestora_web}/category/cartas/",
        f"{gestora_web}/publicaciones/",
    ]
    for url in patrones:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200 and len(r.text) > 500:
                print(f"[cartas] Sección cartas encontrada: {url}")
                return url
        except Exception:
            continue
    return None


def get_carta_urls(seccion_url: str, max_cartas: int = 30) -> list[dict]:
    """
    Pagina la sección de cartas y devuelve lista de {titulo, url, fecha}.
    """
    cartas = []
    page_url = seccion_url
    
    while page_url and len(cartas) < max_cartas:
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Buscar artículos/posts con "carta" o "semestral" en el título
            for article in soup.find_all(["article", "div"], class_=re.compile(r"post|article|entry")):
                a_tag = article.find("a", href=True)
                if not a_tag:
                    continue
                
                href = a_tag["href"]
                title = a_tag.get_text(strip=True)
                
                if not any(w in title.lower() for w in ["carta", "semestral", "informe", "letter"]):
                    continue
                
                # Extraer fecha si existe
                date_el = article.find(["time", "span"], class_=re.compile(r"date|time|fecha"))
                fecha = date_el.get_text(strip=True) if date_el else None
                
                cartas.append({"titulo": title, "url": href, "fecha": fecha})
            
            # Buscar paginación
            next_page = soup.find("a", class_=re.compile(r"next|siguiente|older"))
            page_url = next_page["href"] if next_page and next_page.get("href") else None
            
        except Exception as e:
            print(f"[cartas] Error paginando {page_url}: {e}")
            break
    
    print(f"[cartas] {len(cartas)} cartas encontradas")
    return cartas


def get_carta_content(url: str) -> str | None:
    """
    Descarga el contenido de una carta (HTML o PDF).
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        
        content_type = r.headers.get("content-type", "")
        
        if "pdf" in content_type.lower() or url.lower().endswith(".pdf"):
            # PDF: devolver bytes para procesarlo con Claude directamente
            return f"[PDF_URL:{url}]"
        
        # HTML: extraer texto limpio
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Eliminar nav, footer, sidebar
        for tag in soup.find_all(["nav", "footer", "aside", "script", "style"]):
            tag.decompose()
        
        # Buscar el contenido principal
        main = soup.find(["article", "main", "div"], class_=re.compile(r"content|entry|post-body"))
        if main:
            return main.get_text(separator="\n", strip=True)
        
        return soup.get_text(separator="\n", strip=True)[:8000]
    
    except Exception as e:
        print(f"[cartas] Error descargando carta {url}: {e}")
        return None


def extract_carta_data(content: str, url: str) -> dict | None:
    """
    Usa Claude API para extraer datos estructurados de una carta.
    """
    import json

    if content.startswith("[PDF_URL:"):
        # Para PDFs, usar la URL directamente con Claude
        pdf_url = content[9:-1]
        prompt = f"{EXTRACTION_PROMPT}\n[Carta en PDF: {pdf_url}]\nPor favor analiza esta carta de la gestora de fondos."
        text_content = f"Carta disponible en PDF: {pdf_url}"
    else:
        # Truncar si es muy largo
        text_content = content[:6000]
        prompt = f"{EXTRACTION_PROMPT}\n{text_content}"
    
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        raw = response.content[0].text.strip()
        
        # Limpiar posibles marcadores markdown
        raw = re.sub(r"```json|```", "", raw).strip()
        
        data = json.loads(raw)
        data["fuente_url"] = url
        return data
    
    except json.JSONDecodeError as e:
        print(f"[cartas] Error parseando JSON de Claude: {e}")
        return None
    except Exception as e:
        print(f"[cartas] Error llamando a Claude API: {e}")
        return None


def extract_all_cartas(gestora_web: str, isin: str) -> dict:
    """
    Extrae toda la información cualitativa de las cartas semestrales.
    
    Returns:
    {
        "cartas_procesadas": int,
        "evolucion_por_periodo": [...],
        "vision_actual": {...},
        "exposicion_rv_narrativa": [...],
        "rentabilidades_desde_cartas": [...]
    }
    """
    print(f"[cartas] Extrayendo cartas de {gestora_web}...")
    
    # 1. Encontrar sección de cartas
    seccion_url = find_cartas_url(gestora_web)
    if not seccion_url:
        print(f"[cartas] No se encontró sección de cartas en {gestora_web}")
        return {"cartas_procesadas": 0, "evolucion_por_periodo": [], "vision_actual": None}
    
    # 2. Obtener lista de cartas
    carta_urls = get_carta_urls(seccion_url)
    if not carta_urls:
        return {"cartas_procesadas": 0, "evolucion_por_periodo": [], "vision_actual": None}
    
    # 3. Procesar cada carta
    evolucion = []
    rentabilidades = []
    exposicion_rv = []
    vision_actual = None
    
    for i, carta in enumerate(carta_urls):
        print(f"[cartas] Procesando carta {i+1}/{len(carta_urls)}: {carta['titulo']}")
        
        content = get_carta_content(carta["url"])
        if not content:
            continue
        
        data = extract_carta_data(content, carta["url"])
        if not data:
            continue
        
        # Construir hito de evolución
        if data.get("hito_principal"):
            evolucion.append({
                "periodo":     data.get("periodo", "desconocido"),
                "hito":        data["hito_principal"],
                "detalle":     data.get("vision_mercado", ""),
                "cambios":     data.get("cambios_cartera", []),
                "fuente":      {"documento": carta["titulo"], "url": carta["url"]}
            })
        
        # Rentabilidades
        if data.get("rentabilidad_periodo_pct") is not None and data.get("periodo"):
            rentabilidades.append({
                "periodo": data["periodo"],
                "pct":     data["rentabilidad_periodo_pct"],
                "fuente":  carta["url"]
            })
        
        # Exposición RV narrativa
        if data.get("rv_pct") is not None:
            exposicion_rv.append({
                "periodo":                data.get("periodo", "desconocido"),
                "rv_pct_aprox":           data["rv_pct"],
                "rf_pct":                 data.get("rf_pct"),
                "liquidez_pct":           data.get("liquidez_pct"),
                "geografia_esp_pct":      data.get("geografia_esp_pct"),
                "fuente":                 {"documento": carta["titulo"], "url": carta["url"]}
            })
        
        # La primera carta procesada (más reciente) es la visión actual
        if i == 0:
            vision_actual = {
                "texto":   data.get("vision_mercado", ""),
                "tono":    data.get("tono"),
                "fecha":   carta.get("fecha"),
                "fuente":  {"documento": carta["titulo"], "url": carta["url"]}
            }
        
        # Rate limiting: no saturar la API
        time.sleep(1)
    
    print(f"[cartas] Procesadas {len(carta_urls)} cartas")
    
    return {
        "cartas_procesadas":      len(carta_urls),
        "evolucion_por_periodo":  list(reversed(evolucion)),  # orden cronológico
        "vision_actual":          vision_actual,
        "exposicion_rv_narrativa": list(reversed(exposicion_rv)),
        "rentabilidades_desde_cartas": rentabilidades
    }
