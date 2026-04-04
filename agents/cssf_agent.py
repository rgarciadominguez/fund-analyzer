"""
CSSF Agent — Regulador luxemburgués (fondos LU)

Obtiene datos de registro del regulador CSSF (Commission de Surveillance du
Secteur Financier) para fondos con prefijo ISIN LU.

Complementa intl_agent (que extrae datos financieros del annual report).
Datos disponibles: nombre oficial, gestora, depositario, fecha autorización, estado.

Portal CSSF: https://funds.cssf.lu
Búsqueda por ISIN: https://funds.cssf.lu/en/search/?isin={ISIN}

Estrategia de extracción (en orden):
  1. HTML del portal funds.cssf.lu/en/search/?isin={ISIN} → BeautifulSoup
  2. API JSON si existe: /api/ucits/search?isin={ISIN} u otras rutas conocidas
  3. Página de detalle del fondo si hay enlace en los resultados
  4. Fallback → JSON vacío con estado "unknown", pipeline continúa

Convenciones del proyecto:
  - Clase Python con async def run(self) -> dict
  - Logging con rich.console + progress.log
  - Guarda output en data/funds/{ISIN}/cssf_data.json
  - Nunca bloquea el pipeline
"""
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.http_client import get, get_with_headers

console = Console()

# ── Constantes CSSF ──────────────────────────────────────────────────────────
CSSF_BASE = "https://funds.cssf.lu"
CSSF_SEARCH_URL = "https://funds.cssf.lu/en/search/?isin={isin}"

# Posibles rutas de API JSON del portal CSSF (exploradas en orden)
CSSF_API_CANDIDATES = [
    "https://funds.cssf.lu/api/ucits/search?isin={isin}",
    "https://funds.cssf.lu/api/funds/search?isin={isin}",
    "https://funds.cssf.lu/api/search?isin={isin}&lang=en",
    "https://funds.cssf.lu/en/api/search?isin={isin}",
]

# Headers específicos para el portal CSSF (acepta JSON)
CSSF_HEADERS = {
    "Accept": "application/json, text/html, */*",
    "Referer": "https://funds.cssf.lu/",
    "X-Requested-With": "XMLHttpRequest",
}


class CSSFAgent:
    """
    Agente para fondos luxemburgueses (ISIN prefijo LU).
    Consulta el regulador CSSF para obtener datos de registro oficiales.
    Clase con async def run() -> dict según convenio del proyecto.
    """

    def __init__(self, isin: str, config: dict):
        self.isin = isin.strip().upper()
        self.config = config

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

        self.log_path = root / "progress.log"
        self._log = self._make_logger()

    # ── Logger ───────────────────────────────────────────────────────────────

    def _make_logger(self):
        log_path = self.log_path

        def _log(level: str, msg: str):
            ts = datetime.now().strftime("%H:%M:%S")
            line = f"[{ts}] [CSSF] [{level}] {msg}"
            console.log(line)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

        return _log

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> dict:
        """Orquesta la búsqueda CSSF y devuelve cssf_data completo."""
        console.print(Panel(
            f"[bold cyan]CSSF Agent[/bold cyan]\nISIN: [green]{self.isin}[/green]",
            expand=False,
        ))

        self._log("START", f"Consultando CSSF para {self.isin}")

        # Plantilla de resultado vacío
        result = self._empty_result()

        try:
            # Estrategia 1: API JSON del portal CSSF
            api_data = await self._try_api_endpoints()
            if api_data:
                result = self._merge_api_data(result, api_data)
                self._log("OK", f"Datos obtenidos via API JSON CSSF: {result.get('nombre_oficial', '?')}")
            else:
                # Estrategia 2: HTML del portal de búsqueda CSSF
                html_data = await self._scrape_search_page()
                if html_data:
                    result = self._merge_api_data(result, html_data)
                    self._log("OK", f"Datos obtenidos via HTML CSSF: {result.get('nombre_oficial', '?')}")
                else:
                    self._log("WARN", "Portal CSSF no accesible — probando fuentes alternativas")

            # Estrategia 3: Página de detalle si tenemos URL
            if result.get("url_detalle") and not result.get("depositario"):
                detail_data = await self._scrape_detail_page(result["url_detalle"])
                if detail_data:
                    for k, v in detail_data.items():
                        if v and not result.get(k):
                            result[k] = v
                    self._log("OK", "Datos adicionales desde página de detalle CSSF")

            # Estrategia 4: OpenFIGI (fallback gratuito, no requiere API key)
            # Cubre casos donde CSSF no es accesible desde el cliente
            if not result.get("nombre_oficial"):
                openfigi_data = await self._try_openfigi()
                if openfigi_data:
                    result = self._merge_api_data(result, openfigi_data)
                    self._log("OK", f"Datos obtenidos via OpenFIGI: {result.get('nombre_oficial', '?')}")
                else:
                    self._log("WARN", "No se pudieron obtener datos — resultado parcial")

        except Exception as exc:
            self._log("ERROR", f"Error inesperado en CSSFAgent: {exc}")
            import traceback
            self._log("TRACE", traceback.format_exc()[:400])

        # Actualizar timestamp y guardar
        result["ultima_actualizacion"] = datetime.now().isoformat()
        self._save(result)

        return result

    # ── Estrategia 1: API JSON ────────────────────────────────────────────────

    async def _try_api_endpoints(self) -> dict | None:
        """
        Prueba posibles endpoints de API JSON del portal CSSF.
        Retorna el primer dict con datos válidos, o None si ninguno funciona.
        """
        for url_tpl in CSSF_API_CANDIDATES:
            url = url_tpl.format(isin=self.isin)
            try:
                self._log("INFO", f"Probando API: {url}")
                html = await get_with_headers(url, CSSF_HEADERS)

                # Intentar parsear como JSON
                try:
                    data = json.loads(html)
                    parsed = self._parse_api_response(data)
                    if parsed.get("nombre_oficial") or parsed.get("gestora_oficial"):
                        self._log("OK", f"API JSON funciona: {url}")
                        parsed["url_cssf"] = url
                        return parsed
                except (json.JSONDecodeError, ValueError):
                    # No es JSON → ignorar
                    pass

            except Exception as exc:
                self._log("DEBUG", f"API {url} falló: {type(exc).__name__}")
                continue

        return None

    def _parse_api_response(self, data) -> dict:
        """
        Parsea la respuesta JSON de la API CSSF.
        Maneja múltiples estructuras posibles (array de resultados, dict, etc.).
        """
        result = {}

        # Si es lista → tomar el primer elemento que coincida con el ISIN
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    isin_val = (
                        item.get("isin") or item.get("ISIN") or
                        item.get("isinCode") or item.get("isin_code") or ""
                    )
                    if isin_val.upper() == self.isin:
                        data = item
                        break
            else:
                # Tomar el primero si no hay coincidencia exacta
                if data:
                    data = data[0]

        if not isinstance(data, dict):
            return result

        # Intentar mapear campos comunes de APIs de reguladores financieros
        field_map = {
            "nombre_oficial": [
                "fundName", "fund_name", "name", "nombre", "denomination",
                "legalName", "legal_name", "fundDenomination",
            ],
            "gestora_oficial": [
                "managementCompany", "management_company", "gestora",
                "manCoName", "mancoName", "manager", "managementCompanyName",
            ],
            "depositario": [
                "depositary", "depositary_name", "depositoryBank",
                "custodian", "custodianBank",
            ],
            "fecha_autorizacion": [
                "authorizationDate", "authorization_date", "approvalDate",
                "registrationDate", "inceptionDate",
            ],
            "estado": [
                "status", "fundStatus", "fund_status", "state",
            ],
            "tipo_fondo": [
                "fundType", "fund_type", "type", "structure",
                "legalForm", "legal_form",
            ],
        }

        for target_key, source_keys in field_map.items():
            for src in source_keys:
                val = data.get(src)
                if val is not None and str(val).strip():
                    result[target_key] = str(val).strip()
                    break

        # Normalizar estado
        estado_raw = result.get("estado", "").lower()
        if "active" in estado_raw or "actif" in estado_raw or "authorized" in estado_raw:
            result["estado"] = "active"
        elif "liquidat" in estado_raw or "cancel" in estado_raw or "revok" in estado_raw:
            result["estado"] = "liquidated"
        elif result.get("estado"):
            result["estado"] = "unknown"

        # URL del fondo si existe
        for url_key in ["url", "link", "detailUrl", "detail_url", "href"]:
            val = data.get(url_key)
            if val and isinstance(val, str) and val.startswith("http"):
                result["url_detalle"] = val
                break

        return result

    # ── Estrategia 2: Scraping HTML ───────────────────────────────────────────

    async def _scrape_search_page(self) -> dict | None:
        """
        Hace scraping del portal web de búsqueda CSSF.
        URL: https://funds.cssf.lu/en/search/?isin={isin}
        """
        url = CSSF_SEARCH_URL.format(isin=self.isin)
        result = {"url_cssf": url}

        try:
            self._log("INFO", f"Scraping HTML: {url}")
            html = await get(url)

            if not html or len(html) < 200:
                self._log("WARN", "Respuesta HTML vacía o muy corta")
                return None

            soup = BeautifulSoup(html, "lxml")

            # ── Buscar resultados de búsqueda ──────────────────────────────
            # El portal CSSF usa distintos patrones de markup según la versión

            # Patrón 1: tabla de resultados
            parsed = self._parse_html_table(soup)
            if parsed:
                result.update(parsed)

            # Patrón 2: cards / divs de resultados
            if not result.get("nombre_oficial"):
                parsed = self._parse_html_cards(soup)
                if parsed:
                    result.update(parsed)

            # Patrón 3: JSON embebido en la página (window.__data__ o similar)
            if not result.get("nombre_oficial"):
                parsed = self._parse_embedded_json(html)
                if parsed:
                    result.update(parsed)

            if result.get("nombre_oficial") or result.get("gestora_oficial"):
                return result

            self._log("WARN", f"No se encontraron datos estructurados en HTML de {url}")
            return None

        except Exception as exc:
            self._log("WARN", f"Error scraping {url}: {exc}")
            return None

    def _parse_html_table(self, soup: BeautifulSoup) -> dict:
        """Extrae datos de tablas de resultados HTML."""
        result = {}

        # Buscar tablas con datos de fondos
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue

                # Buscar la fila del fondo por ISIN
                row_text = row.get_text(" ", strip=True)
                if self.isin in row_text:
                    # Intentar extraer nombre y gestora de las celdas
                    cell_texts = [c.get_text(strip=True) for c in cells]

                    # El nombre suele ser la primera celda no-ISIN
                    for i, text in enumerate(cell_texts):
                        if text and text != self.isin and len(text) > 5:
                            if not result.get("nombre_oficial"):
                                result["nombre_oficial"] = text
                            elif not result.get("gestora_oficial"):
                                result["gestora_oficial"] = text
                                break

                    # Buscar link al detalle del fondo
                    link = row.find("a", href=True)
                    if link:
                        href = link["href"]
                        full_url = href if href.startswith("http") else urljoin(CSSF_BASE, href)
                        result["url_detalle"] = full_url

        return result

    def _parse_html_cards(self, soup: BeautifulSoup) -> dict:
        """Extrae datos de cards o divs de resultados."""
        result = {}

        # Patrones de selectores CSS comunes en portales de reguladores
        selectors = [
            ".fund-result", ".search-result", ".result-item",
            "[data-isin]", "[data-fund]",
            ".fund-card", ".fund-item",
        ]

        for selector in selectors:
            items = soup.select(selector)
            for item in items:
                item_text = item.get_text(" ", strip=True)
                if self.isin in item_text:
                    # Buscar campos con labels conocidos
                    labels = item.find_all(["dt", "th", "label", "strong", "b"])
                    for label in labels:
                        label_text = label.get_text(strip=True).lower()
                        # El valor suele estar en el siguiente sibling
                        value_el = label.find_next_sibling()
                        if not value_el:
                            continue
                        value = value_el.get_text(strip=True)
                        if not value:
                            continue

                        if any(k in label_text for k in ["name", "fund", "denomination", "nom"]):
                            if not result.get("nombre_oficial"):
                                result["nombre_oficial"] = value
                        elif any(k in label_text for k in ["management", "manager", "gestora", "gérant"]):
                            if not result.get("gestora_oficial"):
                                result["gestora_oficial"] = value
                        elif any(k in label_text for k in ["deposit", "custod"]):
                            if not result.get("depositario"):
                                result["depositario"] = value
                        elif any(k in label_text for k in ["authoriz", "approv", "date"]):
                            if not result.get("fecha_autorizacion"):
                                result["fecha_autorizacion"] = value
                        elif "status" in label_text or "état" in label_text:
                            if not result.get("estado"):
                                result["estado"] = value

                    # Link al detalle
                    link = item.find("a", href=True)
                    if link and not result.get("url_detalle"):
                        href = link["href"]
                        result["url_detalle"] = (
                            href if href.startswith("http") else urljoin(CSSF_BASE, href)
                        )

                    if result.get("nombre_oficial"):
                        return result

        return result

    def _parse_embedded_json(self, html: str) -> dict:
        """
        Busca JSON embebido en el HTML de la página.
        Algunos portales SPA inyectan datos en window.__data__, __NEXT_DATA__, etc.
        """
        result = {}

        patterns = [
            r'window\.__data__\s*=\s*(\{.*?\});',
            r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
            r'__NEXT_DATA__["\']?\s*[=:]\s*(\{.*?\})',
            r'window\.APP_DATA\s*=\s*(\{.*?\});',
            r'"funds?"\s*:\s*(\[.*?\])',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
            for match in matches:
                try:
                    data = json.loads(match)
                    # Buscar el ISIN en los datos
                    data_str = json.dumps(data)
                    if self.isin in data_str:
                        # Búsqueda recursiva del ISIN
                        found = self._find_isin_in_json(data, self.isin)
                        if found:
                            parsed = self._parse_api_response(found)
                            if parsed:
                                result.update(parsed)
                                return result
                except (json.JSONDecodeError, Exception):
                    continue

        return result

    def _find_isin_in_json(self, data, isin: str):
        """Busca recursivamente un objeto que contenga el ISIN dado."""
        if isinstance(data, dict):
            # Comprobar si este dict tiene el ISIN
            for v in data.values():
                if isinstance(v, str) and v.upper() == isin.upper():
                    return data
            # Buscar en valores
            for v in data.values():
                found = self._find_isin_in_json(v, isin)
                if found:
                    return found
        elif isinstance(data, list):
            for item in data:
                found = self._find_isin_in_json(item, isin)
                if found:
                    return found
        return None

    # ── Estrategia 3: Página de detalle ──────────────────────────────────────

    async def _scrape_detail_page(self, url: str) -> dict:
        """
        Hace scraping de la página de detalle del fondo en el portal CSSF.
        Extrae depositario, fecha autorización y otros campos adicionales.
        """
        result = {}
        try:
            self._log("INFO", f"Scraping página de detalle: {url}")
            html = await get(url)
            if not html:
                return result

            soup = BeautifulSoup(html, "lxml")

            # Buscar pares label-valor en la página de detalle
            # Patrón DL/DT/DD (common en portales de reguladores)
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    label = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    if not value:
                        continue

                    if any(k in label for k in ["deposit", "custod", "dépositaire"]):
                        result["depositario"] = value
                    elif any(k in label for k in ["authoriz", "date auth", "approv"]):
                        result["fecha_autorizacion"] = value
                    elif any(k in label for k in ["management", "gestiona", "gérant"]):
                        if not result.get("gestora_oficial"):
                            result["gestora_oficial"] = value
                    elif "status" in label or "état" in label:
                        result["estado"] = value
                    elif any(k in label for k in ["type", "structure", "form"]):
                        result["tipo_fondo"] = value

            # Tablas con información detallada
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) == 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if not value:
                            continue

                        if any(k in label for k in ["deposit", "custod"]):
                            if not result.get("depositario"):
                                result["depositario"] = value
                        elif any(k in label for k in ["authoriz", "date"]):
                            if not result.get("fecha_autorizacion"):
                                result["fecha_autorizacion"] = value

        except Exception as exc:
            self._log("WARN", f"Error en página de detalle: {exc}")

        return result

    # ── Estrategia 4: OpenFIGI ────────────────────────────────────────────────

    async def _try_openfigi(self) -> dict | None:
        """
        Consulta la API gratuita de OpenFIGI para obtener nombre y gestora del fondo.
        No requiere API key. Cubre casos donde el portal CSSF no es accesible.

        Documentación: https://www.openfigi.com/api
        """
        import httpx
        url = "https://api.openfigi.com/v3/mapping"
        payload = [{"idType": "ID_ISIN", "idValue": self.isin}]

        try:
            self._log("INFO", f"Consultando OpenFIGI para {self.isin}")
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                r = await client.post(
                    url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0",
                    }
                )
                r.raise_for_status()
                data = r.json()

            # OpenFIGI devuelve [{data: [{figi, name, ticker, ...}]}]
            if not data or not isinstance(data, list):
                return None

            items = data[0].get("data", [])
            if not items:
                self._log("WARN", f"OpenFIGI: no se encontraron resultados para {self.isin}")
                return None

            # Tomar el primer resultado (generalmente la clase principal)
            item = items[0]
            nombre = item.get("name", "")
            ticker = item.get("ticker", "")
            exch = item.get("exchCode", "")

            if not nombre:
                return None

            self._log("OK", f"OpenFIGI: {nombre} ({ticker}, {exch})")
            return {
                "nombre_oficial": nombre,
                "estado": "active",  # si está en OpenFIGI es activo
                "fuente_nombre": "OpenFIGI",
            }

        except Exception as exc:
            self._log("WARN", f"OpenFIGI no disponible: {exc}")
            return None

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _empty_result(self) -> dict:
        """Retorna la plantilla de resultado vacío."""
        return {
            "isin": self.isin,
            "ultima_actualizacion": "",
            "nombre_oficial": None,
            "gestora_oficial": None,
            "depositario": None,
            "fecha_autorizacion": None,
            "estado": "unknown",
            "pais_domicilio": "Luxembourg",
            "tipo_fondo": "UCITS",
            "url_cssf": CSSF_SEARCH_URL.format(isin=self.isin),
            "documentos": [],
        }

    def _merge_api_data(self, base: dict, override: dict) -> dict:
        """Fusiona datos obtenidos con la plantilla base."""
        for k, v in override.items():
            if v is not None and str(v).strip():
                base[k] = v
        return base

    def _save(self, result: dict) -> None:
        """Guarda el resultado en cssf_data.json."""
        output_path = self.fund_dir / "cssf_data.json"
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._log("OK", f"cssf_data.json guardado en {output_path}")


# ── Ejecución standalone ─────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    ISIN = "LU0840158819"
    agent = CSSFAgent(ISIN, {})
    result = asyncio.run(agent.run())

    console.print(Panel(
        f"[bold green]CSSF Agent completado[/bold green]\n"
        f"ISIN: {result.get('isin', '-')}\n"
        f"Nombre oficial: {result.get('nombre_oficial', '-')}\n"
        f"Gestora: {result.get('gestora_oficial', '-')}\n"
        f"Depositario: {result.get('depositario', '-')}\n"
        f"Estado: {result.get('estado', '-')}\n"
        f"Fecha autorización: {result.get('fecha_autorizacion', '-')}",
        title=ISIN,
        expand=False,
    ))
