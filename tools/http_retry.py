"""GET con reintentos para fetchers de datos (Morningstar, etc.).

Problema (audit MEDIO red silenciosa): los fetchers hacían `httpx.get` en un
try/except que devolvía vacío ante CUALQUIER fallo → un parpadeo de red (timeout,
429 rate-limit, 5xx) era indistinguible de "el fondo no tiene cobertura", dejaba
el dato a medias y NO reintentaba. Un run golpea Morningstar 6+ veces → 429 probable.

`get_json` reintenta los errores TRANSITORIOS (timeout/conexión/429/5xx) con backoff
(respeta `Retry-After`); los 4xx genuinos no se reintentan. Si tras los reintentos
sigue fallando, lanza `FetchError` (DISTINTO de "sin datos") para que el llamante
pueda distinguir y, si quiere, marcar el dato como incompleto-por-error.
"""
from __future__ import annotations

import time

import httpx

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FetchError(Exception):
    """La descarga falló tras agotar los reintentos (error de red, no 'sin datos')."""


def get_json(url: str, headers: dict | None = None, timeout: float = 20.0,
             retries: int = 3, backoff: float = 1.5, max_wait: float = 12.0):
    """GET → JSON con reintentos en errores transitorios. Lanza FetchError si, tras
    `retries` intentos, no se pudo. Un 4xx (salvo 429) NO se reintenta (es genuino)."""
    last = ""
    for attempt in range(retries):
        try:
            r = httpx.get(url, headers=headers, timeout=timeout, follow_redirects=True)
            if r.status_code in _RETRYABLE_STATUS:
                last = f"HTTP {r.status_code}"
                ra = r.headers.get("Retry-After", "")
                try:
                    wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else backoff * (attempt + 1)
                except Exception:
                    wait = backoff * (attempt + 1)
                if attempt < retries - 1:
                    time.sleep(min(wait, max_wait))
                continue
            r.raise_for_status()       # 4xx no-retryable → HTTPStatusError
            return r.json()
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError,
                httpx.RemoteProtocolError, httpx.PoolTimeout) as e:
            last = type(e).__name__
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
            continue
        except httpx.HTTPStatusError as e:
            # 4xx genuino (404, 403...) → no reintentar; es "no hay / no accesible"
            raise FetchError(f"HTTP {e.response.status_code}") from e
        except ValueError as e:        # JSON inválido
            last = f"json:{str(e)[:40]}"
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
            continue
    raise FetchError(f"agotados {retries} intentos: {last}")
