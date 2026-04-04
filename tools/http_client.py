"""
Async HTTP client with retry, backoff, cookie persistence, and browser-like headers.
"""
import asyncio
import httpx
from rich.console import Console

console = Console()

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

TIMEOUT_HTML = 30.0
TIMEOUT_PDF = 120.0
BACKOFF_DELAYS = [1, 2, 4]  # seconds between retries

# Shared cookie jar persists across all calls in a process session
_shared_cookies = httpx.Cookies()


async def _request(
    url: str,
    extra_headers: dict | None = None,
    as_bytes: bool = False,
) -> str | bytes:
    """Core GET with retry/backoff. Used by all public functions."""
    headers = {**DEFAULT_HEADERS, **(extra_headers or {})}
    timeout = TIMEOUT_PDF if as_bytes else TIMEOUT_HTML

    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=timeout,
        cookies=_shared_cookies,
    ) as client:
        last_exc: Exception | None = None
        for attempt, delay in enumerate(BACKOFF_DELAYS, 1):
            try:
                response = await client.get(url)
                response.raise_for_status()
                _shared_cookies.update(response.cookies)
                return response.content if as_bytes else response.text
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt < len(BACKOFF_DELAYS):
                    console.log(
                        f"[yellow]Intento {attempt}/{len(BACKOFF_DELAYS)} fallido "
                        f"para {url}: {exc}. Reintentando en {delay}s..."
                    )
                    await asyncio.sleep(delay)
        raise last_exc


async def get(url: str) -> str:
    """GET → HTML/text. Retries 3x with exponential backoff."""
    return await _request(url)


async def get_bytes(url: str) -> bytes:
    """GET → bytes (PDFs). Timeout extendido a 120s."""
    return await _request(url, as_bytes=True)


async def get_with_headers(url: str, headers: dict) -> str:
    """GET con cabeceras adicionales fusionadas sobre las por defecto."""
    return await _request(url, extra_headers=headers)


async def post_form(url: str, data: dict) -> str:
    """POST de formulario (application/x-www-form-urlencoded). Retries 3x."""
    headers = {**DEFAULT_HEADERS}
    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=TIMEOUT_HTML,
        cookies=_shared_cookies,
    ) as client:
        last_exc: Exception | None = None
        for attempt, delay in enumerate(BACKOFF_DELAYS, 1):
            try:
                response = await client.post(url, data=data)
                response.raise_for_status()
                _shared_cookies.update(response.cookies)
                return response.text
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt < len(BACKOFF_DELAYS):
                    console.log(
                        f"[yellow]Intento POST {attempt}/{len(BACKOFF_DELAYS)} fallido "
                        f"para {url}: {exc}. Reintentando en {delay}s..."
                    )
                    await asyncio.sleep(delay)
        raise last_exc


def clear_cookies() -> None:
    """Limpia el jar de cookies (útil entre sesiones de fondos distintos)."""
    _shared_cookies.clear()
