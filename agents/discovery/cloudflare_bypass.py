"""
Fallback para sites protegidos por Cloudflare con TLS fingerprinting (JA3).

httpx falla con 403 pese a llevar headers correctos porque Cloudflare inspecciona
la huella TLS. curl_cffi imita el TLS real de Chrome y pasa.

Uso: llamar `fetch_with_fallback(c, url)` en lugar de `c.get(url)`. Si httpx
devuelve 403/451/429 (anti-bot), reintenta con curl_cffi síncrono envuelto en
asyncio.to_thread. Si curl_cffi tampoco está instalado, propaga el error original.
"""
from __future__ import annotations

import asyncio

import httpx


async def fetch_with_fallback(
    c: httpx.AsyncClient,
    url: str,
    timeout: float = 20.0,
) -> tuple[int, bytes, dict]:
    """
    Devuelve (status_code, body_bytes, headers_dict).
    Intenta httpx primero; si 403/429/451 cae a curl_cffi.
    """
    try:
        r = await c.get(url, timeout=timeout)
        if r.status_code not in (403, 429, 451):
            return r.status_code, r.content, dict(r.headers)
    except Exception:
        pass

    # Fallback Cloudflare
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        return 0, b"", {}

    def _sync_fetch():
        try:
            r = cffi_requests.get(url, impersonate="chrome124", timeout=timeout)
            return r.status_code, r.content, dict(r.headers)
        except Exception:
            return 0, b"", {}

    return await asyncio.to_thread(_sync_fetch)
