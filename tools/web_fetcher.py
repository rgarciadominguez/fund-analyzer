"""
Robust web fetcher with escalation strategy and disk cache.

Strategy (escalates on failure):
  1. httpx (async, fast, browser-like headers)
  2. cloudscraper (Cloudflare bypass)
  3. Playwright headless (optional, JS-heavy pages)

Cache: data/cache/fetches/{sha256(url)[:16]}.json — TTL 365 days.
"""
import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console

console = Console()

ROOT = Path(__file__).parent.parent
CACHE_DIR = ROOT / "data" / "cache" / "fetches"
CACHE_TTL_DAYS = 365


@dataclass
class FetchResult:
    url: str
    text: str = ""
    html: str = ""
    ok: bool = False
    status_code: int = 0
    error: str = ""
    fetched_at: str = ""
    method: str = ""


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _html_to_text(html: str, max_chars: int = 15000) -> str:
    """Extract readable text from HTML, removing scripts/styles/nav."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # Collapse multiple blank lines
    lines = [l.strip() for l in text.splitlines()]
    text = "\n".join(l for l in lines if l)
    return text[:max_chars]


# ═══════════════════════════════════════════════════════════════
# Cache
# ═══════════════════════════════════════════════════════════════

def _cache_path(url: str) -> Path:
    return CACHE_DIR / f"{_url_hash(url)}.json"


def _cache_read(url: str) -> FetchResult | None:
    """Read from cache if exists and not expired."""
    p = _cache_path(url)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        fetched_at = data.get("fetched_at", "")
        if fetched_at:
            from datetime import datetime as dt
            age_days = (dt.now() - dt.fromisoformat(fetched_at)).days
            if age_days > CACHE_TTL_DAYS:
                return None  # expired
        return FetchResult(**{k: v for k, v in data.items() if k in FetchResult.__dataclass_fields__})
    except Exception:
        return None


def _cache_write(result: FetchResult) -> None:
    """Persist FetchResult to disk cache."""
    if not result.ok:
        return  # don't cache failures
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(result.url)
    data = asdict(result)
    # Don't store full HTML in cache (save disk) — keep text + metadata
    data["html"] = data["html"][:5000] if data["html"] else ""
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
# Fetchers
# ═══════════════════════════════════════════════════════════════

async def _fetch_httpx(url: str, max_chars: int) -> FetchResult:
    """Fetch with httpx (async, retries, browser headers)."""
    from tools.http_client import get as http_get
    try:
        html = await http_get(url)
        text = _html_to_text(html, max_chars)
        return FetchResult(
            url=url, text=text, html=html[:50000], ok=bool(text and len(text) > 50),
            status_code=200, fetched_at=datetime.now().isoformat(), method="httpx",
        )
    except Exception as exc:
        code = 0
        if hasattr(exc, "response") and exc.response is not None:
            code = exc.response.status_code
        return FetchResult(
            url=url, ok=False, status_code=code,
            error=f"httpx: {str(exc)[:200]}", method="httpx",
        )


def _fetch_cloudscraper(url: str, max_chars: int) -> FetchResult:
    """Fetch with cloudscraper (Cloudflare bypass, sync)."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        resp = scraper.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text
        text = _html_to_text(html, max_chars)
        return FetchResult(
            url=url, text=text, html=html[:50000], ok=bool(text and len(text) > 50),
            status_code=resp.status_code, fetched_at=datetime.now().isoformat(),
            method="cloudscraper",
        )
    except Exception as exc:
        return FetchResult(
            url=url, ok=False, error=f"cloudscraper: {str(exc)[:200]}",
            method="cloudscraper",
        )


async def _fetch_playwright(url: str, max_chars: int) -> FetchResult:
    """Fetch with Playwright headless Chromium (JS-heavy pages)."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return FetchResult(
            url=url, ok=False, error="playwright not installed",
            method="playwright",
        )

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                )
            )
            await page.goto(url, wait_until="networkidle", timeout=30000)
            html = await page.content()
            await browser.close()

        text = _html_to_text(html, max_chars)
        return FetchResult(
            url=url, text=text, html=html[:50000], ok=bool(text and len(text) > 50),
            status_code=200, fetched_at=datetime.now().isoformat(),
            method="playwright",
        )
    except Exception as exc:
        return FetchResult(
            url=url, ok=False, error=f"playwright: {str(exc)[:200]}",
            method="playwright",
        )


# ═══════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════

async def fetch_url(
    url: str,
    use_browser: bool = False,
    max_chars: int = 15000,
    force_refresh: bool = False,
) -> FetchResult:
    """Fetch URL with escalation strategy. Uses disk cache (365d TTL).

    Args:
        url: URL to fetch
        use_browser: If True, allows Playwright as last resort
        max_chars: Max chars of extracted text
        force_refresh: Skip cache even if valid

    Returns:
        FetchResult with .ok, .text, .error, .method
    """
    if not force_refresh:
        cached = _cache_read(url)
        if cached:
            return cached

    # Step 1: httpx
    result = await _fetch_httpx(url, max_chars)
    if result.ok:
        _log_fetch(result)
        _cache_write(result)
        return result

    # Step 2: cloudscraper (sync, run in thread)
    result = await asyncio.to_thread(_fetch_cloudscraper, url, max_chars)
    if result.ok:
        _log_fetch(result)
        _cache_write(result)
        return result

    # Step 3: Playwright (optional)
    if use_browser:
        result = await _fetch_playwright(url, max_chars)
        if result.ok:
            _log_fetch(result)
            _cache_write(result)
            return result

    # All failed
    _log_fetch(result)
    return result


async def fetch_pdf_bytes(url: str) -> bytes | None:
    """Download PDF as bytes. Uses http_client with extended timeout."""
    try:
        from tools.http_client import get_bytes
        return await get_bytes(url)
    except Exception as exc:
        console.log(f"[red][FETCH] PDF FAIL {url[:60]}: {exc}")
        return None


def _log_fetch(result: FetchResult) -> None:
    if result.ok:
        console.log(
            f"[green][FETCH] OK {result.method} {result.url[:70]} "
            f"({len(result.text)} chars)"
        )
    else:
        console.log(
            f"[red][FETCH] FAIL {result.method} {result.url[:70]} "
            f"-- {result.error[:80]}"
        )
