"""Extracción de transcripts de vídeos de YouTube (subtítulos auto/manuales).

Sin coste y sin API key (usa `youtube-transcript-api`). Convierte una URL de
YouTube en el TEXTO HABLADO del vídeo, para que `readings_collector` pueda
ingerir el contenido de entrevistas/podcasts/webinars de los gestores en vez
de solo el metadato (título de la página).

Diseño defensivo:
- Si la librería no está instalada → devuelve None (el caller cae al fetch HTML
  normal, sin regresión).
- Si el vídeo no tiene subtítulos / están desactivados / falla la red → None.
- Multi-idioma: prueba una lista de idiomas preferidos y, si no, cualquiera
  disponible (incluidos auto-generados). El extractor downstream ya es
  multi-idioma (K22), así que devolvemos el transcript en su idioma original.

Uso:
    from tools.youtube_transcript import is_video_url, get_transcript
    if is_video_url(url):
        texto = get_transcript(url)  # str o None
"""
from __future__ import annotations

import re
from typing import Optional

# Idiomas preferidos (orden de preferencia). Si ninguno, se intenta cualquiera.
PREFERRED_LANGS = ["es", "en", "fr", "de", "it", "pt", "nl", "ca"]

# Tope de longitud del transcript devuelto (coherente con el fetch HTML, que
# corta a 20000). Una entrevista de 1h son ~9-10k palabras; 20k chars cubre lo
# relevante sin disparar tokens downstream.
MAX_CHARS = 20000

_YT_HOST_RE = re.compile(r"(?:^|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)$", re.I)


def is_video_url(url: str) -> bool:
    """True si la URL es de YouTube (único proveedor con transcript-api gratis)."""
    if not url or not isinstance(url, str):
        return False
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return bool(_YT_HOST_RE.search(host))


def extract_video_id(url: str) -> Optional[str]:
    """Extrae el video_id de las formas comunes de URL de YouTube."""
    if not url:
        return None
    try:
        from urllib.parse import urlparse, parse_qs
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.endswith("youtu.be"):
            vid = p.path.lstrip("/").split("/")[0]
            return vid or None
        # youtube.com/watch?v=ID
        qs = parse_qs(p.query)
        if "v" in qs and qs["v"]:
            return qs["v"][0]
        # /embed/ID, /shorts/ID, /v/ID, /live/ID
        m = re.match(r"^/(?:embed|shorts|v|live)/([^/?&]+)", p.path)
        if m:
            return m.group(1)
    except Exception:
        return None
    return None


def get_transcript(url: str, languages: Optional[list[str]] = None,
                   max_chars: int = MAX_CHARS) -> Optional[str]:
    """Devuelve el texto del transcript de un vídeo de YouTube, o None.

    Robusto frente a versiones de la librería (0.6.x API estática vs 1.x API de
    instancia) y a ausencia de subtítulos. Nunca lanza: cualquier fallo → None.
    """
    video_id = extract_video_id(url)
    if not video_id:
        return None
    langs = languages or PREFERRED_LANGS

    try:
        from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore
    except Exception:
        # Librería no instalada → el caller usará el fetch HTML normal.
        return None

    snippets = _fetch_snippets(YouTubeTranscriptApi, video_id, langs)
    if not snippets:
        return None

    # snippets: lista de dicts {"text": ...} (0.6.x) u objetos con .text (1.x)
    parts: list[str] = []
    for s in snippets:
        t = s.get("text") if isinstance(s, dict) else getattr(s, "text", None)
        if t:
            parts.append(str(t).strip())
    text = " ".join(p for p in parts if p)
    # Normalizar espacios y saltos de auto-captions
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if not text or len(text) < 50:
        return None
    return text[:max_chars]


def _fetch_snippets(api, video_id: str, langs: list[str]):
    """Obtiene la lista de snippets probando ambas APIs de la librería.

    Devuelve lista (dicts u objetos con .text) o None.
    """
    # --- API estática 0.6.x: get_transcript(...) ---
    get_transcript_fn = getattr(api, "get_transcript", None)
    if callable(get_transcript_fn):
        try:
            return get_transcript_fn(video_id, languages=langs)
        except Exception:
            pass
        # Sin idioma preferido disponible → listar y coger cualquiera
        list_fn = getattr(api, "list_transcripts", None)
        if callable(list_fn):
            try:
                tlist = list_fn(video_id)
                for tr in tlist:  # iterable de transcripts disponibles
                    try:
                        return tr.fetch()
                    except Exception:
                        continue
            except Exception:
                pass

    # --- API de instancia 1.x: YouTubeTranscriptApi().fetch(...) ---
    try:
        inst = api()
        fetch_fn = getattr(inst, "fetch", None)
        if callable(fetch_fn):
            try:
                fetched = fetch_fn(video_id, languages=langs)
            except Exception:
                fetched = fetch_fn(video_id)  # cualquier idioma
            # 1.x devuelve un objeto iterable de snippets (.text); puede tener
            # .to_raw_data() para lista de dicts
            to_raw = getattr(fetched, "to_raw_data", None)
            if callable(to_raw):
                try:
                    return to_raw()
                except Exception:
                    pass
            return list(fetched)
    except Exception:
        pass

    return None


__all__ = ["is_video_url", "extract_video_id", "get_transcript", "PREFERRED_LANGS"]
