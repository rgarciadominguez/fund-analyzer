"""Tests de tools/youtube_transcript.py (parsing offline + degradación segura).

El fetch real de transcripts depende de red/YouTube → no se testea aquí
(no determinista). Se cubre el parsing de URLs y que get_transcript no lance
y devuelva None para entradas no-vídeo.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.youtube_transcript import is_video_url, extract_video_id, get_transcript


def test_is_video_url():
    assert is_video_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    assert is_video_url("https://youtu.be/dQw4w9WgXcQ")
    assert is_video_url("https://m.youtube.com/watch?v=x")
    assert is_video_url("https://www.youtube-nocookie.com/embed/x")
    assert not is_video_url("https://rankia.com/blog/fondo")
    assert not is_video_url("https://vimeo.com/123")  # no soportado (transcript-api es YT)
    assert not is_video_url("")
    assert not is_video_url(None)  # type: ignore


def test_extract_video_id():
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ?t=10") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/shorts/abc123XYZ") == "abc123XYZ"
    assert extract_video_id("https://www.youtube.com/embed/abc123XYZ") == "abc123XYZ"
    assert extract_video_id("https://www.youtube.com/live/abc123XYZ") == "abc123XYZ"
    assert extract_video_id("https://rankia.com/blog") is None
    assert extract_video_id("") is None


def test_get_transcript_none_for_non_video():
    # URL sin video_id → None sin lanzar
    assert get_transcript("https://rankia.com/blog/fondo") is None
    assert get_transcript("") is None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
