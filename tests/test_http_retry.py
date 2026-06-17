"""Audit MEDIO (red silenciosa): get_json reintenta errores transitorios (429/5xx/
timeout) para que un parpadeo de red NO deje el dato quant a medias. 4xx genuino
no se reintenta. Tras agotar reintentos → FetchError (distinto de 'sin datos')."""
import httpx
import pytest

from tools import http_retry
from tools.http_retry import get_json, FetchError


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._p = payload
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("err", request=None, response=self)

    def json(self):
        return self._p


def test_retries_on_429_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, **k):
        calls["n"] += 1
        return _Resp(429) if calls["n"] < 3 else _Resp(200, {"ok": 1})

    monkeypatch.setattr(http_retry.httpx, "get", fake_get)
    monkeypatch.setattr(http_retry.time, "sleep", lambda s: None)
    assert get_json("u", retries=5) == {"ok": 1}
    assert calls["n"] == 3  # 2 reintentos + éxito


def test_4xx_not_retried(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, **k):
        calls["n"] += 1
        return _Resp(404)

    monkeypatch.setattr(http_retry.httpx, "get", fake_get)
    monkeypatch.setattr(http_retry.time, "sleep", lambda s: None)
    with pytest.raises(FetchError):
        get_json("u", retries=5)
    assert calls["n"] == 1  # 404 NO se reintenta


def test_exhausts_then_raises(monkeypatch):
    def fake_get(url, **k):
        raise httpx.TimeoutException("timeout")

    monkeypatch.setattr(http_retry.httpx, "get", fake_get)
    monkeypatch.setattr(http_retry.time, "sleep", lambda s: None)
    with pytest.raises(FetchError):
        get_json("u", retries=3)
