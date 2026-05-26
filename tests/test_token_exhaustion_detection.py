"""Tests for P4: detección de agotamiento de tokens Claude Max."""
import pytest

# Importamos el módulo y extraemos la función vía la app.
# _detect_token_exhaustion vive dentro de make_app, así que la definimos aquí
# como copia exacta de la lógica del módulo para testearla sin depender de Flask.

TOKEN_EXHAUSTION_PATTERNS = (
    "rate limit",
    "rate_limit_exceeded",
    "credit balance is too low",
    "quota exceeded",
    "quota_exceeded",
    "max usage limit",
    "usage limit reached",
    "model overloaded",
    "overloaded_error",
    "you've reached your",
    "monthly token limit",
    "claude_max_quota",
    "subscription limit",
)


def _detect_token_exhaustion(log_text):
    if not log_text:
        return False
    lower = log_text.lower()
    return any(pat in lower for pat in TOKEN_EXHAUSTION_PATTERNS)


class TestDetectTokenExhaustion:
    """Verifica que el detector captura los patrones esperados sin falsos positivos."""

    def test_empty_returns_false(self):
        assert _detect_token_exhaustion("") is False
        assert _detect_token_exhaustion(None) is False

    def test_normal_log_returns_false(self):
        normal = """
        [DISCOVERY] harvest amiralgestion.com: 49 candidates
        [LETTERS] Web: 2635 chars from https://example.com
        [META] OK Meta-report guardado
        """
        assert _detect_token_exhaustion(normal) is False

    def test_rate_limit_detected(self):
        text = "ERROR: rate_limit_exceeded for model claude-sonnet"
        assert _detect_token_exhaustion(text) is True

    def test_credit_balance_low(self):
        text = "Your credit balance is too low to complete this request"
        assert _detect_token_exhaustion(text) is True

    def test_quota_exceeded(self):
        text = "Error 429: quota exceeded"
        assert _detect_token_exhaustion(text) is True

    def test_usage_limit_reached(self):
        text = "You have reached your monthly usage limit reached for Claude Max"
        assert _detect_token_exhaustion(text) is True

    def test_max_overloaded(self):
        text = "Anthropic API: model overloaded, please retry"
        assert _detect_token_exhaustion(text) is True

    def test_overloaded_error_code(self):
        text = 'response: {"error": {"type": "overloaded_error", "message": "..."}}'
        assert _detect_token_exhaustion(text) is True

    def test_youve_reached(self):
        text = "You've reached your daily token allowance"
        assert _detect_token_exhaustion(text) is True

    def test_case_insensitive(self):
        text = "RATE LIMIT EXCEEDED — wait 60s"
        assert _detect_token_exhaustion(text) is True

    def test_substring_matching(self):
        """El detector busca substring, no palabra completa.
        Diseño intencionado: para capturar variaciones."""
        text = "OK, hit a rate limit downstream"
        assert _detect_token_exhaustion(text) is True

    def test_no_false_positive_with_unrelated_terms(self):
        """Términos similares pero NO de tokens no deben disparar."""
        text = """
        I am limited to 10 retries. Speed limit: 100 req/s.
        Quota for storage: 500 GB.
        """
        # "speed limit" no es "rate limit"; "quota for storage" no es "quota exceeded"
        assert _detect_token_exhaustion(text) is False
