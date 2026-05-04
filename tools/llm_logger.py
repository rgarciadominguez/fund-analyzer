"""
LLM Logger — helper genérico para registrar coste de cualquier llamada LLM.

Centraliza la lógica de extracción de tokens (Anthropic vs Gemini) y delega
a `tools.cost_tracker.track()` que ya escribe a `cost_report.json` por fondo
+ `cost_log.jsonl` global vía cost_monitor.

Uso:
    from tools.llm_logger import log_llm_response

    # Anthropic
    resp = client.messages.create(...)
    log_llm_response(resp, agent="manager_profiler", isin=self.isin,
                     model="claude-opus-4-7", provider="anthropic")

    # Gemini
    resp = client.models.generate_content(...)
    log_llm_response(resp, agent="discovery_v2", isin=self.isin,
                     model="gemini-2.5-flash", provider="gemini")

Failsafe: NUNCA propaga excepción — si log falla, el caller sigue.
Cost-Opt Fase 2 (2026-05-03): cobertura completa post-€6 inesperados.
"""
from __future__ import annotations
import os
from typing import Any


def log_llm_response(resp: Any, agent: str, isin: str = "",
                     model: str = "", provider: str = "auto") -> None:
    """Extrae tokens de la respuesta LLM y registra coste vía cost_tracker.

    Args:
        resp: objeto respuesta del SDK (Anthropic Message o Gemini GenerateContentResponse)
        agent: nombre del agente que disparó la call
        isin: ISIN del fondo (si vacío, intenta env CURRENT_FUND_ISIN)
        model: identificador del modelo. Si vacío, intenta detectar.
        provider: "anthropic" | "gemini" | "auto" (auto-detecta por estructura)
    """
    try:
        if not isin:
            isin = os.environ.get("CURRENT_FUND_ISIN", "")
        if not isin:
            return  # no-op si no sabemos el fondo

        inp, out = 0, 0
        # Anthropic: resp.usage.input_tokens / output_tokens
        if provider in ("anthropic", "auto") and hasattr(resp, "usage") and resp.usage:
            inp = getattr(resp.usage, "input_tokens", 0) or 0
            out = getattr(resp.usage, "output_tokens", 0) or 0
            if not model:
                model = getattr(resp, "model", "claude-unknown")
        # Gemini: resp.usage_metadata.prompt_token_count / candidates_token_count
        if (inp == 0 and out == 0 and provider in ("gemini", "auto")
                and hasattr(resp, "usage_metadata") and resp.usage_metadata):
            um = resp.usage_metadata
            inp = getattr(um, "prompt_token_count", 0) or 0
            out = getattr(um, "candidates_token_count", 0) or 0
            if not model:
                model = "gemini-unknown"

        if inp == 0 and out == 0:
            return  # nada que medir

        from tools.cost_tracker import track
        track(isin, model, inp, out, agent)
    except Exception:
        pass  # NUNCA romper el caller


def log_llm_tokens(input_tokens: int, output_tokens: int,
                   agent: str, isin: str = "", model: str = "") -> None:
    """Variante explícita: el caller pasa tokens manualmente.
    Útil cuando la respuesta no tiene `usage`/`usage_metadata` (errores, mock,
    streaming sin contadores).
    """
    try:
        if not isin:
            isin = os.environ.get("CURRENT_FUND_ISIN", "")
        if not isin or (input_tokens == 0 and output_tokens == 0):
            return
        from tools.cost_tracker import track
        track(isin, model or "unknown", input_tokens, output_tokens, agent)
    except Exception:
        pass
