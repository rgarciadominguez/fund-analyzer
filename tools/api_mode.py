"""api_mode — single source of truth for choosing Cowork-skill mode vs API legacy.

Refactor L2 (2026-05-05). Reduces API spend by deferring Gemini/Anthropic
extraction calls to Claude Code skills running under Claude Max. Each
refactored agent (cnmv_agent, cnmv_enrichment, intl_extractor_v2,
manager_profiler, manager_deep_agent, letters_deep_agent) checks
`is_cowork_mode()` at the point where the API call would happen. In Cowork
mode the agent emits a task into the appropriate `pending_*.json` manifest
and skips the API call. In API mode the agent runs the legacy code path
unchanged.

Default is Cowork. The orchestrator flips this to API by setting the env var
**before** lazy-importing any agent module:

    if args.api_fallback:
        os.environ["FUND_ANALYZER_MODE"] = "api"
    from agents.cnmv_agent import CNMVAgent  # lazy import AFTER the flip

⚠️  THREAD-SAFETY WARNING

    The env var FUND_ANALYZER_MODE is *process-global*. This module is NOT
    safe for:
      - Multi-threaded pipelines that mix cowork + api modes in the same
        process (don't do that — pick one mode per process).
      - Multi-process batch runs sharing the same parent shell where one
        child needs cowork and another needs api. Each child must set its
        own env var before importing agents.

    If a future refactor needs concurrent mode mixing, migrate this module
    to a parameter propagated through agent constructors
    (`api_fallback: bool`) and delete `is_cowork_mode()`. The call sites
    are grepable: `grep -rn "is_cowork_mode" agents/`.
"""
from __future__ import annotations

import os


def is_cowork_mode() -> bool:
    """True if the agent should defer extraction to a Cowork skill.

    Returns True (Cowork mode) by default. Returns False only when the env
    var FUND_ANALYZER_MODE is explicitly set to "api".
    """
    return os.environ.get("FUND_ANALYZER_MODE", "cowork") != "api"


def get_mode_label() -> str:
    """Return the active mode as a label for logging."""
    return "api" if not is_cowork_mode() else "cowork"
