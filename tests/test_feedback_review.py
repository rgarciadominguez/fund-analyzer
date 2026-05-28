"""Tests T3.11 (2026-05-28): tools/feedback_review.py CLI."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import feedback_review as fr


@pytest.fixture
def isolated_log(tmp_path, monkeypatch):
    """Redirige GLOBAL_LOG a tmp."""
    log = tmp_path / "feedback_global.jsonl"
    monkeypatch.setattr(fr, "GLOBAL_LOG", log)
    return log


def _write_entries(log: Path, entries: list[dict]):
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


# ════════════════════════════════════════════════════════════════════
# _load_log_entries
# ════════════════════════════════════════════════════════════════════


def test_load_empty(isolated_log):
    assert fr._load_log_entries() == []


def test_load_handles_bad_json(isolated_log):
    isolated_log.parent.mkdir(parents=True, exist_ok=True)
    isolated_log.write_text(
        '{"a": 1}\nbad json line\n{"b": 2}\n',
        encoding="utf-8",
    )
    entries = fr._load_log_entries()
    assert len(entries) == 2


# ════════════════════════════════════════════════════════════════════
# _filter_entries
# ════════════════════════════════════════════════════════════════════


def test_filter_by_isin(isolated_log):
    _write_entries(isolated_log, [
        {"ts": "2026-05-01T00:00:00Z", "isin": "IE00ABC", "target_path": "nombre"},
        {"ts": "2026-05-02T00:00:00Z", "isin": "LU00ABC", "target_path": "gestora"},
    ])
    entries = fr._load_log_entries()
    filtered = fr._filter_entries(entries, isin="IE00ABC")
    assert len(filtered) == 1
    assert filtered[0]["isin"] == "IE00ABC"


def test_filter_skips_event_lines(isolated_log):
    _write_entries(isolated_log, [
        {"ts": "2026-05-01T00:00:00Z", "isin": "IE00", "target_path": "nombre"},
        {"event": "item_resolved", "feedback_id": "fb_1", "item_idx": 0},
        {"event": "deleted", "feedback_id": "fb_2"},
    ])
    entries = fr._load_log_entries()
    filtered = fr._filter_entries(entries)
    assert len(filtered) == 1  # solo el item, no los eventos


def test_filter_since(isolated_log):
    _write_entries(isolated_log, [
        {"ts": "2026-04-30T00:00:00Z", "isin": "X"},
        {"ts": "2026-05-15T00:00:00Z", "isin": "Y"},
    ])
    entries = fr._load_log_entries()
    filtered = fr._filter_entries(entries, since="2026-05-01")
    assert len(filtered) == 1
    assert filtered[0]["isin"] == "Y"


# ════════════════════════════════════════════════════════════════════
# _suggestions
# ════════════════════════════════════════════════════════════════════


def test_suggestions_empty():
    sugs = fr._suggestions([])
    assert len(sugs) == 1
    assert "sin entries" in sugs[0].lower()


def test_suggestions_detects_repeated_target_path():
    # 3+ entries con mismo target_path → sugerencia
    entries = [
        {"target_path": "nombre", "isin_prefix": "IE", "gestora": "X"},
        {"target_path": "nombre", "isin_prefix": "IE", "gestora": "Y"},
        {"target_path": "nombre", "isin_prefix": "IE", "gestora": "Z"},
    ]
    sugs = fr._suggestions(entries)
    joined = " ".join(sugs)
    assert "nombre" in joined
    assert "name_recovery" in joined or "dashboard_quality_agent" in joined
    assert "IE" in joined


def test_suggestions_detects_gestora_concentration():
    entries = [
        {"gestora": "Amiral Gestion", "target_path": f"f{i}"}
        for i in range(5)
    ]
    sugs = fr._suggestions(entries)
    joined = " ".join(sugs)
    assert "Amiral" in joined
    assert "gestoras_registry" in joined


def test_suggestions_detects_url_host_repeated_per_gestora():
    entries = [
        {"gestora": "Amiral", "source_urls": ["https://morningstar.fr/a"]},
        {"gestora": "Amiral", "source_urls": ["https://morningstar.fr/b"]},
    ]
    sugs = fr._suggestions(entries)
    joined = " ".join(sugs)
    assert "morningstar.fr" in joined
    assert "html_fallback_useful_domains" in joined


def test_suggestions_detects_section_revisar_repeated():
    entries = [
        {"action": "revisar", "target_section": "gestores"},
        {"action": "revisar", "target_section": "gestores"},
        {"action": "revisar", "target_section": "gestores"},
    ]
    sugs = fr._suggestions(entries)
    joined = " ".join(sugs)
    assert "gestores" in joined
    assert "analyst-cowork" in joined or "prompt" in joined


# ════════════════════════════════════════════════════════════════════
# CLI (integración)
# ════════════════════════════════════════════════════════════════════


def test_cli_runs_without_log(tmp_path, monkeypatch):
    """python -m tools.feedback_review con log vacío termina exit 0."""
    monkeypatch.setattr(fr, "GLOBAL_LOG", tmp_path / "nope.jsonl")
    monkeypatch.setattr(sys, "argv", ["feedback_review"])
    rc = fr.main()
    assert rc == 0


def test_cli_json_output(isolated_log, monkeypatch, capsys):
    _write_entries(isolated_log, [
        {"ts": "2026-05-01T00:00:00Z", "isin": "IE00ABC", "isin_prefix": "IE",
         "gestora": "X", "target_path": "nombre", "action": "set", "confidence": "high"},
    ])
    monkeypatch.setattr(sys, "argv", ["feedback_review", "--json"])
    rc = fr.main()
    assert rc == 0
    out = capsys.readouterr().out
    # Parsear el JSON desde el primer '{' usando raw_decode (tolera trailing)
    idx = out.find("{")
    assert idx >= 0, f"no se encontró '{{' en output: {out!r}"
    decoder = json.JSONDecoder()
    data, _ = decoder.raw_decode(out[idx:])
    assert data["n_entries"] == 1
    assert "suggestions" in data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
