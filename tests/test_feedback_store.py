"""Tests T3.1 (2026-05-28): tools/feedback_store.py persistence layer."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import feedback_store as fs


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    """Redirige FUNDS_DIR y GLOBAL_LOG a tmp_path para no contaminar disco real."""
    funds_dir = tmp_path / "data" / "funds"
    global_log = tmp_path / "data" / "feedback_global.jsonl"
    monkeypatch.setattr(fs, "FUNDS_DIR", funds_dir)
    monkeypatch.setattr(fs, "GLOBAL_LOG", global_log)
    return funds_dir, global_log


def _items(*item_kwargs):
    """Helper para crear structured_items."""
    return [
        {
            "target_path": k.get("path"),
            "target_section": k.get("section"),
            "action": k.get("action", "set"),
            "value": k.get("value"),
            "confidence": k.get("confidence", "high"),
            "source_urls": k.get("urls", []),
            "rationale": k.get("rationale", ""),
        }
        for k in item_kwargs
    ]


# ════════════════════════════════════════════════════════════════════
# list / get
# ════════════════════════════════════════════════════════════════════


def test_list_empty_returns_empty(isolated_dirs):
    assert fs.list_feedback("IE00BDR0JY05") == []
    assert fs.get_pending("IE00BDR0JY05") == []
    assert fs.get_feedback_by_id("IE00BDR0JY05", "fb_x") is None


# ════════════════════════════════════════════════════════════════════
# append_feedback
# ════════════════════════════════════════════════════════════════════


def test_append_creates_file_and_returns_feedback(isolated_dirs):
    funds_dir, global_log = isolated_dirs
    items = _items(
        {"path": "nombre", "action": "set", "value": "Ashoka WhiteOak"},
        {"section": "gestores", "action": "revisar", "urls": ["https://x.com"]},
    )
    fb = fs.append_feedback(
        "IE00BDR0JY05",
        raw_text="el nombre es Ashoka, añade gestor X de https://x.com",
        raw_urls=["https://x.com"],
        structured_items=items,
        fund_name="Ashoka WhiteOak",
    )
    assert fb["id"].startswith("fb_")
    assert fb["estado"] == "pending"
    assert fb["applied_at"] is None
    assert fb["resolved_items"] == []
    assert len(fb["structured_items"]) == 2

    # Persistido en disco
    file_path = funds_dir / "IE00BDR0JY05" / "human_feedback.json"
    assert file_path.exists()
    data = json.loads(file_path.read_text(encoding="utf-8"))
    assert data["isin"] == "IE00BDR0JY05"
    assert data["fund_name_at_creation"] == "Ashoka WhiteOak"
    assert len(data["feedbacks"]) == 1


def test_append_multiple_accumulates(isolated_dirs):
    isin = "ES0123456789"
    fb1 = fs.append_feedback(isin, "primer feedback", structured_items=_items(
        {"path": "nombre", "value": "X"},
    ))
    fb2 = fs.append_feedback(isin, "segundo feedback", structured_items=_items(
        {"section": "estrategia", "action": "revisar"},
    ))
    feedbacks = fs.list_feedback(isin)
    assert len(feedbacks) == 2
    assert feedbacks[0]["id"] == fb1["id"]
    assert feedbacks[1]["id"] == fb2["id"]
    assert fs.get_feedback_by_id(isin, fb1["id"]) == fb1


def test_append_writes_to_global_log_one_line_per_item(isolated_dirs):
    funds_dir, global_log = isolated_dirs
    items = _items(
        {"path": "nombre", "value": "X"},
        {"section": "gestores", "action": "revisar"},
        {"path": "kpis.aum_actual_meur", "value": 350.0},
    )
    fb = fs.append_feedback("LU0123456789", "tres cosas a la vez",
                            structured_items=items, fund_name="Test Fund")
    assert global_log.exists()
    lines = [json.loads(l) for l in global_log.read_text(encoding="utf-8").splitlines()]
    # 3 items → 3 líneas
    assert len(lines) == 3
    for line in lines:
        assert line["feedback_id"] == fb["id"]
        assert line["isin"] == "LU0123456789"
        assert line["isin_prefix"] == "LU"
        assert line["resolved"] is False


def test_append_requires_isin(isolated_dirs):
    with pytest.raises(ValueError):
        fs.append_feedback("", "x")
    with pytest.raises(ValueError):
        fs.append_feedback("   ", "x")


# ════════════════════════════════════════════════════════════════════
# delete_feedback
# ════════════════════════════════════════════════════════════════════


def test_delete_pending_works(isolated_dirs):
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "Y"},
    ))
    assert fs.delete_feedback(isin, fb["id"]) is True
    assert fs.list_feedback(isin) == []


def test_delete_applied_refused(isolated_dirs):
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "Y"},
    ))
    fs.mark_applied(isin, fb["id"], "run_001")
    assert fs.delete_feedback(isin, fb["id"]) is False
    # sigue ahí, en estado applied
    feedbacks = fs.list_feedback(isin)
    assert len(feedbacks) == 1
    assert feedbacks[0]["estado"] == "applied"


def test_delete_nonexistent_returns_false(isolated_dirs):
    assert fs.delete_feedback("IE00BDR0JY05", "fb_nope") is False


# ════════════════════════════════════════════════════════════════════
# mark_applied / mark_items_resolved
# ════════════════════════════════════════════════════════════════════


def test_mark_applied_sets_estado_and_run_id(isolated_dirs):
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "Y"},
    ))
    assert fs.mark_applied(isin, fb["id"], "run_xyz") is True
    feedbacks = fs.list_feedback(isin)
    assert feedbacks[0]["estado"] == "applied"
    assert feedbacks[0]["run_id_applied"] == "run_xyz"
    assert feedbacks[0]["applied_at"] is not None


def test_mark_items_resolved_partial_then_full(isolated_dirs):
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "X1"},
        {"path": "gestora", "value": "X2"},
        {"section": "estrategia", "action": "revisar"},
    ))
    # Resolver solo el item 0
    res = fs.mark_items_resolved(isin, fb["id"], [0])
    assert res["estado"] == "partially_resolved"
    assert res["resolved_items"] == [0]
    # Resolver 1 y 2
    res = fs.mark_items_resolved(isin, fb["id"], [1, 2])
    assert res["estado"] == "resolved"
    assert res["resolved_items"] == [0, 1, 2]


def test_mark_items_resolved_idempotent(isolated_dirs):
    """Marcar el mismo item dos veces no duplica."""
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "X1"},
    ))
    fs.mark_items_resolved(isin, fb["id"], [0])
    res = fs.mark_items_resolved(isin, fb["id"], [0])  # otra vez
    assert res["resolved_items"] == [0]
    assert res["estado"] == "resolved"


def test_mark_items_resolved_logs_event(isolated_dirs):
    funds_dir, global_log = isolated_dirs
    isin = "IE00BDR0JY05"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "X1"},
    ))
    fs.mark_items_resolved(isin, fb["id"], [0])
    lines = [json.loads(l) for l in global_log.read_text(encoding="utf-8").splitlines()]
    # 1 append (creación) + 1 resolved event
    events = [l for l in lines if l.get("event") == "item_resolved"]
    assert len(events) == 1
    assert events[0]["feedback_id"] == fb["id"]
    assert events[0]["item_idx"] == 0


def test_delete_logs_event(isolated_dirs):
    funds_dir, global_log = isolated_dirs
    isin = "ES0123456789"
    fb = fs.append_feedback(isin, "x", structured_items=_items(
        {"path": "nombre", "value": "X1"},
    ))
    fs.delete_feedback(isin, fb["id"])
    lines = [json.loads(l) for l in global_log.read_text(encoding="utf-8").splitlines()]
    events = [l for l in lines if l.get("event") == "deleted"]
    assert len(events) == 1
    assert events[0]["feedback_id"] == fb["id"]


# ════════════════════════════════════════════════════════════════════
# get_pending
# ════════════════════════════════════════════════════════════════════


def test_get_pending_excludes_applied_and_resolved(isolated_dirs):
    isin = "ES0123456789"
    fb1 = fs.append_feedback(isin, "fb1", structured_items=_items({"path": "nombre"}))
    fb2 = fs.append_feedback(isin, "fb2", structured_items=_items({"path": "gestora"}))
    fb3 = fs.append_feedback(isin, "fb3", structured_items=_items({"path": "kpis.aum"}))
    # fb1 → applied
    fs.mark_applied(isin, fb1["id"], "run_001")
    # fb2 → resolved
    fs.mark_applied(isin, fb2["id"], "run_001")
    fs.mark_items_resolved(isin, fb2["id"], [0])
    # fb3 → pending
    pending = fs.get_pending(isin)
    assert [p["id"] for p in pending] == [fb3["id"]]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
