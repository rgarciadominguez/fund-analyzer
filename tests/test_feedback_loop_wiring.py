"""Fix C + Fix D (2026-06-06): cableado del lazo de feedback end-to-end.

Fix C: bundle_exporter.build_bundle_feedback incluye feedback `pending` (no solo
       applied) para que la skill analyst-cowork lo vea en el re-run del botón ♺.
Fix D: analizar_fondo.bat NO salta la skill analyst-cowork cuando hay
       --apply-feedback (fuerza re-run para que el LLM accione el feedback).
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.bundle_exporter import build_bundle_feedback


# ════════════════════════════════════════════════════════════════════
# Fix C — build_bundle_feedback
# ════════════════════════════════════════════════════════════════════


def _fb(estado, items, fb_id="fb1", resolved_items=None, raw="texto"):
    return {
        "id": fb_id,
        "estado": estado,
        "raw_text": raw,
        "resolved_items": resolved_items or [],
        "structured_items": items,
    }


def _revisar(section="cartera"):
    return {"action": "revisar", "target_section": section, "target_path": None,
            "value": None}


def test_pending_feedback_is_exported():
    """CLAVE Fix C: feedback pending DEBE exportarse (era el bug raíz: bundle
    vacío en el re-run del botón ♺)."""
    hf = {"feedbacks": [_fb("pending", [_revisar("cartera")])]}
    out = build_bundle_feedback(hf, "LU0168736675")
    assert out["n_relevant_items"] == 1
    assert out["items"][0]["target_section"] == "cartera"
    assert out["items"][0]["feedback_id"] == "fb1"
    assert out["items"][0]["item_idx"] == 0


def test_applied_and_partially_resolved_still_exported():
    hf = {"feedbacks": [
        _fb("applied", [_revisar("resumen")], fb_id="a"),
        _fb("partially_resolved", [_revisar("gestores")], fb_id="b"),
    ]}
    out = build_bundle_feedback(hf, "X")
    assert out["n_relevant_items"] == 2


def test_resolved_feedback_excluded():
    hf = {"feedbacks": [_fb("resolved", [_revisar("cartera")])]}
    out = build_bundle_feedback(hf, "X")
    assert out["n_relevant_items"] == 0


def test_resolved_items_within_feedback_skipped():
    hf = {"feedbacks": [_fb("pending", [_revisar("cartera"), _revisar("resumen")],
                            resolved_items=[0])]}
    out = build_bundle_feedback(hf, "X")
    # idx 0 ya resuelto → solo idx 1 (resumen)
    assert out["n_relevant_items"] == 1
    assert out["items"][0]["target_section"] == "resumen"
    assert out["items"][0]["item_idx"] == 1


def test_non_narrative_set_item_skipped():
    """Un `set` sin target_section no es narrativa → no va al bundle del analyst."""
    item = {"action": "set", "target_section": None, "target_path": "kpis.aum_actual_meur",
            "value": 350.0}
    hf = {"feedbacks": [_fb("pending", [item])]}
    out = build_bundle_feedback(hf, "X")
    assert out["n_relevant_items"] == 0


def test_empty_or_missing_feedback_is_safe():
    assert build_bundle_feedback({}, "X")["n_relevant_items"] == 0
    assert build_bundle_feedback({"feedbacks": []}, "X")["n_relevant_items"] == 0


# ════════════════════════════════════════════════════════════════════
# Fix D — analizar_fondo.bat fuerza re-run del analyst con --apply-feedback
# ════════════════════════════════════════════════════════════════════


def test_bat_forces_analyst_rerun_on_apply_feedback():
    bat = (ROOT / "analizar_fondo.bat").read_text(encoding="utf-8", errors="ignore")
    # La línea que limpia el skip cuando hay feedback debe existir y estar
    # DESPUÉS del bloque que setea SKIP_ANALYST=1.
    assert "if defined APPLY_FEEDBACK set SKIP_ANALYST=" in bat
    idx_set = bat.find("set SKIP_ANALYST=1")
    idx_clear = bat.find("if defined APPLY_FEEDBACK set SKIP_ANALYST=")
    assert idx_set != -1 and idx_clear != -1
    assert idx_clear > idx_set, "el clear debe ir tras el set para anularlo"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
