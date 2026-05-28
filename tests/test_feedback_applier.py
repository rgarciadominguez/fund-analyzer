"""Tests T3.6 (2026-05-28): tools/feedback_applier.py."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import feedback_store as fs
from tools import feedback_applier as fa


@pytest.fixture
def fund_setup(tmp_path, monkeypatch):
    """Crea fund_dir con output.json mínimo + redirige feedback paths a tmp."""
    funds_dir = tmp_path / "data" / "funds"
    global_log = tmp_path / "data" / "feedback_global.jsonl"
    monkeypatch.setattr(fs, "FUNDS_DIR", funds_dir)
    monkeypatch.setattr(fs, "GLOBAL_LOG", global_log)

    isin = "IE00BDR0JY05"
    fund_dir = funds_dir / isin
    fund_dir.mkdir(parents=True)
    output = {
        "isin": isin,
        "nombre": "IE00BDR0JY05",  # mal
        "gestora": "",
        "kpis": {"aum_actual_meur": None, "ter_pct": None},
        "cualitativo": {"gestores": []},
        "_manual_edits": [],
        "analyst_synthesis": {
            "resumen": {"texto": "texto previo"},
            "estrategia": {"texto": ""},
        },
    }
    (fund_dir / "output.json").write_text(
        json.dumps(output), encoding="utf-8",
    )
    return isin, fund_dir


def _item(**kwargs):
    return {
        "target_path": kwargs.get("path"),
        "target_section": kwargs.get("section"),
        "action": kwargs.get("action", "set"),
        "value": kwargs.get("value"),
        "confidence": kwargs.get("confidence", "high"),
        "source_urls": kwargs.get("urls", []),
        "rationale": kwargs.get("rationale", ""),
    }


# ════════════════════════════════════════════════════════════════════
# apply_pending_feedback
# ════════════════════════════════════════════════════════════════════


def test_apply_set_replaces_value_and_marks_manual_edit(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "el nombre es X", structured_items=[
        _item(path="nombre", action="set", value="Ashoka WhiteOak"),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    assert res["applied"] is True
    assert res["n_items_applied"] == 1
    # Output.json se actualizó
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert out["nombre"] == "Ashoka WhiteOak"
    assert "nombre" in out["_manual_edits"]
    # Feedback marcado como applied
    fbs = fs.list_feedback(isin)
    assert fbs[0]["estado"] == "applied"
    assert fbs[0]["run_id_applied"] == "r1"


def test_apply_set_skips_if_no_change(fund_setup):
    isin, fund_dir = fund_setup
    # nombre actual ya es IE00BDR0JY05 → set con ese mismo valor no debe contar
    fs.append_feedback(isin, "x", structured_items=[
        _item(path="nombre", action="set", value="IE00BDR0JY05"),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    # Item no contó porque prev==value
    assert res["n_items_applied"] == 0


def test_apply_add_to_list(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "añade gestor X", structured_items=[
        _item(path="cualitativo.gestores", action="add", value="Prashant Khemka"),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["n_items_applied"] == 1
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert "Prashant Khemka" in out["cualitativo"]["gestores"]


def test_apply_replace_list(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "gestores", structured_items=[
        _item(path="cualitativo.gestores", action="replace",
              value=["A", "B", "C"]),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["n_items_applied"] == 1
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert out["cualitativo"]["gestores"] == ["A", "B", "C"]


def test_apply_consultar_fuente_appends_to_fuentes_adicionales(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "fuente", raw_urls=["https://x.com"],
                       structured_items=[
        _item(action="consultar_fuente", urls=["https://x.com"]),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["n_items_applied"] == 1
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert "https://x.com" in out["fuentes_adicionales"]


def test_apply_revisar_records_hint(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "el resumen no menciona la estrategia value",
                       structured_items=[
        _item(section="resumen", action="revisar",
              rationale="falta estrategia value"),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["n_items_applied"] == 1
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert "_feedback_revisar" in out
    revs = out["_feedback_revisar"]
    assert len(revs) == 1
    assert revs[0]["target_section"] == "resumen"
    assert "value" in revs[0]["rationale"]


def test_apply_set_aum(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "aum es 350", structured_items=[
        _item(path="kpis.aum_actual_meur", action="set", value=350.0),
    ])
    fa.apply_pending_feedback(isin, fund_dir)
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert out["kpis"]["aum_actual_meur"] == 350.0
    assert "kpis.aum_actual_meur" in out["_manual_edits"]


def test_apply_no_pending_is_noop(fund_setup):
    isin, fund_dir = fund_setup
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["applied"] is False
    assert res["reason"] == "no_pending"


def test_apply_skips_already_applied(fund_setup):
    isin, fund_dir = fund_setup
    fb = fs.append_feedback(isin, "x", structured_items=[
        _item(path="nombre", action="set", value="X"),
    ])
    # Aplicar primera vez
    fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    # Segunda llamada: no debe re-aplicar
    res = fa.apply_pending_feedback(isin, fund_dir, run_id="r2")
    assert res["applied"] is False
    assert res["reason"] == "no_pending"


def test_apply_multiple_items_single_feedback(fund_setup):
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "varias cosas", structured_items=[
        _item(path="nombre", action="set", value="Ashoka WhiteOak"),
        _item(path="gestora", action="set", value="WhiteOak Capital"),
        _item(path="cualitativo.gestores", action="add", value="Prashant Khemka"),
    ])
    res = fa.apply_pending_feedback(isin, fund_dir)
    assert res["n_items_applied"] == 3
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    assert out["nombre"] == "Ashoka WhiteOak"
    assert out["gestora"] == "WhiteOak Capital"
    assert "Prashant Khemka" in out["cualitativo"]["gestores"]


# ════════════════════════════════════════════════════════════════════
# verify_resolved_after_run
# ════════════════════════════════════════════════════════════════════


def test_verify_marks_set_item_resolved_when_value_changed(fund_setup):
    isin, fund_dir = fund_setup
    fb = fs.append_feedback(isin, "x", structured_items=[
        _item(path="nombre", action="set", value="Ashoka WhiteOak"),
    ])
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    # Simular que el run completó sin cambiar más nada
    verify = fa.verify_resolved_after_run(
        isin, fund_dir, apply_res["snapshots_pre"]
    )
    assert verify["n_resolved"] == 1
    # Feedback estado pasa a resolved
    fbs = fs.list_feedback(isin)
    assert fbs[0]["estado"] == "resolved"
    assert 0 in (fbs[0]["resolved_items"] or [])


def test_verify_does_not_mark_resolved_if_value_unchanged(fund_setup):
    isin, fund_dir = fund_setup
    # Item set con MISMO valor que el actual → no debería resolver
    fs.append_feedback(isin, "x", structured_items=[
        _item(path="nombre", action="set", value="IE00BDR0JY05"),  # ya es ese
    ])
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    verify = fa.verify_resolved_after_run(
        isin, fund_dir, apply_res["snapshots_pre"],
    )
    assert verify["n_resolved"] == 0


def test_verify_consultar_fuente_resolved_when_url_in_fuentes(fund_setup):
    isin, fund_dir = fund_setup
    fb = fs.append_feedback(isin, "ver esta URL", raw_urls=["https://x.com"],
                            structured_items=[
        _item(action="consultar_fuente", urls=["https://x.com"]),
    ])
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    verify = fa.verify_resolved_after_run(
        isin, fund_dir, apply_res["snapshots_pre"],
    )
    assert verify["n_resolved"] == 1


def test_verify_t312_registers_useful_url_hosts(fund_setup, monkeypatch):
    """T3.12: tras un item consultar_fuente resolved, los hosts deben ser
    pasados a persist_html_fallback_to_registry (mocked)."""
    isin, fund_dir = fund_setup
    # gestora válida (no == ISIN) requerida para que T3.12 dispare
    output = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    output["gestora"] = "Test Gestora"
    (fund_dir / "output.json").write_text(json.dumps(output), encoding="utf-8")

    fs.append_feedback(
        isin, "ver esta URL",
        raw_urls=["https://morningstar.fr/x", "https://quantalys.com/y"],
        structured_items=[
            _item(action="consultar_fuente",
                  urls=["https://morningstar.fr/x", "https://quantalys.com/y"]),
        ],
    )
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")

    # Mock persist_html_fallback_to_registry
    captured = {}
    def fake_persist(isin, gestora, useful_domains):
        captured["isin"] = isin
        captured["gestora"] = gestora
        captured["domains"] = useful_domains
        return True
    import agents.discovery_v2 as dv2
    monkeypatch.setattr(dv2, "persist_html_fallback_to_registry", fake_persist)

    verify = fa.verify_resolved_after_run(
        isin, fund_dir, apply_res["snapshots_pre"]
    )
    assert verify["n_resolved"] == 1
    assert captured.get("gestora") == "Test Gestora"
    hosts = captured.get("domains", [])
    assert "morningstar.fr" in hosts
    assert "quantalys.com" in hosts


def test_apply_persists_item_results_with_reasons(fund_setup):
    """T3.X: tras aplicar, cada item del feedback debe tener `item_results`
    con applied=bool + reason explicativa."""
    isin, fund_dir = fund_setup
    fs.append_feedback(isin, "varios items", structured_items=[
        _item(path="nombre", action="set", value="Nuevo Nombre"),
        # gestora ya es "" en el fixture → set gestora="" no debe aplicar
        _item(path="gestora", action="set", value=""),
        _item(action="consultar_fuente", urls=["https://example.com"]),
    ])
    fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    fbs = fs.list_feedback(isin)
    fb = fbs[0]
    item_results = fb.get("item_results")
    assert isinstance(item_results, list)
    assert len(item_results) == 3
    # Item 0: set nombre → aplicado
    assert item_results[0]["applied"] is True
    assert "actualizado" in item_results[0]["reason"]
    # Item 1: set gestora = valor previo (vacío) → NO aplicado, razón explica el por qué
    assert item_results[1]["applied"] is False
    assert "ya era" in item_results[1]["reason"]
    # Item 2: consultar_fuente nuevo → aplicado
    assert item_results[2]["applied"] is True


def test_verify_persists_verify_reasons(fund_setup):
    """Tras verify, item_results tiene resolved + verify_reason."""
    isin, fund_dir = fund_setup
    # Paths distintos para evitar interferencia entre items
    fs.append_feedback(isin, "test", structured_items=[
        _item(path="nombre", action="set", value="Cambio Real"),  # sí cambia
        _item(path="gestora", action="set", value=""),  # ya era "" → no cambia
    ])
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    fa.verify_resolved_after_run(isin, fund_dir, apply_res["snapshots_pre"])
    fbs = fs.list_feedback(isin)
    item_results = fbs[0].get("item_results") or []
    assert len(item_results) == 2
    # Item 0: resolved (nombre cambió)
    assert item_results[0]["resolved"] is True
    assert "cambió" in item_results[0]["verify_reason"]
    # Item 1: NO resolved (gestora sigue ""), razón explica
    assert item_results[1]["resolved"] is False
    assert "no cambió" in item_results[1]["verify_reason"]


def test_verify_revisar_with_section_resolved_if_texto_non_empty(fund_setup):
    isin, fund_dir = fund_setup
    # resumen ya tenía texto "texto previo" — la heurística mira len > 100
    fs.append_feedback(isin, "el resumen está mal", structured_items=[
        _item(section="resumen", action="revisar",
              rationale="texto vago"),
    ])
    apply_res = fa.apply_pending_feedback(isin, fund_dir, run_id="r1")
    # Simular que el analyst regeneró el resumen con contenido largo
    out = json.loads((fund_dir / "output.json").read_text(encoding="utf-8"))
    out["analyst_synthesis"]["resumen"]["texto"] = "x" * 150
    (fund_dir / "output.json").write_text(
        json.dumps(out), encoding="utf-8"
    )
    verify = fa.verify_resolved_after_run(
        isin, fund_dir, apply_res["snapshots_pre"]
    )
    assert verify["n_resolved"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
