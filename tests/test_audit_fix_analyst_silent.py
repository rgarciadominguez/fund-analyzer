"""Audit fix #1 (2026-06-17): el fallo del analyst NO debe pasar como OK silencioso.

El mecanismo que dispara el fix: si la skill analyst no dejó
`analyst_synthesis_cowork.json`, `_consume_cowork_analyst` DEBE lanzar excepción
(que el pipeline convierte en summary["analyst"]["error"] -> orchestrator main()
sale exit 3 -> el bat marca consume-all-cowork crítico -> sync omitido).

Si esta excepción dejara de lanzarse, el fallo volvería a ser silencioso.
"""
import pytest


def test_consume_analyst_raises_when_cowork_json_missing(tmp_path):
    from agents.orchestrator import _consume_cowork_analyst
    logs = []
    # fund_dir vacío -> no hay analyst_synthesis_cowork.json
    with pytest.raises(Exception):
        _consume_cowork_analyst("ES0000000000", tmp_path, lambda *a: logs.append(a))


def test_consume_analyst_raises_when_cowork_json_lacks_synthesis(tmp_path):
    import json
    from agents.orchestrator import _consume_cowork_analyst
    # json presente pero SIN analyst_synthesis -> también debe fallar (no consumir basura)
    (tmp_path / "analyst_synthesis_cowork.json").write_text(
        json.dumps({"otra_cosa": 1}), encoding="utf-8")
    (tmp_path / "output.json").write_text(json.dumps({"isin": "ES0000000000"}), encoding="utf-8")
    with pytest.raises(Exception):
        _consume_cowork_analyst("ES0000000000", tmp_path, lambda *a: None)
