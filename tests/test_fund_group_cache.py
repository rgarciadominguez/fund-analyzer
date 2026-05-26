"""Tests del cache compartido fund_group (F6).

Fixture: 2 ISINs Sextant (FR001400CEK6 / FR001400CEG4) que pertenecen al mismo
fund_group (gestora=Amiral Gestion, nombre_base=Sextant Grand Large). Verifican
que el segundo ISIN reutiliza los outputs del primero sin re-correr agentes.

NO ejecutan el pipeline real (no LLM calls): mockean fund_dirs en tmp_path y
testean directamente los helpers `_fg_*` del orchestrator.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents import orchestrator as orch  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers de fixtures
# ---------------------------------------------------------------------------

SEXTANT_A = "FR001400CEK6"  # ISIN canónico (primer run del fund_group)
SEXTANT_B = "FR001400CEG4"  # ISIN hermano (debería reutilizar cache)

SEXTANT_IDENTITY_A = {"nombre": "Sextant Grand Large I EUR", "gestora": "Amiral Gestion"}
SEXTANT_IDENTITY_B = {"nombre": "Sextant Grand Large A EUR", "gestora": "Amiral Gestion"}


def _silent_log(*args, **kwargs):
    pass


def _write_sample_outputs(fund_dir: Path) -> None:
    """Crea los 3 JSONs típicos en el fund_dir del ISIN canónico."""
    fund_dir.mkdir(parents=True, exist_ok=True)
    (fund_dir / "manager_profile.json").write_text(
        json.dumps({"isin": SEXTANT_A, "equipo_gestor": ["Louis d'Arvieu"]}),
        encoding="utf-8",
    )
    (fund_dir / "intl_discovery_data.json").write_text(
        json.dumps({"isin": SEXTANT_A, "documents": [{"url": "x"} for _ in range(5)]}),
        encoding="utf-8",
    )
    (fund_dir / "readings_data.json").write_text(
        json.dumps({"isin": SEXTANT_A, "lecturas": [{"id": "L1"}]}),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Helpers unitarios
# ---------------------------------------------------------------------------

def test_normalize_nombre_base_strips_class_currency():
    assert orch._fg_normalize_nombre_base("Sextant Grand Large I EUR") == "Sextant Grand Large"
    assert orch._fg_normalize_nombre_base("Sextant Grand Large A EUR") == "Sextant Grand Large"


def test_compute_id_es_deterministico_y_namespace_oid():
    """Misma entrada → mismo UUID. Y debe usar NAMESPACE_OID (no random)."""
    id1 = orch._fg_compute_id("Sextant Grand Large", "Amiral Gestion")
    id2 = orch._fg_compute_id("Sextant Grand Large", "Amiral Gestion")
    assert id1 == id2

    # Debe coincidir con uuid5(NAMESPACE_OID, "amiral gestion::sextant grand large")
    expected = str(uuid.uuid5(uuid.NAMESPACE_OID, "amiral gestion::sextant grand large"))
    assert id1 == expected


def test_compute_id_es_case_insensitive():
    a = orch._fg_compute_id("Sextant Grand Large", "Amiral Gestion")
    b = orch._fg_compute_id("SEXTANT grand LARGE", "amiral GESTION")
    assert a == b


def test_sextant_a_y_b_comparten_fund_group_id():
    """Las 2 clases hermanas DEBEN compartir fund_group_id."""
    base_a = orch._fg_normalize_nombre_base(SEXTANT_IDENTITY_A["nombre"])
    base_b = orch._fg_normalize_nombre_base(SEXTANT_IDENTITY_B["nombre"])
    assert base_a == base_b == "Sextant Grand Large"

    id_a = orch._fg_compute_id(base_a, SEXTANT_IDENTITY_A["gestora"])
    id_b = orch._fg_compute_id(base_b, SEXTANT_IDENTITY_B["gestora"])
    assert id_a == id_b


# ---------------------------------------------------------------------------
# Apply / save cache E2E (sin LLM, todo en tmp_path)
# ---------------------------------------------------------------------------

def test_save_then_apply_copia_los_3_jsons(tmp_path, monkeypatch):
    """Run A: guarda cache. Run B: debería copiar los 3 JSONs."""
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "data" / "fund_groups_cache")
    monkeypatch.delenv("DISABLE_FUND_GROUP_CACHE", raising=False)

    # Run A: ISIN canónico ya tiene los outputs
    dir_a = tmp_path / "data" / "funds" / SEXTANT_A
    _write_sample_outputs(dir_a)
    orch._fg_save_cache(SEXTANT_A, SEXTANT_IDENTITY_A, _silent_log)

    # Verificar fichero de cache escrito
    fund_group_id = orch._fg_compute_id(
        orch._fg_normalize_nombre_base(SEXTANT_IDENTITY_A["nombre"]),
        SEXTANT_IDENTITY_A["gestora"],
    )
    cache_file = tmp_path / "data" / "fund_groups_cache" / f"{fund_group_id}.json"
    assert cache_file.exists()
    saved = json.loads(cache_file.read_text(encoding="utf-8"))
    assert saved["pointer_isin"] == SEXTANT_A

    # Run B: ISIN hermano, fund_dir vacío
    dir_b = tmp_path / "data" / "funds" / SEXTANT_B
    dir_b.mkdir(parents=True, exist_ok=True)
    copied = orch._fg_apply_cache(SEXTANT_B, dir_b, SEXTANT_IDENTITY_B, _silent_log)

    assert copied == {
        "manager_profile.json",
        "intl_discovery_data.json",
        "readings_data.json",
    }
    for fname in copied:
        assert (dir_b / fname).exists(), f"{fname} no copiado"
        # Contenido idéntico al pointer
        assert (dir_b / fname).read_text(encoding="utf-8") == (dir_a / fname).read_text(encoding="utf-8")


def test_apply_devuelve_vacio_si_no_hay_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "fg_cache")
    monkeypatch.delenv("DISABLE_FUND_GROUP_CACHE", raising=False)
    dir_b = tmp_path / "funds" / SEXTANT_B
    dir_b.mkdir(parents=True)
    copied = orch._fg_apply_cache(SEXTANT_B, dir_b, SEXTANT_IDENTITY_B, _silent_log)
    assert copied == set()


def test_apply_respeta_ttl_30d(tmp_path, monkeypatch):
    """Cache de >30d NO se debe aplicar."""
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "fg_cache")
    monkeypatch.delenv("DISABLE_FUND_GROUP_CACHE", raising=False)

    # Pointer dir + cache file
    dir_a = tmp_path / "data" / "funds" / SEXTANT_A
    _write_sample_outputs(dir_a)
    orch._fg_save_cache(SEXTANT_A, SEXTANT_IDENTITY_A, _silent_log)

    fund_group_id = orch._fg_compute_id(
        orch._fg_normalize_nombre_base(SEXTANT_IDENTITY_A["nombre"]),
        SEXTANT_IDENTITY_A["gestora"],
    )
    cache_file = tmp_path / "fg_cache" / f"{fund_group_id}.json"

    # Backdate el mtime a hace 45 días
    old_ts = time.time() - 45 * 86400
    os.utime(cache_file, (old_ts, old_ts))

    dir_b = tmp_path / "data" / "funds" / SEXTANT_B
    dir_b.mkdir(parents=True)
    copied = orch._fg_apply_cache(SEXTANT_B, dir_b, SEXTANT_IDENTITY_B, _silent_log)
    assert copied == set(), "cache expirado NO debe copiar"


def test_disable_env_var(tmp_path, monkeypatch):
    """DISABLE_FUND_GROUP_CACHE=1 desactiva tanto apply como save."""
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "fg_cache")
    monkeypatch.setenv("DISABLE_FUND_GROUP_CACHE", "1")

    dir_a = tmp_path / "data" / "funds" / SEXTANT_A
    _write_sample_outputs(dir_a)
    orch._fg_save_cache(SEXTANT_A, SEXTANT_IDENTITY_A, _silent_log)
    # save no debió escribir nada
    cache_dir = tmp_path / "fg_cache"
    assert not cache_dir.exists() or not any(cache_dir.iterdir())

    # apply tampoco copia (aunque el cache hubiera existido)
    dir_b = tmp_path / "data" / "funds" / SEXTANT_B
    dir_b.mkdir(parents=True)
    copied = orch._fg_apply_cache(SEXTANT_B, dir_b, SEXTANT_IDENTITY_B, _silent_log)
    assert copied == set()


def test_apply_sin_identity_es_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "fg_cache")
    monkeypatch.delenv("DISABLE_FUND_GROUP_CACHE", raising=False)
    dir_b = tmp_path / "data" / "funds" / SEXTANT_B
    dir_b.mkdir(parents=True)
    assert orch._fg_apply_cache(SEXTANT_B, dir_b, {}, _silent_log) == set()
    assert orch._fg_apply_cache(SEXTANT_B, dir_b, {"nombre": "X"}, _silent_log) == set()
    assert orch._fg_apply_cache(SEXTANT_B, dir_b, {"gestora": "Y"}, _silent_log) == set()


def test_apply_no_copia_si_pointer_es_mismo_isin(tmp_path, monkeypatch):
    """Edge case: si el cache apunta al mismo ISIN que estamos analizando,
    NO debe copiar (sería sobrescribir consigo mismo)."""
    monkeypatch.setattr(orch, "ROOT", tmp_path)
    monkeypatch.setattr(orch, "FUND_GROUP_CACHE_DIR", tmp_path / "fg_cache")
    monkeypatch.delenv("DISABLE_FUND_GROUP_CACHE", raising=False)
    dir_a = tmp_path / "data" / "funds" / SEXTANT_A
    _write_sample_outputs(dir_a)
    orch._fg_save_cache(SEXTANT_A, SEXTANT_IDENTITY_A, _silent_log)
    # Mismo ISIN A
    copied = orch._fg_apply_cache(SEXTANT_A, dir_a, SEXTANT_IDENTITY_A, _silent_log)
    assert copied == set()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
