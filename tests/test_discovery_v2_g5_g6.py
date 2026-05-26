"""Tests unitarios para G5 (queries multi-idioma) + G6 (candidate_domains) +
G7 (auto-learn registry) del discovery_v2.

NO ejecutan Google CSE ni Serper — testean helpers puros + persistencia local.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents import discovery_v2 as dv2  # noqa: E402


# ---------------------------------------------------------------------------
# G6 — candidate_domains
# ---------------------------------------------------------------------------

def test_slugify_amiral_gestion():
    slugs = dv2._slugify_gestora("Amiral Gestion")
    assert "amiralgestion" in slugs
    assert "amiral-gestion" in slugs
    assert "amiral" in slugs  # primer token como fallback


def test_slugify_handles_caracteres_especiales():
    slugs = dv2._slugify_gestora("M&G Investments")
    # & se elimina
    assert any("mg" in s or "minvestments" in s or "investments" in s for s in slugs)


def test_slugify_vacio_devuelve_lista_vacia():
    assert dv2._slugify_gestora("") == []
    assert dv2._slugify_gestora(None) == []


def test_candidate_domains_amiral_FR():
    cands = dv2.candidate_domains("Amiral Gestion", "FR")
    # debe incluir .fr y .com
    assert "amiralgestion.fr" in cands
    assert "amiralgestion.com" in cands
    assert len(cands) <= 10


def test_candidate_domains_fundsmith_GB():
    cands = dv2.candidate_domains("Fundsmith", "GB")
    assert "fundsmith.co.uk" in cands
    assert "fundsmith.com" in cands


def test_candidate_domains_region_desconocida_usa_default():
    cands = dv2.candidate_domains("XYZ Capital", "ZZ_FAKE")
    # Fallback EN_GLOBAL: .com + .net
    assert any(c.endswith(".com") for c in cands)
    assert any(c.endswith(".net") for c in cands)


def test_candidate_domains_gestora_vacia():
    assert dv2.candidate_domains("", "FR") == []


# ---------------------------------------------------------------------------
# G5 — _detect_region_from_isin
# ---------------------------------------------------------------------------

def test_detect_region_isin_prefix_explicito():
    assert dv2._detect_region_from_isin("FR001400CEK6") == "FR"
    assert dv2._detect_region_from_isin("DE000A1H72N0") == "DE"
    assert dv2._detect_region_from_isin("GB00B0J19K27") == "GB"
    assert dv2._detect_region_from_isin("ES0119199000") == "ES"


def test_detect_region_LU_por_gestora_FR():
    """Caso multidomicilio: LU por gestora francesa → FR."""
    assert dv2._detect_region_from_isin("LU1234567890", "DNCA Investments") == "FR"


def test_detect_region_LU_sin_hint_es_global():
    assert dv2._detect_region_from_isin("LU1234567890", "Gestora Desconocida") == "EN_GLOBAL"


def test_detect_region_IE_por_gestora_GB():
    assert dv2._detect_region_from_isin("IE00BYVDZH74", "Fundsmith LLP") == "GB"


# ---------------------------------------------------------------------------
# G5 — _generate_g5_queries
# ---------------------------------------------------------------------------

class _StubAgent:
    """Stub mínimo para llamar métodos de instancia de DiscoveryV2 sin instanciar todo."""
    pass


def _make_stub(isin: str, identity: dict | None = None) -> dv2.DiscoveryV2:
    """Crea DiscoveryV2 mínimo sin fund_dir real (in-memory)."""
    return dv2.DiscoveryV2(
        isin=isin,
        identity=identity or {},
        gap={},
        fund_dir=Path("/tmp/_test_nonexistent"),
    )


def test_g5_queries_fr_genera_rapport_annuel():
    stub = _make_stub("FR001400CEK6", {"gestora_oficial": "Amiral Gestion"})
    queries = stub._generate_g5_queries("FR", "Sextant Grand Large", "Amiral Gestion")
    assert len(queries) > 0
    assert any('"rapport annuel"' in q for q in queries)
    assert any("filetype:pdf" in q for q in queries)
    assert any('"Sextant Grand Large"' in q for q in queries)


def test_g5_queries_de_genera_jahresbericht():
    stub = _make_stub("DE000123456")
    queries = stub._generate_g5_queries("DE", "DWS Top Dividende")
    assert any('"Jahresbericht"' in q for q in queries)


def test_g5_queries_es_genera_informe_anual():
    stub = _make_stub("ES0119199000")
    queries = stub._generate_g5_queries("ES", "Cobas Internacional")
    assert any('"informe anual"' in q for q in queries)


def test_g5_queries_sin_fund_name_devuelve_vacio():
    stub = _make_stub("FR001400CEK6")
    assert stub._generate_g5_queries("FR", "") == []


def test_g5_queries_cap_a_6():
    stub = _make_stub("FR001400CEK6", {"gestora_oficial": "Amiral"})
    queries = stub._generate_g5_queries("FR", "Test Fund", "Amiral")
    assert len(queries) <= 6


# ---------------------------------------------------------------------------
# G7 — _persist_to_registry (E2E con tmp_path)
# ---------------------------------------------------------------------------

def test_g7_persist_crea_entry_nueva(tmp_path, monkeypatch):
    # Mock registry path
    fake_registry = tmp_path / "data" / "gestoras_registry.json"
    fake_registry.parent.mkdir(parents=True)
    fake_registry.write_text(json.dumps({"gestoras": {}}), encoding="utf-8")

    # Mock Path.resolve().parent.parent → tmp_path
    monkeypatch.setattr(
        dv2, "Path",
        type("PathMock", (), {
            "__new__": lambda cls, *args, **kwargs: Path(*args, **kwargs),
            "resolve": staticmethod(lambda: type("X", (), {"parent": type("Y", (), {"parent": tmp_path})})()),
        }),
    )
    # Más simple: parchamos directamente la lectura/escritura usando un monkeypatch del __file__
    # Re-enfoque: en vez de mockear Path, llamamos _persist_to_registry con un instance custom
    # cuyo __file__ esté en tmp_path. Skip ese approach y usa workaround directo:
    monkeypatch.setattr(dv2, "__file__", str(tmp_path / "agents" / "discovery_v2.py"))
    (tmp_path / "agents").mkdir(exist_ok=True)

    stub = _make_stub("FR001400CEK6", {"gestora_oficial": "Amiral Gestion"})
    stub._persist_to_registry("amiralgestion.com", 3)

    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    entry = reg["gestoras"].get("Amiral Gestion")
    assert entry is not None
    assert entry["auto_learned"] is True
    assert entry["from_isin"] == "FR001400CEK6"
    assert entry["discovered_doc_count"] == 3
    assert "amiralgestion.com" in entry["web"]
    assert "FR001400CEK6" in entry["funds"]


def test_g7_respeta_entry_manual_auto_learned_false(tmp_path, monkeypatch):
    fake_registry = tmp_path / "data" / "gestoras_registry.json"
    fake_registry.parent.mkdir(parents=True)
    fake_registry.write_text(json.dumps({
        "gestoras": {
            "Amiral Gestion": {
                "web": "https://manual.example.com",
                "auto_learned": False,  # manual!
                "reports_url": "https://manual.example.com/reports",
            }
        }
    }), encoding="utf-8")
    monkeypatch.setattr(dv2, "__file__", str(tmp_path / "agents" / "discovery_v2.py"))
    (tmp_path / "agents").mkdir(exist_ok=True)

    stub = _make_stub("FR001400CEK6", {"gestora_oficial": "Amiral Gestion"})
    stub._persist_to_registry("amiralgestion.com", 5)

    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    entry = reg["gestoras"]["Amiral Gestion"]
    # NO debe haberse sobrescrito
    assert entry["web"] == "https://manual.example.com"
    assert entry["auto_learned"] is False
    assert entry["reports_url"] == "https://manual.example.com/reports"


def test_g7_no_persist_si_doc_count_cero(tmp_path, monkeypatch):
    fake_registry = tmp_path / "data" / "gestoras_registry.json"
    fake_registry.parent.mkdir(parents=True)
    fake_registry.write_text(json.dumps({"gestoras": {}}), encoding="utf-8")
    monkeypatch.setattr(dv2, "__file__", str(tmp_path / "agents" / "discovery_v2.py"))
    (tmp_path / "agents").mkdir(exist_ok=True)

    stub = _make_stub("FR001400CEK6", {"gestora_oficial": "Amiral Gestion"})
    stub._persist_to_registry("amiralgestion.com", 0)

    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    assert "Amiral Gestion" not in reg["gestoras"]


def test_g7_no_persist_si_falta_gestora(tmp_path, monkeypatch):
    fake_registry = tmp_path / "data" / "gestoras_registry.json"
    fake_registry.parent.mkdir(parents=True)
    fake_registry.write_text(json.dumps({"gestoras": {}}), encoding="utf-8")
    monkeypatch.setattr(dv2, "__file__", str(tmp_path / "agents" / "discovery_v2.py"))
    (tmp_path / "agents").mkdir(exist_ok=True)

    stub = _make_stub("FR001400CEK6", {})  # sin gestora_oficial
    stub._persist_to_registry("amiralgestion.com", 5)

    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    assert reg["gestoras"] == {}


# ---------------------------------------------------------------------------
# F6 extension — intl_data.json en FUND_GROUP_CACHE_FILES
# ---------------------------------------------------------------------------

def test_f6_extension_intl_data_in_cache_files():
    from agents import orchestrator as orch
    assert "intl_data.json" in orch.FUND_GROUP_CACHE_FILES
    assert "manager_profile.json" in orch.FUND_GROUP_CACHE_FILES
    assert "intl_discovery_data.json" in orch.FUND_GROUP_CACHE_FILES
    assert "readings_data.json" in orch.FUND_GROUP_CACHE_FILES


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
