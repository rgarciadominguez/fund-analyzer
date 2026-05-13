"""Tests del refactor de import_taxonomy al schema Supabase v2-cowork."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.import_taxonomy import (  # noqa: E402
    agrupar_por_fondo,
    extract_gestora,
    normalize_nombre_base,
)


# ---------------------------------------------------------------------------
# normalize_nombre_base
# ---------------------------------------------------------------------------

def test_normalize_nombre_base_clase_simple():
    assert normalize_nombre_base("Cobas Internacional Clase A") == "Cobas Internacional"


def test_normalize_nombre_base_clase_b():
    assert normalize_nombre_base("Cobas Internacional Clase B") == "Cobas Internacional"


def test_normalize_nombre_base_class_english():
    assert (
        normalize_nombre_base("Magallanes European Equity Class I EUR")
        == "Magallanes European Equity"
    )


def test_normalize_nombre_base_paren_noise():
    assert (
        normalize_nombre_base("Templeton Latin America Fund A(acc)USD")
        == "Templeton Latin America Fund"
    )


def test_normalize_nombre_base_sin_sufijos_no_toca_palabras_validas():
    # No debe trim agresivo si no hay sufijo claro
    assert normalize_nombre_base("Cobas Selección") == "Cobas Selección"


def test_normalize_nombre_base_strip_fi():
    assert normalize_nombre_base("Renta 4 Bolsa FI") == "Renta 4 Bolsa"


# ---------------------------------------------------------------------------
# Hedged y combinaciones
# ---------------------------------------------------------------------------

def test_normalize_hedged():
    assert normalize_nombre_base("DNCA Alpha Bonds I EUR Hedged") == "DNCA Alpha Bonds"


def test_normalize_h_prefix_hedged():
    # "H-EUR" como prefijo de clase hedged
    assert normalize_nombre_base("DNCA Alpha Bonds H-EUR") == "DNCA Alpha Bonds"


def test_normalize_hedged_currency_only():
    assert normalize_nombre_base("DNCA Alpha Bonds I EUR") == "DNCA Alpha Bonds"


# ---------------------------------------------------------------------------
# extract_gestora
# ---------------------------------------------------------------------------

def test_extract_gestora_simple():
    assert extract_gestora("Cobas Internacional Clase A") == "Cobas"


def test_extract_gestora_dnca():
    assert extract_gestora("DNCA Alpha Bonds I EUR Hedged") == "DNCA"


def test_extract_gestora_renta_4():
    # Caso especial: "Renta 4" como gestora multi-palabra
    assert extract_gestora("Renta 4 Bolsa FI") == "Renta 4"


# ---------------------------------------------------------------------------
# agrupar_por_fondo
# ---------------------------------------------------------------------------

def _cobas_4_classes():
    """Construye un dict con 4 clases de Cobas Internacional (A/B/C/D)."""
    return {
        "ES0124037001": {
            "isin": "ES0124037001",
            "nombre": "Cobas Internacional Clase A",
            "divisa": "EUR",
            "ter": 1.75,
            "clasificacion_user": "Top",
            "opinion": "Tesis value ibérica",
            "filosofia": "Value investing",
            "categoria": "Gestionado",
            "tipo_activo": "RV",
            "geografia": "Global",
        },
        "ES0124037002": {
            "isin": "ES0124037002",
            "nombre": "Cobas Internacional Clase B",
            "divisa": "EUR",
            "ter": 1.50,
            "clasificacion_user": "Top",
        },
        "ES0124037003": {
            "isin": "ES0124037003",
            "nombre": "Cobas Internacional Clase C",
            "divisa": "EUR",
            "ter": 1.25,
        },
        "ES0124037004": {
            "isin": "ES0124037004",
            "nombre": "Cobas Internacional Clase D",
            "divisa": "EUR",
            "ter": 1.00,
        },
    }


def test_agrupar_clases_4_isins_misma_gestora():
    funds = _cobas_4_classes()
    fund_groups, funds_data = agrupar_por_fondo(funds)

    assert len(fund_groups) == 1, "Las 4 clases deben colapsar en un único fund_group"
    grupo = fund_groups[0]
    assert grupo["nombre_base"] == "Cobas Internacional"
    assert grupo["gestora"] == "Cobas"
    assert sorted(grupo["class_isins_known"]) == [
        "ES0124037001", "ES0124037002", "ES0124037003", "ES0124037004",
    ]

    # Y debe haber 4 funds_data (1 por clase) apuntando todos al mismo group_id
    assert len(funds_data) == 4
    group_ids = {f["fund_group_id"] for f in funds_data}
    assert group_ids == {grupo["fund_group_id"]}


def test_agrupar_fund_group_hereda_campos_qualitativos_de_cualquier_clase():
    """Si solo la clase A tiene filosofía/categoría, el group debe heredarlas."""
    funds = _cobas_4_classes()
    fund_groups, _ = agrupar_por_fondo(funds)
    g = fund_groups[0]
    assert g["filosofia"] == "Value investing"
    assert g["categoria"] == "Gestionado"
    assert g["tipo_activo"] == "RV"
    assert g["geografia"] == "Global"


def test_agrupar_genera_fund_group_id_deterministico():
    """Misma entrada -> mismo fund_group_id (idempotencia del upsert)."""
    funds = _cobas_4_classes()
    g1, _ = agrupar_por_fondo(funds)
    g2, _ = agrupar_por_fondo(funds)
    assert g1[0]["fund_group_id"] == g2[0]["fund_group_id"]


def test_agrupar_dos_fondos_distintos_dan_dos_groups():
    funds = {
        "ES0124037001": {"isin": "ES0124037001", "nombre": "Cobas Internacional Clase A"},
        "ES0159259011": {"isin": "ES0159259011", "nombre": "Magallanes European Equity M"},
    }
    fund_groups, _ = agrupar_por_fondo(funds)
    assert len(fund_groups) == 2
    nombres = {g["nombre_base"] for g in fund_groups}
    assert nombres == {"Cobas Internacional", "Magallanes European Equity"}


# ---------------------------------------------------------------------------
# Dry-run no escribe nada ni llama a Supabase
# ---------------------------------------------------------------------------

def test_upload_dry_run_no_llama_a_supabase(monkeypatch, capsys, tmp_path):
    """Con --dry-run NO se invoca get_client ni upsert."""
    # Mock get_client del módulo supabase_client
    from tools import supabase_client as sc_module
    fake_client = MagicMock()
    fake_get_client = MagicMock(return_value=fake_client)
    monkeypatch.setattr(sc_module, "get_client", fake_get_client)

    # Mock build_taxonomy para no leer Excel real
    fake_taxonomy = {
        "version": "2.0",
        "generated_at": "2026-05-13",
        "n_funds": 1,
        "n_fund_groups": 1,
        "stats": {},
        "funds": {"ES0124037001": {"isin": "ES0124037001", "nombre": "X"}},
        "fund_groups_data": [{
            "fund_group_id": "abc", "nombre_base": "X", "gestora": "Y", "class_isins_known": ["ES0124037001"],
        }],
        "funds_data": [{"isin": "ES0124037001", "fund_group_id": "abc", "nombre_clase": "X"}],
    }
    from tools import import_taxonomy as it
    monkeypatch.setattr(it, "build_taxonomy", lambda *a, **kw: fake_taxonomy)

    output_file = tmp_path / "fund_taxonomy.json"
    monkeypatch.setattr(it, "OUTPUT_FILE", output_file)

    # Simula argv para --dry-run
    monkeypatch.setattr(sys, "argv", ["import_taxonomy", "--dry-run"])
    exit_code = it.main()

    assert exit_code == 0
    # No se debe haber creado el JSON
    assert not output_file.exists(), "--dry-run NO debe escribir fund_taxonomy.json"
    # No se debe haber llamado a Supabase
    fake_get_client.assert_not_called()
    fake_client.table.assert_not_called()


def test_upload_supabase_invoca_upsert(monkeypatch, tmp_path):
    """Con --upload-supabase SÍ se llama a client.table().upsert()."""
    from tools import supabase_client as sc_module
    fake_table = MagicMock()
    fake_table.upsert.return_value.execute.return_value = MagicMock(data=[])
    fake_client = MagicMock()
    fake_client.table.return_value = fake_table
    monkeypatch.setattr(sc_module, "get_client", lambda: fake_client)

    fake_taxonomy = {
        "version": "2.0",
        "generated_at": "2026-05-13",
        "n_funds": 1,
        "n_fund_groups": 1,
        "stats": {},
        "funds": {"ES0124037001": {"isin": "ES0124037001", "nombre": "X"}},
        "fund_groups_data": [{"fund_group_id": "abc", "nombre_base": "X", "gestora": "Y", "class_isins_known": ["ES0124037001"]}],
        "funds_data": [{"isin": "ES0124037001", "fund_group_id": "abc"}],
    }
    from tools import import_taxonomy as it
    monkeypatch.setattr(it, "build_taxonomy", lambda *a, **kw: fake_taxonomy)
    monkeypatch.setattr(it, "OUTPUT_FILE", tmp_path / "fund_taxonomy.json")

    monkeypatch.setattr(sys, "argv", ["import_taxonomy", "--upload-supabase"])
    exit_code = it.main()

    assert exit_code == 0
    # Se debe haber llamado a table("fund_groups").upsert(...) y table("funds").upsert(...)
    call_args = [c.args[0] for c in fake_client.table.call_args_list]
    assert "fund_groups" in call_args
    assert "funds" in call_args


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
