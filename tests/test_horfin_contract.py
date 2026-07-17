"""
Contrato de datos con Horizonte Financiero (2026-07-17).

Lo que protegen estos tests:
  1. BLINDADO: descripcion/opinion/categoria_activo de Horizonte NUNCA se pisan.
  2. `descripcion` no valora ("costes altos" es opinion, no descripcion).
  3. Vocabulario de benchmark: un concepto = una grafía (incluido lo que acuña el modelo).
  4. `categoria_activo` es lista cerrada.

Offline: no tocan Supabase ni LLM.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Palabras de juicio: si aparecen en una `descripcion`, la regla está rota.
JUICIO = re.compile(
    r"\b(alto|altos|alta|altas|elevad\w+|caro|cara|barat\w+|buen\w*|mal\w*|"
    r"sólid\w+|solid\w+|excelent\w+|atractiv\w+|competitiv\w+|contenid\w+|"
    r"razonabl\w+|destac\w+|recomend\w+)\b",
    re.I,
)


def test_categoria_activo_lista_cerrada():
    from tools.horfin_ingest import VALID_CATEGORIA
    assert VALID_CATEGORIA == {"Indexado", "Gestionado", "Hedgefund"}


def test_vocabulario_una_grafia_por_concepto():
    """Las grafías sucias de Horizonte colapsan en un solo canónico."""
    from tools.benchmark_vocab import canon
    assert canon("EURIBOR") == canon("Euribor") == "Euribor"
    assert canon("sp500") == canon("SP500") == "SP500"
    assert canon("MSCI Europa") == canon("MSCI Europe") == "MSCI Europe"
    assert canon("Renta FIja Meedio Plazo") == "Renta Fija Medio Plazo"
    # mixtos: la misma cartera escrita al revés es el mismo cubo
    assert canon("60% RF 40% RV") == canon("60% RF y 40% RV") == "40% RV y 60% RF"
    assert canon("75% RF 25% RV") == canon("25% RV 75% RF") == "25% RV y 75% RF"
    assert canon("") is None


def test_vocabulario_normaliza_tambien_lo_acunado():
    """El clasificador acuñó 'Cat Bonds' Y 'Bonos Catástrofe' para lo mismo. No debe repetirse."""
    from tools.benchmark_vocab import canon
    assert canon("Cat Bonds") == canon("Bonos Catástrofe") == "Bonos Catástrofe"
    assert canon("Renta Fija Nordics") == "Renta Fija Nórdica"


def test_descripcion_no_valora_regex():
    """El gate que usamos para auditar: detecta juicio en una descripcion."""
    assert JUICIO.search("Fondo monetario con costes altos")          # esto es opinion
    assert JUICIO.search("Fondo con buen track record")
    assert not JUICIO.search("Fondo monetario de deuda publica española principalmente")
    assert not JUICIO.search("Fondo de renta variable europea centrado en microcaps")


def test_ingest_blindado_no_pisa_a_horizonte(monkeypatch):
    """Si Horizonte escribió la opinión, el plan de ingest NO la cambia por la nuestra."""
    from tools import horfin_ingest as mod

    monkeypatch.setattr(mod, "load_aporte", lambda: {
        "ES0000000001": {"isin": "ES0000000001", "descripcion": "Fondo monetario deuda pública",
                         "opinion": "OPINION DE HORIZONTE", "categoria_activo": "Gestionado",
                         "horfin_id": "1"},
    })

    class _Q:
        def __init__(self, out): self._out = out
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def update(self, d): self._out.append(d); return self
        def execute(self): return type("R", (), {"data": self._out if self._out else [
            {"isin": "ES0000000001", "opinion_user": "MI OPINION VIEJA", "horfin_id": ""}]})()

    writes = []

    class _C:
        def table(self, _): return _Q(writes)

    monkeypatch.setattr(mod, "REPORT", Path(__file__).parent / "_tmp_report.json")
    monkeypatch.setitem(sys.modules, "tools.supabase_client",
                        type("M", (), {"get_client": staticmethod(lambda: _C())}))
    rep = mod.ingest(apply=False)

    plan = rep["plan"][0]["update"]
    # gana Horizonte, no nosotros
    assert plan["opinion_user"] == "OPINION DE HORIZONTE"
    assert plan["descripcion"] == "Fondo monetario deuda pública"
    assert plan["categoria_activo"] == "Gestionado"
    assert not rep["avisos"]


def test_categoria_fuera_de_lista_avisa_y_no_escribe(monkeypatch):
    """Un valor nuevo de categoria_activo se avisa; no entra por la puerta de atrás."""
    from tools import horfin_ingest as mod

    monkeypatch.setattr(mod, "load_aporte", lambda: {
        "ES0000000002": {"isin": "ES0000000002", "categoria_activo": "Cuantitativo"},
    })

    class _Q:
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def execute(self): return type("R", (), {"data": [
            {"isin": "ES0000000002", "opinion_user": "", "horfin_id": ""}]})()

    class _C:
        def table(self, _): return _Q()

    monkeypatch.setattr(mod, "REPORT", Path(__file__).parent / "_tmp_report2.json")
    monkeypatch.setitem(sys.modules, "tools.supabase_client",
                        type("M", (), {"get_client": staticmethod(lambda: _C())}))
    rep = mod.ingest(apply=False)

    assert rep["avisos"] and "Cuantitativo" in rep["avisos"][0]
    assert not rep["plan"]


def test_kid_solo_si_abre_de_verdad(monkeypatch):
    """'Preferimos null a un enlace que no abre': un 200 que no es PDF no vale."""
    from tools import kid_and_subscription as mod

    class _R:
        def __init__(self, code, body): self.status_code, self.content = code, body

    cases = {
        "https://x/ok.pdf": _R(200, b"%PDF-1.4 real"),
        "https://x/html.pdf": _R(200, b"<html>Not found</html>"),   # 200 pero no es PDF
        "https://x/dead.pdf": _R(404, b""),
    }
    monkeypatch.setattr(mod.httpx, "get", lambda url, **k: cases[url])
    assert mod.verify_url("https://x/ok.pdf") is True
    assert mod.verify_url("https://x/html.pdf") is False
    assert mod.verify_url("https://x/dead.pdf") is False
    # ni se intenta: sin esquema http no es una URL pública
    assert mod.verify_url("") is False
    assert mod.verify_url("file:///C:/Users/RafaelGarcía/Downloads/kid.pdf") is False


def test_redondeo_2_decimales_y_null_no_cero():
    from tools.catalog_fees_stars import _r2
    assert _r2(0.8999999999999999) == 0.9
    assert _r2(1.7000000000000002) == 1.7
    assert _r2(0.27999999999999997) == 0.28
    assert _r2(None) is None          # "no sé" NO es 0
    assert _r2("") is None
    assert _r2(0) == 0                # 0 significa "no la tiene": se respeta


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
