"""
Test N3 (branch v2-cowork, 2026-05-20): pipeline INT end-to-end con HTML
fallback exitoso para Sextant Quality Focus (Amiral Gestion).

Verifica el flujo completo:
  intl_extractor_v2._fallback_html_extract → AUM + posiciones en output
  → registry recibe entry Amiral con auto_learned=true + useful_domains
  → intl_data.json escrito con _html_fallback trace

Mock infrastructure:
  - fetch_url: devuelve texto sintético del fondo
  - google.genai.Client: devuelve JSON con kpis + aum_source_url específico
  - _registry_path: redirigido a tmp para no contaminar data/gestoras_registry
"""
import asyncio
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.intl_extractor_v2 import IntlExtractor, _empty_output


ISIN = "FR001400CEK6"
FUND_NAME = "Sextant Quality Focus"
GESTORA = "Amiral Gestion"


class _FakeFetchResult:
    def __init__(self, url: str, text: str, ok: bool = True):
        self.url = url
        self.text = text
        self.ok = ok
        self.html = text
        self.status_code = 200 if ok else 0
        self.error = ""


class _FakeGeminiModels:
    def __init__(self, payload: str):
        self.payload = payload
        self.calls = 0

    def generate_content(self, model, contents, **kwargs):
        self.calls += 1
        class R:
            text = self.payload
        return R()


class _FakeGeminiClient:
    def __init__(self, api_key=None, payload: str = ""):
        self.models = _FakeGeminiModels(payload)


def test_amiral_sextant_end_to_end_fills_output_and_registry(tmp_path, monkeypatch):
    """Pipeline mínimo (extractor + fallback) que valida:
      - output["kpis"]["aum_actual_meur"] != None
      - output["posiciones"]["actuales"] tiene 3 holdings
      - output["_html_fallback"] presente, urls_processed >= 1
      - registry escribe entry Amiral Gestion con auto_learned=true
      - registry tiene html_fallback_useful_domains con morningstar primero
        (porque aum_source_url apuntaba a morningstar)
    """
    fund_dir = tmp_path / "data" / "funds" / ISIN
    fund_dir.mkdir(parents=True)

    # search_cache con mezcla amiralgestion + morningstar (aggregator)
    search_cache = {
        "queries": {},
        "urls": {
            "https://www.amiralgestion.com/en/sextant-quality-focus":
                {"title": "Sextant Quality Focus", "snippet": "fund page"},
            "https://www.morningstar.fr/fr/funds/snapshot/snapshot.aspx?fund=sextant-quality-focus":
                {"title": "Sextant QF | Morningstar", "snippet": "AUM data"},
            "https://www.amiralgestion.com/en/notre-vision":
                {"title": "Vision", "snippet": "corporate (debe filtrarse)"},
        },
    }
    (fund_dir / "search_cache.json").write_text(
        json.dumps(search_cache, ensure_ascii=False), encoding="utf-8",
    )

    extractor = IntlExtractor(ISIN, config={"nombre": FUND_NAME, "gestora": GESTORA})
    extractor.fund_dir = fund_dir

    # Isolate registry
    fake_registry = tmp_path / "gestoras_registry.json"
    from agents import discovery_v2 as _dv2
    monkeypatch.setattr(_dv2, "_registry_path", lambda: fake_registry)

    fetched: list[str] = []

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        fetched.append(url)
        if "morningstar" in url:
            text = (
                f"Sextant Quality Focus ({ISIN}) - Net Assets: 350.7 M EUR "
                "as of 2026-04-30. Domicile France. Currency EUR. "
                "Top holdings: Microsoft, Visa, Alphabet. "
            ) * 5
        elif "sextant-quality-focus" in url:
            text = (
                "Sextant Quality Focus is a quality equity fund. "
                "Managed by Amiral Gestion. "
            ) * 30
        else:
            text = "boilerplate " * 100
        return _FakeFetchResult(url=url, text=text, ok=True)

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {
            "aum_actual_meur": 350.7,
            "aum_source_url": "https://www.morningstar.fr/fr/funds/snapshot/snapshot.aspx?fund=sextant-quality-focus",
            "fecha_aum": "2026-04-30",
            "num_participes": 4250,
            "ter_pct": 1.95,
            "divisa_base": "EUR",
        },
        "posiciones_actuales": [
            {"nombre": "Microsoft", "peso_pct": 5.2, "sector": "IT", "pais": "USA"},
            {"nombre": "Visa", "peso_pct": 4.5, "sector": "Financials", "pais": "USA"},
            {"nombre": "Alphabet", "peso_pct": 4.1, "sector": "Comm", "pais": "USA"},
        ],
        "gestores": [
            {"nombre": "Vincent Mercadier", "cargo": "Lead PM"},
        ],
        "anti_invencion_note": "",
    })

    fake_module = types.ModuleType("google.genai")
    fake_module.Client = lambda api_key=None: _FakeGeminiClient(api_key, payload)
    google_pkg = types.ModuleType("google")
    google_pkg.genai = fake_module
    monkeypatch.setitem(sys.modules, "google", google_pkg)
    monkeypatch.setitem(sys.modules, "google.genai", fake_module)
    monkeypatch.setenv("GOOGLE_API_KEY", "dummy")

    out = _empty_output(ISIN, FUND_NAME, GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))

    # 1. Fallback exitoso
    assert ok, "el fallback debió rellenar campos"

    # 2. output schema correcto
    assert out["kpis"]["aum_actual_meur"] == 350.7
    assert out["kpis"]["num_participes"] == 4250
    assert out["kpis"]["ter_pct"] == 1.95
    assert len(out["posiciones"]["actuales"]) == 3
    assert {p["nombre"] for p in out["posiciones"]["actuales"]} == {"Microsoft", "Visa", "Alphabet"}

    # 3. Gestor extraído
    assert any(g["nombre"] == "Vincent Mercadier" for g in out["cualitativo"]["gestores"])

    # 4. serie_aum sembrada con el año del corte
    assert any(
        e["periodo"] == "2026" and e["valor_meur"] == 350.7
        for e in out["cuantitativo"]["serie_aum"]
    )

    # 5. _html_fallback trace
    fb = out["_html_fallback"]
    assert fb["model"] == "gemini-2.5-flash"
    assert fb["urls_processed"] >= 1
    # N4: aum_source_url domain (morningstar.fr) viene PRIMERO en useful_domains
    assert fb["useful_domains"][0] == "www.morningstar.fr", (
        f"useful_domains[0] debería ser morningstar.fr (priority), got: {fb['useful_domains']}"
    )

    # 6. Registry escrito con auto_learned=true + useful_domains poblado
    assert fake_registry.exists()
    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    entry = (reg.get("gestoras") or {}).get(GESTORA) or {}
    assert entry, f"gestora '{GESTORA}' no en registry: {reg}"
    assert entry.get("auto_learned") is True
    assert ISIN in entry.get("funds") or []
    useful = entry.get("html_fallback_useful_domains") or []
    assert "www.morningstar.fr" in useful, useful

    # 7. blacklist URL no se fetchó
    assert not any("/notre-vision" in u for u in fetched), (
        f"notre-vision URL no debió fetcharse: {fetched}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
