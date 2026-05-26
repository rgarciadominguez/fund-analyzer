"""
Tests G11+G13 (branch v2-cowork, 2026-05-19): fallback HTML para INT.

Valida que:
  - `tools.intl_url_filter.rank_fund_page_urls` filtra correctamente blacklist
    (notre-vision, journal-de-bord, informations-reglementaires) y conserva
    páginas del fondo (sextant-quality-focus, publications-adminmenu).
  - `IntlExtractor._fallback_html_extract` orquesta fetcher + Gemini + merge,
    rellenando `kpis.aum_actual_meur` cuando Gemini devuelve un valor.

Fixture: usa `data/funds/FR001400CEK6/search_cache.json` real (121 URLs, 17
amiralgestion). Mockea `tools.web_fetcher.fetch_url` y el cliente Gemini para
que el test sea offline + determinista.
"""
import asyncio
import json
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.intl_extractor_v2 import IntlExtractor, _empty_output
from tools.intl_url_filter import is_fund_page_url, rank_fund_page_urls


FIXTURE_ISIN = "FR001400CEK6"
FIXTURE_FUND_NAME = "Sextant Quality Focus"
FIXTURE_GESTORA = "Amiral Gestion"
FIXTURE_DIR = ROOT / "data" / "funds" / FIXTURE_ISIN


# ════════════════════════════════════════════════════════════════════
# G13 — URL pre-filter
# ════════════════════════════════════════════════════════════════════


def test_filter_drops_blacklist_paths():
    """notre-vision, equipe, journal-de-bord deben filtrarse fuera."""
    blacklist_examples = [
        "https://www.amiralgestion.com/en/notre-vision",
        "https://www.amiralgestion.com/en/investissement-responsable",
        "https://www.amiralgestion.com/en/informations-reglementaires",
        "https://www.amiralgestion.com/fr/actualites/journal-de-bord-de-l-equipage-de-sextant-regatta-2031-mai-2025",
        "https://www.amiralgestion.com/en/gestion-privee",
    ]
    for url in blacklist_examples:
        assert not is_fund_page_url(
            url, isin=FIXTURE_ISIN, fund_name=FIXTURE_FUND_NAME,
            gestora=FIXTURE_GESTORA,
        ), f"esperaba bloquear {url}"


def test_filter_keeps_fund_pages():
    """Páginas del sub-fondo deben pasar el filtro."""
    keep_examples = [
        "https://www.amiralgestion.com/en/sextant-quality-focus",
        "https://www.amiralgestion.com/es/sextant-quality-focus",
        "https://www.amiralgestion.com/en/publications-adminmenu/sextant-quality-focus-a-annuel",
    ]
    for url in keep_examples:
        assert is_fund_page_url(
            url, isin=FIXTURE_ISIN, fund_name=FIXTURE_FUND_NAME,
            gestora=FIXTURE_GESTORA,
        ), f"esperaba conservar {url}"


def test_filter_isin_in_url_overrides_blacklist():
    """Si la URL contiene el ISIN exacto, pasa aunque el path parezca corporate."""
    url = f"https://www.amiralgestion.com/en/notre-vision?fund={FIXTURE_ISIN}"
    assert is_fund_page_url(url, isin=FIXTURE_ISIN, fund_name=FIXTURE_FUND_NAME)


def test_rank_uses_real_search_cache():
    """Con el search_cache real, el filtro devuelve URLs del sub-fondo entre las primeras."""
    sc_path = FIXTURE_DIR / "search_cache.json"
    if not sc_path.exists():
        pytest.skip(f"fixture {sc_path} no presente — solo ejecutable en repo completo")
    sc = json.loads(sc_path.read_text(encoding="utf-8"))
    urls = [u for u in sc.get("urls", {}).keys() if isinstance(u, str)]
    assert len(urls) >= 50, "se esperaban >=50 URLs cacheadas para FR001400CEK6"

    ranked = rank_fund_page_urls(
        urls, isin=FIXTURE_ISIN, fund_name=FIXTURE_FUND_NAME,
        gestora=FIXTURE_GESTORA, max_urls=10,
    )
    assert ranked, "el filtro descartó todas las URLs amiralgestion"

    # Heurísticas: aparece al menos una URL con el slug del fondo
    fund_token = "sextant-quality-focus"
    matches = [u for u in ranked if fund_token in u.lower()]
    assert matches, (
        "ranked no contiene URLs con slug 'sextant-quality-focus'; "
        f"top={ranked[:3]}"
    )

    # Y ninguna URL claramente blacklist sobrevive
    for url in ranked:
        url_lc = url.lower()
        for blocked in ("/notre-vision", "/informations-reglementaires",
                        "/gestion-privee", "/journal-de-bord"):
            assert blocked not in url_lc, f"blacklist {blocked} sobrevivió en ranked"


# ════════════════════════════════════════════════════════════════════
# G11 — _fallback_html_extract orchestration
# ════════════════════════════════════════════════════════════════════


class _FakeFetchResult:
    """Mock mínimo de web_fetcher.FetchResult."""

    def __init__(self, url: str, text: str = "", ok: bool = True):
        self.url = url
        self.text = text
        self.ok = ok
        self.html = text  # innecesario para el fallback pero por simetría
        self.status_code = 200 if ok else 0
        self.error = ""


class _FakeGeminiResponse:
    def __init__(self, text: str):
        self.text = text


class _FakeGeminiModels:
    def __init__(self, payload: str):
        self._payload = payload
        self.calls = []

    def generate_content(self, model: str, contents: str, **kwargs):
        self.calls.append({"model": model, "contents_len": len(contents)})
        return _FakeGeminiResponse(self._payload)


class _FakeGeminiClient:
    def __init__(self, api_key: str = "", payload: str = ""):
        self.api_key = api_key
        self.models = _FakeGeminiModels(payload)


def _build_extractor(
    tmp_path: Path, extra_cache_urls: dict | None = None,
) -> IntlExtractor:
    """Crea un IntlExtractor apuntando a un fund_dir temporal con un
    search_cache.json sintético basado en URLs reales amiralgestion.

    `extra_cache_urls`: dict {url: {title, snippet}} para extender el cache
    sintético (usado por tests de BUG-C que necesitan URLs aggregator).
    """
    fund_dir = tmp_path / "data" / "funds" / FIXTURE_ISIN
    fund_dir.mkdir(parents=True)

    urls = {
        "https://www.amiralgestion.com/en/sextant-quality-focus":
            {"title": "Sextant Quality Focus", "snippet": "fund page"},
        "https://www.amiralgestion.com/en/publications-adminmenu/sextant-quality-focus-a-annuel":
            {"title": "SQF Annual", "snippet": ""},
        "https://www.amiralgestion.com/en/notre-vision":
            {"title": "Notre vision", "snippet": "corporate"},
        "https://www.amiralgestion.com/en/informations-reglementaires":
            {"title": "Reglementaire", "snippet": "corporate"},
        "https://www.amiralgestion.com/fr/actualites/journal-de-bord-de-l-equipage-de-sextant-regatta-2031-mai-2025":
            {"title": "Journal de bord", "snippet": "news"},
    }
    if extra_cache_urls:
        urls.update(extra_cache_urls)

    synthetic_cache = {"queries": {}, "urls": urls}
    (fund_dir / "search_cache.json").write_text(
        json.dumps(synthetic_cache, ensure_ascii=False), encoding="utf-8",
    )

    # Forzar self.fund_dir a apuntar al tmp dir (sin tocar el real)
    extractor = IntlExtractor(
        FIXTURE_ISIN,
        config={"nombre": FIXTURE_FUND_NAME, "gestora": FIXTURE_GESTORA},
    )
    extractor.fund_dir = fund_dir
    return extractor


def _isolate_registry(tmp_path: Path, monkeypatch) -> Path:
    """Apunta `_registry_path` a un fichero temporal para que los tests no
    contaminen `data/gestoras_registry.json` real (BUG-D auto-learn lo escribe)."""
    fake_registry = tmp_path / "gestoras_registry.json"
    from agents import discovery_v2 as _dv2
    monkeypatch.setattr(_dv2, "_registry_path", lambda: fake_registry)
    return fake_registry


def _install_fake_gemini(monkeypatch, payload: str) -> None:
    """Stubbea `from google import genai` para devolver `payload` en
    `client.models.generate_content(...)`."""
    fake_module = types.ModuleType("google.genai")
    fake_module.Client = lambda api_key=None: _FakeGeminiClient(api_key, payload)
    google_pkg = types.ModuleType("google")
    google_pkg.genai = fake_module
    monkeypatch.setitem(sys.modules, "google", google_pkg)
    monkeypatch.setitem(sys.modules, "google.genai", fake_module)
    monkeypatch.setenv("GOOGLE_API_KEY", "dummy-key-for-test")


def test_fallback_extracts_aum_with_mocked_gemini(tmp_path, monkeypatch):
    """End-to-end del fallback con fetcher + Gemini mockeados.

    Validar: tras `_fallback_html_extract`, kpis.aum_actual_meur != None,
    al menos una posición añadida, y `_html_fallback` trace presente.
    """
    extractor = _build_extractor(tmp_path)
    _isolate_registry(tmp_path, monkeypatch)

    # Fake fetch_url devuelve texto distinto según URL
    fetched: list[str] = []

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        fetched.append(url)
        if "sextant-quality-focus" in url:
            text = (
                "Sextant Quality Focus is a global equity fund managed by Amiral Gestion. "
                "AUM: 245.6 M EUR (as of 2026-03-31). Top holdings include Microsoft, "
                "Alphabet, Visa. Fund managers: Vincent Mercadier (lead), Pierre Mouton (co)."
            )
        else:
            text = "corporate boilerplate without fund data"
        return _FakeFetchResult(url=url, text=text, ok=True)

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {
            "aum_actual_meur": 245.6,
            "fecha_aum": "2026-03-31",
            "num_participes": None,
            "ter_pct": 1.85,
            "divisa_base": "EUR",
        },
        "posiciones_actuales": [
            {"nombre": "Microsoft", "peso_pct": 5.2, "sector": "IT", "pais": "USA"},
            {"nombre": "Alphabet", "peso_pct": 4.8, "sector": "Comm", "pais": "USA"},
            {"nombre": "Visa", "peso_pct": 3.9, "sector": "Financials", "pais": "USA"},
        ],
        "gestores": [
            {"nombre": "Vincent Mercadier", "cargo": "Lead Portfolio Manager"},
            {"nombre": "Pierre Mouton", "cargo": "Co-PM"},
        ],
        "anti_invencion_note": "",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)

    ok = asyncio.run(extractor._fallback_html_extract(out))

    assert ok, "el fallback debería haber devuelto True"
    assert out["kpis"]["aum_actual_meur"] == 245.6, (
        f"AUM no se merge correctamente: {out['kpis']['aum_actual_meur']}"
    )
    assert out["kpis"]["ter_pct"] == 1.85
    assert len(out["posiciones"]["actuales"]) == 3
    assert out["posiciones"]["actuales"][0]["nombre"] == "Microsoft"
    assert len(out["cualitativo"]["gestores"]) == 2
    assert any(
        g["nombre"] == "Vincent Mercadier"
        for g in out["cualitativo"]["gestores"]
    )

    # Trace
    fb = out.get("_html_fallback") or {}
    assert fb.get("model") == "gemini-2.5-flash"
    assert "kpis.aum_actual_meur" in (fb.get("fields_filled") or [])

    # serie_aum sembrada con el año del corte
    aum_serie = out["cuantitativo"]["serie_aum"]
    assert any(e.get("periodo") == "2026" and e.get("valor_meur") == 245.6 for e in aum_serie)

    # Verificar que el filtro NO fetcheó URLs blacklist
    assert any("sextant-quality-focus" in u for u in fetched)
    assert not any("/notre-vision" in u for u in fetched)
    assert not any("/informations-reglementaires" in u for u in fetched)
    assert not any("journal-de-bord" in u for u in fetched)


def test_fallback_returns_false_when_no_urls(tmp_path, monkeypatch):
    """Sin URLs harvested, el fallback no debe llamar a Gemini ni cambiar `out`."""
    fund_dir = tmp_path / "data" / "funds" / FIXTURE_ISIN
    fund_dir.mkdir(parents=True)
    extractor = IntlExtractor(
        FIXTURE_ISIN, config={"nombre": FIXTURE_FUND_NAME, "gestora": FIXTURE_GESTORA},
    )
    extractor.fund_dir = fund_dir

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))
    assert ok is False
    assert out["kpis"]["aum_actual_meur"] is None


def test_fallback_skips_when_anti_invencion_triggers(tmp_path, monkeypatch):
    """Si Gemini devuelve todo null + anti_invencion_note, no debe escribir."""
    extractor = _build_extractor(tmp_path)
    _isolate_registry(tmp_path, monkeypatch)

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        return _FakeFetchResult(url=url, text="some text " * 200, ok=True)

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {
            "aum_actual_meur": None, "num_participes": None,
            "ter_pct": None, "divisa_base": None,
        },
        "posiciones_actuales": [],
        "gestores": [],
        "anti_invencion_note": "El texto no menciona el sub-fondo target",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))
    assert ok is False, "anti_invencion_note debería abortar el merge"
    assert out["kpis"]["aum_actual_meur"] is None
    assert "_html_fallback" not in out


# ════════════════════════════════════════════════════════════════════
# BUG-C — URL expansion (prospectus + aggregators) when candidates <3
# ════════════════════════════════════════════════════════════════════


def test_aggregator_expansion_kicks_in_when_few_candidates(tmp_path, monkeypatch):
    """Si rank_fund_page_urls devuelve <3, expand añade morningstar/quantalys.

    Construimos un search_cache donde la única URL del fondo válida vía G13 es
    una página de gestora. Añadimos varias URLs morningstar/quantalys que
    mencionan el slug del fondo. _expand_candidates_from_aggregators debe
    recuperarlas y aparecer en `pages`.
    """
    extra = {
        "https://www.morningstar.fr/fr/funds/snapshot/snapshot.aspx?id=FOFR000000000XYZ&fund=sextant-quality-focus":
            {"title": "Sextant Quality Focus | Morningstar", "snippet": "AUM 320 M EUR"},
        "https://www.quantalys.com/Fonds/Fiche/sextant-quality-focus-a":
            {"title": "Sextant Quality Focus A | Quantalys", "snippet": "AUM, perf, frais"},
        "https://citywire.fr/fund/sextant-quality-focus-a/c/12345":
            {"title": "Sextant Quality Focus A | Citywire", "snippet": "Class A fund page"},
    }
    extractor = _build_extractor(tmp_path, extra_cache_urls=extra)
    _isolate_registry(tmp_path, monkeypatch)

    fetched: list[str] = []

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        fetched.append(url)
        if "morningstar" in url:
            text = (
                "Sextant Quality Focus (FR001400CEK6). Net Assets: 320.4 M EUR "
                "as of 2026-04-30. Domicile: France. Currency: EUR. "
            ) * 5  # ~600 chars para superar el umbral de 200
        else:
            text = "fund snapshot details " * 50
        return _FakeFetchResult(url=url, text=text, ok=True)

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {
            "aum_actual_meur": 320.4,
            "aum_source_url": "https://www.morningstar.fr/fr/funds/snapshot/snapshot.aspx?id=FOFR000000000XYZ&fund=sextant-quality-focus",
            "fecha_aum": "2026-04-30",
            "num_participes": None,
            "ter_pct": None,
            "divisa_base": "EUR",
        },
        "posiciones_actuales": [],
        "gestores": [],
        "anti_invencion_note": "",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))

    assert ok, "el fallback debería tener éxito tras expand BUG-C"
    assert out["kpis"]["aum_actual_meur"] == 320.4

    fb = out["_html_fallback"]
    # Al menos una URL morningstar/quantalys/citywire pasó por el fetcher
    assert any("morningstar" in u for u in fetched), (
        f"morningstar URL nunca se fetcheó. fetched={fetched}"
    )
    # useful_domains debe contener morningstar.fr (la URL que aportó AUM via aum_source_url)
    assert any("morningstar" in d for d in fb["useful_domains"]), (
        f"useful_domains no incluye morningstar: {fb['useful_domains']}"
    )
    assert fb["urls_processed"] >= 3


def test_prospectus_url_extraction_returns_urls(tmp_path, monkeypatch):
    """Si raw/discovery/ contiene un PDF clasificado como prospectus, el
    helper _expand_candidates_from_prospectus extrae URLs http(s) del texto."""
    extractor = _build_extractor(tmp_path)
    # Crear un PDF dummy en raw/discovery/
    raw_disc = extractor.fund_dir / "raw" / "discovery"
    raw_disc.mkdir(parents=True)
    fake_pdf = raw_disc / "amiral_prospectus_2026.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\nfake content\n%%EOF")

    # Mockear extract_page_range para devolver texto con URLs útiles
    pdf_text_with_urls = (
        "Pour plus d'information visitez "
        "https://www.amiralgestion.com/fr/sextant-quality-focus "
        "ou consultez la fiche Morningstar: "
        "https://www.morningstar.fr/funds/snapshot/sextant?id=XYZ "
        "Boilerplate not useful: https://www.example.com/regulatory"
    )

    def fake_extract(pdf_path, start, end):
        return pdf_text_with_urls

    import tools.pdf_extractor as _pdfx
    monkeypatch.setattr(_pdfx, "extract_page_range", fake_extract)

    extracted = extractor._expand_candidates_from_prospectus(existing=[])
    # Al menos las dos URLs amiralgestion + morningstar
    assert any("amiralgestion" in u for u in extracted), (
        f"extracted={extracted!r}"
    )
    assert any("morningstar" in u for u in extracted), (
        f"extracted={extracted!r}"
    )
    # example.com NO debe entrar (no es gestora ni agregador)
    assert not any("example.com" in u for u in extracted)


# ════════════════════════════════════════════════════════════════════
# BUG-D — Auto-learn registry from HTML fallback success
# ════════════════════════════════════════════════════════════════════


def test_html_fallback_writes_registry_on_success(tmp_path, monkeypatch):
    """Tras éxito del HTML fallback, gestoras_registry.json contiene la entrada
    con auto_learned=true y html_fallback_useful_domains poblado."""
    extractor = _build_extractor(tmp_path)
    fake_registry = _isolate_registry(tmp_path, monkeypatch)

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        return _FakeFetchResult(
            url=url,
            text="Sextant Quality Focus AUM 250 M EUR fund details " * 30,
            ok=True,
        )

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {
            "aum_actual_meur": 250.0,
            "fecha_aum": "2026-05-01",
            "num_participes": None,
            "ter_pct": None,
            "divisa_base": "EUR",
        },
        "posiciones_actuales": [
            {"nombre": "Microsoft", "peso_pct": 5.0, "sector": "IT", "pais": "USA"},
        ],
        "gestores": [],
        "anti_invencion_note": "",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))
    assert ok

    assert fake_registry.exists(), "el registry debería haberse escrito"
    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    entry = (reg.get("gestoras") or {}).get(FIXTURE_GESTORA) or {}
    assert entry, f"gestora '{FIXTURE_GESTORA}' no encontrada en registry: {reg}"
    assert entry.get("auto_learned") is True
    assert entry.get("from_isin") == FIXTURE_ISIN
    assert FIXTURE_ISIN in (entry.get("funds") or [])
    useful = entry.get("html_fallback_useful_domains") or []
    assert useful, f"html_fallback_useful_domains vacío: {entry}"
    assert any("amiralgestion" in d for d in useful), useful


def test_html_fallback_respects_manual_registry_entry(tmp_path, monkeypatch):
    """Si la gestora tiene auto_learned=false (manual) en el registry,
    BUG-D auto-learn NO debe sobrescribirla."""
    extractor = _build_extractor(tmp_path)
    fake_registry = _isolate_registry(tmp_path, monkeypatch)

    # Sembrar registry con entry manual
    fake_registry.write_text(json.dumps({
        "_description": "test",
        "gestoras": {
            FIXTURE_GESTORA: {
                "web": "https://manual-set.com",
                "auto_learned": False,
                "notes": "manually curated",
            },
        },
    }, ensure_ascii=False), encoding="utf-8")

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        return _FakeFetchResult(
            url=url, text="Sextant Quality Focus AUM 100 M EUR details " * 30, ok=True,
        )

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {"aum_actual_meur": 100.0, "fecha_aum": "2026-05-01"},
        "posiciones_actuales": [], "gestores": [],
        "anti_invencion_note": "",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    asyncio.run(extractor._fallback_html_extract(out))

    reg = json.loads(fake_registry.read_text(encoding="utf-8"))
    entry = reg["gestoras"][FIXTURE_GESTORA]
    assert entry["auto_learned"] is False, "auto_learned manual fue sobrescrito"
    assert entry.get("web") == "https://manual-set.com"
    # NO debe haberse añadido html_fallback_useful_domains a la entry manual
    assert "html_fallback_useful_domains" not in entry, (
        f"entry manual fue modificada: {entry}"
    )


def test_html_fallback_skips_persist_when_only_gestores_filled(tmp_path, monkeypatch):
    """BUG-D: si Gemini solo aporta gestores (sin AUM ni posiciones), el auto-
    learn NO debe disparar (condición es aum OR posiciones)."""
    extractor = _build_extractor(tmp_path)
    fake_registry = _isolate_registry(tmp_path, monkeypatch)

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        return _FakeFetchResult(
            url=url, text="Equipo gestor Sextant Quality Focus info " * 30, ok=True,
        )

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    payload = json.dumps({
        "kpis": {"aum_actual_meur": None, "ter_pct": None, "num_participes": None},
        "posiciones_actuales": [],
        "gestores": [{"nombre": "Pierre Dupont", "cargo": "PM"}],
        "anti_invencion_note": "",
    })
    _install_fake_gemini(monkeypatch, payload)

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    ok = asyncio.run(extractor._fallback_html_extract(out))
    assert ok, "el merge debió aportar el gestor"
    # Registry NO debe haberse creado/escrito
    assert not fake_registry.exists() or not (
        (json.loads(fake_registry.read_text(encoding="utf-8")).get("gestoras") or {}).get(FIXTURE_GESTORA)
    ), "registry no debería escribirse si solo se rellenaron gestores"


def test_prompt_includes_anti_aggregation_directive(tmp_path, monkeypatch):
    """Smoke test BUG-C: el prompt enviado a Gemini contiene la directiva
    anti-agregación SICAV/umbrella."""
    extractor = _build_extractor(tmp_path)
    _isolate_registry(tmp_path, monkeypatch)

    async def fake_fetch_url(url, max_chars=15000, **kwargs):
        return _FakeFetchResult(url=url, text="fund content snippet " * 50, ok=True)

    monkeypatch.setattr("tools.web_fetcher.fetch_url", fake_fetch_url)

    # Capturar el prompt que recibió Gemini
    captured_prompts: list[str] = []

    class _CapturingModels:
        def generate_content(self, model, contents, **kwargs):
            captured_prompts.append(contents)
            return _FakeGeminiResponse(json.dumps({
                "kpis": {"aum_actual_meur": None},
                "posiciones_actuales": [], "gestores": [],
                "anti_invencion_note": "",
            }))

    class _CapturingClient:
        def __init__(self, api_key=None):
            self.models = _CapturingModels()

    fake_module = types.ModuleType("google.genai")
    fake_module.Client = lambda api_key=None: _CapturingClient(api_key)
    google_pkg = types.ModuleType("google")
    google_pkg.genai = fake_module
    monkeypatch.setitem(sys.modules, "google", google_pkg)
    monkeypatch.setitem(sys.modules, "google.genai", fake_module)
    monkeypatch.setenv("GOOGLE_API_KEY", "dummy")

    out = _empty_output(FIXTURE_ISIN, FIXTURE_FUND_NAME, FIXTURE_GESTORA)
    asyncio.run(extractor._fallback_html_extract(out))

    assert captured_prompts, "Gemini no fue invocado"
    prompt = captured_prompts[0]
    assert "ANTI-AGREGACIÓN" in prompt
    assert "SICAV" in prompt or "umbrella" in prompt.lower()
    assert "PROHIBIDO" in prompt


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
