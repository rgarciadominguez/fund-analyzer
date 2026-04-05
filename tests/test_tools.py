"""
Tests básicos para tools/. Usan mocks para no depender de red, PDFs reales ni API.

Ejecutar: pytest tests/test_tools.py -v
"""
import asyncio
import io
import json
import os
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from xml.etree import ElementTree as ET

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# http_client
# ─────────────────────────────────────────────────────────────────────────────

class TestHttpClient:
    """Tests para tools/http_client.py"""

    @pytest.mark.asyncio
    async def test_get_returns_text(self):
        """get() devuelve el texto de la respuesta."""
        import httpx
        from tools.http_client import get

        mock_response = MagicMock()
        mock_response.text = "<html>OK</html>"
        mock_response.raise_for_status = MagicMock()
        mock_response.cookies = {}

        with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response)):
            result = await get("https://example.com")
        assert result == "<html>OK</html>"

    @pytest.mark.asyncio
    async def test_get_bytes_returns_bytes(self):
        """get_bytes() devuelve bytes (para PDFs)."""
        from tools.http_client import get_bytes

        mock_response = MagicMock()
        mock_response.content = b"%PDF-1.4"
        mock_response.raise_for_status = MagicMock()
        mock_response.cookies = {}

        with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response)):
            result = await get_bytes("https://example.com/doc.pdf")
        assert result == b"%PDF-1.4"

    @pytest.mark.asyncio
    async def test_get_with_headers_merges_headers(self):
        """get_with_headers() fusiona cabeceras extra con las por defecto."""
        import httpx
        from tools.http_client import get_with_headers, DEFAULT_HEADERS

        captured_headers = {}

        mock_response = MagicMock()
        mock_response.text = "ok"
        mock_response.raise_for_status = MagicMock()
        mock_response.cookies = {}

        async def fake_get(url, **kwargs):
            return mock_response

        with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=fake_get)):
            result = await get_with_headers(
                "https://example.com", {"X-Custom": "test-value"}
            )
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Reintenta 3 veces ante error de red y lanza la excepción final."""
        import httpx
        from tools.http_client import get

        call_count = 0

        async def failing_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            raise httpx.NetworkError("Connection refused")

        with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=failing_get)):
            with patch("asyncio.sleep", new=AsyncMock()):  # evitar esperas reales
                with pytest.raises(httpx.NetworkError):
                    await get("https://unreachable.example.com")

        assert call_count == 3  # 3 intentos = len(BACKOFF_DELAYS)

    def test_clear_cookies(self):
        """clear_cookies() no lanza excepción."""
        from tools.http_client import clear_cookies
        clear_cookies()  # debe ejecutarse sin error


# ─────────────────────────────────────────────────────────────────────────────
# xml_parser
# ─────────────────────────────────────────────────────────────────────────────

def _write_cnmv_xml(path: Path, isin: str, periodo: str, patrimonio: str,
                    participes: str, ter: str = None) -> None:
    """Helper: escribe un XML CNMV en formato real (FechaDatos en root, Clase como fondo)."""
    ter_block = f"<RatioTotalGastos>{ter}</RatioTotalGastos>" if ter else ""
    xml = textwrap.dedent(f"""<?xml version="1.0" encoding="UTF-8"?>
    <FondMens>
      <FechaDatos>{periodo}</FechaDatos>
      <Entidad>
        <Clase>
          <ISIN>{isin}</ISIN>
          <DenominacionClase>Fondo de Prueba</DenominacionClase>
          <NIF>V04869202</NIF>
          <PatrimonioDiario>{patrimonio}</PatrimonioDiario>
          <ParticipesDiario>{participes}</ParticipesDiario>
          {ter_block}
        </Clase>
        <Clase>
          <ISIN>ES9999999999</ISIN>
          <DenominacionClase>Otro Fondo</DenominacionClase>
          <PatrimonioDiario>500000</PatrimonioDiario>
          <ParticipesDiario>100</ParticipesDiario>
        </Clase>
      </Entidad>
    </FondMens>
    """)
    path.write_text(xml, encoding="utf-8")


class TestXmlParser:
    """Tests para tools/xml_parser.py"""

    def test_parse_cnmv_iic_xml_finds_isin(self, tmp_path):
        """parse_cnmv_iic_xml() extrae correctamente el fondo por ISIN."""
        from tools.xml_parser import parse_cnmv_iic_xml

        xml_file = tmp_path / "mensual_202401.xml"
        _write_cnmv_xml(xml_file, "ES0112231008", "202401", "123456000", "2500")

        result = parse_cnmv_iic_xml(str(xml_file), "ES0112231008")

        assert result["isin"] == "ES0112231008"
        assert result["num_participes"] == 2500
        assert result["periodo"] == "202401"

    def test_parse_cnmv_iic_xml_patrimonio_conversion(self, tmp_path):
        """El patrimonio se convierte de EUR a millones correctamente."""
        from tools.xml_parser import parse_cnmv_iic_xml

        xml_file = tmp_path / "mensual_202401.xml"
        _write_cnmv_xml(xml_file, "ES0112231008", "202401", "123456000", "2500")

        result = parse_cnmv_iic_xml(str(xml_file), "ES0112231008")

        assert "patrimonio_meur" in result
        assert abs(result["patrimonio_meur"] - 123.456) < 0.001

    def test_parse_cnmv_iic_xml_isin_not_found(self, tmp_path):
        """Devuelve {} si el ISIN no está en el XML."""
        from tools.xml_parser import parse_cnmv_iic_xml

        xml_file = tmp_path / "mensual_202401.xml"
        _write_cnmv_xml(xml_file, "ES0112231008", "202401", "123456000", "2500")

        result = parse_cnmv_iic_xml(str(xml_file), "ES9999000000")
        assert result == {}

    def test_parse_cnmv_iic_xml_filters_other_funds(self, tmp_path):
        """No devuelve datos de otro fondo que esté en el mismo XML."""
        from tools.xml_parser import parse_cnmv_iic_xml

        xml_file = tmp_path / "mensual_202401.xml"
        _write_cnmv_xml(xml_file, "ES0112231008", "202401", "123456000", "2500")

        result = parse_cnmv_iic_xml(str(xml_file), "ES9999999999")
        assert result.get("num_participes") == 100  # el otro fondo tiene 100

    def test_build_historical_series_aggregates_xmls(self, tmp_path):
        """build_historical_series() agrega datos de múltiples XMLs."""
        from tools.xml_parser import build_historical_series

        xml_dir = tmp_path / "xml"
        xml_dir.mkdir()

        isin = "ES0112231008"
        _write_cnmv_xml(xml_dir / "FONDMENS_202401.xml", isin, "202401", "100000000", "2000")
        _write_cnmv_xml(xml_dir / "FONDMENS_202402.xml", isin, "202402", "105000000", "2100")
        _write_cnmv_xml(xml_dir / "FONDMENS_202403.xml", isin, "202403", "110000000", "2200")

        result = build_historical_series(str(xml_dir), isin)

        assert "serie_aum" in result
        assert len(result["serie_aum"]) == 3
        assert len(result["serie_participes"]) == 3

        # Verificar que están ordenados por periodo
        periodos = [r["periodo"] for r in result["serie_aum"]]
        assert periodos == sorted(periodos)

    def test_build_historical_series_empty_dir(self, tmp_path):
        """Devuelve {} si no hay XMLs en el directorio."""
        from tools.xml_parser import build_historical_series

        empty_dir = tmp_path / "xml_vacio"
        empty_dir.mkdir()
        result = build_historical_series(str(empty_dir), "ES0112231008")
        assert result == {}

    def test_parse_cnmv_iic_xml_with_ter(self, tmp_path):
        """Extrae el TER cuando está presente en el XML."""
        from tools.xml_parser import parse_cnmv_iic_xml

        xml_file = tmp_path / "trimestral_2024Q1.xml"
        _write_cnmv_xml(xml_file, "ES0112231008", "2024Q1", "123456", "2500", ter="1.25")

        result = parse_cnmv_iic_xml(str(xml_file), "ES0112231008")
        assert result.get("ter_pct") == 1.25


# ─────────────────────────────────────────────────────────────────────────────
# pdf_extractor
# ─────────────────────────────────────────────────────────────────────────────

def _make_mock_page(text: str) -> MagicMock:
    """Crea un mock de página pdfplumber con extract_text()."""
    page = MagicMock()
    page.extract_text.return_value = text
    return page


def _make_mock_pdf(pages_text: list[str]) -> MagicMock:
    """Crea un mock de pdfplumber.PDF con las páginas dadas."""
    mock_pdf = MagicMock()
    mock_pdf.pages = [_make_mock_page(t) for t in pages_text]
    mock_pdf.metadata = {"Title": "Test Report", "Author": "Test"}
    mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
    mock_pdf.__exit__ = MagicMock(return_value=False)
    return mock_pdf


class TestPdfExtractor:
    """Tests para tools/pdf_extractor.py"""

    def test_parse_toc_extracts_fund_names(self):
        """parse_toc() extrae nombres de fondos y páginas del TOC."""
        from tools.pdf_extractor import parse_toc

        toc_page_text = textwrap.dedent("""
        TABLE OF CONTENTS

        ALPHA BONDS  134
        FLEX INFLATION  142
        EURO EQUITY  198
        """)

        pages = ["Cover"] + [toc_page_text] + [""] * 6

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = parse_toc("dummy.pdf")

        assert "ALPHA BONDS" in result
        assert result["ALPHA BONDS"]["doc_page"] == 134
        assert "FLEX INFLATION" in result
        assert result["FLEX INFLATION"]["doc_page"] == 142

    def test_parse_toc_empty_when_no_pattern(self):
        """parse_toc() devuelve {} si no hay líneas con patrón nombre + número."""
        from tools.pdf_extractor import parse_toc

        pages = ["Normal text without TOC pattern"] * 8

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = parse_toc("dummy.pdf")

        assert result == {}

    def test_find_fund_in_toc_exact_match(self):
        """find_fund_in_toc() encuentra por coincidencia exacta."""
        from tools.pdf_extractor import find_fund_in_toc

        toc = {
            "ALPHA BONDS": {"doc_page": 134, "sections": {}},
            "FLEX INFLATION": {"doc_page": 142, "sections": {}},
        }
        result = find_fund_in_toc(toc, fund_name="ALPHA BONDS")
        assert result["doc_page"] == 134

    def test_find_fund_in_toc_partial_match(self):
        """find_fund_in_toc() encuentra por coincidencia parcial."""
        from tools.pdf_extractor import find_fund_in_toc

        toc = {
            "DNCA INVEST - ALPHA BONDS": {"doc_page": 134, "sections": {}},
        }
        result = find_fund_in_toc(toc, fund_name="ALPHA BONDS")
        assert result["doc_page"] == 134

    def test_find_fund_in_toc_not_found(self):
        """find_fund_in_toc() devuelve {} si el fondo no está en el TOC."""
        from tools.pdf_extractor import find_fund_in_toc

        toc = {"ALPHA BONDS": {"doc_page": 134, "sections": {}}}
        result = find_fund_in_toc(toc, fund_name="UNKNOWN FUND XYZ 123")
        assert result == {}

    def test_extract_page_range(self):
        """extract_page_range() extrae texto de las páginas indicadas (end_page exclusivo)."""
        from tools.pdf_extractor import extract_page_range

        # Contenido único por página para verificar sin ambigüedad
        pages = ["alfa", "beta", "gamma", "delta", "epsilon"]

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = extract_page_range("dummy.pdf", start_page=1, end_page=3)

        assert "beta" in result    # página índice 1 → incluida
        assert "gamma" in result   # página índice 2 → incluida
        assert "alfa" not in result    # índice 0 → excluida
        assert "delta" not in result   # índice 3 → excluida (end exclusivo)

    def test_extract_pages_by_keyword(self):
        """extract_pages_by_keyword() encuentra páginas con el keyword dado."""
        from tools.pdf_extractor import extract_pages_by_keyword

        pages = [
            "General introduction",
            "ALPHA BONDS performance overview",
            "Continued analysis of portfolio",
            "FLEX INFLATION section starts here",
            "Conclusion",
        ]

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = extract_pages_by_keyword(
                "dummy.pdf", keywords=["ALPHA BONDS"], context_pages=0
            )

        assert "ALPHA BONDS" in result
        assert "FLEX INFLATION" not in result

    def test_extract_pages_by_keyword_with_context(self):
        """context_pages=1 incluye páginas adyacentes."""
        from tools.pdf_extractor import extract_pages_by_keyword

        pages = [
            "Página antes",
            "ALPHA BONDS aquí",
            "Página después",
            "Página lejana",
        ]

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = extract_pages_by_keyword(
                "dummy.pdf", keywords=["ALPHA BONDS"], context_pages=1
            )

        assert "Página antes" in result
        assert "ALPHA BONDS" in result
        assert "Página después" in result
        assert "Página lejana" not in result

    def test_extract_text_section(self):
        """extract_text_section() extrae texto entre dos keywords."""
        from tools.pdf_extractor import extract_text_section

        pages = [
            "Intro text",
            "BEGIN SECTION\nContent line 1\nContent line 2\nEND SECTION\nMore text",
        ]

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            result = extract_text_section("dummy.pdf", "BEGIN SECTION", "END SECTION")

        assert "Content line 1" in result
        assert "Content line 2" in result
        assert "More text" not in result

    def test_get_pdf_metadata(self, tmp_path):
        """get_pdf_metadata() devuelve num_pages y file_size_mb."""
        from tools.pdf_extractor import get_pdf_metadata

        dummy_pdf = tmp_path / "test.pdf"
        dummy_pdf.write_bytes(b"fake pdf content" * 100)

        pages = ["page"] * 10
        mock_pdf = _make_mock_pdf(pages)
        mock_pdf.metadata = {"Title": "Annual Report 2024", "Author": "DNCA"}

        with patch("pdfplumber.open", return_value=mock_pdf):
            result = get_pdf_metadata(str(dummy_pdf))

        assert result["num_pages"] == 10
        assert result["title"] == "Annual Report 2024"
        assert "file_size_mb" in result

    def test_calculate_pdf_offset(self):
        """calculate_pdf_offset() detecta el offset entre TOC y PDF real."""
        from tools.pdf_extractor import calculate_pdf_offset

        toc = {"ALPHA BONDS": {"doc_page": 134, "sections": {}}}

        # Simular 200 páginas, el fondo está realmente en la 136 (offset +2)
        pages = [""] * 200
        pages[136] = "ALPHA BONDS\nPerformance review for the year..."

        with patch("pdfplumber.open", return_value=_make_mock_pdf(pages)):
            offset = calculate_pdf_offset("dummy.pdf", toc)

        assert offset == 2


# ─────────────────────────────────────────────────────────────────────────────
# claude_extractor
# ─────────────────────────────────────────────────────────────────────────────

def _mock_claude_response(json_content: dict | list) -> MagicMock:
    """Crea un mock de respuesta Anthropic con JSON embebido."""
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=json.dumps(json_content))]
    mock_message.usage = MagicMock(input_tokens=100, output_tokens=50)
    return mock_message


class TestClaudeExtractor:
    """Tests para tools/claude_extractor.py"""

    def setup_method(self):
        """Asegurar que ANTHROPIC_API_KEY está disponible para tests."""
        os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")

    def test_extract_structured_data_returns_dict(self):
        """extract_structured_data() devuelve el JSON parseado."""
        from tools.claude_extractor import extract_structured_data

        expected = {"estrategia": "Renta fija europea", "gestores": []}
        schema = {"estrategia": "descripción", "gestores": "lista"}

        with patch("anthropic.Anthropic.messages") as mock_messages:
            mock_messages.create = MagicMock(return_value=_mock_claude_response(expected))
            result = extract_structured_data("Texto de prueba", schema)

        assert result["estrategia"] == "Renta fija europea"

    def test_extract_structured_data_parses_markdown_json(self):
        """Maneja respuestas de Claude con bloques ```json ... ```."""
        from tools.claude_extractor import _parse_json_response

        response_with_markdown = '```json\n{"key": "value"}\n```'
        result = _parse_json_response(response_with_markdown)
        assert result == {"key": "value"}

    def test_parse_json_response_clean_json(self):
        """_parse_json_response() maneja JSON limpio sin markdown."""
        from tools.claude_extractor import _parse_json_response

        result = _parse_json_response('{"isin": "LU123", "nombre": "Test Fund"}')
        assert result["isin"] == "LU123"

    def test_parse_json_response_raises_on_invalid(self):
        """_parse_json_response() lanza ValueError si no hay JSON válido."""
        from tools.claude_extractor import _parse_json_response

        with pytest.raises(ValueError):
            _parse_json_response("Esto no es JSON en absoluto.")

    def test_extract_performance_table_returns_list(self):
        """extract_performance_table() devuelve una lista."""
        from tools.claude_extractor import extract_performance_table

        expected_response = {
            "performances": [
                {"clase": "A EUR", "rentabilidad_pct": 3.83, "benchmark_pct": 5.16, "anio": 2024}
            ]
        }

        with patch("tools.claude_extractor._get_client") as mock_client:
            mock_client.return_value.messages.create.return_value = \
                _mock_claude_response(expected_response)
            result = extract_performance_table("Tabla de rentabilidades...")

        assert isinstance(result, list)
        assert result[0]["clase"] == "A EUR"
        assert result[0]["rentabilidad_pct"] == 3.83

    def test_extract_top_holdings_returns_list(self):
        """extract_top_holdings() devuelve una lista de posiciones."""
        from tools.claude_extractor import extract_top_holdings

        expected_response = {
            "holdings": [
                {"nombre": "US Treasury 4.5% 2030", "ticker": None, "peso_pct": 5.2,
                 "sector": "Government", "pais": "USA"}
            ]
        }

        with patch("tools.claude_extractor._get_client") as mock_client:
            mock_client.return_value.messages.create.return_value = \
                _mock_claude_response(expected_response)
            result = extract_top_holdings("Top 10 Holdings...")

        assert isinstance(result, list)
        assert result[0]["peso_pct"] == 5.2

    def test_extract_portfolio_breakdown_returns_dict(self):
        """extract_portfolio_breakdown() devuelve dict con por_pais y por_sector."""
        from tools.claude_extractor import extract_portfolio_breakdown

        expected_response = {
            "por_pais": [{"pais": "USA", "pct": 28.98}],
            "por_sector": [{"sector": "Government", "pct": 92.81}],
        }

        with patch("tools.claude_extractor._get_client") as mock_client:
            mock_client.return_value.messages.create.return_value = \
                _mock_claude_response(expected_response)
            result = extract_portfolio_breakdown("Country breakdown...")

        assert "por_pais" in result
        assert "por_sector" in result
        assert result["por_pais"][0]["pct"] == 28.98

    def test_get_client_raises_without_api_key(self):
        """_get_client() lanza EnvironmentError si no hay API key."""
        from tools.claude_extractor import _get_client

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="ANTHROPIC_API_KEY"):
                _get_client()


# ─────────────────────────────────────────────────────────────────────────────
# schemas/fund_output.json
# ─────────────────────────────────────────────────────────────────────────────

class TestFundOutputSchema:
    """Verifica que el schema JSON tiene la estructura esperada."""

    @pytest.fixture
    def schema(self):
        schema_path = Path(__file__).parent.parent / "schemas" / "fund_output.json"
        return json.loads(schema_path.read_text(encoding="utf-8"))

    def test_top_level_fields(self, schema):
        required = {"isin", "nombre", "gestora", "tipo", "ultima_actualizacion",
                    "kpis", "cualitativo", "cuantitativo", "analisis_consistencia",
                    "posiciones", "fuentes"}
        assert required.issubset(set(schema.keys()))

    def test_kpis_fields(self, schema):
        kpis = schema["kpis"]
        assert "aum_actual_meur" in kpis
        assert "ter_pct" in kpis
        assert "num_participes" in kpis

    def test_cuantitativo_series(self, schema):
        cuant = schema["cuantitativo"]
        assert "serie_aum" in cuant
        assert "serie_rentabilidad" in cuant
        assert "serie_ter" in cuant
        assert "mix_activos_historico" in cuant
        assert "mix_geografico_historico" in cuant

    def test_posiciones_structure(self, schema):
        posiciones = schema["posiciones"]
        assert "actuales" in posiciones
        assert "historicas" in posiciones

    def test_fuentes_structure(self, schema):
        fuentes = schema["fuentes"]
        assert "informes_descargados" in fuentes
        assert "xmls_cnmv" in fuentes
        assert "cartas_gestores" in fuentes
