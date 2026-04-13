"""
Schema común para los agentes de regulador internacional.

Cada regulador (CNMV, Bundesanzeiger, AMF, CSSF, CBI, FCA) debe devolver su
output en este formato para que el agente de discovery pueda consumirlos
uniformemente y detectar qué documentos faltan.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Literal, Any


DocType = Literal[
    "annual_report",
    "semi_annual_report",
    "quarterly_report",
    "monthly_report",
    "prospectus",
    "kid",
    "kiid",
    "regulation",
    "other",
]


@dataclass
class Document:
    """Referencia a un documento del fondo (descargado o solo listado)."""
    doc_type: DocType
    periodo: str                   # "2024", "2024-12-31", "2024-H1", "2024-09", etc.
    title: str = ""
    url: str = ""                  # URL de descarga directa si existe
    downloaded_path: str = ""      # ruta local si descargado (relativa a data/funds/{ISIN}/)
    content_type: str = "pdf"      # pdf | html | xml
    size_bytes: int = 0
    source: str = "regulator"      # regulator | gestora | google | knowledge_base
    source_detail: str = ""        # ej. "Bundesanzeiger", "AMF GECO", "natixis-cdn"
    download_ok: bool = False      # ¿se descargó el binario correctamente?
    validated: bool = False        # ¿contiene el ISIN/nombre del fondo?
    notes: str = ""


@dataclass
class Identity:
    """Identity card del fondo. Lo que todo regulador debe validar/aportar."""
    isin: str
    nombre_oficial: str = ""
    sub_fondo: str = ""
    sicav_paraguas: str = ""
    gestora_oficial: str = ""
    depositario: str = ""
    fecha_autorizacion: str = ""
    pais_domicilio: str = ""
    tipo_fondo: str = ""           # "UCITS" | "AIF" | "FCP" | "SICAV" | "ICAV" | ...
    estado: str = "unknown"        # "active" | "liquidated" | "unknown"
    moneda: str = ""
    clase: str = ""
    lei: str = ""
    internal_refs: dict = field(default_factory=dict)  # ids propios del regulador


@dataclass
class RegulatorOutput:
    """Output estándar de cada agente de regulador."""
    isin: str
    regulator: str                 # "CNMV" | "BUNDESANZEIGER" | "AMF" | "CSSF" | "CBI" | "FCA"
    ultima_actualizacion: str = ""
    identity: Identity = None
    documents: list[Document] = field(default_factory=list)
    notes: str = ""
    source_urls: list[str] = field(default_factory=list)

    def __post_init__(self):
        if self.identity is None:
            self.identity = Identity(isin=self.isin)
        if not self.ultima_actualizacion:
            self.ultima_actualizacion = datetime.now().isoformat(timespec="seconds")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    # ── Helpers para el gap analysis que hará el orchestrator ────────────────

    def has_doc(self, doc_type: DocType, periodo: str) -> bool:
        """¿Ya tenemos un doc del tipo y período dados?"""
        return any(
            d.doc_type == doc_type and d.periodo == periodo and d.download_ok
            for d in self.documents
        )

    def missing_periods(self, doc_type: DocType, target_periods: list[str]) -> list[str]:
        """Períodos del objetivo que aún no tenemos descargados."""
        have = {d.periodo for d in self.documents if d.doc_type == doc_type and d.download_ok}
        return [p for p in target_periods if p not in have]
