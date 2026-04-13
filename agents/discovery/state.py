"""
SharedState del discovery — todos los tracks lo comparten para no duplicar.

REGLA DE ORO: antes de CUALQUIER acción (HTTP, Google, descarga, LLM) se
consulta el estado. Si ya está hecho, se reusa.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# Budget compartido por todos los tracks de UN fondo
@dataclass
class Budget:
    http_remaining: int = 60
    google_remaining: int = 10
    llm_remaining: int = 4

    def try_http(self) -> bool:
        if self.http_remaining <= 0:
            return False
        self.http_remaining -= 1
        return True

    def try_google(self) -> bool:
        if self.google_remaining <= 0:
            return False
        self.google_remaining -= 1
        return True

    def try_llm(self) -> bool:
        if self.llm_remaining <= 0:
            return False
        self.llm_remaining -= 1
        return True


@dataclass
class DiscoveredDoc:
    """Un documento descubierto y descargado por el discovery."""
    doc_type: str                          # annual_report | semi_annual_report | …
    periodo: str                           # "2024", "2024-12-31", "2024-H1" …
    url: str
    local_path: str                        # absoluta
    source: str                            # gestora_web | google | knowledge_base
    source_detail: str = ""                # "natixis-cdn", "dnca-investments.com/documents", …
    content_type: str = "pdf"              # pdf | html | xml
    size_bytes: int = 0
    fecha_publicacion: str = ""            # ISO si la conocemos
    validated: bool = False                # contiene ISIN y es parseable
    contains: set[str] = field(default_factory=set)  # otros doc_types detectados dentro
    lang: str = ""                         # en | es | fr | de …


@dataclass
class SharedState:
    """Estado compartido entre tracks + locking + budget."""
    isin: str
    identity: dict
    gap: dict
    fund_dir: Path
    budget: Budget = field(default_factory=Budget)

    # Lo que ya hicimos — MIRAR ANTES DE ACTUAR
    fetched_urls: set[str] = field(default_factory=set)
    gestora_pages_cache: dict[str, str] = field(default_factory=dict)   # url → html
    google_queries_done: set[str] = field(default_factory=set)
    attempted_downloads: set[str] = field(default_factory=set)          # urls de descarga intentadas
    # Índice de lo encontrado — ambos tracks escriben, ambos leen
    downloaded_docs: list[DiscoveredDoc] = field(default_factory=list)
    # Knowledge base per-fund (se carga al inicio, se guarda al final)
    kb: dict = field(default_factory=dict)
    # Locking para mutaciones concurrentes
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # ── Consultas (sin lock, lectura) ─────────────────────────────────────

    def already_fetched(self, url: str) -> bool:
        return url in self.fetched_urls

    def page_cached(self, url: str) -> str | None:
        return self.gestora_pages_cache.get(url)

    def google_done(self, query: str) -> bool:
        return query in self.google_queries_done

    def already_downloaded(self, url: str) -> DiscoveredDoc | None:
        for d in self.downloaded_docs:
            if d.url == url:
                return d
        return None

    def coverage(self, doc_type: str, periodo: str) -> DiscoveredDoc | None:
        """¿Algún doc descargado cubre este doc_type+periodo (directo o por 'contains')?"""
        for d in self.downloaded_docs:
            # Match directo
            if d.doc_type == doc_type and (not periodo or periodo in d.periodo or d.periodo in periodo):
                if d.validated:
                    return d
            # Match por contenido indexado (ej. AR que incluye carta anual)
            if doc_type in d.contains and (not periodo or periodo in d.periodo or d.periodo in periodo):
                if d.validated:
                    return d
        return None

    def missing_doc_targets(self) -> list[tuple[str, str]]:
        """Lista de (doc_type, periodo) que aún faltan por cubrir."""
        out = []
        gap = self.gap
        for year in gap.get("missing_annual_reports", []):
            out.append(("annual_report", str(year)))
        for year in gap.get("missing_semi_annual_reports", []):
            out.append(("semi_annual_report", str(year)))
        # Extras: latest-only
        if not self.coverage("factsheet", ""):
            out.append(("factsheet", ""))
        if not self.coverage("prospectus", ""):
            out.append(("prospectus", ""))
        if not self.coverage("kid", ""):
            out.append(("kid", ""))
        if not self.coverage("manager_presentation", ""):
            out.append(("manager_presentation", ""))
        # Cartas trimestrales: al menos una por año desde inception, capado a
        # los últimos 5 años (más allá rara vez está disponible online).
        inception = (self.identity.get("fecha_autorizacion") or "")[:4]
        if inception.isdigit():
            from datetime import datetime
            now = datetime.now().year
            start = max(int(inception), now - 5)
            for y in range(start, now + 1):
                if not self.coverage("quarterly_letter", str(y)):
                    out.append(("quarterly_letter", str(y)))
        return out

    def is_fully_covered(self) -> bool:
        return len(self.missing_doc_targets()) == 0

    # ── Mutaciones (con lock) ─────────────────────────────────────────────

    async def mark_fetched(self, url: str) -> None:
        async with self._lock:
            self.fetched_urls.add(url)

    async def cache_page(self, url: str, html: str) -> None:
        async with self._lock:
            self.gestora_pages_cache[url] = html
            self.fetched_urls.add(url)

    async def mark_google_done(self, query: str) -> None:
        async with self._lock:
            self.google_queries_done.add(query)

    async def mark_download_attempted(self, url: str) -> None:
        async with self._lock:
            self.attempted_downloads.add(url)

    async def add_doc(self, doc: DiscoveredDoc) -> None:
        async with self._lock:
            # Dedup por URL
            existing = next((d for d in self.downloaded_docs if d.url == doc.url), None)
            if existing:
                return
            # Regla de desempate: si ya existe mismo (doc_type, periodo), gana el de fecha más reciente
            for i, d in enumerate(self.downloaded_docs):
                if d.doc_type == doc.doc_type and d.periodo == doc.periodo and doc.validated and d.validated:
                    if (doc.fecha_publicacion or "") > (d.fecha_publicacion or ""):
                        self.downloaded_docs[i] = doc
                    return
            self.downloaded_docs.append(doc)
