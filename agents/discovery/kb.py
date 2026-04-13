"""
Knowledge base per-fund: URLs que funcionaron para este ISIN concreto.

Estructura:
  data/funds/{ISIN}/discovery_kb.json
  {
    "isin": "LU1694789451",
    "ultima_actualizacion": "2026-04-13T...",
    "known_urls": {
      "annual_report:2024": "https://...pdf",
      "semi_annual_report:2024": "https://...pdf",
      "factsheet:": "https://...pdf",
      ...
    },
    "gestora_pages_worth_crawling": [
      "https://www.dnca-investments.com/en/documents", ...
    ]
  }
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def _kb_path(fund_dir: Path) -> Path:
    return fund_dir / "discovery_kb.json"


def load_kb(fund_dir: Path, isin: str) -> dict:
    path = _kb_path(fund_dir)
    if not path.exists():
        return {
            "isin": isin,
            "ultima_actualizacion": "",
            "known_urls": {},
            "gestora_pages_worth_crawling": [],
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "isin": isin,
            "ultima_actualizacion": "",
            "known_urls": {},
            "gestora_pages_worth_crawling": [],
        }


def save_kb(fund_dir: Path, kb: dict) -> None:
    kb["ultima_actualizacion"] = datetime.now().isoformat(timespec="seconds")
    path = _kb_path(fund_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(kb, ensure_ascii=False, indent=2), encoding="utf-8")


def kb_key(doc_type: str, periodo: str) -> str:
    return f"{doc_type}:{periodo}"


def lookup(kb: dict, doc_type: str, periodo: str) -> str | None:
    return kb.get("known_urls", {}).get(kb_key(doc_type, periodo))


def remember(kb: dict, doc_type: str, periodo: str, url: str) -> None:
    kb.setdefault("known_urls", {})[kb_key(doc_type, periodo)] = url


def remember_gestora_page(kb: dict, url: str) -> None:
    pages = kb.setdefault("gestora_pages_worth_crawling", [])
    if url not in pages:
        pages.append(url)
