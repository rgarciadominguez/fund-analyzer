"""Detección de contaminación cross-fund (un documento que habla MÁS de otro fondo).

Lógica portada de `readings_collector` (Fix B Fase J, ya probada en producción):
anchors multi-palabra para evitar falsos positivos con vocabulario genérico del
dominio ("value", "equity", "fund"...). CONSERVADORA: solo marca contaminación
cuando otro fondo conocido DOMINA (otras menciones > este+2 y ≥3) y el ISIN del
fondo NO aparece en la URL/título (señal de que sí es de este fondo).

El auditor (2026-06-17) detectó que esta red existía SOLO en readings; las cartas
no tenían ninguna. Este módulo la comparte para aplicarla también a letters_data.json
(marca `extracted_error`, que el analyst y el bundle ya excluyen).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"


def _anchors(name: str) -> set:
    """Conjunto de anchors distintivos. UMBRELLA-AWARE: incluye el bigrama de los 2
    primeros tokens (marca/paraguas) Y el del sub-fondo tras el último ' - ' (lo
    realmente distintivo: 'DNCA INVEST - ALPHA BONDS' → {'dnca invest','alpha bonds'}).
    Sin esto, el anchor era el prefijo paraguas y no casaba cuando el doc nombra el
    sub-fondo por su nombre corto (causa de que el detector cazara 0 en paraguas)."""
    out: set[str] = set()
    segs = [s for s in re.split(r"\s+-\s+", name or "") if s.strip()]
    candidates = [name or ""]
    if len(segs) > 1:
        candidates.append(segs[-1])  # sub-fondo tras el último ' - '
    for seg in candidates:
        cleaned = re.sub(r"[,;:.()\"']", " ", seg).lower()
        toks = [w for w in cleaned.split() if len(w) > 3]
        if len(toks) >= 2:
            out.add(f"{toks[0]} {toks[1]}")
        elif toks:
            out.add(toks[0])
    return {a for a in out if a}


def load_known_fund_names(exclude_isin: str = "") -> dict:
    """{nombre_short_lower: isin} de todos los output.json (excluye exclude_isin)."""
    exclude_isin = (exclude_isin or "").upper()
    out: dict[str, str] = {}
    if not FUNDS_DIR.exists():
        return out
    for fd in FUNDS_DIR.iterdir():
        if not fd.is_dir() or fd.name.upper() == exclude_isin:
            continue
        p = fd / "output.json"
        if not p.exists():
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            nombre = (d.get("nombre") or "").strip()
            if nombre and len(nombre) > 5:
                out[nombre.split(",")[0].strip().lower()] = fd.name
        except Exception:
            continue
    return out


def count_mentions(text: str, this_name: str, known_funds: dict, this_isin: str = "") -> tuple:
    """(this_count, other_max, other_name): menciones del fondo vs el otro que más sale.
    Usa el MÁXIMO sobre los anchors de cada fondo; para los otros excluye los anchors
    que comparten con este fondo (el prefijo paraguas), para no contar el paraguas común."""
    this_anchors = _anchors(this_name)
    if not this_anchors:
        return (0, 0, "")
    tl = (text or "").lower()
    this_count = max(tl.count(a) for a in this_anchors)
    other_max, other_name = 0, ""
    this_isin = (this_isin or "").upper()
    for other_short, other_isin in known_funds.items():
        if (other_isin or "").upper() == this_isin:
            continue
        oa = _anchors(other_short) - this_anchors  # excluye el paraguas compartido
        if not oa:
            continue
        c = max(tl.count(a) for a in oa)
        if c > other_max:
            other_max, other_name = c, other_short
    return (this_count, other_max, other_name)


def classify(text: str, this_name: str, this_isin: str, known_funds: dict,
             extra_signal: str = "") -> tuple:
    """Nivel de contaminación cross-fund. Devuelve (level, info):
      'hard' → casi seguro de otro fondo (este fondo NO se menciona y otro domina ≥4):
               seguro de excluir (extracted_error).
      'soft' → otro fondo domina (>este+2 y ≥3) pero este sí aparece: marcar para
               revisar (no destruir contenido).
      None   → ok.
    `extra_signal` = URL/título: si el ISIN aparece ahí, nunca se marca."""
    this_count, other_count, other_name = count_mentions(text, this_name, known_funds, this_isin)
    info = {"this": this_count, "other": other_count, "other_fund": other_name}
    if bool(this_isin) and this_isin.lower() in (extra_signal or "").lower():
        return None, info
    if this_count == 0 and other_count >= 4:
        return "hard", info
    if other_count > this_count + 2 and other_count >= 3:
        return "soft", info
    return None, info


def _carta_text(c: dict) -> str:
    return " ".join(str(c.get(k) or "") for k in (
        "texto_completo", "tesis_gestora", "decisiones_tomadas", "contexto_mercado",
        "perspectivas", "titulo", "resumen"))


def flag_contaminated_letters(isin: str, log=None) -> dict:
    """Marca `extracted_error` las cartas de letters_data.json que hablan más de OTRO
    fondo conocido. Conservador (no borra; el analyst/bundle ya excluyen extracted_error).
    Devuelve {flagged, items}."""
    isin = (isin or "").upper().strip()
    p = FUNDS_DIR / isin / "letters_data.json"
    op = FUNDS_DIR / isin / "output.json"
    if not p.exists() or not op.exists():
        return {"flagged": 0, "items": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        out = json.loads(op.read_text(encoding="utf-8"))
    except Exception:
        return {"flagged": 0, "items": []}
    this_name = (out.get("nombre") or "").split(",")[0].strip()
    if not this_name or len(this_name) < 5:
        return {"flagged": 0, "items": []}
    known = load_known_fund_names(exclude_isin=isin)
    if not known:
        return {"flagged": 0, "items": []}
    cartas = data.get("cartas") or data.get("analisis") or []
    hard, soft = [], []
    for c in cartas if isinstance(cartas, list) else []:
        if not isinstance(c, dict) or c.get("extracted_error"):
            continue
        signal = f"{c.get('url') or ''} {c.get('titulo') or ''}"
        level, info = classify(_carta_text(c), this_name, isin, known, signal)
        if level == "hard":
            c["extracted_error"] = True          # excluido del analyst/bundle
            c["_contamination"] = info
            hard.append({"periodo": c.get("periodo") or c.get("fecha"), **info})
        elif level == "soft":
            c["_contamination_suspect"] = info    # marcado, NO destruido (revisar)
            soft.append({"periodo": c.get("periodo") or c.get("fecha"), **info})
    if hard or soft:
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)
        if log:
            if hard:
                log("CONTAM_GUARD", "WARN",
                    f"{len(hard)} carta(s) EXCLUIDAS (otro fondo, este no se menciona): "
                    f"{[h.get('other_fund') for h in hard]}")
            if soft:
                log("CONTAM_GUARD", "INFO",
                    f"{len(soft)} carta(s) sospechosas (revisar): "
                    f"{[s.get('other_fund') for s in soft]}")
    return {"hard": len(hard), "soft": len(soft), "items_hard": hard, "items_soft": soft}


def main():
    import sys
    if len(sys.argv) < 2:
        print("uso: python -m tools.cross_fund <ISIN>")
        return
    print(json.dumps(flag_contaminated_letters(sys.argv[1].upper(),
          log=lambda a, l, m: print(f"[{l}] {m}")), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
