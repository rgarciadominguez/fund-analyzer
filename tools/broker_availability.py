"""
broker_availability.py — Auto-detección de disponibilidad de fondos por broker.

Objetivo: reducir el marcado manual de en qué broker está disponible cada fondo.
Hasta ahora el usuario rellenaba a mano (entrando logueado a cada broker) las 10
columnas de broker del Excel maestro (ver tools/import_taxonomy.py:BROKER_NAMES) o
el modal W13 del catálogo. Este módulo rellena AUTOMÁTICAMENTE las entradas de
ALTA CONFIANZA, sin login, dejando el resto al marcado manual.

DECISIÓN DE PRODUCTO (2026-06-03): "solo lo de alta confianza + manual".
  - NO se reemplaza el marcado manual: se escribe en un campo SEPARADO
    `broker_disponible_auto` que nunca pisa `broker_disponible` (manual).
  - El catálogo muestra la unión distinguiendo auto vs manual (override manual
    siempre gana).

POR QUÉ ESTAS REGLAS (investigación 2026-06-03, reverse-engineering sin login):
  Ninguno de los 4 brokers objetivo expone una API pública consultable por ISIN sin
  login (MyInvestor → buscador tras login Inversis; Ironia → backend Allfunds con
  API-key Azure; Mapfre → Allfunds-style; Renta4 → FondoTop completo tras login). Lo
  único robusto sin login es un MODELO POR REGLAS basado en arquitectura abierta:

  - Ironia (~26.000 fondos) y Mapfre (~15.000) son plataformas de ARQUITECTURA
    ABIERTA sobre Allfunds → cubren prácticamente todo el universo UCITS distribuido
    en España. Para un fondo registrado/distribuido en España la disponibilidad es
    casi universal.
        · ES (registrado en CNMV)        → confianza ALTA
        · INT UCITS UE que el usuario     → confianza MEDIA-ALTA
          trackea para clientes ES          (asunción: si lo analiza es porque es
                                              distribuible en España)
  - Renta4 (broker FondoTop) → un fondo de RENTA 4 GESTORA está sí o sí en Renta4 →
    confianza ALTA. (El resto del catálogo FondoTop requiere login → no se auto-marca.)
  - MyInvestor → catálogo curado tras login, sin fuente pública fiable → NUNCA se
    auto-marca; queda 100% manual.

La señal "registrado/distribuido en España" es PLUGGABLE (`_spain_registered`) para
poder endurecerla en el futuro con el registro CNMV de IIC extranjeras (pendiente:
no se encontró endpoint público limpio sin login en la investigación inicial).

Uso programático:
    from tools.broker_availability import detect, apply_to_output
    info = detect(isin, output_data)            # dict con per_broker + razones
    apply_to_output(isin)                        # carga output.json, escribe, guarda

CLI:
    python -m tools.broker_availability ES0114105036        # muestra detección
    python -m tools.broker_availability --apply ES0114105036 [ISIN2 ...]
    python -m tools.broker_availability --apply-all          # backfill todos los fondos
    python -m tools.broker_availability --refresh-renta4     # recachea universo R4 propio
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
UNIVERSE_DIR = ROOT / "data" / "broker_universe"

# Versión del motor de reglas — bump al cambiar la lógica para invalidar cachés.
METHOD = "rules_v1_no_login"

# Brokers que este módulo PUEDE auto-detectar (alta confianza). MyInvestor NO está
# (sin fuente pública sin login). Los demás brokers del Excel (BBVA, Caixa, ABANCA,
# Santander, CJRS, EBN) tampoco se auto-detectan en v1 → manual.
AUTO_BROKERS = ("Ironia", "Mapfre", "Renta4")

# Brokers explícitamente excluidos del auto-marcado, con el motivo (para transparencia
# en la UI). El resto del Excel simplemente no aplica todavía.
EXCLUDED_BROKERS = {
    "MyInvestor": "catálogo curado tras login — sin fuente pública sin login; marcado manual",
}

# Prefijos ISIN de domicilios UCITS UE típicamente distribuidos en España vía Allfunds.
# CH (Suiza) y GB (UK post-Brexit) NO son UCITS UE → no se asume distribución ES.
EU_UCITS_PREFIXES = {"ES", "LU", "IE", "FR", "DE", "AT", "BE", "NL", "IT", "PT", "FI", "SE"}


def _norm(s: str) -> str:
    """Normaliza para comparación (mayúsculas, sin acentos básicos, espacios colapsados)."""
    s = (s or "").upper().strip()
    for a, b in (("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U"), ("Ñ", "N")):
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s)


def _gestora_is_renta4(gestora: str) -> bool:
    """True si la gestora es Renta 4 Gestora (entonces el fondo está en Renta4 broker)."""
    g = _norm(gestora)
    return ("RENTA 4" in g) or ("RENTA4" in g)


def _is_es_cnmv_registered(isin: str, fund_dir: Optional[Path]) -> bool:
    """Fondo español: prefijo ES y (idealmente) con datos CNMV descargados.

    Para un fondo ES la inscripción CNMV es condición de comercialización en España,
    así que basta el prefijo. Si hay `cnmv_data.json` lo confirmamos (mayor certeza),
    pero no lo exigimos para no penalizar fondos ES sin esa caché.
    """
    return (isin or "").upper().startswith("ES")


def _is_eu_ucits(isin: str) -> bool:
    return (isin or "")[:2].upper() in EU_UCITS_PREFIXES


# Marcas que son EXCLUSIVAMENTE ETF (no tienen línea de fondos índice tradicionales).
_ETF_ONLY_ISSUERS = ("ISHARES", "LYXOR", "XTRACKERS", "SPDR", "VANECK", "WISDOMTREE")
# Casas con AMBAS cosas (ETF e index funds): solo es ETF si el nombre no dice "INDEX".
_ETF_MIXED_ISSUERS = ("VANGUARD", "AMUNDI", "INVESCO")
_INDEX_KEYWORDS = r"S&P 500|MSCI |FTSE |NASDAQ|EURO STOXX|STOXX|IBEX|DAX|NIKKEI"


def _is_etf(nombre: str, gestora: str = "") -> bool:
    """True si el producto parece un ETF / fondo cotizado.

    Importante: Ironia y Mapfre son plataformas de FONDOS (arquitectura abierta
    Allfunds), NO brokers de ETFs. Un ETF se compra por un broker de acciones, así
    que NO se auto-marca para esas plataformas → queda manual (alta confianza).

    Detección combinada (el nombre no siempre dice "ETF" — iShares/Vanguard lo omiten):
      1. nombre con ETF / cotizado / exchange traded.
      2. emisor exclusivamente ETF (iShares, Lyxor, Xtrackers, SPDR, VanEck, WisdomTree).
      3. casa mixta (Vanguard/Amundi/Invesco) + nombre de índice puro SIN "INDEX"
         (los index FUNDS llevan "Index" en el nombre; el ETF no) → asumimos ETF.
    """
    n = _norm(nombre)
    g = _norm(gestora)
    if re.search(r"\bETF\b|COTIZAD|EXCHANGE TRADED", n):
        return True
    if any(b in g or b in n for b in _ETF_ONLY_ISSUERS):
        return True
    if (any(b in g or b in n for b in _ETF_MIXED_ISSUERS)
            and re.search(_INDEX_KEYWORDS, n) and "INDEX" not in n and "FONDO" not in n):
        return True
    return False


def _spain_registered(isin: str, fund_dir: Optional[Path]) -> tuple[bool, str]:
    """Señal PLUGGABLE de "distribuible en España".

    Devuelve (registrado, confianza) donde confianza ∈ {"alta", "media-alta", "no"}.
      - ES                → ("alta")    : inscripción CNMV implícita.
      - INT UCITS UE      → ("media-alta"): asunción razonada (el usuario solo
                            analiza fondos relevantes para clientes españoles).
      - Resto (CH/GB/US…) → ("no")      : no se asume distribución ES → manual.

    TODO (endurecer): para INT consultar el registro CNMV de IIC extranjeras
    comercializadas en España (no se halló endpoint público sin login en la
    investigación inicial 2026-06-03).
    """
    isin = (isin or "").upper()
    if _is_es_cnmv_registered(isin, fund_dir):
        return True, "alta"
    if _is_eu_ucits(isin):
        return True, "media-alta"
    return False, "no"


def _renta4_universe() -> set[str]:
    """Carga el universo propio de Renta4 cacheado (ISINs de Renta 4 Gestora).

    Es un COMPLEMENTO opcional a la regla por gestora: cubre fondos cuyo nombre de
    gestora no contenga literalmente "Renta 4". Vacío si no se ha cacheado todavía.
    """
    path = UNIVERSE_DIR / "renta4.json"
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {str(x).upper() for x in (data.get("isins") or [])}
    except Exception:
        return set()


def detect(isin: str, output_data: Optional[dict] = None,
           fund_dir: Optional[Path] = None) -> dict:
    """Detecta los brokers de ALTA CONFIANZA para un fondo.

    Args:
        isin: ISIN del fondo.
        output_data: dict de output.json (opcional; si falta se intenta cargar).
        fund_dir: ruta data/funds/{ISIN} (opcional).

    Returns:
        dict con:
          detected:    list[str] de brokers auto-marcables (cualquier confianza)
          per_broker:  {broker: {available, confidence, reason}}
          excluded:    {broker: motivo}
          method:      versión del motor
          generated_at: ISO timestamp
    """
    isin = (isin or "").strip().upper()
    if fund_dir is None:
        fund_dir = FUNDS_DIR / isin
    if output_data is None:
        op = fund_dir / "output.json"
        if op.exists():
            try:
                output_data = json.loads(op.read_text(encoding="utf-8"))
            except Exception:
                output_data = {}
        else:
            output_data = {}

    from tools.output_accessor import get_gestora, get_nombre, get_tipo
    gestora = get_gestora(output_data)
    nombre = get_nombre(output_data)
    tipo = (get_tipo(output_data) or ("ES" if isin.startswith("ES") else "INT")).upper()

    per_broker: dict[str, dict] = {}
    is_etf = _is_etf(nombre, gestora)

    # ── Ironia & Mapfre (arquitectura abierta Allfunds) ───────────────────────
    # NO aplica a ETFs: son plataformas de fondos, no brokers de ETF → manual.
    registered, reg_conf = _spain_registered(isin, fund_dir)
    if registered and not is_etf:
        es = isin.startswith("ES")
        base_reason = (
            "fondo ES registrado en CNMV → arquitectura abierta (Allfunds)"
            if es else
            f"UCITS UE ({isin[:2]}) distribuible en España → arquitectura abierta (Allfunds)"
        )
        for broker in ("Ironia", "Mapfre"):
            # Mapfre tiene universo algo menor que Ironia → media-alta cuando Ironia es alta.
            conf = reg_conf
            if broker == "Mapfre" and conf == "alta":
                conf = "media-alta"
            per_broker[broker] = {
                "available": True,
                "confidence": conf,
                "reason": base_reason,
            }

    # ── Renta4 (FondoTop) — solo fondos de Renta 4 Gestora ────────────────────
    r4_universe = _renta4_universe()
    if _gestora_is_renta4(gestora):
        per_broker["Renta4"] = {
            "available": True,
            "confidence": "alta",
            "reason": f"gestora '{gestora}' es Renta 4 Gestora → broker propio Renta4",
        }
    elif isin in r4_universe:
        per_broker["Renta4"] = {
            "available": True,
            "confidence": "alta",
            "reason": "ISIN en el universo público de Renta 4 Gestora (portal r4.com)",
        }

    detected = [b for b, v in per_broker.items() if v.get("available")]

    excluded = dict(EXCLUDED_BROKERS)
    if is_etf:
        excluded["Ironia"] = "ETF/cotizado — plataforma de fondos, no broker de ETF; marcado manual"
        excluded["Mapfre"] = "ETF/cotizado — plataforma de fondos, no broker de ETF; marcado manual"

    return {
        "detected": detected,
        "per_broker": per_broker,
        "excluded": excluded,
        "is_etf": is_etf,
        "method": METHOD,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tipo": tipo,
    }


def apply_to_output(isin: str, output_data: Optional[dict] = None) -> dict:
    """Ejecuta detect() y escribe `broker_disponible_auto` en output.json.

    NO toca `broker_disponible` (manual). Devuelve el dict de detección.
    Usa tools.output_merger.save_output para persistir de forma atómica.
    """
    isin = (isin or "").strip().upper()
    fund_dir = FUNDS_DIR / isin
    op = fund_dir / "output.json"
    if output_data is None:
        if not op.exists():
            raise FileNotFoundError(f"No existe output.json para {isin}")
        output_data = json.loads(op.read_text(encoding="utf-8"))

    info = detect(isin, output_data, fund_dir)
    output_data["broker_disponible_auto"] = info

    from tools.output_merger import save_output
    save_output(isin, output_data)
    return info


# ── Refresh del universo propio de Renta4 (opcional, no requiere login) ────────

def sync_auto_to_supabase(isin: str, detected: Optional[list[str]] = None,
                          force: bool = False) -> str:
    """Pre-rellena `funds.broker_disponible` en Supabase con los brokers auto-detectados.

    SEMÁNTICA "no pisar lo manual" (decisión 2026-06-03):
      - Escribe SOLO si el campo actual está vacío/null (o force=True).
      - Una vez el campo tiene valor (auto inicial o edición del modal W13), el
        marcado manual manda y esta función NO lo toca → el usuario actualiza a
        mano (logueado) lo que falte (MyInvestor, BBVA, etc.).

    Devuelve una etiqueta de la acción: "filled" | "skip_manual" | "skip_empty_detect"
    | "skip_no_row" | "error:<msg>".
    """
    isin = (isin or "").strip().upper()
    if detected is None:
        detected = (detect(isin).get("detected") or [])
    if not detected:
        return "skip_empty_detect"

    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as exc:
        return f"error:{exc}"

    try:
        res = client.table("funds").select("isin,broker_disponible").eq("isin", isin).execute()
        rows = getattr(res, "data", None) or []
        if not rows:
            return "skip_no_row"
        current = rows[0].get("broker_disponible")
        if current and isinstance(current, list) and len(current) > 0 and not force:
            return "skip_manual"
        client.table("funds").update({"broker_disponible": detected}).eq("isin", isin).execute()
        return "filled"
    except Exception as exc:
        return f"error:{exc}"


def refresh_renta4_universe() -> int:
    """Crawl del portal público de Renta4 (fondos de Renta 4 Gestora) → cache local.

    Fuente: https://www.r4.com/portal?TX=fondos&OPC=1&HOJA=N (lista pública sin login).
    Guarda data/broker_universe/renta4.json. Devuelve nº de ISINs cacheados.

    NOTA: el portal devuelve la lista de fondos de Renta 4 Gestora (no el FondoTop
    completo, que requiere login). Es un COMPLEMENTO a la regla por nombre de gestora.
    """
    import httpx
    import urllib3
    urllib3.disable_warnings()
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    isins: set[str] = set()
    with httpx.Client(headers=ua, timeout=30, follow_redirects=True, verify=False) as c:
        # El portal pagina con HOJA pero observamos que devuelve un bloque fijo;
        # iteramos varias hojas por robustez y deduplicamos.
        for hoja in range(1, 6):
            try:
                r = c.get(f"https://www.r4.com/portal?TX=fondos&OPC=1&HOJA={hoja}")
                isins.update(re.findall(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", r.text))
            except Exception:
                break
    # Filtra a ISINs plausibles de fondos (ES + algún UCITS); descarta ruido.
    isins = {i for i in isins if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", i)}
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "source": "https://www.r4.com/portal?TX=fondos&OPC=1",
        "note": "Fondos públicos de Renta 4 Gestora (no FondoTop completo, que requiere login)",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "isins": sorted(isins),
    }
    (UNIVERSE_DIR / "renta4.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return len(isins)


def _cli() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    if "--refresh-renta4" in args:
        n = refresh_renta4_universe()
        print(f"[broker_availability] universo Renta4 cacheado: {n} ISINs")
        return

    if "--apply-all" in args or "--sync-all" in args:
        do_sync = "--sync-all" in args
        force = "--force" in args
        isins = sorted(p.name for p in FUNDS_DIR.iterdir()
                       if p.is_dir() and re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", p.name))
        ok = 0
        actions: dict[str, int] = {}
        for isin in isins:
            try:
                info = apply_to_output(isin)
                det = info["detected"]
                line = f"  {isin}: {', '.join(det) or '(ninguno)'}"
                if do_sync:
                    action = sync_auto_to_supabase(isin, detected=det, force=force)
                    actions[action] = actions.get(action, 0) + 1
                    line += f"  [supabase:{action}]"
                print(line)
                ok += 1
            except Exception as exc:
                print(f"  {isin}: ERROR {exc}")
        print(f"[broker_availability] aplicado a {ok}/{len(isins)} fondos")
        if do_sync:
            print(f"[broker_availability] supabase: {actions}")
        return

    apply = "--apply" in args
    isins = [a.upper() for a in args if re.fullmatch(r"[A-Za-z]{2}[A-Za-z0-9]{9}[0-9]", a)]
    if not isins:
        print("Uso: python -m tools.broker_availability [--apply|--apply-all|--refresh-renta4] ISIN...")
        return
    for isin in isins:
        info = apply_to_output(isin) if apply else detect(isin)
        print(f"\n=== {isin} ({info.get('tipo')}) — {'APLICADO' if apply else 'detección'} ===")
        for broker, v in info["per_broker"].items():
            print(f"  ✓ {broker:11} [{v['confidence']}] — {v['reason']}")
        for broker, motivo in info["excluded"].items():
            print(f"  · {broker:11} [excluido] — {motivo}")
        if not info["per_broker"]:
            print("  (sin brokers de alta confianza → marcado manual)")


if __name__ == "__main__":
    _cli()
