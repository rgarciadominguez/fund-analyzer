"""
contract_sync.py — Valida y conforma el export al contrato `contrato_sync.json`.

Regla de oro (del contrato): un valor que no está en la lista de su campo NO se emite:
se pone `null` y se REPORTA para acordarlo. Nunca se inventa. Los dos lados validan
contra el mismo fichero → las dos BDD no pueden divergir.

Este módulo:
  1. Carga contrato_sync.json.
  2. Transforma (mapea el vocabulario grueso mío al del contrato):
     - region/geografia: Global, World → ACWI
     - distribucion: Reparto → Distribución
     - estilo: 'Divisa cubierta' → 'Cubre divisa'; 'Indexado' → fuera (es categoria_activo)
     - tipo_activo: compone el granular (Fondo RV / Fondo RF <plazo> / High Yield /
       Floating / Fondo Mixto / Fondo Monetario / ETF*/REITs/Materias primas) desde
       tipo_activo grueso + plazo + benchmark + caracteristicas_especiales + nombre.
     - plazo: rellena para RF desde benchmark / categoria Morningstar.
  3. Valida cada campo `enum` contra su lista. Fuera de lista → null + report.
  4. Devuelve (activos_conformes, report).

El export queda como VISTA conforme al contrato; Supabase sigue con su vocabulario grueso.

CLI:
    python -m tools.contract_sync --dump   # imprime el report contra el export actual
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path
from tools.paths import HORFIN_DIR

ROOT = Path(__file__).resolve().parent.parent
CONTRACT_EXT = HORFIN_DIR / "contrato_sync.json"
CONTRACT_REPO = ROOT / "data" / "contrato_sync.json"
EXPORT = HORFIN_DIR / "catalogo_supabase.json"

# benchmark: valores que SE NULIFICAN por redundantes con tipo_activo/region (contrato v2).
# Se CONSERVAN los índices reales y la asignación de mixtos (Cartera Permanente, 40/60...).
_BENCH_NULIFICAR = {
    "Renta Fija Corto Plazo", "Renta Fija Medio Plazo", "Renta Fija Largo Plazo",
    "Renta Fija High Yield", "REITs", "RV UK",
}

# campo del export -> campo del contrato (los enum se validan por este mapeo)
FIELD_MAP = {
    "geografia": "geografia",
    "divisa": "divisa",
    "srri": "riesgo_ucits",
    "distribucion": "tipo_reparto",
    "tipo_activo": "tipo_activo",
    "estilo": "estilo",
    "plazo": "horizonte",
    "categoria_rf": "emisor",
    "categoria_activo": "tipo_gestion",
}

# Campos multi-valor del contrato v3: se validan pieza a pieza y viajan como CSV.
MULTI = {"estilo", "horizonte"}

# Vocabulario viejo -> v3, para lo que el export sigue produciendo con nombres antiguos.
_TRAD = {
    "estilo": {"Divisa cubierta": "Cubre divisa", "SmallCaps": "Small Caps",
               "Small/Mid Caps": "Small Caps", "Sector financiero": "Financiero",
               "Indexado": None, "Indexado/ETF": None},   # None = pertenece a otro campo
    "categoria_activo": {"Indexado": "Pasiva", "Gestionado": "Activa", "Hedgefund": "Activa"},
    "geografia": {"Global": "ACWI", "World": "ACWI", "spain": "España", "europe": "Europa"},
    "distribucion": {"Reparto": "Distribución"},
    "categoria_rf": {"Flexible/Diversificada": "Mixto"},
}

def load_contract() -> dict:
    """Lee el contrato de la carpeta de Horizonte; cae al copia del repo si no hay acceso."""
    for p in (CONTRACT_EXT, CONTRACT_REPO):
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError("contrato_sync.json no encontrado (ni externo ni en repo)")


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("["):
            try:
                return json.loads(s.replace("'", '"'))
            except Exception:
                return [s]
        return [s]
    return [v]


def _has(chars, *needles):
    joined = " ".join(str(x) for x in chars).lower()
    return any(n.lower() in joined for n in needles)


def derive_plazo(row) -> str | None:
    """Plazo para RF: existente → benchmark → categoría Morningstar. null si no se puede."""
    if row.get("plazo") in ("Corto", "Medio", "Largo"):
        return row["plazo"]
    b = row.get("benchmark") or ""
    if b in ("Renta Fija Corto Plazo", "Euribor"):
        return "Corto"
    if b == "Renta Fija Medio Plazo":
        return "Medio"
    if b == "Renta Fija Largo Plazo":
        return "Largo"
    ms = (row.get("categoria_morningstar") or "").lower()
    if "ultra short" in ms or "short-term" in ms or "short term" in ms or "money market" in ms:
        return "Corto"
    if "long" in ms and "short" not in ms:   # evita "Long/Short"
        return "Largo"
    return None


def compose_tipo_activo(row, report_new):
    """Tipo de activo del contrato v3: vocabulario CORTO (RV, RF, Mixto, Monetario,
    Inmobiliario, Materias primas, Alternativos) con el prefijo `ETF ` cuando el vehículo
    es un ETF. Lo que antes iba dentro del nombre del tipo (High Yield, Floating Rate, Oro,
    REITs) ya no es tipo: es Estilo, y lo recoge `compose_estilo`.
    """
    coarse = row.get("tipo_activo")
    chars = _as_list(row.get("caracteristicas_especiales"))
    nombre = (row.get("nombre") or "").upper()
    bench = row.get("benchmark") or ""
    is_etf = "ETF" in nombre or (_has(chars, "Indexado/ETF") and "ETF" in nombre)
    is_reit = _has(chars, "REIT") or bench == "REITs"

    if coarse == "RV":
        # Un fondo inmobiliario es de tipo Inmobiliario; si es ETF, el vehículo manda
        # (el matiz REITs se conserva en Estilo).
        if is_reit and not is_etf:
            return "Inmobiliario", None
        return ("ETF RV" if is_etf else "RV"), None

    if coarse == "RF":
        # ILS/catástrofe sigue siendo Alternativos (decisión de Rafa, cierre v2).
        if _has(chars, "ILS", "Catástrofe", "Catastrofe"):
            return "Alternativos", None
        return ("ETF RF" if is_etf else "RF"), None

    if coarse in ("Mixtos", "Mixto"):
        return ("ETF Mixto" if is_etf else "Mixto"), None
    if coarse == "Monetario":
        return "Monetario", None
    if coarse in ("Materias_primas", "Materias primas"):
        return ("ETF Materias primas" if is_etf else "Materias primas"), None
    if coarse == "Alternativos":
        return "Alternativos", None
    if coarse is None:
        return None, None
    report_new.append((row["isin"], "tipo_activo", f"tipo grueso no mapeado: {coarse!r}"))
    return None, coarse


def compose_estilo(row, tipo_v3):
    """Estilo del contrato v3: MULTI-VALOR. Recoge lo que ya venía en `estilo` y le suma lo
    que antes vivía en el nombre del tipo o en `caracteristicas_especiales` — que en v3 se
    elimina como campo. Así el cambio de vocabulario no pierde información.
    Devuelve una lista de valores ya traducidos (sin validar todavía).
    """
    vals, chars = [], _as_list(row.get("caracteristicas_especiales"))
    nombre = (row.get("nombre") or "").upper()
    bench = row.get("benchmark") or ""

    for v in str(row.get("estilo") or "").split(","):
        v = v.strip()
        if not v:
            continue
        vals.append(v)

    def add(v):
        if v and v not in vals:
            vals.append(v)

    if _has(chars, "Cubre divisa", "Divisa cubierta"):        add("Cubre divisa")
    if _has(chars, "Small/Mid Caps", "SmallCaps", "Small Caps"): add("Small Caps")
    if _has(chars, "Retorno absoluto"):                       add("Retorno absoluto")
    if _has(chars, "Apalancado"):                             add("Apalancado")
    if _has(chars, "ILS", "Catástrofe", "Catastrofe"):        add("ILS/Catástrofe")
    if _has(chars, "REIT") or bench == "REITs":               add("Inmobiliario/REITs")
    if _has(chars, "High Yield") or bench == "Renta Fija High Yield": add("High Yield")
    if _has(chars, "Floating"):                               add("Floating rate")
    if any(x in nombre for x in ("ORO", "GOLD")):             add("Oro")
    if "hedged" in (row.get("categoria_morningstar") or "").lower(): add("Cubre divisa")
    return vals


def apply_contract(activos: list) -> tuple[list, dict]:
    C = load_contract()["campos"]
    # v3: hay enum de un valor y enum_multi (estilo, horizonte). Ambos tienen lista cerrada.
    enums = {f: set(spec.get("valores") or []) for f, spec in C.items()
             if spec.get("tipo") in ("enum", "enum_multi") and spec.get("valores")}
    # A qué tipos de activo aplica cada valor (v3). Sin entrada = aplica a todos.
    aplica = {f: (spec.get("aplica_a_por_valor") or {}) for f, spec in C.items()}

    EQ = load_contract().get("equivalencias_vocabulario_viejo", {})

    def _traducir(campo, valor):
        """Vocabulario viejo -> v3, con la tabla que viaja DENTRO del contrato.
        Devuelve (valor, destino): destino != None significa que ese valor pertenece a otro
        campo (p.ej. 'Gubernamental' en estilo es en realidad Emisor). '' = hay que descartarlo.
        """
        if valor is None:
            return None, None
        tabla = EQ.get(campo, {})
        if valor not in tabla:
            return valor, None
        nuevo = tabla[valor]
        if isinstance(nuevo, str) and nuevo.startswith("@"):
            destino, val = nuevo[1:].split(":", 1)
            return None, (destino, val)
        return (nuevo or None), None

    def _aplica(campo, valor, tipo):
        reglas = aplica.get(campo, {})
        if valor not in reglas or not tipo:
            return True
        return tipo in [x.strip() for x in str(reglas[valor]).split(",")]

    out = []
    fuera = Counter()            # valores fuera de contrato puestos a null
    fuera_ej = {}
    propuestas = []              # valores nuevos a acordar
    bench_categoria = Counter()

    for a in activos:
        r = dict(a)

        # --- traducción del vocabulario viejo, con la tabla del propio contrato ---
        # El export puede venir con el granular de v2 ('Fondo RF Medio Plazo') o con lo grueso
        # de Supabase ('RF'): las dos formas entran por aquí y salen en v3.
        for ex_field, co_field in FIELD_MAP.items():
            if ex_field == "estilo":
                continue                       # el estilo lo compone compose_estilo
            nuevo, destino = _traducir(co_field, r.get(ex_field))
            if destino:                        # el valor pertenece a otro campo: se muda si está libre
                dst_ex = next((k for k, vv in FIELD_MAP.items() if vv == destino[0]), None)
                if dst_ex and not r.get(dst_ex):
                    r[dst_ex] = destino[1]
                r[ex_field] = None
            else:
                r[ex_field] = nuevo
        # tema_sector y caracteristicas_especiales salen del contrato v3: no se emiten.
        r.pop("tema_sector", None)

        # plazo derivado (para RF), útil por sí mismo
        if r.get("tipo_activo") == "RF" and not r.get("plazo"):
            p = derive_plazo(r)
            if p:
                r["plazo"] = p

        # tipo_activo granular (compone). Idempotente: si ya es un valor granular válido
        # (p.ej. re-exportando sin grupo del que re-sourcear), se deja tal cual.
        if r.get("tipo_activo") not in enums["tipo_activo"]:
            corto, prop = compose_tipo_activo(r, propuestas)
            r["tipo_activo"] = corto
        # El estilo se compone DESPUÉS del tipo, porque el tipo decide qué estilos aplican.
        estilos = compose_estilo(r, r.get("tipo_activo"))
        limpios = []
        for e in estilos:
            e, destino = _traducir("estilo", e)
            if destino:                       # 'Gubernamental' en estilo es Emisor, no estilo
                dst_ex = next((k for k, vv in FIELD_MAP.items() if vv == destino[0]), None)
                if dst_ex and not r.get(dst_ex):
                    r[dst_ex] = destino[1]
                continue
            if e is None:
                continue
            if e not in enums.get("estilo", set()):
                fuera[f"estilo={e!r}"] += 1
                fuera_ej.setdefault(f"estilo={e!r}", r["isin"])
                continue
            if not _aplica("estilo", e, r.get("tipo_activo")):
                fuera[f"estilo={e!r} (no aplica a {r.get('tipo_activo')})"] += 1
                fuera_ej.setdefault(f"estilo={e!r} (no aplica a {r.get('tipo_activo')})", r["isin"])
                continue
            limpios.append(e)
        r["estilo"] = ",".join(limpios) if limpios else None
        r.pop("caracteristicas_especiales", None)   # eliminado del contrato v3

        # benchmark: nulifica lo redundante con tipo_activo/region (contrato v2).
        # Conserva índices reales + asignación de mixtos (Cartera Permanente, 40/60...).
        if r.get("benchmark") in _BENCH_NULIFICAR:
            bench_categoria[r["benchmark"]] += 1
            r["benchmark"] = None

        # --- validación enum: fuera de lista → null + report ---
        for ex_field, co_field in FIELD_MAP.items():
            if co_field not in enums:
                continue
            if ex_field == "estilo":
                continue                       # ya validado arriba, pieza a pieza
            v = r.get(ex_field)
            if v is None:
                continue
            if co_field in MULTI:              # multi-valor: se valida cada pieza
                piezas = [x.strip() for x in str(v).split(",") if x.strip()]
                ok = [x for x in piezas if x in enums[co_field] and _aplica(co_field, x, r.get("tipo_activo"))]
                for x in piezas:
                    if x not in ok:
                        fuera[f"{ex_field}={x!r}"] += 1
                        fuera_ej.setdefault(f"{ex_field}={x!r}", r["isin"])
                r[ex_field] = ",".join(ok) if ok else None
                continue
            if v not in enums[co_field] or not _aplica(co_field, v, r.get("tipo_activo")):
                fuera[f"{ex_field}={v!r}"] += 1
                fuera_ej.setdefault(f"{ex_field}={v!r}", r["isin"])
                r[ex_field] = None

        out.append(r)

    report = {
        "n_filas": len(out),
        "valores_puestos_a_null_por_fuera_de_contrato": dict(fuera),
        "ejemplo_isin": fuera_ej,
        "propuestas_valor_nuevo": [
            {"isin": i, "campo": c, "motivo": m} for i, c, m in propuestas
        ],
        "benchmark_nulificado_redundante": dict(bench_categoria),
        "tipo_activo_resultante": dict(Counter(x.get("tipo_activo") for x in out)),
        "plazo_relleno": sum(1 for x in out if x.get("plazo")),
    }
    return out, report


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    doc = json.loads(EXPORT.read_text(encoding="utf-8"))
    out, rep = apply_contract(doc["activos"])
    print(json.dumps(rep, ensure_ascii=False, indent=1))
