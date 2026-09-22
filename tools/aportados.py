"""
aportados.py — Ingesta de material PROFESIONAL aportado por Rafa (docs de gestoras
gated, análisis externos) al pulsar "actualizar" en el portal. Se trata como fuente
PRIORITARIA (material curado > discovery automático) para mejorar el output final.

Flujo:
  Portal (al actualizar) → sube ficheros a Storage `uploads/{ISIN}/` y añade al item de la
  cola:  "docs_aportados": ["url1.pdf", ...],  "analisis_externos": [{url, nota}, ...]
  Worker → aportados.ingest(isin, docs_urls, analisis_externos):
     - descarga los PDFs a data/funds/{ISIN}/raw/aportados/
     - escribe manifiesto data/funds/{ISIN}/aportados.json
     - register_for_extraction(): añade los PDFs a pending_extraction.json (flag
       aportado=alta prioridad) para que la skill extract-pdfs los procese
     - los análisis externos se inyectan como readings prioritarios (readings_data.json)

El analyst da MÁS peso a lo aportado (contexto marca "APORTADO POR EL ASESOR — fuente
prioritaria y fiable"). No sustituye al pipeline: lo complementa.

CLI (test):
    python -m tools.aportados --isin LU1623762843 --doc file:///C:/ruta/informe.pdf
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _fund_dir(isin: str) -> Path:
    return ROOT / "data" / "funds" / isin.upper()


def _slug(name: str) -> str:
    name = (name or "doc.pdf").encode("ascii", "ignore").decode()
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return re.sub(r"_+", "_", name).strip("_") or "doc.pdf"


def _download(url: str, dest: Path) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fund-analyzer"})
        data = urllib.request.urlopen(req, timeout=90).read()
        if not data:
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return True
    except Exception:
        return False


def ingest(isin: str, docs_urls: list[str] | None = None,
           analisis_externos: list[dict] | None = None, log=print) -> dict:
    """Descarga los docs aportados, escribe el manifiesto y registra para extracción +
    readings. Devuelve resumen. Idempotente."""
    isin = isin.upper()
    fd = _fund_dir(isin)
    apo_dir = fd / "raw" / "aportados"
    docs_urls = docs_urls or []
    analisis_externos = analisis_externos or []

    manifest = {"isin": isin, "actualizado": datetime.now(timezone.utc).isoformat(),
                "docs": [], "analisis_externos": analisis_externos}
    for url in docs_urls:
        name = _slug(Path(url.split("?")[0]).name) or "aportado.pdf"
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
        dest = apo_dir / name
        if _download(url, dest):
            manifest["docs"].append({"nombre": name, "url": url,
                                     "local_path": str(dest), "bytes": dest.stat().st_size})
            log(f"[APORTADO] descargado {name} ({dest.stat().st_size} bytes)")
        else:
            log(f"[APORTADO] no se pudo descargar {url[:60]}")

    if not manifest["docs"] and not analisis_externos:
        return {"ok": True, "n_docs": 0, "n_externos": 0}

    (fd).mkdir(parents=True, exist_ok=True)
    (fd / "aportados.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    register_for_extraction(isin, manifest, log=log)
    inject_readings(isin, analisis_externos, log=log)
    return {"ok": True, "n_docs": len(manifest["docs"]), "n_externos": len(analisis_externos)}


# Versión del esquema de extracción de aportados. SUBIRLA al cambiar `_aportado_schema()`:
# el id de la task la incluye, así un cambio de esquema genera un id nuevo → su extract no existe
# → el gate de extract re-extrae solo, y el extract de la versión vieja se borra aquí. (Antes
# había que invalidar a mano; ahora es del sistema.)
APORTADO_SCHEMA_VERSION = 5   # v5: graficos_documento = series DIGITALIZADAS (gráficos propios), no recortes


def task_id_for(nombre: str) -> str:
    """Id canónico de la task/extract de un doc aportado (lo usan también los gates)."""
    return f"aportado_v{APORTADO_SCHEMA_VERSION}_{_slug(nombre)}"


def _aportado_schema() -> dict:
    """Esquema RICO de los AR (posiciones, allocations, statistics, performance, cualitativo…)
    + los extras propios de material profesional. NO ad-hoc: un esquema escrito a mano perdió
    `performance`/`statistics` (caso MontLake: tabla de rentabilidades 2020-24 sin capturar)."""
    try:
        from agents.intl_extractor_v2 import AR_SUBFUND_SCHEMA
        import copy
        sch = copy.deepcopy(AR_SUBFUND_SCHEMA)
    except Exception:
        sch = {}
    sch["periodo"] = ("string YYYY-MM — fecha de los DATOS del documento (no la de publicación), con "
                      "MES: es un snapshot intra-anual y debe SUMAR un punto a la evolución, no "
                      "pisar el del AR del mismo año")
    sch["performance"] = ("list[{periodo:'YYYY', clase, rentabilidad_pct, benchmark_pct, vehiculo}] — "
                          "TODAS las rentabilidades por año natural que traiga el doc (tablas de "
                          "'Historical performance'/'Track record', también vs peers/benchmark). "
                          "`vehiculo` = qué vehículo generó ese año si el track viene de PREDECESORES "
                          "(certificado, RAIF, fondo previo) — ver track_record_lineage")
    sch.update({
        "texto_cualitativo": "string — resumen del documento",
        "track_record_lineage": ("list[{desde, hasta, vehiculo, isin}] si el doc indica que el track "
                                 "record se construye con vehículos PREDECESORES (p.ej. 'performance "
                                 "until June 2021 derived from <certificado> XS…'). Literal."),
        "criterios_inversion": {
            "spread_objetivo": "p.ej. +300 pb sobre tasa libre de riesgo",
            "calidad_crediticia_minima": "p.ej. BBB- / solo investment grade",
            "tamano_minimo_emisor": "p.ej. capitalización mínima $5bn",
            "otros_limites": "list — duración, concentración, geografía, divisa…",
        },
        "estructura_gestion": {
            "management_company": "ManCo / plataforma legal (p.ej. Waystone/MontLake)",
            "investment_manager": "gestor de inversión REAL que toma las decisiones (p.ej. Fortune)",
            "roles": "quién hace qué: regulatorio/legal/administración vs gestión de cartera",
            "por_que": "razón del modelo (plataforma UCITS para gestoras boutique, etc.)",
        },
        "vision_gestores": {"decisiones_clave": "list", "cambios_cartera": "list",
                            "cambios_estrategia": "list", "outlook": "string — visión a futuro"},
        "sector_allocation_history": ("list[{periodo:'YYYY-MM', sectores:{sector:peso}}] SOLO si el doc "
                                      "trae la exposición por sector EN VARIAS FECHAS con valores legibles"),
        "geographic_allocation_history": "list[{periodo:'YYYY-MM', zonas:{region:peso}}] idem",
        "clases_documento": ("list[{codigo, isin, divisa, cubierta:bool, reparto:'Acc'|'Dist', "
                             "comision_gestion_pct, comision_exito_pct, comision_exito_detalle, "
                             "inversion_minima, activa:bool}] — TODAS las filas de la tabla de clases "
                             "('List of share classes'), una por clase, LITERAL. `cubierta`=true si la "
                             "divisa dice Hedged. `comision_exito_detalle` = base y condiciones (p.ej. "
                             "'10% sobre el tipo libre de riesgo, con high-water mark'). Si una clase no "
                             "cobra éxito: comision_exito_pct=0. No omitas clases inactivas (activa=false)."),
        "graficos_documento": (
            "list — gráficos del documento que se RE-DIBUJARÁN en el dashboard con formato propio. Los "
            "NÚMEROS no los lees tú: ya están medidos en el fichero de gráficos digitalizados que indica "
            "el contexto de la task (cada gráfico tiene un `id` tipo 'p48#2'). Tú ELIGES, NOMBRAS e "
            "INTERPRETAS. Cada item: {id, titulo (español, claro), seccion:'cartera'|'rentabilidad'|"
            "'riesgo'|'patrimonio', dimension:'rating'|'sector'|'geografia'|'tipo_activo'|'rotacion'|'duracion'|'yield'|"
            "'patrimonio'|'otro' (qué mide; rating/sector/geografia/tipo_activo/rotacion son las dimensiones ESTÁNDAR "
            "que van en el cuerpo de todos los fondos), clave:bool (máx 2 en total: gráficos que definen a "
            "ESTE tipo de fondo aunque no sean estándar, p.ej. duración en renta fija; el resto va a un "
            "anexo), formato:'linea'|'barras'|'area_apilada'|'barras_apiladas', unidad "
            "('%', 'años', 'M USD'…), series:[{color_hex, nombre}] (SOLO las series a pintar; nombre mirando la leyenda de la página. "
            "Categorías GENÉRICAS en español (sectores, países, Gobiernos, Financiero…); la TERMINOLOGÍA "
            "TÉCNICA DE MERCADO SE DEJA EN INGLÉS tal cual la usa el sector — tipos/prelación de deuda "
            "(Senior Unsecured, Tier 2, AT1, Sr Non Preferred, 1st lien), tramos de rating (Investment Grade, "
            "High Yield, BBB-), 'Yield to worst', 'Duration'… — porque la traducción literal no es "
            "representativa; omite restos/ruido), x_inicio y "
            "x_fin ('YYYY-MM', fecha del primer y último punto leída del eje X de la página — "
            "OBLIGATORIO si el gráfico digitalizado tiene eje_x='relativo' y es temporal), categorias "
            "(list, en orden, si eje_x='relativo' y NO es temporal, p.ej. ['2021','2022',…]), desglose "
            "(opcional: {serie:<nombre de una serie de ESTE gráfico>, grafico:<id de otro gráfico que reparte esa "
            "serie en partes, p.ej. bancos/aseguradoras dentro de Financiero>, x_inicio, x_fin, partes:[{color_hex,"
            "nombre}]} → las partes sustituyen a la serie en el mismo gráfico; úsalo en vez de publicar el reparto "
            "como gráfico aparte), lectura "
            "(2-3 frases: qué enseña sobre la estrategia/cartera: tendencia, niveles inicio→fin, cambios "
            "de régimen y qué decisión del gestor revelan)}. CRITERIO: solo gráficos de EVOLUCIÓN TEMPORAL "
            "del propio fondo que ayuden a entender cartera, riesgo, patrimonio o resultados (exposición "
            "IG/no-IG, sectores, subordinación, yield, duración, AUM/flujos, rotación, atribución anual). "
            "NO: gráficos de mercado genéricos, fotos de un solo momento, los que dupliquen lo que el "
            "dashboard ya calcula (rentabilidad acumulada, drawdown, volatilidad) ni series ilegibles. "
            "Máximo 8, los más valiosos. Si un gráfico clave NO está digitalizado (es una imagen), puedes "
            "darlo con `aproximado:true` y series:[{nombre, puntos:[[etiqueta, valor]]}] leídos del eje, "
            "con pocos puntos (anuales/semestrales)."),
        "datos_clave": "dict — resto de datos relevantes (custodio, auditor, registros por país…)",
    })
    return sch


def register_for_extraction(isin: str, manifest: dict, log=print) -> int:
    """Añade los PDFs aportados como tasks en pending_extraction.json, marcados como fuente
    prioritaria fiable (el extractor y el analyst les dan más peso). Si la task existente es de
    OTRA versión de esquema, la sustituye y borra su extract (invalidación automática)."""
    isin = isin.upper()
    fd = _fund_dir(isin)
    pe_path = fd / "pending_extraction.json"
    pe = {}
    if pe_path.exists():
        try:
            pe = json.loads(pe_path.read_text(encoding="utf-8"))
        except Exception:
            pe = {}
    pe.setdefault("isin", isin)
    tasks = pe.setdefault("tasks", [])
    n = 0
    for doc in manifest.get("docs", []):
        lp = doc["local_path"]
        tid = task_id_for(doc["nombre"])
        charts_rel = digitize_doc(isin, doc["nombre"], log=log)
        if any(isinstance(t, dict) and t.get("id") == tid for t in tasks):
            continue   # ya registrada con el esquema vigente
        # Task del MISMO pdf con otra versión de esquema → fuera (y su extract, que quedó stale)
        viejas = [t for t in tasks if isinstance(t, dict) and t.get("aportado") and t.get("pdf_path") == lp]
        for t in viejas:
            tasks.remove(t)
            old = fd / "extracted" / f"{t.get('id')}.json"
            if old.exists():
                try:
                    old.unlink()
                    log(f"[APORTADO] esquema v{APORTADO_SCHEMA_VERSION}: extract antiguo invalidado ({old.name})")
                except Exception:
                    pass
        tasks.append({
            "id": tid,
            "agent": "intl_extractor_v2",
            "pdf_path": lp,
            "schema": _aportado_schema(),
            "schema_version": APORTADO_SCHEMA_VERSION,
            "context": (f"DOCUMENTO APORTADO POR EL ASESOR para el fondo {isin} — fuente "
                        "PRIORITARIA, curada y fiable (material profesional de la gestora o "
                        "análisis externo de calidad). Dale MÁS peso que a las fuentes "
                        "automáticas al sintetizar. Suele ser una PRESENTACIÓN (muchas páginas son "
                        "tablas/gráficos): recórrela ENTERA y captura LITERALMENTE (a) criterios de "
                        "inversión (spread objetivo, rating mínimo, tamaño mínimo de emisor, límites), "
                        "(b) estructura de gestión (ManCo/plataforma vs gestor real), (c) visión de los "
                        "gestores, (d) TODAS las tablas numéricas: rentabilidades por año, desglose por "
                        "rating/sector/país/tipo de activo, estadísticas, (e) el linaje del track "
                        "record si viene de vehículos predecesores, (f) la tabla COMPLETA de clases "
                        "(clases_documento) y (g) los gráficos a re-dibujar (graficos_documento). Lee como "
                        "IMAGEN las páginas de "
                        "tablas/gráficos. No inventes valores de un gráfico sin cifras legibles."
                        + (f" GRÁFICOS DIGITALIZADOS (valores ya medidos sobre los ejes, úsalos para "
                           f"`graficos_documento` y para tu lectura): lee {charts_rel} (ruta relativa al "
                           f"repo) — trae por gráfico: id, página, título detectado, eje, y por serie "
                           f"color, nombre detectado (puede venir sucio), nº de puntos y valores "
                           f"inicio/fin/mín/máx." if charts_rel else "")),
            "aportado": True,
            "two_stage": True,
        })
        n += 1
    pe["updated_at"] = datetime.now(timezone.utc).isoformat()
    pe_path.write_text(json.dumps(pe, ensure_ascii=False, indent=2), encoding="utf-8")
    if n:
        log(f"[APORTADO] {n} docs añadidos a la cola de extracción (prioritarios)")
    return n


def charts_paths(isin: str, nombre: str) -> tuple[Path, Path]:
    ext = _fund_dir(isin.upper()) / "extracted"
    return ext / f"charts_{_slug(nombre)}.full.json", ext / f"charts_{_slug(nombre)}.resumen.json"


def digitize_doc(isin: str, nombre: str, log=print) -> str | None:
    """Digitaliza los gráficos vectoriales del PDF aportado (tools/pdf_chart_digitizer) ANTES de la
    extracción: `.full.json` (todas las cifras, lo usa aportado_publish) y `.resumen.json` (compacto,
    lo lee el extractor para elegir/nombrar). Determinista e idempotente. Devuelve la ruta relativa
    del resumen, o None si no hay gráficos / falla (la extracción sigue igual)."""
    try:
        pdf = _fund_dir(isin.upper()) / "raw" / "aportados" / nombre
        if not pdf.exists():
            return None
        full, res = charts_paths(isin, nombre)
        if not (full.exists() and res.exists() and full.stat().st_mtime >= pdf.stat().st_mtime):
            import pdfplumber
            from tools.pdf_chart_digitizer import digitize
            with pdfplumber.open(str(pdf)) as d:
                n = len(d.pages)
            data = digitize(str(pdf), list(range(1, n + 1)))
            data = {k: v for k, v in data.items() if isinstance(v, list) and v}
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            comp = []
            for pg, charts in data.items():
                for c in charts:
                    comp.append({"id": c["id"], "pagina": int(pg), "titulo_detectado": c["titulo_detectado"],
                                 "eje_y": c["eje_y"], "eje_x": c["eje_x"], "apilado": c["apilado"],
                                 "series": [{"color_hex": s_["color_hex"], "nombre_detectado": s_["nombre_detectado"],
                                             "tipo": s_["tipo"], "n": len(s_["puntos"]),
                                             "inicio": s_["puntos"][0], "fin": s_["puntos"][-1],
                                             "min": min(p_[1] for p_ in s_["puntos"]),
                                             "max": max(p_[1] for p_ in s_["puntos"])} for s_ in c["series"]]})
            res.write_text(json.dumps(comp, ensure_ascii=False, indent=1), encoding="utf-8")
            log(f"[APORTADO] {len(comp)} gráficos digitalizados de {nombre}")
        return res.relative_to(ROOT).as_posix() if res.exists() else None
    except Exception as e:  # noqa: BLE001
        log(f"[APORTADO] digitalización de gráficos falló ({type(e).__name__}: {e}) — se sigue sin ella")
        return None


def register_from_folder(isin: str, log=print) -> int:
    """RECONCILE: escanea raw/aportados/*.pdf y registra en pending_extraction.json los que falten.
    Blindaje: garantiza que un doc aportado SIEMPRE entra a extracción, aunque el prep regenerara
    el manifiesto (pisando el task) o el análisis se lanzara sin pasar por ingest(). Idempotente."""
    isin = isin.upper()
    apo_dir = _fund_dir(isin) / "raw" / "aportados"
    if not apo_dir.exists():
        return 0
    docs = []
    for p in sorted(apo_dir.glob("*.pdf")):
        try:
            docs.append({"nombre": p.name, "local_path": str(p), "bytes": p.stat().st_size})
        except Exception:
            continue
    if not docs:
        return 0
    n = register_for_extraction(isin, {"docs": docs}, log=log)
    purge_stale_extracts(isin, [d["nombre"] for d in docs], log=log)
    return n


def current_extracts(isin: str) -> list[Path]:
    """Extracts de docs aportados, UNO por documento: el de esquema más reciente. Los consumidores
    deben usar esto y no un glob `aportado_*.json` (pueden convivir dos versiones del mismo PDF
    entre que se re-extrae con el esquema nuevo y se purga el viejo)."""
    import re as _re
    ext = _fund_dir(isin.upper()) / "extracted"
    best: dict[str, tuple[int, Path]] = {}
    for f in ext.glob("aportado*.json") if ext.exists() else []:
        m = _re.match(r"aportado(?:_v(\d+))?_(.+)\.json$", f.name)
        if not m:
            continue
        ver, slug = int(m.group(1) or 0), m.group(2)
        if slug not in best or ver > best[slug][0]:
            best[slug] = (ver, f)
    return [p for _, p in sorted(best.values(), key=lambda x: x[1].name)]


def purge_stale_extracts(isin: str, nombres: list[str], log=print) -> int:
    """Borra los extracts de un doc aportado hechos con un esquema ANTERIOR, pero solo cuando ya
    existe el de la versión vigente (así un fallo de extracción no deja el fondo sin datos del
    aporte). Sin esto convivían dos extracts del mismo PDF y los consumidores que hacen glob
    `aportado_*.json` (series históricas) leían también el viejo."""
    ext = _fund_dir(isin.upper()) / "extracted"
    if not ext.exists():
        return 0
    n = 0
    for nombre in nombres:
        cur = ext / f"{task_id_for(nombre)}.json"
        if not cur.exists():
            continue
        slug = _slug(nombre)
        for f in ext.glob("aportado*.json"):
            if f.name != cur.name and f.name.endswith(f"{slug}.json"):
                try:
                    f.unlink()
                    n += 1
                    log(f"[APORTADO] extract de esquema anterior eliminado ({f.name})")
                except Exception:
                    pass
    return n


def inject_readings(isin: str, analisis_externos: list[dict], log=print) -> int:
    """Inyecta los análisis externos aportados como readings PRIORITARIOS."""
    if not analisis_externos:
        return 0
    isin = isin.upper()
    rp = _fund_dir(isin) / "readings_data.json"
    R = {}
    if rp.exists():
        try:
            R = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            R = {}
    lst = R.setdefault("analisis_completos", [])
    known = {(r.get("url") or "") for r in lst if isinstance(r, dict)}
    n = 0
    for a in analisis_externos:
        url = a.get("url") or ""
        if url and url in known:
            continue
        lst.append({
            "url": url, "fuente": a.get("fuente") or "Aportado por el asesor",
            "titulo": a.get("nota") or "Análisis externo aportado",
            "texto": a.get("texto") or a.get("nota") or "",
            "aportado": True, "prioridad": "alta",
        })
        n += 1
    R["updated_at"] = datetime.now(timezone.utc).isoformat()
    rp.write_text(json.dumps(R, ensure_ascii=False, indent=2), encoding="utf-8")
    if n:
        log(f"[APORTADO] {n} análisis externos inyectados como readings prioritarios")
    return n


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    ap.add_argument("--doc", action="append", default=[], help="URL/file:// de un PDF aportado")
    ap.add_argument("--externo", action="append", default=[], help="URL de un análisis externo")
    ap.add_argument("--reconcile", action="store_true",
                    help="registra en pending_extraction los PDFs de raw/aportados/ que falten (blindaje)")
    a = ap.parse_args()
    if a.reconcile:
        n = register_from_folder(a.isin)
        print(json.dumps({"ok": True, "reconciled": n}, ensure_ascii=False))
        return
    ext = [{"url": u} for u in a.externo]
    r = ingest(a.isin, docs_urls=a.doc, analisis_externos=ext)
    print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()
