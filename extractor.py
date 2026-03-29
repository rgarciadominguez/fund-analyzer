#!/usr/bin/env python3
"""
extractor.py — Fund Analyzer
Uso: python extractor.py ES0112231008 [--gestora-web https://www.avantagecapital.com]

Dado un ISIN de un fondo español:
1. Resuelve NIF y metadatos desde CNMV
2. Descarga XMLs CNMV → serie histórica cuantitativa (AUM, partícipes, TER, exposición)
3. Extrae cartas semestrales de la gestora → cualitativo completo
4. Genera fund_data.json validado contra el schema
5. Guarda en /data/[ISIN].json
"""

import sys
import json
import argparse
from datetime import date, datetime
from pathlib import Path

import jsonschema
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
SCHEMA_PATH = ROOT / "schema" / "fund.schema.json"
DATA_DIR.mkdir(exist_ok=True)

from sources.cnmv_meta import resolve_isin, get_gestora_url
from sources.cnmv_xml import extract_serie_historica
from sources.cartas import extract_all_cartas


# ─── Helpers ────────────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FundAnalyzer/1.0)"}

def load_schema() -> dict:
    with open(SCHEMA_PATH) as f:
        return json.load(f)

def validate(data: dict) -> bool:
    schema = load_schema()
    try:
        jsonschema.validate(instance=data, schema=schema)
        print("[validate] ✓ JSON válido contra schema")
        return True
    except jsonschema.ValidationError as e:
        print(f"[validate] ✗ Error de validación: {e.message}")
        return False

def save(isin: str, data: dict):
    path = DATA_DIR / f"{isin}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[save] ✓ Guardado en {path}")
    return path

def load_existing(isin: str) -> dict | None:
    path = DATA_DIR / f"{isin}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


# ─── Snapshot actual desde web gestora ──────────────────────────────────────

def get_snapshot_gestora(gestora_web: str) -> dict:
    """
    Extrae datos actuales del fondo desde la web de la gestora.
    """
    snapshot = {
        "fecha": str(date.today()),
        "aum_meur": None,
        "participes": None,
        "vl_clase_a_eur": None,
        "vl_clase_b_eur": None,
        "cagr_desde_inicio_pct": None,
        "rentabilidad_acumulada_pct": None
    }
    
    try:
        r = requests.get(gestora_web, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        
        # AUM en millones
        aum_match = r"(\d{1,4}[.,]\d{1,3})\s*M[€$]?(?:\s*(?:de)?\s*(?:euros?|patrimonio))"
        m = __import__("re").search(aum_match, text, __import__("re").IGNORECASE)
        if m:
            snapshot["aum_meur"] = float(m.group(1).replace(",", "."))
        
        # Partícipes
        part_match = r"(\d{1,6})\s*partícipes?"
        m = __import__("re").search(part_match, text, __import__("re").IGNORECASE)
        if m:
            snapshot["participes"] = int(m.group(1).replace(".", ""))
        
    except Exception as e:
        print(f"[snapshot] No se pudo extraer snapshot de {gestora_web}: {e}")
    
    return snapshot


# ─── Estructura base del JSON ────────────────────────────────────────────────

def build_base_json(meta: dict, gestora_web: str) -> dict:
    """Construye la estructura base del JSON con todos los campos presentes."""
    return {
        "meta": {
            "isin":              meta["isin"],
            "nombre":            meta.get("nombre"),
            "gestora":           meta.get("gestora"),
            "asesor":            None,
            "nif_fondo":         meta.get("nif"),
            "registro_cnmv":     meta.get("registro_cnmv"),
            "fecha_creacion":    meta.get("fecha_creacion"),
            "ultima_extraccion": str(date.today()),
            "fuentes_procesadas": [],
            "extraccion_estado": {
                "cualitativo":        "pendiente",
                "cuantitativo_serie": "pendiente",
                "rentabilidades":     "pendiente"
            }
        },
        "ficha": {
            "descripcion":  None,
            "tipo":         None,
            "sfdr":         None,
            "depositario":  meta.get("depositario"),
            "auditora":     None,
            "clases":       {},
            "personas_clave": []
        },
        "cuantitativo": {
            "snapshot_actual":        {"fecha": str(date.today()), "aum_meur": None, "participes": None},
            "serie_historica":        [],
            "rentabilidades_anuales": []
        },
        "estrategia": {
            "filosofia":             {"texto": None, "fuente": None},
            "proceso":               None,
            "vision_actual":         None,
            "evolucion_por_periodo": []
        },
        "cambios_relevantes":    [],
        "lecturas_recomendadas": []
    }


# ─── Merge inteligente ───────────────────────────────────────────────────────

def merge_serie(existing: list, new: list) -> list:
    """
    Combina la serie histórica existente con los nuevos datos.
    Si un periodo ya existe, actualiza solo los campos nulos.
    """
    existing_by_periodo = {e["periodo"]: e for e in existing}
    
    for item in new:
        periodo = item["periodo"]
        if periodo not in existing_by_periodo:
            existing_by_periodo[periodo] = item
        else:
            # Actualizar solo campos nulos
            for k, v in item.items():
                if existing_by_periodo[periodo].get(k) is None and v is not None:
                    existing_by_periodo[periodo][k] = v
    
    return sorted(existing_by_periodo.values(), key=lambda x: (x["año"], x["mes"]))


def rentabilidades_desde_serie(serie: list) -> list:
    """
    Calcula rentabilidades anuales a partir de la serie semestral.
    Solo si se tienen ambos semestres del año.
    """
    by_year = {}
    for item in serie:
        y = item.get("año")
        m = item.get("mes")
        if y and m and item.get("rent_pct") is not None:
            if y not in by_year:
                by_year[y] = {}
            semestre = "H1" if m <= 6 else "H2"
            by_year[y][semestre] = item["rent_pct"]
    
    result = []
    for año, semestres in sorted(by_year.items()):
        if "H1" in semestres and "H2" in semestres:
            # Rentabilidad anual = (1+H1/100)*(1+H2/100) - 1
            rent_anual = round(((1 + semestres["H1"]/100) * (1 + semestres["H2"]/100) - 1) * 100, 2)
            result.append({"año": año, "pct": rent_anual, "fuente": "calculado_desde_xml_cnmv"})
        elif "H2" in semestres:
            result.append({"año": año, "pct": semestres["H2"], "fuente": "xml_cnmv_2H"})
    
    return result


# ─── Main ────────────────────────────────────────────────────────────────────

def run(isin: str, gestora_web: str | None = None, force: bool = False):
    print(f"\n{'='*60}")
    print(f" Fund Analyzer — {isin}")
    print(f"{'='*60}\n")

    # 1. Cargar JSON existente si hay
    fund_data = load_existing(isin)
    is_new = fund_data is None

    # 2. Resolver metadatos CNMV
    print("── Paso 1: Resolver ISIN en CNMV ──")
    meta = resolve_isin(isin)
    
    if not meta.get("nif"):
        print(f"[ERROR] No se pudo resolver el ISIN {isin} en la CNMV.")
        print("Verifica que el ISIN es correcto y es un fondo español (ES...).")
        sys.exit(1)

    # 3. Construir base si es nuevo
    if is_new:
        fund_data = build_base_json(meta, gestora_web or "")
    else:
        # Actualizar NIF si ahora lo tenemos
        if meta.get("nif"):
            fund_data["meta"]["nif_fondo"] = meta["nif"]

    # 4. Detectar web gestora si no se proporcionó
    if not gestora_web:
        gestora_web = get_gestora_url(meta["nif"])
        if gestora_web:
            print(f"[meta] Web gestora detectada: {gestora_web}")
        else:
            print("[meta] No se pudo detectar web gestora automáticamente.")
            gestora_web = input("Introduce la URL de la web de la gestora (o Enter para saltar): ").strip()

    # 5. Serie histórica cuantitativa desde XMLs CNMV
    print("\n── Paso 2: Extraer serie histórica desde XMLs CNMV ──")
    año_inicio = 2014
    if meta.get("fecha_creacion"):
        try:
            año_inicio = int(meta["fecha_creacion"][:4])
        except ValueError:
            pass
    
    serie_nueva = extract_serie_historica(isin, año_inicio=año_inicio)
    
    if serie_nueva:
        serie_existente = fund_data["cuantitativo"].get("serie_historica", [])
        fund_data["cuantitativo"]["serie_historica"] = merge_serie(serie_existente, serie_nueva)
        
        # Calcular rentabilidades anuales desde la serie
        rent_calculadas = rentabilidades_desde_serie(fund_data["cuantitativo"]["serie_historica"])
        if rent_calculadas:
            # Merge con las existentes (no sobreescribir datos manuales)
            rent_existente = {r["año"]: r for r in fund_data["cuantitativo"].get("rentabilidades_anuales", [])}
            for r in rent_calculadas:
                if r["año"] not in rent_existente or rent_existente[r["año"]].get("pct") is None:
                    rent_existente[r["año"]] = r
            fund_data["cuantitativo"]["rentabilidades_anuales"] = sorted(rent_existente.values(), key=lambda x: x["año"])
        
        # Actualizar estado extracción
        tiene_datos = any(s.get("aum_meur") is not None for s in serie_nueva)
        fund_data["meta"]["extraccion_estado"]["cuantitativo_serie"] = "completo" if tiene_datos else "pendiente"
        
        if "XML CNMV" not in str(fund_data["meta"]["fuentes_procesadas"]):
            fund_data["meta"]["fuentes_procesadas"].append("XMLs trimestrales CNMV")
    
    # 6. Snapshot actual
    if gestora_web:
        print("\n── Paso 3: Snapshot actual desde web gestora ──")
        snapshot = get_snapshot_gestora(gestora_web)
        fund_data["cuantitativo"]["snapshot_actual"].update(
            {k: v for k, v in snapshot.items() if v is not None}
        )

    # 7. Cualitativo desde cartas
    if gestora_web and (is_new or force or fund_data["meta"]["extraccion_estado"]["cualitativo"] != "completo"):
        print("\n── Paso 4: Extraer cartas semestrales ──")
        cartas_data = extract_all_cartas(gestora_web, isin)
        
        if cartas_data["cartas_procesadas"] > 0:
            # Evolución por periodo
            if cartas_data.get("evolucion_por_periodo"):
                fund_data["estrategia"]["evolucion_por_periodo"] = cartas_data["evolucion_por_periodo"]
            
            # Visión actual
            if cartas_data.get("vision_actual"):
                fund_data["estrategia"]["vision_actual"] = cartas_data["vision_actual"]
            
            # Exposición RV narrativa
            if cartas_data.get("exposicion_rv_narrativa"):
                fund_data["cuantitativo"]["exposicion_rv_narrativa"] = cartas_data["exposicion_rv_narrativa"]
            
            # Rentabilidades adicionales desde cartas (para años no cubiertos por XML)
            for r in cartas_data.get("rentabilidades_desde_cartas", []):
                pass  # Se manejan en paso 5
            
            fund_data["meta"]["extraccion_estado"]["cualitativo"] = "completo"
            
            if gestora_web not in str(fund_data["meta"]["fuentes_procesadas"]):
                fund_data["meta"]["fuentes_procesadas"].append(f"Cartas semestrales {gestora_web}")
    
    # 8. Actualizar fecha extracción
    fund_data["meta"]["ultima_extraccion"] = str(date.today())

    # 9. Validar y guardar
    print("\n── Paso 5: Validar y guardar ──")
    validate(fund_data)
    output_path = save(isin, fund_data)
    
    # 10. Resumen
    print(f"""
{'='*60}
 ✓ Extracción completada: {isin}
{'='*60}
 Fondo:         {fund_data['meta'].get('nombre', 'N/A')}
 Gestora:       {fund_data['meta'].get('gestora', 'N/A')}
 Cualitativo:   {fund_data['meta']['extraccion_estado']['cualitativo']}
 Serie hist.:   {fund_data['meta']['extraccion_estado']['cuantitativo_serie']}
 Periodos:      {len(fund_data['cuantitativo']['serie_historica'])}
 Guardado en:   {output_path}
{'='*60}
""")
    
    return fund_data


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fund Analyzer — Extrae datos completos de un fondo español dado su ISIN"
    )
    parser.add_argument("isin", help="ISIN del fondo (ej: ES0112231008)")
    parser.add_argument("--gestora-web", help="URL web de la gestora (ej: https://www.avantagecapital.com)")
    parser.add_argument("--force", action="store_true", help="Forzar re-extracción aunque ya exista el JSON")
    
    args = parser.parse_args()
    
    isin = args.isin.upper().strip()
    
    if not isin.startswith("ES") or len(isin) != 12:
        print(f"[ERROR] ISIN inválido: {isin}. Debe empezar por ES y tener 12 caracteres.")
        sys.exit(1)
    
    run(isin, gestora_web=args.gestora_web, force=args.force)
