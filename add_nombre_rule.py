"""Add the nombre_match_pdf rule to quality_rules.json (idempotent)."""
import json
from pathlib import Path

ROOT = Path(__file__).parent
RULES_PATH = ROOT / "data" / "quality_rules.json"

with open(RULES_PATH, encoding="utf-8") as f:
    qr = json.load(f)

new_rule = {
    "id": "nombre_match_latest_pdf",
    "section": "header",
    "check_type": "nombre_match_pdf",
    "field_path": "nombre",
    "fail_type": "estructura",
    "agente_responsable": "cnmv_agent",
    "problema_template": "Nombre del fondo en output ('{nombre_output}') no coincide con el ultimo PDF semestral CNMV ('{nombre_pdf}', archivo {pdf_file}). El cnmv_agent guardo nombre antiguo del XML en lugar del actual.",
    "accion_template": "Actualizar output.json campo 'nombre' con el valor real del PDF mas reciente. cnmv_agent debe leer el nombre de la primera pagina del ultimo informe semestral, no de XMLs antiguos."
}

# Add 'header' to sections list if not present
if "header" not in qr.get("sections", []):
    qr.setdefault("sections", []).append("header")
    print("Added 'header' to sections")

# Avoid duplicates
existing_ids = {r.get("id") for r in qr.get("rules", [])}
if new_rule["id"] in existing_ids:
    print(f"Rule {new_rule['id']} already exists, skipping")
else:
    qr.setdefault("rules", []).append(new_rule)
    print(f"Added rule {new_rule['id']}")

with open(RULES_PATH, "w", encoding="utf-8") as f:
    json.dump(qr, f, indent=2, ensure_ascii=False)

print(f"Total rules now: {len(qr.get('rules', []))}")
