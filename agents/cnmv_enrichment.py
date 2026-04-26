"""
CNMV Enrichment Agent — fixes systemic gaps en cnmv_data.json sin re-descargar.

Resuelve fallos detectados por dashboard_quality_agent que el cnmv_agent
no rellena por sí solo:
- cartera_posiciones_sectores: infiere sector via Gemini Flash (batch)
- serie_rentabilidad vacía: deriva de serie_vl_base100 (sin LLM)
- mix_activos_no_over_100: normaliza si suma > 100% por error de extracción

Uso:
    from agents.cnmv_enrichment import CNMVEnricher
    enricher = CNMVEnricher(isin, quality_feedback=fallos)
    enricher.run()

NO modifica cnmv_agent.py (LOCKED). Aditivo puro.
"""
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent

# Sectores GICS estándar — restrict LLM output a este set
SECTORES_VALIDOS = [
    "Financials", "Energy", "Industrials", "Consumer Discretionary",
    "Consumer Staples", "Health Care", "Information Technology",
    "Communication Services", "Materials", "Real Estate", "Utilities",
    "Government",  # bonos soberanos
    "Supranational",  # bonos supranacionales (BEI, etc.)
    "Cash",  # liquidez/depositos
    "Other",
]


def _log(isin, level, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{isin}] [ENRICH] [{level}] {msg}")


class CNMVEnricher:
    """Enriquece cnmv_data.json para resolver fallos del quality_agent."""

    def __init__(self, isin: str, quality_feedback: list = None):
        self.isin = isin.strip().upper()
        self.feedback = quality_feedback or []
        self.fund_dir = ROOT / "data" / "funds" / self.isin
        self.cnmv_path = self.fund_dir / "cnmv_data.json"
        self.output_path = self.fund_dir / "output.json"
        self.changes = []

    def _load_cnmv(self) -> dict:
        if not self.cnmv_path.exists():
            return None
        return json.loads(self.cnmv_path.read_text(encoding="utf-8"))

    def _save_cnmv(self, data: dict):
        # Backup
        bak = self.fund_dir / "cnmv_data.pre_enrichment.json"
        if not bak.exists():
            self.cnmv_path.replace(bak) if False else None
            import shutil
            shutil.copy(self.cnmv_path, bak)
        with open(self.cnmv_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _has_fallo(self, regla_id: str) -> bool:
        return any(f.get("regla_id") == regla_id for f in self.feedback)

    # ── Fix 1: serie_rentabilidad ────────────────────────────────────────
    def _enrich_serie_rentabilidad(self, data: dict) -> bool:
        """Calcula rentabilidad anual desde serie_vl_base100."""
        cuant = data.setdefault("cuantitativo", {})
        if cuant.get("serie_rentabilidad"):
            return False

        vl_series = cuant.get("serie_vl_base100") or []
        if len(vl_series) < 2:
            _log(self.isin, "INFO", f"serie_vl_base100 insuficiente ({len(vl_series)} puntos), skip rentabilidad")
            return False

        # Ordenar por periodo
        vl_sorted = sorted(
            [v for v in vl_series if isinstance(v, dict) and v.get("vl")],
            key=lambda v: str(v.get("periodo", "")),
        )

        rents = []
        for i in range(1, len(vl_sorted)):
            prev = vl_sorted[i - 1]
            curr = vl_sorted[i]
            try:
                prev_vl = float(prev["vl"])
                curr_vl = float(curr["vl"])
                if prev_vl > 0:
                    rent_pct = (curr_vl / prev_vl - 1) * 100
                    rents.append({
                        "periodo": str(curr.get("periodo", "")),
                        "rentabilidad_pct": round(rent_pct, 2),
                        "fuente": "calculado_desde_vl_base100",
                    })
            except (TypeError, ValueError):
                continue

        if rents:
            cuant["serie_rentabilidad"] = rents
            self.changes.append(f"serie_rentabilidad: {len(rents)} puntos calculados desde VL")
            return True
        return False

    # ── Fix 2: posiciones sin sector ─────────────────────────────────────
    def _enrich_sectores(self, data: dict) -> bool:
        """Infiere sector para cada posicion sin sector usando Gemini Flash batch."""
        pos_actuales = (data.get("posiciones") or {}).get("actuales") or []
        sin_sector = [p for p in pos_actuales if not (p.get("sector") or "").strip()]
        if not sin_sector:
            return False

        # Skip si Gemini no disponible
        gemini_key = os.environ.get("GOOGLE_API_KEY")
        if not gemini_key:
            _log(self.isin, "WARN", "GOOGLE_API_KEY no disponible, skip sector inference")
            return False

        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
        except Exception as exc:
            _log(self.isin, "WARN", f"Gemini init failed: {exc}")
            return False

        _log(self.isin, "INFO", f"Inferiendo sector para {len(sin_sector)} posiciones (Gemini Flash batch)")

        # Batch en grupos de 40
        BATCH = 40
        sectores_validos_str = ", ".join(SECTORES_VALIDOS)
        cnt_done = 0
        for i in range(0, len(sin_sector), BATCH):
            chunk = sin_sector[i:i + BATCH]
            items_text = "\n".join(
                f"{j+1}. nombre={p.get('nombre','?')[:60]} | tipo={p.get('tipo','?')} | pais={p.get('pais','?')}"
                for j, p in enumerate(chunk)
            )

            prompt = f"""Para cada posicion abajo, asigna UN sector de esta lista exacta:
{sectores_validos_str}

REGLAS:
- BONO de empresa privada → sector de la empresa (ej. SANTANDER → Financials, IBERDROLA → Utilities)
- BONO/LETRA/OBLIGACION del Tesoro de un pais → Government
- BONO de organismo supranacional (BEI, BCE, etc) → Supranational
- ACCION → sector GICS de la empresa
- IIC/FONDO/ETF → Other (no inferir el sector subyacente)
- Repos, deposito, cuenta, liquidez → Cash

POSICIONES:
{items_text}

Responde SOLO un JSON array: [{{"i": 1, "sector": "..."}}, ...]
Sin texto adicional."""

            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                )
                text = resp.text.strip()
                # Strip markdown
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()
                results = json.loads(text)

                for r in results:
                    idx = r.get("i", 0) - 1
                    sector = (r.get("sector") or "").strip()
                    if 0 <= idx < len(chunk) and sector in SECTORES_VALIDOS:
                        chunk[idx]["sector"] = sector
                        cnt_done += 1

                _log(self.isin, "OK", f"Batch {i//BATCH + 1}: {len(results)} sectores")
                time.sleep(0.5)  # rate limit cortesia
            except Exception as exc:
                _log(self.isin, "ERROR", f"Batch {i//BATCH + 1} fallo: {str(exc)[:100]}")

        if cnt_done > 0:
            self.changes.append(f"sectores: {cnt_done}/{len(sin_sector)} posiciones enriquecidas")
            return True
        return False

    # ── Fix 3: mix activos > 100% ────────────────────────────────────────
    def _normalize_mix_over_100(self, data: dict) -> bool:
        """Si mix activos suma > 110%, normaliza a 100%."""
        cuant = data.setdefault("cuantitativo", {})
        mix_hist = cuant.get("mix_activos_historico") or []
        changed = False
        for m in mix_hist:
            if not isinstance(m, dict):
                continue
            keys_pct = [k for k in m.keys() if k.endswith("_pct")]
            total = sum(float(m.get(k, 0) or 0) for k in keys_pct)
            if total > 110:
                # Normalizar
                factor = 100.0 / total
                for k in keys_pct:
                    m[k] = round(float(m.get(k, 0) or 0) * factor, 2)
                changed = True
        if changed:
            self.changes.append(f"mix_activos: normalizado periodos con suma > 110%")
        return changed

    def _sync_to_output(self, cnmv_data: dict):
        """Propaga cambios de cnmv_data.json a output.json (campos: posiciones,
        serie_rentabilidad, mix_activos_historico). NO sobreescribe synthesis."""
        if not self.output_path.exists():
            _log(self.isin, "WARN", "output.json no existe, skip sync")
            return
        with open(self.output_path, encoding="utf-8") as f:
            out = json.load(f)

        # Backup
        bak = self.fund_dir / "output.pre_enrichment.json"
        if not bak.exists():
            import shutil
            shutil.copy(self.output_path, bak)

        # Sync posiciones
        cnmv_pos = (cnmv_data.get("posiciones") or {}).get("actuales") or []
        if cnmv_pos:
            out_pos = out.setdefault("posiciones", {})
            out_pos["actuales"] = cnmv_pos

        # Sync serie_rentabilidad y mix_activos_historico
        cnmv_cuant = cnmv_data.get("cuantitativo") or {}
        out_cuant = out.setdefault("cuantitativo", {})
        if cnmv_cuant.get("serie_rentabilidad"):
            out_cuant["serie_rentabilidad"] = cnmv_cuant["serie_rentabilidad"]
        if cnmv_cuant.get("mix_activos_historico"):
            out_cuant["mix_activos_historico"] = cnmv_cuant["mix_activos_historico"]

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        _log(self.isin, "OK", "output.json sincronizado con cnmv_data enriquecido")

    # ── Run ──────────────────────────────────────────────────────────────
    def run(self) -> dict:
        data = self._load_cnmv()
        if not data:
            _log(self.isin, "ERROR", f"No existe {self.cnmv_path}")
            return {}

        _log(self.isin, "START", f"Enrichment con {len(self.feedback)} fallos en feedback")

        # Aplicar fixes (siempre intentar serie_rentabilidad, mix; sectores solo si feedback lo marca)
        any_change = False
        any_change |= self._enrich_serie_rentabilidad(data)
        any_change |= self._normalize_mix_over_100(data)

        if not self.feedback or self._has_fallo("cartera_posiciones_sectores"):
            any_change |= self._enrich_sectores(data)

        if any_change:
            self._save_cnmv(data)
            self._sync_to_output(data)
            _log(self.isin, "DONE", f"Cambios: {'; '.join(self.changes)}")
        else:
            _log(self.isin, "DONE", "Sin cambios aplicados")

        return {"changes": self.changes, "n_changes": len(self.changes)}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m agents.cnmv_enrichment <ISIN>")
        sys.exit(1)
    # Load .env
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    for isin in sys.argv[1:]:
        enricher = CNMVEnricher(isin.strip().upper())
        enricher.run()
        print()
