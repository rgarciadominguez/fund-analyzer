"""Tests Fase L (2026-04-29): valida fixes L1-L4 con fixtures (sin LLM real).

Cubre los 3 bugs reales descubiertos post-validación AZ Valor:
- L1: analyst._filter_lecturas lee `analisis_completos`/`otros_readings` (schema actual)
      en lugar de `analisis`/`lecturas` (legacy). Sintetiza texto desde campos
      estructurados (resumen + opinión + puntos_clave + citas).
- L2: analyst._filter_letters acepta cartas con texto_completo vacío si tienen
      campos K15 (tesis_gestora, decisiones_tomadas, contexto_mercado, citas).
- L4: analyst._classify_hecho_evento clasifica heurísticamente eventos cuando
      CNMV no aporta epígrafe estructurado.

(L3 fue falsa alarma — `tipo` es el campo correcto, no `tipo_activo`.)

Run: python tests/test_fase_l_fixes.py
"""
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

failures = []


def assert_equal(actual, expected, msg):
    if actual != expected:
        failures.append(f"FAIL {msg}: expected={expected!r}, actual={actual!r}")
        return False
    print(f"  PASS {msg}")
    return True


def assert_ge(actual, threshold, msg):
    if actual < threshold:
        failures.append(f"FAIL {msg}: {actual} < {threshold}")
        return False
    print(f"  PASS {msg} ({actual} >= {threshold})")
    return True


# ── L1: _filter_lecturas lee schema actual + sintetiza texto ───────────────

def test_l1_filter_lecturas_schema_actual():
    print("\n[L1] analyst._filter_lecturas con schema READING_SCHEMA actual:")
    from agents.analyst_agent import AnalystAgent

    agent = AnalystAgent.__new__(AnalystAgent)
    agent._log = lambda lvl, msg: None
    agent._truncate = lambda t, n: (t or "")[:n]

    # readings_data.json shape post-K22 (analisis_completos + otros_readings)
    readings = {
        "analisis_completos": [
            {
                "url": "https://saludfinanciera.substack.com/p/x",
                "source": "Salud Financiera",
                "source_type": "analisis_completo",
                "titulo": "FDV #18",
                "fecha": "2025-02-05",
                "tipo": "resena",
                "resumen": "Caso de estudio inversor 51 años con AzValor Managers 5%.",
                "opinion_sobre_fondo": "Neutral. Lo propone como exposición a emergentes.",
                "puntos_clave": [
                    "AzValor Managers 5% en cartera",
                    "Cubre exposición a China",
                ],
                "citas_relevantes": ["AzValor Managers: 5%"],
                "datos_mencionados": {"rentabilidad": None},
            }
        ],
        "otros_readings": [
            {
                "url": "https://example.com/podcast",
                "source": "YouTube",
                "tipo": "podcast",
                "titulo": "Entrevista azValor",
                "resumen": "Entrevista con Álvaro Guzmán",
                "opinion_sobre_fondo": "Positiva",
            }
        ],
    }
    filtered = agent._filter_lecturas(readings)
    assert_equal(len(filtered["analisis_escritos"]), 1,
                 "1 análisis transferido desde analisis_completos")
    assert_equal(len(filtered["multimedia"]), 1,
                 "1 multimedia transferido (tipo=podcast)")
    a0 = filtered["analisis_escritos"][0]
    assert_ge(len(a0["texto_completo"]), 100,
              "texto_completo sintetizado >100c desde campos K15")
    assert_in_text("AzValor Managers", a0["texto_completo"],
                   "texto contiene contenido de resumen")
    assert_in_text("Salud Financiera", a0["fuente"] or "",
                   "fuente leída desde 'source' (READING_SCHEMA)")


def assert_in_text(needle, haystack, msg):
    if needle.lower() not in haystack.lower():
        failures.append(f"FAIL {msg}: {needle!r} NOT in {haystack[:80]!r}...")
        return False
    print(f"  PASS {msg}")
    return True


# ── L2: _filter_letters sintetiza texto desde K15 ──────────────────────────

def test_l2_filter_letters_sintesis_k15():
    print("\n[L2] analyst._filter_letters sintetiza texto desde campos K15:")
    from agents.analyst_agent import AnalystAgent

    agent = AnalystAgent.__new__(AnalystAgent)
    agent._log = lambda lvl, msg: None

    letters = {
        "cartas": [
            # Carta sin texto_completo pero con campos K15 ricos
            {
                "periodo": "2024-Q4",
                "url_fuente": "https://x.com/a.pdf",
                "tipo": "trimestral",
                "texto_completo": "",
                "tesis_gestora": "Apostamos por value europeo en sectores cíclicos.",
                "decisiones_tomadas": "Aumento de Técnicas Reunidas y Repsol.",
                "contexto_mercado": "Mercado volátil tras subidas de tipos.",
                "citas_textuales": ["No es momento de vender"],
            },
            # Carta antigua sin texto_completo y sin K15 → debe SKIP
            {
                "periodo": "2010-Q1",
                "url_fuente": "https://y.com/old.pdf",
                "tipo": "trimestral",
                "texto_completo": "",
            },
        ]
    }
    filtered = agent._filter_letters(letters)
    cpa = filtered["cartas_por_anio"]
    assert_in("2024", list(cpa.keys()), "2024 presente")
    # 2024 debe tener texto_primario sintetizado
    tp_2024 = cpa.get("2024", {}).get("texto_primario", "")
    assert_ge(len(tp_2024), 100, "texto_primario 2024 sintetizado >100c")
    assert_in_text("TESIS GESTORA", tp_2024, "texto contiene TESIS GESTORA")
    assert_in_text("DECISIONES", tp_2024, "texto contiene DECISIONES")


def assert_in(item, container, msg):
    if item not in container:
        failures.append(f"FAIL {msg}: {item!r} not in {container!r}")
        return False
    print(f"  PASS {msg}")
    return True


# ── L4: _classify_hecho_evento heurística ──────────────────────────────────

def test_l4_classify_hecho_evento():
    print("\n[L4] analyst._classify_hecho_evento clasifica eventos por keyword:")
    from agents.analyst_agent import AnalystAgent
    agent = AnalystAgent.__new__(AnalystAgent)

    # Casos reales AZ Valor + casos canónicos
    cases = [
        ("J. La CNMV ha resuelto: Verificar y registrar... actualización del folleto",
         "Modificación de folleto"),
        ("Se comunica el nombramiento del nuevo auditor de la IIC",
         "Cambio de auditor"),
        ("Incorporar al Registro... acuerdo de delegación de la gestión de inversiones",
         "Delegación de gestión"),
        ("Recuperación de las retenciones practicadas en el extranjero",
         "Recuperación de retenciones"),
        ("Se cambia el depositario del fondo de Cecabank a BNP",
         "Cambio de depositario"),
        ("Suspensión temporal de suscripciones por exceso de demanda",
         "Suspensión de operaciones"),
        ("Liquidación parcial de la clase A",
         "Liquidación de fondo"),
        ("Aumento de la comisión de gestión",
         "Modificación de comisiones"),
        ("Reparto de dividendos del semestre",
         "Reparto de dividendos"),
        ("Mero anuncio sin contenido reconocible",
         "Otro hecho relevante"),
    ]
    ok = 0
    for det, exp in cases:
        got = agent._classify_hecho_evento(det)
        if got == exp:
            ok += 1
            print(f"  PASS '{det[:40]}...' -> {exp!r}")
        else:
            failures.append(f"FAIL classify: got {got!r}, expected {exp!r} for '{det[:40]}...'")
    assert_equal(ok, len(cases), f"clasificación correcta {ok}/{len(cases)}")


def main():
    print("=" * 60)
    print("Tests Fase L (fixes post-validación AZ Valor)")
    print("=" * 60)
    test_l1_filter_lecturas_schema_actual()
    test_l2_filter_letters_sintesis_k15()
    test_l4_classify_hecho_evento()
    print("\n" + "=" * 60)
    if failures:
        print(f"FAIL: {len(failures)} fallos")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    print("PASS: todos los tests Fase L OK")


if __name__ == "__main__":
    main()
