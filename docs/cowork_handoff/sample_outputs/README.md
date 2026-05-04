# Sample outputs — fondos validados como baseline

Estos 7 fondos forman la **batería de no-regresión** del proyecto. Se usan en `tests/test_es_no_regression.py` y `tests/test_int_no_regression.py` para garantizar que cambios futuros no degradan la calidad del output.

Para cowork: aquí está exactamente cómo es el resultado real del sistema. Sirve para:
- Ver el schema completo en uso (no solo el documentado en `CLAUDE.md`)
- Entender qué profundidad de análisis genera el sistema
- Comparar con la salida de una replicación alternativa (gratis o no)

---

## Inventario

### Pipeline ES (4 fondos — CNMV)

| ISIN | Fondo | Gestora | Output JSON | Dashboard HTML |
|---|---|---|---|---|
| `ES0112231008` | Avantage Fund FI | Renta 4 (ex-Avantage Capital) | 802 KB | 178 KB |
| `ES0156572002` | MyInvestor Cartera Permanente FI | Andbank Wealth Management | 1.7 MB | 176 KB |
| `ES0175316001` | Dunas Valor Flexible FI | Dunas Capital Asset Mgmt | 4.0 MB | 215 KB |
| `ES0116567035` | Cartesio X FI | Cartesio Inversiones | 3.8 MB | 229 KB |

### Pipeline INT (3 fondos — regulator router + extractor v3)

| ISIN | Fondo | Gestora / Domicilio | Output JSON | Dashboard HTML |
|---|---|---|---|---|
| `IE00B6T42S66` | Trojan Fund (Ireland) O EUR ACC | Troy Asset Management / IE | 1.8 MB | 233 KB |
| `IE00BF5GGB04` | GAM Star Cat Bond | GAM / IE | 217 KB | 233 KB |
| `LU1694789378` | DNCA INVEST - Alpha Bonds | DNCA Investments / LU | 434 KB | 182 KB |

---

## Cómo usarlo

### 1. Inspeccionar un output.json

```bash
# Top-level keys
python -c "import json; d=json.load(open('json/ES_AvantageFund_RentaCuatro_ES0112231008.output.json')); print(list(d.keys()))"

# Equipo gestor identificado por el sistema
python -c "import json; d=json.load(open('json/ES_AvantageFund_RentaCuatro_ES0112231008.output.json')); print(d.get('analyst_synthesis',{}).get('gestores',{}).get('perfiles'))"

# Schema completo formateado
jq '.' json/ES_AvantageFund_RentaCuatro_ES0112231008.output.json | less
```

### 2. Abrir un dashboard en el navegador

Los dashboards son HTML autocontenidos (sin servidor, sin deps externas — incluyen CSS + JS + datos embebidos). Abre cualquiera con doble click o:

```bash
# Windows
start dashboards/ES_AvantageFund_RentaCuatro_ES0112231008.dashboard.html

# macOS
open dashboards/ES_AvantageFund_RentaCuatro_ES0112231008.dashboard.html

# Linux
xdg-open dashboards/ES_AvantageFund_RentaCuatro_ES0112231008.dashboard.html
```

---

## Por qué estos 7 y no otros

Cubren la diversidad funcional del sistema:

- **Avantage / Cartesio / MyInvestor / Dunas**: cuatro tipos distintos de fondos ES (renta variable, mixto, retorno absoluto, cartera permanente). Cubren todas las ramas del extractor CNMV.
- **Trojan**: caso INT canónico (UCITS irlandés, fondo conservador con histórico largo, gestores estables). Validador del extractor INT v3 + discovery_v2.
- **GAM Star Cat Bond**: caso INT con clase de activo nicho (catastrophe bonds — reaseguro). Validador de discovery + readings con sources especializadas.
- **DNCA Alpha Bonds**: caso INT del paraguas SICAV. Validador del fix Fase E (`_merge_share_classes` no debe agregar AUM del paraguas).

Si una replicación pasa los tests baseline en estos 7, cubre ~90% de los casos de uso reales.

---

## Caveats

1. **Datos a fecha de generación**: cada output.json refleja el estado del fondo al momento de la última ejecución (Abril-Mayo 2026). AUM, partícipes, cartera, gestores pueden haber cambiado desde entonces.

2. **Información pública**: todos los datos provienen de fuentes públicas (CNMV, web gestoras, prensa, Wayback Machine). No hay información confidencial ni datos de inversores.

3. **Quality variable entre fondos**: algunos fondos pequeños (ej. Cartesio X) tienen menos cobertura de prensa especializada → secciones `analyst_synthesis.fuentes_externas` más cortas. Es esperado, no un bug.

4. **Manual edits aplicados**: algunos fondos tienen `_manual_edits` para corregir datos donde el extractor automático fallaba (ej. nombre del fondo tras transferencia gestora). Listados en cada output.json.

---

## Para validar una replicación alternativa

Si replicas el sistema con stack diferente (ej. Llama 3 + Brave Search + Ollama), el output mínimo aceptable debe tener:

- ✅ `nombre`, `gestora`, `isin`, `tipo`, `kpis.aum_actual_meur`, `kpis.num_participes`
- ✅ `posiciones.actuales` con ≥ 5 entries (top holdings)
- ✅ `cuantitativo.serie_aum` con ≥ 3 entries (histórico)
- ✅ `analyst_synthesis.resumen.texto` > 500 chars
- ✅ `analyst_synthesis.gestores.perfiles` con ≥ 1 entry con `cv_bullets` o `trayectoria` no vacíos
- ✅ `analyst_synthesis.cartera.texto` > 200 chars

Si tu replicación produce esto para Avantage (ES) + Trojan (INT), está al nivel mínimo viable.
