# Smoke test runbook — Validar la skill `analyst-cowork`

Procedimiento para validar la migración del analyst de API Anthropic a skill bajo Claude Max. Se ejecuta **manualmente por Rafa** cuando Claude Code haya completado Fase 1.5 y haya llegado al checkpoint de Fase 1.6.

**Objetivo**: confirmar que la skill produce un `output.json` estructuralmente equivalente al baseline `v1-api-stable` para los dos fondos de control:
- AVANTAGE FUND, FI (ES) → ISIN `ES0112231008`
- TROJAN Fund Ireland (INT) → ISIN `IE00B6T42S66`

Tiempo estimado total: 45-60 min (15-20 min por fondo + 5-10 min validación + comparación visual).

---

## Pre-requisitos antes de arrancar

Verifica con `cd` en la raíz del repo y ejecutando:

```
git status                                    # debes estar en branch v2-cowork
git tag --list | grep v1-api-stable          # tag debe existir
ls .claude/skills/analyst-cowork/            # SKILL.md y output_schema.example.json presentes
ls agents/bundle_exporter.py agents/bundle_validator.py  # ambos existen
ls analizar_fondo.bat                        # bat en raíz
ls tools/skill_output_validator.py           # validador presente
claude --version                             # claude CLI disponible
gh auth status                               # gh CLI autenticado (solo necesario en Fase 2)
```

Si falta algo, vuelve a Claude Code y completa la fase correspondiente antes de seguir.

---

## Test 1 — AVANTAGE FUND (ES) · ISIN ES0112231008

### Paso 1.1: Limpiar el output previo del fondo

Para que el smoke test mida la skill desde cero, borra el `analyst_synthesis` del output existente (los datos crudos se conservan):

```
python -c "import json; from pathlib import Path; p=Path('data/funds/ES0112231008/output.json'); d=json.loads(p.read_text(encoding='utf-8')); d.pop('analyst_synthesis', None); p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')"
```

Y también borra el `analyst_synthesis_cowork.json` si existiera de un run previo:

```
del data\funds\ES0112231008\analyst_synthesis_cowork.json 2>nul
```

### Paso 1.2: Lanzar el flujo end-to-end

```
analizar_fondo.bat ES0112231008
```

Esto dispara los 3 pasos. **Tiempo total esperado**: 15-20 min.

Durante la ejecución observa la consola. Hitos esperados:

- `Paso 1/3: Prep determinista` — debe acabar con `Bundle creado en data/funds/ES0112231008/bundle/`. Si tarda <2 min y no descarga nada, los datos crudos ya estaban en disco (esperado para Avantage).
- `Paso 2/3: Skill analyst en Claude Max (headless)` — verás la skill leyendo los inputs y generando las 8 secciones. Si el subagente Sonnet de audit detecta issues, hará una segunda pasada en máximo 3 secciones. Termina con un mensaje "Listo. Comando consume: python -m agents.orchestrator --isin ES0112231008 --consume-cowork".
- `Paso 3/3: Consume + dashboard` — debe acabar con `Dashboard: dashboard/fund-ES0112231008.html`. El bat lo abrirá automáticamente.

**Si el Paso 2 falla**: el bat hará fallback automático al analyst legacy con API. Eso significa que la skill no funcionó. NO continúes con el resto del test, vuelve a Claude Code y debugea.

### Paso 1.3: Validar estructuralmente

```
python -m tools.skill_output_validator ES0112231008 --output-report report_avantage.json
```

Lee la tabla impresa. Esperado:

| Categoría | Esperado |
|---|---|
| Hard checks | 12-14 pass / 0 fail |
| Soft checks | 18-22 pass / 0-2 warn |
| Anti-invención | 0-3 flags (la mayoría falsos positivos esperables por verbos en mayúscula) |
| Verdict | **pass** o **warn** |

Si **verdict = fail**: hay un fallo estructural grave. Revisa el detalle de qué hard check falló. Probables culpables:
- `kpi_*` falla → la skill tocó datos crudos, debería haber leído del bundle pero no editado fuera del `analyst_synthesis`.
- `top5_posiciones_match` < 4/5 → algo raro con la lectura de posiciones.
- `lead_manager_match` falla → la skill identificó un lead distinto al baseline. Revisa `manager_profile.json` en el bundle.

### Paso 1.4: Comparación visual del dashboard

Abre los dos dashboards en pestañas separadas:

```
start dashboard\fund-ES0112231008.html
git show v1-api-stable:dashboard/fund-ES0112231008.html > /tmp/baseline_avantage.html && start /tmp/baseline_avantage.html
```

Recorre las 8 secciones comparando:

| Sección | Qué confirmar |
|---|---|
| Resumen ejecutivo | AUM, gestora, fechas y rentabilidades coinciden. Tono narrativo equivalente. |
| Historia | Hitos cronológicos en el mismo orden. Cifras de cada hito iguales. |
| Gestores | Iván Martín / Juan Gómez Bada y demás identificados igual. Bios cubren mismo arco temporal. |
| Evolución | AUM histórico, rentabilidad anualizada coinciden con el baseline. |
| Estrategia | Fortalezas/riesgos cubren los mismos puntos clave (puede variar la redacción). |
| Cartera | Top 5-10 posiciones idénticas con mismos pesos (±0.5pp). |
| Fuentes externas | Mismas lecturas mencionadas. |
| Documentos | Mismas referencias listadas. |

**Tolerancia aceptable**: redacción narrativa diferente. **No aceptable**: cifras distintas, gestores nuevos no presentes en el baseline, citas inventadas.

### Paso 1.5: Diff JSON estructural

Para una segunda capa de check más mecánica:

```
git show v1-api-stable:data/funds/ES0112231008/output.json > /tmp/baseline_avantage.json
python -c "import json; a=json.load(open('/tmp/baseline_avantage.json',encoding='utf-8')); b=json.load(open('data/funds/ES0112231008/output.json',encoding='utf-8')); print('top-level diff:', set(a.keys())^set(b.keys())); print('analyst_synthesis sections diff:', set(a.get('analyst_synthesis',{}).keys())^set(b.get('analyst_synthesis',{}).keys())); print('kpis diff:', {k:(a['kpis'].get(k),b['kpis'].get(k)) for k in set(a['kpis'])|set(b['kpis']) if a['kpis'].get(k)!=b['kpis'].get(k)})"
```

Lo único esperable como diff es:
- `analyst_synthesis._meta` o campos de metadata del nuevo flow (`generator`, `cowork_runs`).
- `_manual_edits` actualizado con paths nuevos.
- Cero diff en `kpis`, `posiciones`, `cuantitativo`.

Si hay diffs en KPIs o posiciones → la skill o el consume están tocando datos que no deberían.

### Paso 1.6: Decisión Avantage

- ✅ Verdict pass + visual OK + diff JSON limpio → Avantage VALIDADO. Sigue con Trojan.
- ⚠️ Verdict warn + visual OK + diff JSON limpio → Avantage VALIDADO con notas. Documenta los warns en un comentario para Claude Code, sigue con Trojan.
- ❌ Verdict fail O visual con cifras distintas O diff JSON con cambios en datos crudos → PARA. Vuelve a Claude Code con el reporte y debug.

---

## Test 2 — TROJAN Fund Ireland (INT) · ISIN IE00B6T42S66

Repite los pasos 1.1 a 1.6 cambiando el ISIN en cada comando:

```
python -c "import json; from pathlib import Path; p=Path('data/funds/IE00B6T42S66/output.json'); d=json.loads(p.read_text(encoding='utf-8')); d.pop('analyst_synthesis', None); p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')"

del data\funds\IE00B6T42S66\analyst_synthesis_cowork.json 2>nul

analizar_fondo.bat IE00B6T42S66

python -m tools.skill_output_validator IE00B6T42S66 --output-report report_trojan.json

start dashboard\fund-IE00B6T42S66.html
git show v1-api-stable:dashboard/fund-IE00B6T42S66.html > /tmp/baseline_trojan.html && start /tmp/baseline_trojan.html
```

Particularidades de Trojan a vigilar:
- AUM debe estar en torno a **662 M€** (clase O EUR ACC del sub-fondo, NO el SICAV completo). Si aparece >€10B = bug DNCA umbrella resurgió, alerta crítica.
- Lead = Sebastian Lyon, Co = Francis Brooke. Si la skill identifica otros, fallo en lead_manager.
- Cartera con ~36 posiciones, primera posición típicamente oro/cash.
- 97 cartas detectadas previamente — esperado que el bundle traiga muchas y K15 estructurado.

**Decisión Trojan**: misma matriz que Avantage. Si pasa o queda en warn aceptable → migración Fase 1 VALIDADA. Da el OK a Claude Code para arrancar Fase 2.

---

## Cierre del smoke test

Cuando ambos fondos estén validados, dile a Claude Code algo así:

> Smoke test pasado en ambos fondos.
> Avantage: verdict {pass|warn} con {N} warns. Visual OK. Diff JSON limpio.
> Trojan: verdict {pass|warn} con {N} warns. AUM=662M€ (correcto, no umbrella). Visual OK.
> Procede con Fase 2.

Si algo no pasa, sé específico:

> Avantage: verdict fail. El check `kpi_aum_actual_meur` falla — baseline=151.6 actual=148.2 (diff 2.2%, fuera de tolerancia 2%). Revisa en el orchestrator si el consume está sobrescribiendo el AUM por error.

---

## Troubleshooting frecuente

### "claude command not found"

`claude` CLI no está en PATH. Ajusta el bat añadiendo:

```
set PATH=%PATH%;%LOCALAPPDATA%\Programs\Claude
```

al principio. Si la instalación de Claude Code está en otro path, ajusta.

### El paso 2 se cuelga >25 min sin acabar

La skill probablemente entró en un loop. Mata el proceso (Ctrl+C en la consola del bat). Mira `data/funds/{ISIN}/analyst_synthesis_cowork.json` — si existe parcial, la skill avanzó hasta cierta sección. Re-arranca con `analizar_fondo.bat {ISIN}` (la skill debería detectar el JSON parcial y reanudar; si no, bórralo y vuelve a empezar).

### El validador da "fail" en `top5_posiciones_match`

Mira los detalles. Si las 5 posiciones del baseline son DISTINTAS a las actuales (no solo en orden, sino en nombres), es porque el `analyst_synthesis.cartera.top_posiciones` se está poblando desde un sitio incorrecto. Las top posiciones deben venir de `posiciones.actuales` (top-level del schema), no inventadas por la skill. Revisa el SKILL.md sección Cartera.

### Anti-invención flagea muchas entidades en mayúscula

Es esperable algo de ruido — el regex pilla verbos en inicio de frase o títulos. Si el flag-count <10, es ruido. Si es >20, mira las entidades flageadas: si son nombres de personas o empresas no presentes en el bundle, ES un problema real (la skill inventa). Si son verbos como "Construimos", "Iniciando" → ignorar, son falsos positivos del regex.

### El diff JSON muestra cambios en `posiciones.actuales`

NO debería pasar. La skill solo escribe `analyst_synthesis.*`. Si hay diff en `posiciones.*` o `kpis.*`, el consume está mal. Revisa `_consume_cowork_analyst` en orchestrator y confirma que solo toca el path `analyst_synthesis`.

### "Permission denied" al hacer git push (al final de Fase 2)

Tu `gh auth` no tiene permisos de write en el repo. Corre `gh auth refresh -h github.com -s repo` para añadir scope `repo`.
