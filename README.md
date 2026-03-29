# Fund Analyzer

Herramienta de análisis cualitativo y cuantitativo de fondos de inversión españoles a partir de su ISIN.

## Uso rápido

```bash
# Instalar dependencias Python
pip install -r requirements.txt

# Analizar un fondo (la primera vez tarda ~2-3 minutos)
python extractor.py ES0112231008

# Con URL de gestora explícita (más rápido)
python extractor.py ES0112231008 --gestora-web https://www.avantagecapital.com

# Re-extraer aunque ya exista el JSON
python extractor.py ES0112231008 --force
```

## Lo que extrae automáticamente

### Fuente A — Cartas semestrales de la gestora (cualitativo)
- Filosofía de inversión
- Evolución de estrategia por periodo
- Cambios de cartera documentados
- Visión actual del gestor
- Exposición RV/RF/liquidez narrativa

### Fuente B — XMLs CNMV (cuantitativo)
- AUM por semestre desde creación
- Número de partícipes
- TER (ratio de gastos)
- Exposición exacta RV/RF/liquidez
- Rentabilidades semestrales

## Dashboard

```bash
# Terminal 1: servidor de datos
python serve.py

# Terminal 2: dashboard React
cd dashboard
npm install
npm run dev
```

Abre http://localhost:5173

## Estructura

```
fund-analyzer/
  extractor.py          # CLI principal: python extractor.py [ISIN]
  serve.py              # Servidor local de datos para el dashboard
  requirements.txt

  sources/
    cnmv_meta.py        # Resuelve ISIN → NIF y metadatos CNMV
    cnmv_xml.py         # Descarga XMLs CNMV → serie histórica cuantitativa
    cartas.py           # Extrae cartas gestora → cualitativo via Claude API

  schema/
    fund.schema.json    # Schema JSON canónico (validación)

  data/
    ES0112231008.json   # Avantage Fund FI
    *.json              # Un fichero por fondo

  dashboard/
    src/
      App.jsx
      components/
    package.json
    vite.config.js
```

## Variables de entorno

Crea un fichero `.env` en la raíz:

```
ANTHROPIC_API_KEY=sk-ant-...
```

## Añadir un fondo nuevo

```bash
python extractor.py ES0173985005
# → Detecta gestora automáticamente
# → Descarga XMLs CNMV desde año de creación
# → Extrae cartas semestrales
# → Genera data/ES0173985005.json
# → El dashboard lo muestra automáticamente
```

## Estado de extracción

Cada JSON tiene un campo `extraccion_estado` con tres valores posibles:
- `"completo"` — datos completos disponibles
- `"parcial"` — datos parciales (ej: solo últimos años)
- `"pendiente"` — pendiente de extracción

## Fondos incluidos

| ISIN | Nombre | Estado cualitativo | Estado cuantitativo |
|------|--------|-------------------|---------------------|
| ES0112231008 | Avantage Fund FI | ✓ completo | ⏳ pendiente XML CNMV |

## Notas técnicas

- Los XMLs CNMV se descargan directamente (no requieren Chrome ni JavaScript)
- Las cartas se extraen via Claude API (requiere `ANTHROPIC_API_KEY`)
- El dashboard es React + Recharts + Tailwind, deploy directo en Vercel
- Merge inteligente: re-ejecutar no sobreescribe datos ya extraídos

## Deploy en Vercel

1. Push a GitHub
2. Conecta el repo en vercel.com
3. Build command: `cd dashboard && npm install && npm run build`
4. Output directory: `dist`
5. Los JSONs de `/data` se sirven como archivos estáticos
