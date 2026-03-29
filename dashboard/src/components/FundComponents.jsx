// FundStrategy.jsx
export function FundStrategy({ data }) {
  const { estrategia, cuantitativo } = data;
  const rv_narrativa = cuantitativo.exposicion_rv_narrativa || [];

  return (
    <div className="space-y-6">
      {/* Filosofía */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-2">Filosofía de inversión</h3>
        <p className="text-sm text-gray-600 leading-relaxed">{estrategia.filosofia?.texto}</p>
      </div>

      {/* Proceso */}
      {estrategia.proceso && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-2">Proceso de inversión</h3>
          <p className="text-sm text-gray-600 leading-relaxed">{estrategia.proceso}</p>
        </div>
      )}

      {/* Visión actual */}
      {estrategia.vision_actual && (
        <div className="bg-blue-50 rounded-lg border border-blue-200 p-5">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-semibold text-blue-800">Visión actual del gestor</h3>
            {estrategia.vision_actual.fecha && (
              <span className="text-xs text-blue-500">{estrategia.vision_actual.fecha}</span>
            )}
          </div>
          <p className="text-sm text-blue-800 leading-relaxed">{estrategia.vision_actual.texto}</p>
          {estrategia.vision_actual.fuente && (
            <a
              href={estrategia.vision_actual.fuente.pdf || estrategia.vision_actual.fuente.url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-blue-600 hover:underline mt-2 inline-block"
            >
              → {estrategia.vision_actual.fuente.documento}
            </a>
          )}
        </div>
      )}

      {/* Exposición RV narrativa */}
      {rv_narrativa.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Evolución exposición RV (narrativa)</h3>
          <div className="space-y-3">
            {rv_narrativa.map((item, i) => (
              <div key={i} className="flex items-start gap-4 text-sm">
                <div className="w-20 text-gray-500 text-xs shrink-0 mt-0.5">{item.periodo}</div>
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    {item.rv_pct_aprox !== undefined && (
                      <span className="font-semibold text-blue-700">RV {item.rv_pct_aprox}%</span>
                    )}
                    {item.geografia_esp_pct_aprox !== undefined && (
                      <span className="text-gray-500">· España ~{item.geografia_esp_pct_aprox}%</span>
                    )}
                  </div>
                  {item.nota && <p className="text-xs text-gray-500 mt-0.5">{item.nota}</p>}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// FundTimeline.jsx
export function FundTimeline({ data }) {
  const { estrategia, cambios_relevantes } = data;

  const badgeColor = {
    "tesis":      "bg-red-100 text-red-700",
    "estructura": "bg-purple-100 text-purple-700",
    "equipo":     "bg-amber-100 text-amber-700"
  };

  return (
    <div className="space-y-4">
      {/* Cambios relevantes */}
      {cambios_relevantes.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Cambios relevantes</h3>
          <div className="space-y-4">
            {cambios_relevantes.map((c, i) => (
              <div key={i} className="flex gap-3">
                <div className="w-20 text-xs text-gray-400 shrink-0 mt-0.5">{c.fecha}</div>
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${badgeColor[c.tipo] || "bg-gray-100 text-gray-600"}`}>
                      {c.tipo}
                    </span>
                    <span className="text-xs text-gray-400">{c.confianza}</span>
                  </div>
                  <p className="text-sm font-medium text-gray-900">{c.descripcion}</p>
                  <p className="text-xs text-gray-500 mt-0.5">{c.impacto}</p>
                  {c.fuente?.pdf && (
                    <a href={c.fuente.pdf} target="_blank" rel="noopener noreferrer" className="text-xs text-blue-500 hover:underline">→ Fuente</a>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Evolución por periodo */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-4">Hitos por periodo</h3>
        <div className="relative">
          <div className="absolute left-20 top-0 bottom-0 w-px bg-gray-200" />
          <div className="space-y-6">
            {estrategia.evolucion_por_periodo.map((e, i) => (
              <div key={i} className="flex gap-4 relative">
                <div className="w-20 text-xs text-gray-400 shrink-0 text-right pt-0.5">{e.periodo}</div>
                <div className="w-3 h-3 rounded-full bg-blue-500 border-2 border-white shadow shrink-0 mt-0.5 z-10" />
                <div className="flex-1 pb-2">
                  <p className="text-sm font-semibold text-gray-900">{e.hito}</p>
                  <p className="text-xs text-gray-500 mt-1 leading-relaxed">{e.detalle}</p>
                  {e.fuente?.pdf && (
                    <a href={e.fuente.pdf} target="_blank" rel="noopener noreferrer" className="text-xs text-blue-500 hover:underline mt-1 inline-block">
                      → {e.fuente.documento || "Fuente"}
                    </a>
                  )}
                  {e.fuente?.fragmento && (
                    <blockquote className="mt-2 border-l-2 border-gray-200 pl-3 text-xs text-gray-400 italic">
                      "{e.fuente.fragmento}"
                    </blockquote>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// FundSelector.jsx
export function FundSelector({ funds, onSelect, selected }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center gap-3">
        <label className="text-sm font-medium text-gray-700 shrink-0">Fondo:</label>
        <select
          className="flex-1 text-sm border border-gray-300 rounded-md px-3 py-1.5 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
          value={selected?.meta?.isin || ""}
          onChange={e => onSelect(e.target.value)}
        >
          <option value="">— Seleccionar fondo —</option>
          {funds.map(f => (
            <option key={f.isin} value={f.isin}>
              {f.isin} · {f.nombre || f.isin}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}

export default FundStrategy;
