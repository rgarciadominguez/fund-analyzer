export default function FundOverview({ data }) {
  const { ficha, cuantitativo, meta } = data;
  const snap = cuantitativo.snapshot_actual;

  const kpis = [
    { label: "AUM", value: snap.aum_meur ? `${snap.aum_meur}M€` : "—" },
    { label: "Partícipes", value: snap.participes?.toLocaleString("es-ES") ?? "—" },
    { label: "CAGR inicio", value: snap.cagr_desde_inicio_pct ? `${snap.cagr_desde_inicio_pct}%` : "—" },
    { label: "Acumulado", value: snap.rentabilidad_acumulada_pct ? `+${snap.rentabilidad_acumulada_pct}%` : "—" },
    { label: "Morningstar", value: snap.morningstar_estrellas ? "★".repeat(snap.morningstar_estrellas) : "—" },
    { label: "Fecha datos", value: snap.fecha ?? "—" },
  ];

  const rentabilidades = cuantitativo.rentabilidades_anuales.filter(r => r.pct !== null);

  return (
    <div className="space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-3 gap-4">
        {kpis.map(k => (
          <div key={k.label} className="bg-white rounded-lg border border-gray-200 p-4">
            <div className="text-xs text-gray-500 mb-1">{k.label}</div>
            <div className="text-xl font-semibold text-gray-900">{k.value}</div>
          </div>
        ))}
      </div>

      {/* Descripción */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-2">Descripción</h3>
        <p className="text-sm text-gray-600 leading-relaxed">{ficha.descripcion}</p>
        <div className="mt-3 flex flex-wrap gap-2">
          {ficha.tipo && <span className="bg-blue-50 text-blue-700 text-xs px-2 py-1 rounded">{ficha.tipo}</span>}
          {ficha.sfdr && <span className="bg-purple-50 text-purple-700 text-xs px-2 py-1 rounded">{ficha.sfdr}</span>}
        </div>
      </div>

      {/* Personas clave */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Personas clave</h3>
        <div className="space-y-3">
          {ficha.personas_clave.map((p, i) => (
            <div key={i} className="flex items-start gap-3">
              <div className="w-8 h-8 bg-gray-100 rounded-full flex items-center justify-center text-sm font-medium text-gray-600">
                {p.nombre.charAt(0)}
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900">{p.nombre}</div>
                <div className="text-xs text-gray-500">{p.rol}</div>
                {p.desde && <div className="text-xs text-gray-400">Desde {p.desde}</div>}
                {p.compromiso_patrimonial && (
                  <div className="text-xs text-green-600 mt-1">💚 {p.compromiso_patrimonial}</div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Rentabilidades anuales */}
      {rentabilidades.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Rentabilidades anuales</h3>
          <div className="flex gap-2 flex-wrap">
            {rentabilidades.map(r => (
              <div key={r.año} className="text-center">
                <div className="text-xs text-gray-500">{r.año}</div>
                <div className={`text-sm font-semibold ${r.pct >= 0 ? "text-green-600" : "text-red-600"}`}>
                  {r.pct > 0 ? "+" : ""}{r.pct}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Lecturas recomendadas */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Lecturas recomendadas</h3>
        <div className="space-y-2">
          {data.lecturas_recomendadas.map((l, i) => (
            <div key={i} className="flex items-start gap-2">
              <span className="text-xs text-gray-400 mt-0.5 w-4">{l.prioridad}.</span>
              <div>
                <a
                  href={l.pdf || l.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-600 hover:underline"
                >
                  {l.titulo}
                </a>
                <p className="text-xs text-gray-500 mt-0.5">{l.por_que}</p>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Fuentes */}
      <div className="bg-gray-50 rounded-lg border border-gray-200 p-4">
        <div className="text-xs text-gray-500">
          <span className="font-medium">Última extracción:</span> {meta.ultima_extraccion} ·{" "}
          <span className="font-medium">Fuentes:</span> {meta.fuentes_procesadas.join(", ")}
        </div>
      </div>
    </div>
  );
}
