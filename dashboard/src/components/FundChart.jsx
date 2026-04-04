import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";

export default function FundChart({ data }) {
  const { cuantitativo } = data;
  const serie = cuantitativo.serie_historica.filter(s => s.aum_meur !== null);
  const rent = cuantitativo.rentabilidades_anuales.filter(r => r.pct !== null);

  if (serie.length === 0 && rent.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-8 text-center">
        <div className="text-gray-400 text-sm">
          Serie histórica pendiente de extracción desde XMLs CNMV.
        </div>
        <div className="mt-2 text-xs text-gray-400">
          Ejecuta: <code className="bg-gray-100 px-1 rounded">python extractor.py {data.meta.isin}</code>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* AUM histórico */}
      {serie.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Patrimonio (AUM) — M€</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={serie}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="periodo" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v) => [`${v}M€`, "AUM"]} />
              <Line type="monotone" dataKey="aum_meur" stroke="#2563eb" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Partícipes */}
      {serie.filter(s => s.participes).length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Partícipes</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={serie.filter(s => s.participes)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="periodo" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Line type="monotone" dataKey="participes" stroke="#7c3aed" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Exposición RV/RF/Liquidez */}
      {serie.filter(s => s.rv_pct !== null).length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Composición cartera (%)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={serie.filter(s => s.rv_pct !== null)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="periodo" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} domain={[0, 100]} />
              <Tooltip />
              <Legend />
              <Bar dataKey="rv_pct" name="RV %" fill="#2563eb" stackId="a" />
              <Bar dataKey="rf_pct" name="RF %" fill="#7c3aed" stackId="a" />
              <Bar dataKey="liquidez_pct" name="Liquidez %" fill="#d1d5db" stackId="a" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Rentabilidades anuales */}
      {rent.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Rentabilidad anual (%)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={rent}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="año" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v) => [`${v}%`, "Rentabilidad"]} />
              <Bar dataKey="pct" name="Rentabilidad %" fill={(entry) => entry.pct >= 0 ? "#16a34a" : "#dc2626"} radius={[3,3,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
