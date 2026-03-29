import { useState, useEffect } from "react";
import FundOverview from "./components/FundOverview";
import FundChart from "./components/FundChart";
import { FundStrategy, FundTimeline, FundSelector } from "./components/FundComponents";

export default function App() {
  const [funds, setFunds] = useState([]);
  const [selected, setSelected] = useState(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("overview");

  // Cargar índice de fondos disponibles
  useEffect(() => {
    fetch("/api/funds")
      .then(r => r.json())
      .then(setFunds)
      .catch(() => {
        // En desarrollo estático: cargar Avantage Fund directamente
        setFunds([{ isin: "ES0112231008", nombre: "Avantage Fund FI", gestora: "Renta 4 Gestora" }]);
      });
  }, []);

  // Cargar datos del fondo seleccionado
  const loadFund = async (isin) => {
    setLoading(true);
    try {
      const r = await fetch(`/data/${isin}.json`);
      const data = await r.json();
      setSelected(data);
      setActiveTab("overview");
    } catch (e) {
      console.error("Error cargando fondo:", e);
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { id: "overview",  label: "Resumen" },
    { id: "charts",    label: "Serie histórica" },
    { id: "strategy",  label: "Estrategia" },
    { id: "timeline",  label: "Hitos" },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-xl font-semibold text-gray-900">Fund Analyzer</h1>
            <p className="text-sm text-gray-500">Análisis cualitativo de fondos de inversión</p>
          </div>
          {selected && (
            <div className="text-right">
              <div className="text-sm font-medium text-gray-700">{selected.meta.isin}</div>
              <div className={`text-xs px-2 py-0.5 rounded-full inline-block mt-1 ${
                selected.meta.extraccion_estado.cualitativo === "completo"
                  ? "bg-green-100 text-green-700"
                  : "bg-amber-100 text-amber-700"
              }`}>
                {selected.meta.extraccion_estado.cualitativo === "completo" ? "✓ Completo" : "⚠ Parcial"}
              </div>
            </div>
          )}
        </div>
      </header>

      <div className="max-w-6xl mx-auto px-6 py-6">
        {/* Selector */}
        <FundSelector funds={funds} onSelect={loadFund} selected={selected} />

        {loading && (
          <div className="text-center py-16 text-gray-400">Cargando datos del fondo...</div>
        )}

        {selected && !loading && (
          <>
            {/* Nombre del fondo */}
            <div className="mt-6 mb-4">
              <h2 className="text-2xl font-bold text-gray-900">{selected.meta.nombre}</h2>
              <p className="text-gray-500">{selected.meta.gestora} {selected.meta.asesor ? `· Asesor: ${selected.meta.asesor}` : ""}</p>
            </div>

            {/* Tabs */}
            <div className="flex gap-1 border-b border-gray-200 mb-6">
              {tabs.map(tab => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? "border-blue-600 text-blue-600"
                      : "border-transparent text-gray-500 hover:text-gray-700"
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            {/* Contenido */}
            {activeTab === "overview"  && <FundOverview data={selected} />}
            {activeTab === "charts"    && <FundChart data={selected} />}
            {activeTab === "strategy"  && <FundStrategy data={selected} />}
            {activeTab === "timeline"  && <FundTimeline data={selected} />}
          </>
        )}

        {!selected && !loading && (
          <div className="text-center py-24 text-gray-400">
            <div className="text-4xl mb-4">📊</div>
            <p className="text-lg">Selecciona un fondo para comenzar el análisis</p>
          </div>
        )}
      </div>
    </div>
  );
}
