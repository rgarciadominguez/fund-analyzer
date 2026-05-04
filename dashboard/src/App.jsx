import { useState, useEffect } from "react";
import FundHeader from "./components/FundHeader";
import TabResumen from "./components/TabResumen";
import TabHistoria from "./components/TabHistoria";
import TabGestores from "./components/TabGestores";
import TabEvolucion from "./components/TabEvolucion";
import TabEstrategia from "./components/TabEstrategia";
import TabCartera from "./components/TabCartera";
import TabFuentes from "./components/TabFuentes";
import TabDocumentos from "./components/TabDocumentos";

const LAYOUTS = [
  { id: "A", label: "Layout A" },
  { id: "B", label: "Layout B" },
  { id: "C", label: "Layout C" },
];

const TABS = [
  { id: "resumen", label: "Resumen", icon: "📋" },
  { id: "historia", label: "Historia", icon: "📅" },
  { id: "gestores", label: "Gestores", icon: "👤" },
  { id: "evolucion", label: "Evolución", icon: "📈" },
  { id: "estrategia", label: "Estrategia", icon: "🎯" },
  { id: "cartera", label: "Cartera", icon: "💼" },
  { id: "fuentes", label: "Fuentes", icon: "📰" },
  { id: "documentos", label: "Docs", icon: "📁" },
];

export default function App() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState("resumen");
  const [layout, setLayout] = useState("A");

  useEffect(() => {
    document.documentElement.setAttribute("data-layout", layout);
  }, [layout]);

  useEffect(() => {
    fetch("/data/ES0112231008.json")
      .then((r) => r.json())
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { console.error(e); setLoading(false); });
  }, []);

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: "var(--bg-secondary)", color: "var(--text-muted)" }}>
      <div className="text-center">
        <div className="text-3xl mb-3">📊</div>
        Cargando datos del fondo...
      </div>
    </div>
  );

  if (!data) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: "var(--bg-secondary)", color: "var(--text-muted)" }}>
      Error cargando datos
    </div>
  );

  const synthesis = data.analyst_synthesis || {};

  return (
    <div style={{ background: "var(--bg-secondary)", minHeight: "100vh" }}>
      {/* Layout switcher — small bar */}
      <div style={{ background: "var(--bg-card)", borderBottom: "1px solid var(--border)", padding: "0.35rem 1.5rem", display: "flex", gap: "0.375rem", alignItems: "center", justifyContent: "flex-end" }}>
        <span style={{ color: "var(--text-muted)", fontSize: "0.6875rem", marginRight: "0.25rem" }}>Vista:</span>
        {LAYOUTS.map((l) => (
          <button key={l.id} onClick={() => setLayout(l.id)} style={{
            padding: "0.2rem 0.5rem", borderRadius: "4px", fontSize: "0.6875rem", fontWeight: layout === l.id ? 600 : 400,
            background: layout === l.id ? "var(--accent)" : "transparent",
            color: layout === l.id ? "#fff" : "var(--text-muted)",
            border: layout === l.id ? "none" : "1px solid var(--border)", cursor: "pointer",
          }}>
            {l.label}
          </button>
        ))}
      </div>

      {/* Header */}
      <FundHeader data={data} synthesis={synthesis} layout={layout} />

      {/* Tabs */}
      <div style={{ background: "var(--bg-card)", borderBottom: "1px solid var(--border)", position: "sticky", top: 0, zIndex: 10 }}>
        <div style={{ maxWidth: "var(--content-max, 1280px)", margin: "0 auto", padding: "0 1.5rem", display: "flex", gap: "0", overflowX: "auto" }}>
          {TABS.map((tab) => (
            <button key={tab.id} onClick={() => setActiveTab(tab.id)} style={{
              padding: "0.625rem 0.875rem", fontSize: "0.8125rem", fontWeight: activeTab === tab.id ? 600 : 400,
              color: activeTab === tab.id ? "var(--accent)" : "var(--text-muted)",
              background: "transparent", border: "none", cursor: "pointer", whiteSpace: "nowrap",
              borderBottom: activeTab === tab.id ? "2px solid var(--accent)" : "2px solid transparent",
              transition: "color 0.15s",
            }}>
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div style={{ maxWidth: "var(--content-max, 1280px)", margin: "0 auto", padding: "1.5rem" }}>
        {activeTab === "resumen" && <TabResumen synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "historia" && <TabHistoria synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "gestores" && <TabGestores synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "evolucion" && <TabEvolucion synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "estrategia" && <TabEstrategia synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "cartera" && <TabCartera synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "fuentes" && <TabFuentes synthesis={synthesis} data={data} layout={layout} />}
        {activeTab === "documentos" && <TabDocumentos synthesis={synthesis} data={data} layout={layout} />}
      </div>
    </div>
  );
}
