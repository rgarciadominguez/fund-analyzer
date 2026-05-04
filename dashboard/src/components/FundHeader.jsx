import { fmtNum, fmtPct, fmtMeur } from "../utils";

export default function FundHeader({ data, synthesis, layout }) {
  const kpis = data.kpis || {};
  const resumen = synthesis.resumen || {};
  const signal = resumen.signal || "";

  const kpiItems = [
    { label: "AUM", value: fmtMeur(kpis.aum_actual_meur), color: "var(--chart-1)" },
    { label: "Partícipes", value: fmtNum(kpis.num_participes), color: "var(--chart-2)" },
    { label: "TER", value: fmtPct(kpis.ter_pct), color: "var(--chart-3)" },
    { label: "Volatilidad", value: fmtPct(kpis.volatilidad_pct), color: "var(--chart-4)" },
    { label: "Creación", value: fmtNum(kpis.anio_creacion), color: "var(--text-primary)" },
    { label: "Riesgo", value: kpis.perfil_riesgo ? `${kpis.perfil_riesgo}/7` : "—", color: "var(--chart-5)" },
    { label: "Rotación", value: fmtPct(kpis.rotacion_cartera_pct), color: "var(--chart-6)" },
  ];

  return (
    <div style={{ background: "linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%)", padding: "1.5rem", color: "#fff" }}>
      <div style={{ maxWidth: "var(--content-max, 1280px)", margin: "0 auto" }}>
        {/* Fund name + signal */}
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: "1.25rem" }}>
          <div>
            <h1 style={{ fontSize: "1.625rem", fontWeight: 700, margin: 0, letterSpacing: "-0.01em" }}>
              {data.nombre || "—"}
            </h1>
            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.375rem", fontSize: "0.8125rem", opacity: 0.8 }}>
              <span style={{ fontFamily: "var(--font-display)", fontWeight: 600, background: "rgba(255,255,255,0.15)", padding: "0.1rem 0.5rem", borderRadius: "4px" }}>
                {data.isin}
              </span>
              <span>·</span>
              <span>{data.gestora}</span>
              <span>·</span>
              <span>{data.tipo === "ES" ? "🇪🇸 España" : "🌍 Internacional"}</span>
            </div>
          </div>
          {signal && (
            <div style={{
              padding: "0.375rem 1rem", borderRadius: "8px", fontWeight: 700, fontSize: "0.8125rem",
              background: signal === "POSITIVO" ? "rgba(34,197,94,0.2)" : signal === "NEGATIVO" ? "rgba(239,68,68,0.2)" : "rgba(234,179,8,0.2)",
              color: "#fff", border: "1px solid rgba(255,255,255,0.2)",
              display: "flex", alignItems: "center", gap: "0.375rem",
            }}>
              {signal === "POSITIVO" ? "▲" : signal === "NEGATIVO" ? "▼" : "●"} {signal}
            </div>
          )}
        </div>

        {/* KPIs row */}
        <div style={{ display: "grid", gridTemplateColumns: `repeat(${kpiItems.length}, 1fr)`, gap: "0.5rem" }}>
          {kpiItems.map((kpi, i) => (
            <div key={i} style={{
              background: "rgba(255,255,255,0.1)", borderRadius: "8px", padding: "0.75rem 1rem",
              backdropFilter: "blur(8px)", border: "1px solid rgba(255,255,255,0.1)",
            }}>
              <div style={{ fontSize: "0.625rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em", opacity: 0.7, marginBottom: "0.25rem" }}>
                {kpi.label}
              </div>
              <div style={{ fontSize: "1.25rem", fontWeight: 700, fontFamily: "var(--font-display)" }}>
                {kpi.value}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
