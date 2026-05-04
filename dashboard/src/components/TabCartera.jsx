import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, LabelList } from "recharts";
import { fmtNum, fmtPct, tooltipStyle } from "../utils";
import Narrative from "./Narrative";

const COLORS = ["var(--chart-1)", "var(--chart-2)", "var(--chart-3)", "var(--chart-4)", "var(--chart-5)", "var(--chart-6)"];
const TICK = { fill: "var(--text-muted)", fontSize: 10 };

export default function TabCartera({ synthesis, data }) {
  const c = synthesis.cartera || {};
  const posiciones = (data.posiciones || {}).actuales || [];
  const distrib = c.distribucion_tipo || {};
  const concentracion = c.concentracion || {};
  const concHist = c.concentracion_historica || [];

  const sorted = [...posiciones].sort((a, b) => (b.peso_pct || 0) - (a.peso_pct || 0));

  // Distribution donut
  const distribData = Object.entries(distrib)
    .filter(([_, v]) => v > 0)
    .map(([k, v]) => ({
      name: k.replace(/_pct$/, "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase()),
      value: v,
    }));

  // Concentration bars
  const concData = concentracion.top5_pct ? [
    { name: "Top 5", pct: concentracion.top5_pct },
    { name: "Top 10", pct: concentracion.top10_pct },
    { name: "Top 15", pct: concentracion.top15_pct },
  ] : [];

  // Historical concentration
  const concHistData = concHist
    .filter(c => c.periodo && c.fuente)
    .map(c => ({ p: c.periodo, Top5: c.top5_pct, Top10: c.top10_pct, Top15: c.top15_pct }));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      {/* Charts row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "1rem" }}>
        {/* Distribution */}
        {distribData.length > 0 && (
          <div className="card">
            <div className="section-title">Distribución por tipo</div>
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie data={distribData} cx="50%" cy="50%" innerRadius={45} outerRadius={75} dataKey="value" paddingAngle={2}>
                  {distribData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  <LabelList dataKey="value" position="outside" fontSize={10} fill="var(--text-muted)" formatter={(v) => fmtPct(v)} />
                </Pie>
                <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
              </PieChart>
            </ResponsiveContainer>
            {/* Legend */}
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginTop: "0.5rem" }}>
              {distribData.map((d, i) => (
                <span key={i} style={{ fontSize: "0.6875rem", display: "flex", alignItems: "center", gap: "0.25rem" }}>
                  <span style={{ width: "8px", height: "8px", borderRadius: "2px", background: COLORS[i % COLORS.length] }} />
                  {d.name}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Current concentration */}
        {concData.length > 0 && (
          <div className="card">
            <div className="section-title">Concentración actual</div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={concData} layout="vertical">
                <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
                <XAxis type="number" domain={[0, 60]} tick={TICK} />
                <YAxis type="category" dataKey="name" tick={TICK} width={50} />
                <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
                <Bar dataKey="pct" fill="var(--accent)" radius={[0, 4, 4, 0]} barSize={24}>
                  <LabelList dataKey="pct" position="right" fontSize={11} fill="var(--text-secondary)" formatter={(v) => fmtPct(v)} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Historical concentration */}
        {concHistData.length > 1 && (
          <div className="card">
            <div className="section-title">Concentración histórica</div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={concHistData}>
                <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
                <XAxis dataKey="p" tick={TICK} />
                <YAxis tick={TICK} />
                <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
                <Bar dataKey="Top5" fill="var(--chart-1)" radius={[3, 3, 0, 0]} />
                <Bar dataKey="Top10" fill="var(--chart-4)" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Positions table — COMPLETE with all columns */}
      <div className="card-lg" style={{ overflow: "auto" }}>
        <div className="section-title">Posiciones ({sorted.length} total)</div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8125rem" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid var(--border)" }}>
              {["#", "Nombre", "Tipo", "País", "Divisa", "Peso", "Valor (k€)", "Cupón", "Vto."].map((h) => (
                <th key={h} style={{
                  textAlign: ["Peso", "Valor (k€)", "Cupón"].includes(h) ? "right" : "left",
                  padding: "0.5rem 0.5rem", color: "var(--text-muted)", fontWeight: 600,
                  fontSize: "0.6875rem", textTransform: "uppercase", letterSpacing: "0.03em",
                }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((p, i) => (
              <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 === 0 ? "var(--bg-hover)" : "transparent" }}>
                <td style={{ padding: "0.375rem 0.5rem", color: "var(--text-muted)", fontFamily: "var(--font-display)", fontSize: "0.75rem" }}>{i + 1}</td>
                <td style={{ padding: "0.375rem 0.5rem", fontWeight: 500, maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.nombre}</td>
                <td style={{ padding: "0.375rem 0.5rem" }}>
                  <span className={`badge ${p.tipo === "BONO" ? "badge-neutral" : "badge-positive"}`} style={{ fontSize: "0.5625rem" }}>
                    {p.tipo}
                  </span>
                </td>
                <td style={{ padding: "0.375rem 0.5rem", color: "var(--text-secondary)", fontSize: "0.75rem" }}>{p.pais || "—"}</td>
                <td style={{ padding: "0.375rem 0.5rem", color: "var(--text-muted)", fontSize: "0.75rem" }}>{p.divisa || "—"}</td>
                <td style={{ padding: "0.375rem 0.5rem", textAlign: "right", fontFamily: "var(--font-display)", fontWeight: 600 }}>
                  {fmtPct(p.peso_pct)}
                </td>
                <td style={{ padding: "0.375rem 0.5rem", textAlign: "right", fontFamily: "var(--font-display)", color: "var(--text-secondary)", fontSize: "0.75rem" }}>
                  {p.valor_mercado_miles ? fmtNum(p.valor_mercado_miles) : "—"}
                </td>
                <td style={{ padding: "0.375rem 0.5rem", textAlign: "right", fontFamily: "var(--font-display)", color: "var(--text-secondary)", fontSize: "0.75rem" }}>
                  {p.cupon ? fmtPct(p.cupon) : "—"}
                </td>
                <td style={{ padding: "0.375rem 0.5rem", color: "var(--text-secondary)", fontSize: "0.75rem" }}>
                  {p.vencimiento || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Narrative */}
      <div className="card-lg">
        <div className="section-title">Análisis de cartera</div>
        <Narrative text={c.texto} />
      </div>
    </div>
  );
}
