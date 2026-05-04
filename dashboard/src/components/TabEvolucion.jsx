import { LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend, LabelList, AreaChart, Area } from "recharts";
import { fmtNum, fmtPct, tooltipStyle } from "../utils";
import Narrative from "./Narrative";

function ChartCard({ title, children, wide = false }) {
  return (
    <div className="card" style={wide ? { gridColumn: "1 / -1" } : {}}>
      <div className="section-title">{title}</div>
      {children}
    </div>
  );
}

const TICK = { fill: "var(--text-muted)", fontSize: 10 };

export default function TabEvolucion({ synthesis, data }) {
  const cuant = data.cuantitativo || {};
  const graficos = (synthesis.evolucion || {}).datos_graficos || {};
  const texto = (synthesis.evolucion || {}).texto;

  const aumData = (cuant.serie_aum || []).map(s => ({ p: s.periodo, v: s.valor_meur }));
  const partData = (cuant.serie_participes || []).map(s => ({ p: s.periodo, v: s.valor }));
  const vlData = (cuant.serie_vl_base100 || []).map(s => ({ p: s.periodo, v: s.base100 }));
  const rentData = (graficos.rentabilidades_anuales || []).map(s => ({ p: s.periodo, v: s.rentabilidad_pct }));
  const rotData = (cuant.serie_rotacion || []).map(s => ({ p: s.periodo, v: s.rotacion_pct }));

  // Mix activos as STACKED BAR (not area)
  const mixData = (cuant.mix_activos_historico || []).map(s => ({
    p: s.periodo,
    RV: Math.round(s.rv_pct || 0),
    RF: Math.round(s.renta_fija_pct || 0),
    Liquidez: Math.round(s.liquidez_pct || 0),
  })).reverse();

  // TER evolution
  const terData = (cuant.serie_ter || []).map(s => ({ p: s.periodo, v: s.ter_pct }));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      {/* Charts grid */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
        {/* AUM */}
        <ChartCard title="Patrimonio (AUM)">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={aumData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtNum(v, 1) + " M€"]} />
              <Area type="monotone" dataKey="v" stroke="var(--chart-1)" fill="var(--chart-1)" fillOpacity={0.1} strokeWidth={2}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v, 1)} />
              </Area>
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Partícipes */}
        <ChartCard title="Partícipes">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={partData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtNum(v)]} />
              <Bar dataKey="v" fill="var(--chart-2)" radius={[3, 3, 0, 0]}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v)} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* VL base 100 */}
        <ChartCard title="Valor Liquidativo (Base 100)">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={vlData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtNum(v, 1)]} />
              <Line type="monotone" dataKey="v" stroke="var(--chart-3)" strokeWidth={2} dot={{ r: 3 }}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v, 1)} />
              </Line>
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Rentabilidad */}
        <ChartCard title="Rentabilidad Anual (%)">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={rentData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
              <Bar dataKey="v" radius={[3, 3, 0, 0]}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v, 1) + "%"} />
                {rentData.map((entry, i) => (
                  <rect key={i} fill={entry.v >= 0 ? "var(--positive)" : "var(--negative)"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Mix activos — STACKED BARS */}
      <ChartCard title="Mix de Activos (%)" wide>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={mixData}>
            <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
            <XAxis dataKey="p" tick={TICK} />
            <YAxis tick={TICK} />
            <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
            <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            <Bar dataKey="RV" stackId="a" fill="var(--chart-1)" />
            <Bar dataKey="RF" stackId="a" fill="var(--chart-2)" />
            <Bar dataKey="Liquidez" stackId="a" fill="var(--chart-4)" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* TER + Rotación side by side */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
        <ChartCard title="TER (%)">
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={terData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} domain={[0, 'auto']} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
              <Line type="monotone" dataKey="v" stroke="var(--chart-4)" strokeWidth={2} dot={{ r: 3 }}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v, 2) + "%"} />
              </Line>
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Rotación (%)">
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={rotData}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="p" tick={TICK} />
              <YAxis tick={TICK} />
              <Tooltip {...tooltipStyle} formatter={(v) => [fmtPct(v)]} />
              <Bar dataKey="v" fill="var(--chart-6)" radius={[3, 3, 0, 0]}>
                <LabelList dataKey="v" position="top" fontSize={9} fill="var(--text-muted)" formatter={(v) => fmtNum(v, 0) + "%"} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Narrative */}
      <div className="card-lg">
        <div className="section-title">Análisis cuantitativo</div>
        <Narrative text={texto} />
      </div>
    </div>
  );
}
