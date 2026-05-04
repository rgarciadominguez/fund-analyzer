import Narrative from "./Narrative";

export default function TabResumen({ synthesis, layout }) {
  const r = synthesis.resumen || {};

  if (layout === "B") {
    // Layout B: 3-column grid — narrative left, fortalezas/riesgos stacked right
    return (
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: "1.25rem" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
          <div className="card-lg"><Narrative text={r.texto} /></div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
            <InfoCard title="Para quién es adecuado" text={r.para_quien_es} color="var(--accent)" />
            <InfoCard title="Compromiso del gestor" text={r.compromiso_gestor} color="var(--accent)" />
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          <ListCard title="Fortalezas" items={r.fortalezas} color="var(--positive)" bgColor="var(--positive-bg)" />
          <ListCard title="Riesgos" items={r.riesgos} color="var(--negative)" bgColor="var(--negative-bg)" />
        </div>
      </div>
    );
  }

  if (layout === "C") {
    // Layout C: Full width narrative, then compact fortalezas/riesgos as tags
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
        <div className="card-lg"><Narrative text={r.texto} /></div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "1rem" }}>
          <ListCard title="Fortalezas" items={r.fortalezas} color="var(--positive)" bgColor="var(--positive-bg)" />
          <ListCard title="Riesgos" items={r.riesgos} color="var(--negative)" bgColor="var(--negative-bg)" />
          <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
            <InfoCard title="Para quién es" text={r.para_quien_es} color="var(--accent)" />
            <InfoCard title="Compromiso gestor" text={r.compromiso_gestor} color="var(--accent)" />
          </div>
        </div>
      </div>
    );
  }

  // Layout A (default): narrative top, 2x2 grid below
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      <div className="card-lg"><Narrative text={r.texto} /></div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
        <ListCard title="Fortalezas" items={r.fortalezas} color="var(--positive)" bgColor="var(--positive-bg)" />
        <ListCard title="Riesgos" items={r.riesgos} color="var(--negative)" bgColor="var(--negative-bg)" />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
        <InfoCard title="Para quién es adecuado" text={r.para_quien_es} color="var(--accent)" />
        <InfoCard title="Compromiso del gestor" text={r.compromiso_gestor} color="var(--accent)" />
      </div>
    </div>
  );
}

function ListCard({ title, items = [], color, bgColor }) {
  return (
    <div className="card" style={{ borderTop: `3px solid ${color}` }}>
      <div className="section-title" style={{ color }}>{title}</div>
      <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: "0.625rem" }}>
        {items.map((item, i) => (
          <li key={i} style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.5, padding: "0.5rem 0.75rem", background: bgColor, borderRadius: "6px" }}>
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}

function InfoCard({ title, text, color }) {
  return (
    <div className="card" style={{ borderLeft: `3px solid ${color}` }}>
      <div className="section-title" style={{ color }}>{title}</div>
      <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.65, margin: 0 }}>{text || "—"}</p>
    </div>
  );
}
