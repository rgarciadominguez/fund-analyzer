import { useState } from "react";
import Narrative from "./Narrative";

export default function TabEstrategia({ synthesis, layout }) {
  const e = synthesis.estrategia || {};
  const hitos = e.hitos_estrategia || [];
  const [expandedPhase, setExpandedPhase] = useState(null);

  // Split narrative into phases (by **headers**)
  const phases = parsePhases(e.texto || "");

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      {/* Current strategy — highlighted */}
      {e.estrategia_actual_resumen && (
        <div className="card-lg" style={{ borderLeft: "4px solid var(--accent)", background: "var(--bg-accent)" }}>
          <div className="section-title" style={{ color: "var(--accent)" }}>Estrategia actual</div>
          <p style={{ fontSize: "0.875rem", color: "var(--text-primary)", lineHeight: 1.65, margin: 0, fontWeight: 500 }}>
            {e.estrategia_actual_resumen}
          </p>
        </div>
      )}

      {/* Strategic milestones — horizontal scrollable cards */}
      {hitos.length > 0 && (
        <div className="card">
          <div className="section-title">Hitos estratégicos</div>
          <div style={{ display: "flex", gap: "0.625rem", overflowX: "auto", paddingBottom: "0.5rem" }}>
            {hitos.map((h, i) => (
              <div key={i} style={{
                flex: "0 0 200px", padding: "0.75rem 1rem",
                background: "var(--bg-hover)", borderRadius: "8px",
                borderTop: "3px solid var(--accent)",
              }}>
                <div style={{ fontSize: "0.8125rem", fontWeight: 700, color: "var(--accent)", fontFamily: "var(--font-display)", marginBottom: "0.375rem" }}>
                  {h.periodo}
                </div>
                <div style={{ fontSize: "0.75rem", color: "var(--text-secondary)", lineHeight: 1.5 }}>
                  {h.cambio}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Phases as expandable/accordion cards — NOT a wall of text */}
      <div className="section-title">Evolución por fases</div>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.625rem" }}>
        {phases.length > 0 ? phases.map((phase, i) => (
          <div key={i} className="card" style={{ cursor: "pointer", transition: "box-shadow 0.15s" }}
            onClick={() => setExpandedPhase(expandedPhase === i ? null : i)}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <h3 style={{ fontSize: "0.875rem", fontWeight: 600, color: "var(--text-primary)", margin: 0 }}>
                {phase.title}
              </h3>
              <span style={{ color: "var(--text-muted)", fontSize: "0.875rem", transition: "transform 0.2s", transform: expandedPhase === i ? "rotate(180deg)" : "none" }}>
                ▼
              </span>
            </div>
            {/* Preview: first sentence */}
            {expandedPhase !== i && (
              <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)", margin: "0.375rem 0 0", lineHeight: 1.5 }}>
                {phase.preview}
              </p>
            )}
            {/* Expanded: full text */}
            {expandedPhase === i && (
              <div style={{ marginTop: "0.75rem" }}>
                <Narrative text={phase.body} />
              </div>
            )}
          </div>
        )) : (
          // Fallback: full narrative if parsing fails
          <div className="card-lg">
            <Narrative text={e.texto} />
          </div>
        )}
      </div>
    </div>
  );
}

/** Parse narrative text into phases by detecting **bold headers** */
function parsePhases(text) {
  if (!text) return [];
  const parts = text.split(/\*\*([^*]+)\*\*/);
  const phases = [];

  for (let i = 1; i < parts.length; i += 2) {
    const title = parts[i].trim();
    const body = (parts[i + 1] || "").trim();
    if (title && body) {
      const firstSentence = body.split(/\.\s/)[0] + ".";
      phases.push({
        title,
        body,
        preview: firstSentence.length > 150 ? firstSentence.slice(0, 147) + "..." : firstSentence,
      });
    }
  }

  return phases;
}
