import Narrative from "./Narrative";

export default function TabHistoria({ synthesis, layout }) {
  const h = synthesis.historia || {};
  const hitos = h.hitos || [];

  return (
    <div style={{ display: "grid", gridTemplateColumns: layout === "C" ? "1fr" : "300px 1fr", gap: "1.5rem" }}>
      {/* Timeline */}
      <div>
        <div className="section-title">Hitos</div>
        <div style={{ position: "relative", paddingLeft: "1.5rem" }}>
          <div style={{ position: "absolute", left: "5px", top: 0, bottom: 0, width: "2px", background: "var(--border)" }} />
          {hitos.map((hito, i) => (
            <div key={i} style={{ position: "relative", marginBottom: "1.25rem" }}>
              <div style={{
                position: "absolute", left: "-1.5rem", top: "0.25rem",
                width: "12px", height: "12px", borderRadius: "50%",
                background: "var(--accent)", border: "3px solid var(--bg-secondary)",
              }} />
              <div style={{ fontSize: "0.8125rem", fontWeight: 700, color: "var(--accent)", fontFamily: "var(--font-display)" }}>
                {hito.anio}
              </div>
              <div style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.5, marginTop: "0.125rem" }}>
                {hito.evento}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Narrative */}
      {layout !== "C" && (
        <div className="card-lg">
          <div className="section-title">Narrativa</div>
          <Narrative text={h.texto} />
        </div>
      )}

      {/* Layout C: narrative below timeline */}
      {layout === "C" && (
        <div className="card-lg">
          <div className="section-title">Narrativa</div>
          <Narrative text={h.texto} />
        </div>
      )}
    </div>
  );
}
