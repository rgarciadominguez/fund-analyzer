import Narrative from "./Narrative";

export default function TabGestores({ synthesis, layout }) {
  const g = synthesis.gestores || {};
  const perfiles = g.perfiles || [];

  // Layout: left panel = team overview, right panel = individual profiles
  return (
    <div style={{ display: "grid", gridTemplateColumns: layout === "B" ? "1fr 1fr" : "350px 1fr", gap: "1.5rem" }}>
      {/* Left: Team overview */}
      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        <div className="card-lg">
          <div className="section-title">Equipo gestor</div>
          <Narrative text={g.texto} />
        </div>

        {/* Quick roster */}
        <div className="card">
          <div className="section-title">Equipo</div>
          {perfiles.map((p, i) => (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: "0.75rem",
              padding: "0.5rem 0", borderBottom: i < perfiles.length - 1 ? "1px solid var(--border)" : "none",
            }}>
              <div style={{
                width: "36px", height: "36px", borderRadius: "50%", background: "var(--accent-light)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: "0.75rem", fontWeight: 700, color: "var(--accent)", flexShrink: 0,
              }}>
                {(p.nombre || "?").split(" ").map(w => w[0]).join("").slice(0, 2)}
              </div>
              <div>
                <div style={{ fontSize: "0.8125rem", fontWeight: 600 }}>{p.nombre}</div>
                <div style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>{p.cargo}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Right: Detailed profiles */}
      <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
        {perfiles.map((p, i) => (
          <ProfileCard key={i} profile={p} isLead={i === 0} />
        ))}
      </div>
    </div>
  );
}

function ProfileCard({ profile: p, isLead }) {
  return (
    <div className="card-lg" style={isLead ? { borderTop: "3px solid var(--accent)" } : {}}>
      <div style={{ display: "flex", alignItems: "center", gap: "1rem", marginBottom: "1rem" }}>
        <div style={{
          width: isLead ? "56px" : "40px", height: isLead ? "56px" : "40px", borderRadius: "50%",
          background: isLead ? "var(--accent)" : "var(--accent-light)",
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: isLead ? "1rem" : "0.8125rem", fontWeight: 700,
          color: isLead ? "#fff" : "var(--accent)", flexShrink: 0,
        }}>
          {(p.nombre || "?").split(" ").map(w => w[0]).join("").slice(0, 2)}
        </div>
        <div>
          <h3 style={{ fontSize: isLead ? "1.125rem" : "0.9375rem", fontWeight: 700, margin: 0 }}>{p.nombre}</h3>
          <div style={{ fontSize: "0.8125rem", color: "var(--accent)" }}>{p.cargo}</div>
          {isLead && <span className="badge badge-accent" style={{ marginTop: "0.25rem" }}>Gestor principal</span>}
        </div>
      </div>

      {/* Trayectoria */}
      {p.trayectoria && (
        <div style={{ marginBottom: "1rem" }}>
          <div className="section-title">Trayectoria</div>
          <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.65, margin: 0 }}>{p.trayectoria}</p>
        </div>
      )}

      {/* Filosofía */}
      {p.filosofia && (
        <div style={{ marginBottom: "1rem" }}>
          <div className="section-title">Filosofía de inversión</div>
          <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.65, margin: 0 }}>{p.filosofia}</p>
        </div>
      )}

      {/* Decisiones clave */}
      {p.decisiones_clave && p.decisiones_clave.length > 0 && (
        <div>
          <div className="section-title">Decisiones clave</div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
            {p.decisiones_clave.map((d, j) => (
              <div key={j} style={{
                fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.5,
                padding: "0.5rem 0.75rem", background: "var(--bg-hover)", borderRadius: "6px",
                borderLeft: "3px solid var(--accent)",
              }}>
                {d}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Rasgos diferenciales */}
      {p.rasgos_diferenciales && (
        <div style={{ marginTop: "1rem" }}>
          <div className="section-title">Rasgos diferenciales</div>
          <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.65, margin: 0 }}>{p.rasgos_diferenciales}</p>
        </div>
      )}
    </div>
  );
}
