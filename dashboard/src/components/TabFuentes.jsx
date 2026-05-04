import Narrative from "./Narrative";

export default function TabFuentes({ synthesis }) {
  const f = synthesis.fuentes_externas || {};
  const opiniones = f.opiniones_clave || [];

  const sentColor = (s) => {
    if (s === "POSITIVO") return { bg: "var(--positive-bg)", color: "var(--positive)", cls: "badge-positive" };
    if (s === "NEGATIVO") return { bg: "var(--negative-bg)", color: "var(--negative)", cls: "badge-negative" };
    return { bg: "var(--neutral-bg)", color: "var(--neutral)", cls: "badge-neutral" };
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
      {/* Synopsis */}
      <div className="card-lg">
        <div className="section-title">Síntesis de opiniones externas</div>
        <Narrative text={f.texto} />
      </div>

      {/* Article cards grid */}
      {opiniones.length > 0 && (
        <div>
          <div className="section-title">Análisis y opiniones</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: "1rem" }}>
            {opiniones.map((op, i) => {
              const sent = sentColor(op.sentimiento);
              return (
                <div key={i} className="article-card"
                  onClick={() => op.url && window.open(op.url, "_blank")}
                  style={{ borderLeft: `4px solid ${sent.color}` }}
                >
                  {/* Header: source + sentiment */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "0.625rem" }}>
                    <div>
                      <div style={{ fontSize: "0.6875rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                        {op.fuente}
                      </div>
                      {op.titulo && (
                        <h4 style={{ fontSize: "0.875rem", fontWeight: 600, color: "var(--text-primary)", margin: "0.25rem 0 0", lineHeight: 1.4 }}>
                          {op.titulo}
                        </h4>
                      )}
                    </div>
                    <span className={`badge ${sent.cls}`} style={{ flexShrink: 0, marginLeft: "0.5rem" }}>
                      {op.sentimiento}
                    </span>
                  </div>

                  {/* Opinion summary */}
                  <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", lineHeight: 1.6, margin: 0 }}>
                    {op.opinion}
                  </p>

                  {/* Read more link */}
                  {op.url && (
                    <div style={{ marginTop: "0.625rem", fontSize: "0.75rem", color: "var(--accent)", fontWeight: 500 }}>
                      Leer análisis completo →
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
