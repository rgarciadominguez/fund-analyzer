export default function TabDocumentos({ synthesis }) {
  const d = synthesis.documentos || {};

  const Section = ({ title, items, renderItem }) => (
    <div>
      <h4 style={{ fontSize: "0.75rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "0.5rem" }}>
        {title} ({items.length})
      </h4>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
        {items.map((item, i) => (
          <div key={i} style={{
            padding: "0.375rem 0.75rem",
            fontSize: "0.8125rem",
            color: "var(--text-secondary)",
            background: i % 2 === 0 ? "var(--bg-hover)" : "transparent",
            borderRadius: "var(--radius)",
          }}>
            {renderItem(item)}
          </div>
        ))}
      </div>
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
      {/* Summary */}
      <div className="card" style={{ display: "flex", gap: "2rem", alignItems: "center" }}>
        <div style={{ fontSize: "2rem", fontWeight: 700, fontFamily: "var(--font-display)", color: "var(--accent)" }}>
          {d.total_fuentes || 0}
        </div>
        <div style={{ color: "var(--text-secondary)", fontSize: "0.875rem" }}>fuentes totales consultadas</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem" }}>
        {/* XMLs */}
        {(d.xmls_cnmv || []).length > 0 && (
          <div className="card">
            <Section
              title="XMLs CNMV"
              items={d.xmls_cnmv}
              renderItem={(item) => item.archivo || item}
            />
          </div>
        )}

        {/* PDFs */}
        {(d.informes_pdf || []).length > 0 && (
          <div className="card">
            <Section
              title="Informes PDF"
              items={d.informes_pdf}
              renderItem={(item) => item.archivo || item}
            />
          </div>
        )}

        {/* Cartas */}
        {(d.cartas_urls || []).length > 0 && (
          <div className="card">
            <Section
              title="Cartas del gestor"
              items={d.cartas_urls}
              renderItem={(url) => (
                <a href={url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)", textDecoration: "none" }}>
                  {url.split("/").pop() || url}
                </a>
              )}
            />
          </div>
        )}

        {/* External URLs */}
        {(d.fuentes_externas_urls || []).length > 0 && (
          <div className="card">
            <Section
              title="Fuentes externas"
              items={d.fuentes_externas_urls}
              renderItem={(url) => (
                <a href={url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)", textDecoration: "none", wordBreak: "break-all" }}>
                  {new URL(url).hostname + new URL(url).pathname.slice(0, 40)}
                </a>
              )}
            />
          </div>
        )}
      </div>
    </div>
  );
}
