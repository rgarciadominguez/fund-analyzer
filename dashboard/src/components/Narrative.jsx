import { parseNarrative } from "../utils";

export default function Narrative({ text, maxHeight = null, className = "" }) {
  if (!text) return <p style={{ color: "var(--text-muted)", fontStyle: "italic" }}>Información no disponible</p>;

  const blocks = parseNarrative(text);

  return (
    <div className={`narrative ${className}`} style={maxHeight ? { maxHeight, overflowY: "auto", paddingRight: "0.5rem" } : {}}>
      {blocks.map((block, i) => {
        if (block.type === "heading") {
          return (
            <h3 key={i} style={{
              fontSize: "0.875rem",
              fontWeight: 700,
              color: "var(--text-primary)",
              margin: i === 0 ? "0 0 0.625rem 0" : "1.25rem 0 0.625rem 0",
              padding: "0.5rem 0.75rem",
              borderLeft: "3px solid var(--accent)",
              background: "var(--bg-accent, var(--bg-hover))",
              borderRadius: "0 var(--radius) var(--radius) 0",
            }}>
              {block.content}
            </h3>
          );
        }
        return (
          <p key={i} dangerouslySetInnerHTML={{ __html: block.content }}
            style={{ fontSize: "0.8125rem", lineHeight: 1.7, color: "var(--text-secondary)", marginBottom: "0.75em" }}
          />
        );
      })}
    </div>
  );
}
