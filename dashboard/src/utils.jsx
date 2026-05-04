/** Spanish number formatting */
export function fmtNum(val, decimals = 0) {
  if (val === null || val === undefined || val === "") return "—";
  const n = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(n)) return "—";
  return n.toLocaleString("de-DE", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function fmtPct(val) {
  if (val === null || val === undefined || val === "") return "—";
  const n = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(n)) return "—";
  return n.toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "%";
}

export function fmtMeur(val) {
  if (val === null || val === undefined) return "—";
  const n = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(n)) return "—";
  return n.toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + " M€";
}

/** Recharts custom label for data points */
export function chartLabel({ x, y, value, fill = "var(--text-muted)" }) {
  if (value === null || value === undefined) return null;
  return (
    <text x={x} y={y - 8} fill={fill} fontSize={9} textAnchor="middle" fontFamily="var(--font-display)">
      {typeof value === "number" ? fmtNum(value, value < 10 ? 1 : 0) : value}
    </text>
  );
}

/**
 * Parse narrative text into structured blocks.
 * Detects **Subsection Title** at start of paragraph as section headers.
 * Returns: [{type: "heading"|"paragraph", content: "html string"}]
 */
export function parseNarrative(text) {
  if (!text) return [];

  const blocks = [];
  const paragraphs = text.split(/\n\n+/).filter(p => p.trim());

  for (const p of paragraphs) {
    const trimmed = p.trim();

    // Check if paragraph IS a heading: starts with ** and the bold part is the whole line (or most of it)
    const headingMatch = trimmed.match(/^\*\*([^*]+)\*\*\s*$/);
    if (headingMatch) {
      blocks.push({ type: "heading", content: headingMatch[1].trim() });
      continue;
    }

    // Check if paragraph STARTS with a bold heading followed by content
    const headingWithContent = trimmed.match(/^\*\*([^*]+)\*\*\s*\n([\s\S]+)/);
    if (headingWithContent) {
      blocks.push({ type: "heading", content: headingWithContent[1].trim() });
      const body = formatInline(headingWithContent[2].trim());
      blocks.push({ type: "paragraph", content: body });
      continue;
    }

    // Check for numbered section headers like "1. Title" or "1. **Title**"
    const numberedMatch = trimmed.match(/^(\d+)\.\s+\*\*([^*]+)\*\*\s*$/);
    if (numberedMatch) {
      blocks.push({ type: "heading", content: numberedMatch[2].trim() });
      continue;
    }

    // Regular paragraph — apply inline formatting
    blocks.push({ type: "paragraph", content: formatInline(trimmed) });
  }

  return blocks;
}

function formatInline(text) {
  let html = text;
  // Bold: **text**
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  // Italic: *text*
  html = html.replace(/(?<!\*)\*([^*]+)\*(?!\*)/g, '<em>$1</em>');
  // Line breaks
  html = html.replace(/\n/g, '<br/>');
  return html;
}

/** Recharts tooltip formatter with Spanish numbers */
export const tooltipStyle = {
  contentStyle: {
    background: "var(--bg-card)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    fontSize: "0.75rem",
    padding: "0.5rem 0.75rem",
  },
  labelStyle: { color: "var(--text-primary)", fontWeight: 600, marginBottom: "0.25rem" },
  formatter: (value) => [fmtNum(value, value < 10 ? 1 : 0), ""],
};
