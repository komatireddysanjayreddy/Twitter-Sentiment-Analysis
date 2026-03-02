/**
 * StatsBar — Displays high-level positive / negative / neutral counts.
 */

export default function StatsBar({ summary }) {
  if (!summary || !summary.total_tweets) {
    return (
      <div style={styles.bar}>
        <span style={styles.label}>Loading stats…</span>
      </div>
    );
  }

  const { total_tweets, positive_count, negative_count, neutral_count, avg_compound_score } = summary;

  const tiles = [
    { label: "Total Tweets", value: Number(total_tweets).toLocaleString(), color: "#64748b" },
    { label: "Positive", value: `${positive_count} (${pct(positive_count, total_tweets)}%)`, color: "#22c55e" },
    { label: "Negative", value: `${negative_count} (${pct(negative_count, total_tweets)}%)`, color: "#ef4444" },
    { label: "Neutral",  value: `${neutral_count}  (${pct(neutral_count,  total_tweets)}%)`, color: "#f59e0b" },
    { label: "Avg Sentiment", value: Number(avg_compound_score).toFixed(3), color: "#6366f1" },
  ];

  return (
    <div style={styles.bar}>
      {tiles.map((t) => (
        <div key={t.label} style={{ ...styles.tile, borderTop: `4px solid ${t.color}` }}>
          <div style={styles.tileLabel}>{t.label}</div>
          <div style={{ ...styles.tileValue, color: t.color }}>{t.value}</div>
        </div>
      ))}
    </div>
  );
}

function pct(part, total) {
  if (!total) return "0.0";
  return ((part / total) * 100).toFixed(1);
}

const styles = {
  bar: {
    display: "flex",
    flexWrap: "wrap",
    gap: "1rem",
    marginBottom: "2rem",
  },
  tile: {
    flex: "1 1 140px",
    background: "#1e293b",
    borderRadius: "8px",
    padding: "1rem",
    textAlign: "center",
  },
  label: { color: "#94a3b8", fontSize: "0.8rem" },
  tileLabel: { color: "#94a3b8", fontSize: "0.75rem", marginBottom: "0.4rem" },
  tileValue: { fontSize: "1.25rem", fontWeight: 700 },
};
