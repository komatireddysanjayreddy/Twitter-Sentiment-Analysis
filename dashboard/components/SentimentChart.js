/**
 * SentimentChart — Real-time line chart of positive / negative / neutral
 * tweet counts per minute using Recharts.
 */

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";

const COLORS = {
  positive: "#22c55e",
  negative: "#ef4444",
  neutral:  "#f59e0b",
};

/**
 * Pivot flat timeSeries rows → [{bucket, positive, negative, neutral}]
 */
function pivotTimeSeries(timeSeries = []) {
  const map = {};
  for (const row of timeSeries) {
    const key = row.bucket;
    if (!map[key]) {
      map[key] = {
        bucket: new Date(key).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        positive: 0,
        negative: 0,
        neutral: 0,
      };
    }
    map[key][row.sentiment] = Number(row.tweet_count);
  }
  return Object.values(map).sort((a, b) => a.bucket.localeCompare(b.bucket));
}

export default function SentimentChart({ timeSeries }) {
  const data = pivotTimeSeries(timeSeries);

  if (!data.length) {
    return (
      <div style={styles.placeholder}>
        No time-series data yet. Waiting for tweets…
      </div>
    );
  }

  return (
    <div style={styles.container}>
      <h2 style={styles.title}>Sentiment Over Time (per minute)</h2>
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={data} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="bucket" tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} allowDecimals={false} />
          <Tooltip
            contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6 }}
            labelStyle={{ color: "#e2e8f0" }}
          />
          <Legend wrapperStyle={{ color: "#94a3b8" }} />
          {Object.entries(COLORS).map(([key, color]) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={color}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 5 }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

const styles = {
  container: {
    background: "#1e293b",
    borderRadius: "8px",
    padding: "1.5rem",
    marginBottom: "2rem",
  },
  title: { color: "#e2e8f0", fontSize: "1rem", fontWeight: 600, marginBottom: "1rem" },
  placeholder: { color: "#94a3b8", textAlign: "center", padding: "3rem" },
};
