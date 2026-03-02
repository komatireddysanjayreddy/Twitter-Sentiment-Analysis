/**
 * pages/index.js — Real-time sentiment dashboard.
 *
 * Improvements over v1:
 *   • Loading skeleton on first paint (no blank flash)
 *   • Exponential backoff on fetch errors (max 60s)
 *   • Rolling 5-min average panel
 *   • "Source" badge on tweet cards (LIVE vs MOCK)
 *   • Manual refresh button
 */

import { useState, useEffect, useRef, useCallback } from "react";
import Head from "next/head";
import StatsBar from "../components/StatsBar";
import SentimentChart from "../components/SentimentChart";
import TweetFeed from "../components/TweetFeed";

const BASE_POLL_MS = 10_000;
const MAX_BACKOFF  = 60_000;

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// ─── Rolling 5-min panel ─────────────────────────────────────────────────────
const ROLLING_COLORS = { positive: "#22c55e", negative: "#ef4444", neutral: "#f59e0b" };

function RollingPanel({ rolling }) {
  if (!rolling?.length) return null;
  return (
    <div style={styles.card}>
      <h2 style={styles.cardTitle}>Rolling 5-Minute Sentiment Average</h2>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "1rem" }}>
        {rolling.map((r) => (
          <div key={r.sentiment}
               style={{ ...styles.rollingTile, borderTop: `4px solid ${ROLLING_COLORS[r.sentiment] ?? "#64748b"}` }}>
            <div style={{ color: "#94a3b8", fontSize: "0.75rem", marginBottom: "0.3rem", textTransform: "capitalize" }}>
              {r.sentiment}
            </div>
            <div style={{ fontSize: "1.5rem", fontWeight: 700, color: ROLLING_COLORS[r.sentiment] ?? "#e2e8f0" }}>
              {r.tweet_count} <span style={{ fontSize: "0.8rem", color: "#94a3b8" }}>tweets</span>
            </div>
            <div style={{ fontSize: "0.85rem", color: "#e2e8f0", marginTop: "0.25rem" }}>
              avg {Number(r.avg_compound).toFixed(3)}
            </div>
            <div style={{ fontSize: "0.7rem", color: "#475569", marginTop: "0.2rem" }}>
              {r.window_start ? new Date(r.window_start).toLocaleTimeString() : "–"} →{" "}
              {r.window_end   ? new Date(r.window_end).toLocaleTimeString()   : "–"}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Main page ────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [stats,       setStats]       = useState(null);
  const [tweets,      setTweets]      = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error,       setError]       = useState(null);
  const [loading,     setLoading]     = useState(true);
  const [filter,      setFilter]      = useState("all");

  const backoffRef  = useRef(BASE_POLL_MS);
  const timerRef    = useRef(null);

  const loadData = useCallback(async () => {
    const sentimentParam = filter !== "all" ? `&sentiment=${filter}` : "";
    try {
      const [statsData, tweetsData] = await Promise.all([
        fetchJSON("/api/stats"),
        fetchJSON(`/api/sentiments?limit=50${sentimentParam}`),
      ]);
      setStats(statsData);
      setTweets(tweetsData.data ?? []);
      setLastUpdated(new Date());
      setError(null);
      setLoading(false);
      backoffRef.current = BASE_POLL_MS;   // reset backoff on success
    } catch (err) {
      setError(err.message);
      setLoading(false);
      backoffRef.current = Math.min(backoffRef.current * 2, MAX_BACKOFF);
    }
    // Schedule next poll with current backoff
    timerRef.current = setTimeout(loadData, backoffRef.current);
  }, [filter]);

  useEffect(() => {
    backoffRef.current = BASE_POLL_MS;
    loadData();
    return () => clearTimeout(timerRef.current);
  }, [loadData]);

  const manualRefresh = () => {
    clearTimeout(timerRef.current);
    backoffRef.current = BASE_POLL_MS;
    loadData();
  };

  // ─── Loading skeleton ───────────────────────────────────────────────────────
  if (loading) {
    return (
      <div style={{ ...styles.page, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", color: "#64748b" }}>
          <div style={{ fontSize: "2rem", marginBottom: "1rem" }}>⏳</div>
          <div>Loading dashboard…</div>
        </div>
      </div>
    );
  }

  const nextPollSecs = Math.round(backoffRef.current / 1000);

  return (
    <>
      <Head>
        <title>Twitter Sentiment Analysis</title>
        <meta name="description" content="Real-time Twitter sentiment — Kafka + PySpark + VADER" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      <div style={styles.page}>
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <header style={styles.header}>
          <div>
            <h1 style={styles.title}>Twitter Sentiment Analysis</h1>
            <p style={styles.subtitle}>Kafka · PySpark · VADER · Neon PostgreSQL · Vercel</p>
          </div>
          <div style={{ textAlign: "right", display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "0.4rem" }}>
            <div style={error ? styles.errorIndicator : styles.liveIndicator}>
              <span style={error ? styles.errorDot : styles.liveDot} />
              {error ? "RECONNECTING" : "LIVE"}
            </div>
            {lastUpdated && (
              <div style={styles.lastUpdated}>
                Updated {lastUpdated.toLocaleTimeString()} · next in {nextPollSecs}s
              </div>
            )}
            <button onClick={manualRefresh} style={styles.refreshBtn} title="Refresh now">
              ↻ Refresh
            </button>
          </div>
        </header>

        {/* ── Error banner ────────────────────────────────────────────────── */}
        {error && (
          <div style={styles.errorBanner}>
            ⚠ {error} — retrying in {nextPollSecs}s (backoff active)
          </div>
        )}

        {/* ── Stats ───────────────────────────────────────────────────────── */}
        <StatsBar summary={stats?.summary} />

        {/* ── Rolling 5-min averages ──────────────────────────────────────── */}
        <RollingPanel rolling={stats?.rolling} />

        {/* ── Time-series chart ───────────────────────────────────────────── */}
        <SentimentChart timeSeries={stats?.timeSeries ?? []} />

        {/* ── Hashtag cloud ───────────────────────────────────────────────── */}
        {stats?.hashtags?.length > 0 && (
          <div style={styles.card}>
            <h2 style={styles.cardTitle}>Trending Hashtags (24h)</h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
              {stats.hashtags.slice(0, 20).map((h) => {
                const maxCount = stats.hashtags[0]?.mention_count ?? 1;
                const size = 0.75 + (0.55 * (h.mention_count / maxCount));
                return (
                  <span key={h.hashtag}
                        style={{ ...styles.hashtag, fontSize: `${size.toFixed(2)}rem` }}>
                    #{h.hashtag}
                    <span style={styles.hashtagCount}> {h.mention_count}</span>
                  </span>
                );
              })}
            </div>
          </div>
        )}

        {/* ── Tweet feed ──────────────────────────────────────────────────── */}
        <div style={styles.card}>
          <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem", flexWrap: "wrap", alignItems: "center" }}>
            {["all", "positive", "negative", "neutral"].map((f) => (
              <button key={f} onClick={() => setFilter(f)}
                      style={{
                        ...styles.filterBtn,
                        background: filter === f ? "#6366f1" : "#334155",
                        color:      filter === f ? "#fff"    : "#94a3b8",
                      }}>
                {f.charAt(0).toUpperCase() + f.slice(1)}
              </button>
            ))}
            <span style={{ color: "#475569", fontSize: "0.75rem", marginLeft: "auto" }}>
              {tweets.length} tweet{tweets.length !== 1 ? "s" : ""}
            </span>
          </div>
          <TweetFeed tweets={tweets} />
        </div>
      </div>
    </>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "#0f172a",
    color: "#e2e8f0",
    fontFamily: "'Segoe UI', system-ui, -apple-system, sans-serif",
    padding: "2rem",
    maxWidth: "1280px",
    margin: "0 auto",
  },
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
    marginBottom: "2rem",
    flexWrap: "wrap",
    gap: "1rem",
  },
  title:    { fontSize: "1.75rem", fontWeight: 800, color: "#f1f5f9", margin: 0 },
  subtitle: { color: "#64748b", fontSize: "0.85rem", margin: "0.25rem 0 0" },
  liveIndicator: {
    display: "inline-flex", alignItems: "center", gap: "0.4rem",
    background: "#14532d", border: "1px solid #22c55e", color: "#86efac",
    borderRadius: "999px", padding: "4px 12px", fontSize: "0.75rem", fontWeight: 700,
  },
  errorIndicator: {
    display: "inline-flex", alignItems: "center", gap: "0.4rem",
    background: "#7f1d1d", border: "1px solid #ef4444", color: "#fca5a5",
    borderRadius: "999px", padding: "4px 12px", fontSize: "0.75rem", fontWeight: 700,
  },
  liveDot: {
    width: "8px", height: "8px", background: "#22c55e",
    borderRadius: "50%", animation: "pulse 2s infinite", display: "inline-block",
  },
  errorDot: {
    width: "8px", height: "8px", background: "#ef4444",
    borderRadius: "50%", animation: "pulse 1s infinite", display: "inline-block",
  },
  lastUpdated: { color: "#64748b", fontSize: "0.75rem" },
  refreshBtn: {
    background: "#334155", border: "1px solid #475569", color: "#94a3b8",
    borderRadius: "6px", padding: "4px 12px", fontSize: "0.75rem",
    cursor: "pointer", fontWeight: 600,
  },
  errorBanner: {
    background: "#7f1d1d", border: "1px solid #ef4444", borderRadius: "6px",
    padding: "0.75rem 1rem", color: "#fca5a5", marginBottom: "1.5rem", fontSize: "0.85rem",
  },
  card: {
    background: "#1e293b", borderRadius: "8px",
    padding: "1.5rem", marginBottom: "2rem",
  },
  cardTitle: {
    color: "#e2e8f0", fontSize: "1rem", fontWeight: 600,
    marginBottom: "1rem", marginTop: 0,
  },
  rollingTile: {
    flex: "1 1 150px", background: "#0f172a",
    borderRadius: "8px", padding: "1rem",
  },
  hashtag: {
    background: "#334155", borderRadius: "4px",
    padding: "4px 10px", color: "#93c5fd", fontWeight: 600, cursor: "default",
  },
  hashtagCount: { color: "#64748b", fontSize: "0.75em" },
  filterBtn: {
    border: "none", borderRadius: "6px",
    padding: "6px 16px", cursor: "pointer", fontWeight: 600, fontSize: "0.85rem",
  },
};
