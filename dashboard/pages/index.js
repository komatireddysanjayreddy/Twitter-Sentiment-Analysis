/**
 * pages/index.js — Main dashboard page.
 *
 * Polls /api/stats and /api/sentiments every 10 seconds
 * for near-real-time updates without a WebSocket.
 */

import { useState, useEffect } from "react";
import Head from "next/head";
import StatsBar from "../components/StatsBar";
import SentimentChart from "../components/SentimentChart";
import TweetFeed from "../components/TweetFeed";

const POLL_INTERVAL_MS = 10_000;

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

export default function Dashboard() {
  const [stats, setStats]       = useState(null);
  const [tweets, setTweets]     = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError]       = useState(null);
  const [filter, setFilter]     = useState("all");

  const loadData = async () => {
    try {
      const sentimentParam = filter !== "all" ? `&sentiment=${filter}` : "";
      const [statsData, tweetsData] = await Promise.all([
        fetchJSON("/api/stats"),
        fetchJSON(`/api/sentiments?limit=50${sentimentParam}`),
      ]);
      setStats(statsData);
      setTweets(tweetsData.data ?? []);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      setError(err.message);
    }
  };

  // Initial load + polling
  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [filter]);

  return (
    <>
      <Head>
        <title>Twitter Sentiment Analysis</title>
        <meta name="description" content="Real-time Twitter sentiment dashboard" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      <div style={styles.page}>
        {/* Header */}
        <header style={styles.header}>
          <div>
            <h1 style={styles.title}>Twitter Sentiment Analysis</h1>
            <p style={styles.subtitle}>
              Real-time stream — Kafka + PySpark + VADER
            </p>
          </div>
          <div style={{ textAlign: "right" }}>
            <div style={styles.liveIndicator}>
              <span style={styles.liveDot} />
              LIVE
            </div>
            {lastUpdated && (
              <div style={styles.lastUpdated}>
                Updated {lastUpdated.toLocaleTimeString()}
              </div>
            )}
          </div>
        </header>

        {/* Error banner */}
        {error && (
          <div style={styles.errorBanner}>
            Connection error: {error}. Retrying every {POLL_INTERVAL_MS / 1000}s…
          </div>
        )}

        {/* Stats bar */}
        <StatsBar summary={stats?.summary} />

        {/* Time-series chart */}
        <SentimentChart timeSeries={stats?.timeSeries ?? []} />

        {/* Hashtag cloud */}
        {stats?.hashtags?.length > 0 && (
          <div style={styles.card}>
            <h2 style={styles.cardTitle}>Trending Hashtags (24h)</h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
              {stats.hashtags.slice(0, 20).map((h) => (
                <span
                  key={h.hashtag}
                  style={{
                    ...styles.hashtag,
                    fontSize: `${Math.min(0.75 + h.mention_count / 20, 1.3)}rem`,
                  }}
                >
                  #{h.hashtag}
                  <span style={styles.hashtagCount}> {h.mention_count}</span>
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Filter + Live Feed */}
        <div style={styles.card}>
          <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem", flexWrap: "wrap" }}>
            {["all", "positive", "negative", "neutral"].map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                style={{
                  ...styles.filterBtn,
                  background: filter === f ? "#6366f1" : "#334155",
                  color: filter === f ? "#fff" : "#94a3b8",
                }}
              >
                {f.charAt(0).toUpperCase() + f.slice(1)}
              </button>
            ))}
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
  title: {
    fontSize: "1.75rem",
    fontWeight: 800,
    color: "#f1f5f9",
    margin: 0,
  },
  subtitle: {
    color: "#64748b",
    fontSize: "0.85rem",
    margin: "0.25rem 0 0",
  },
  liveIndicator: {
    display: "inline-flex",
    alignItems: "center",
    gap: "0.4rem",
    background: "#14532d",
    border: "1px solid #22c55e",
    color: "#86efac",
    borderRadius: "999px",
    padding: "4px 12px",
    fontSize: "0.75rem",
    fontWeight: 700,
  },
  liveDot: {
    width: "8px",
    height: "8px",
    background: "#22c55e",
    borderRadius: "50%",
    animation: "pulse 2s infinite",
    display: "inline-block",
  },
  lastUpdated: { color: "#64748b", fontSize: "0.75rem", marginTop: "0.25rem" },
  errorBanner: {
    background: "#7f1d1d",
    border: "1px solid #ef4444",
    borderRadius: "6px",
    padding: "0.75rem 1rem",
    color: "#fca5a5",
    marginBottom: "1.5rem",
    fontSize: "0.85rem",
  },
  card: {
    background: "#1e293b",
    borderRadius: "8px",
    padding: "1.5rem",
    marginBottom: "2rem",
  },
  cardTitle: {
    color: "#e2e8f0",
    fontSize: "1rem",
    fontWeight: 600,
    marginBottom: "1rem",
    marginTop: 0,
  },
  hashtag: {
    background: "#334155",
    borderRadius: "4px",
    padding: "4px 10px",
    color: "#93c5fd",
    fontWeight: 600,
    cursor: "default",
  },
  hashtagCount: { color: "#64748b", fontSize: "0.75em" },
  filterBtn: {
    border: "none",
    borderRadius: "6px",
    padding: "6px 16px",
    cursor: "pointer",
    fontWeight: 600,
    fontSize: "0.85rem",
  },
};
