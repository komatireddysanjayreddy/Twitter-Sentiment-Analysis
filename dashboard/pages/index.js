/**
 * pages/index.js — Hashtag sentiment search dashboard.
 *
 * User flow:
 *   1. Type one or more hashtags in the search box (e.g. "AIRevolution, Bitcoin")
 *   2. Optionally pick a sentiment pill (All / Positive / Negative / Neutral)
 *   3. Hit Enter or click Search
 *   4. See matching tweets + live sentiment breakdown
 *
 * No continuous producer required — queries the existing Neon DB on demand.
 */

import { useState, useRef } from "react";
import Head from "next/head";

const SENTIMENT_COLORS = {
  positive: { bg: "#14532d", border: "#22c55e", text: "#86efac", dot: "#22c55e" },
  negative: { bg: "#7f1d1d", border: "#ef4444", text: "#fca5a5", dot: "#ef4444" },
  neutral:  { bg: "#1c1917", border: "#f59e0b", text: "#fcd34d", dot: "#f59e0b" },
};

const SUGGESTED_TAGS = [
  "AIRevolution", "Bitcoin", "BreakingNews", "python", "kafka",
  "crypto", "machinelearning", "spark", "devops", "tech",
];

// ── Tweet card ────────────────────────────────────────────────────────────────
function TweetCard({ tweet }) {
  const c = SENTIMENT_COLORS[tweet.sentiment] ?? SENTIMENT_COLORS.neutral;
  const score = Number(tweet.compound_score ?? 0).toFixed(3);
  const time = new Date(tweet.processed_at).toLocaleString();

  return (
    <div style={{
      background: "#1e293b",
      border: `1px solid ${c.border}`,
      borderLeft: `4px solid ${c.border}`,
      borderRadius: "8px",
      padding: "1rem 1.25rem",
      marginBottom: "0.75rem",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "0.5rem", marginBottom: "0.5rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ fontWeight: 700, color: "#93c5fd" }}>@{tweet.author}</span>
          <span style={{ color: "#475569", fontSize: "0.75rem" }}>{tweet.author_followers?.toLocaleString()} followers</span>
        </div>
        <span style={{
          background: c.bg, border: `1px solid ${c.border}`, color: c.text,
          borderRadius: "999px", padding: "2px 10px", fontSize: "0.7rem", fontWeight: 700,
          whiteSpace: "nowrap", textTransform: "uppercase",
        }}>
          {tweet.sentiment} {score}
        </span>
      </div>

      <p style={{ color: "#e2e8f0", margin: "0 0 0.6rem", lineHeight: 1.5, fontSize: "0.95rem" }}>
        {tweet.text}
      </p>

      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", marginBottom: "0.5rem" }}>
        {(tweet.hashtags ?? []).map((h) => (
          <span key={h} style={{
            background: "#334155", borderRadius: "4px",
            padding: "2px 8px", color: "#93c5fd", fontSize: "0.75rem", fontWeight: 600,
          }}>#{h}</span>
        ))}
      </div>

      <div style={{ color: "#475569", fontSize: "0.72rem", display: "flex", gap: "1rem" }}>
        <span>🔁 {tweet.retweet_count}</span>
        <span>❤️ {tweet.like_count}</span>
        <span>{time}</span>
      </div>
    </div>
  );
}

// ── Summary bar ───────────────────────────────────────────────────────────────
function SummaryBar({ summary, loading }) {
  if (!summary && !loading) return null;

  const pct = (n) =>
    summary?.total ? Math.round((100 * n) / summary.total) : 0;

  return (
    <div style={{
      display: "flex", flexWrap: "wrap", gap: "0.75rem",
      marginBottom: "1.5rem",
    }}>
      {[
        { label: "Total",    value: summary?.total    ?? "–", color: "#93c5fd", bg: "#1e3a5f" },
        { label: "Positive", value: summary?.positive ?? "–", sub: pct(summary?.positive) + "%", color: "#86efac", bg: "#14532d" },
        { label: "Negative", value: summary?.negative ?? "–", sub: pct(summary?.negative) + "%", color: "#fca5a5", bg: "#7f1d1d" },
        { label: "Neutral",  value: summary?.neutral  ?? "–", sub: pct(summary?.neutral)  + "%", color: "#fcd34d", bg: "#1c1917" },
      ].map(({ label, value, sub, color, bg }) => (
        <div key={label} style={{
          flex: "1 1 120px", background: bg,
          borderRadius: "8px", padding: "0.8rem 1rem",
          minWidth: "100px",
        }}>
          <div style={{ color: "#94a3b8", fontSize: "0.7rem", marginBottom: "0.25rem", textTransform: "uppercase", letterSpacing: "0.05em" }}>{label}</div>
          <div style={{ fontSize: "1.6rem", fontWeight: 800, color, lineHeight: 1 }}>{loading ? "…" : value}</div>
          {sub && !loading && <div style={{ color: "#64748b", fontSize: "0.75rem", marginTop: "0.2rem" }}>{sub}</div>}
        </div>
      ))}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [inputValue,   setInputValue]   = useState("");
  const [filter,       setFilter]       = useState("all");
  const [tweets,       setTweets]       = useState([]);
  const [summary,      setSummary]      = useState(null);
  const [loading,      setLoading]      = useState(false);
  const [error,        setError]        = useState(null);
  const [searched,     setSearched]     = useState(false);
  const [activeQuery,  setActiveQuery]  = useState("");
  const inputRef = useRef(null);

  const doSearch = async (hashtagStr, sentimentFilter) => {
    const tags = hashtagStr.trim();
    setLoading(true);
    setError(null);
    setSearched(true);
    setActiveQuery(tags || "all tweets");

    const params = new URLSearchParams({ limit: "100" });
    if (tags) params.set("hashtags", tags);
    if (sentimentFilter !== "all") params.set("sentiment", sentimentFilter);

    try {
      const res  = await fetch(`/api/sentiments?${params}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const json = await res.json();
      setTweets(json.data ?? []);
      setSummary(json.summary ?? null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => doSearch(inputValue, filter);

  const handleKeyDown = (e) => {
    if (e.key === "Enter") handleSearch();
  };

  const handleSentimentChange = (s) => {
    setFilter(s);
    if (searched) doSearch(inputValue, s);
  };

  const handleSuggestedTag = (tag) => {
    const current = inputValue.split(",").map((t) => t.trim()).filter(Boolean);
    if (!current.includes(tag)) {
      const next = [...current, tag].join(", ");
      setInputValue(next);
    }
  };

  return (
    <>
      <Head>
        <title>Twitter Sentiment Search</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      <div style={styles.page}>
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <header style={styles.header}>
          <div>
            <h1 style={styles.title}>Twitter Sentiment Analysis</h1>
            <p style={styles.subtitle}>Search tweets by hashtag · Kafka · PySpark · VADER · Neon PostgreSQL</p>
          </div>
        </header>

        {/* ── Search box ──────────────────────────────────────────────────── */}
        <div style={styles.searchCard}>
          <label style={styles.searchLabel}>Search by Hashtags</label>
          <div style={styles.searchRow}>
            <input
              ref={inputRef}
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="e.g. AIRevolution, Bitcoin, BreakingNews"
              style={styles.searchInput}
            />
            <button onClick={handleSearch} style={styles.searchBtn} disabled={loading}>
              {loading ? "Searching…" : "Search"}
            </button>
          </div>

          {/* Suggested tags */}
          <div style={{ marginTop: "0.75rem", display: "flex", flexWrap: "wrap", gap: "0.4rem", alignItems: "center" }}>
            <span style={{ color: "#475569", fontSize: "0.75rem" }}>Quick add:</span>
            {SUGGESTED_TAGS.map((tag) => (
              <button key={tag} onClick={() => handleSuggestedTag(tag)} style={styles.suggestBtn}>
                #{tag}
              </button>
            ))}
          </div>

          {/* Sentiment filter */}
          <div style={{ marginTop: "1rem", display: "flex", gap: "0.5rem", flexWrap: "wrap", alignItems: "center" }}>
            <span style={{ color: "#475569", fontSize: "0.75rem" }}>Sentiment:</span>
            {["all", "positive", "negative", "neutral"].map((s) => {
              const active = filter === s;
              const c = s !== "all" ? SENTIMENT_COLORS[s] : null;
              return (
                <button key={s} onClick={() => handleSentimentChange(s)} style={{
                  ...styles.filterBtn,
                  background: active ? (c?.bg ?? "#1e3a5f") : "#1e293b",
                  border:     `1px solid ${active ? (c?.border ?? "#6366f1") : "#334155"}`,
                  color:      active ? (c?.text ?? "#93c5fd") : "#94a3b8",
                  fontWeight: active ? 700 : 400,
                }}>
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              );
            })}
          </div>
        </div>

        {/* ── Error ───────────────────────────────────────────────────────── */}
        {error && (
          <div style={styles.errorBanner}>⚠ {error}</div>
        )}

        {/* ── Results area ────────────────────────────────────────────────── */}
        {!searched && !loading && (
          <div style={styles.emptyState}>
            <div style={{ fontSize: "3rem", marginBottom: "1rem" }}>🔍</div>
            <div style={{ color: "#94a3b8", fontSize: "1.1rem" }}>
              Enter a hashtag above and click <strong style={{ color: "#93c5fd" }}>Search</strong> to explore tweet sentiments.
            </div>
            <div style={{ color: "#475569", fontSize: "0.85rem", marginTop: "0.5rem" }}>
              Try: #AIRevolution · #Bitcoin · #BreakingNews · #python
            </div>
          </div>
        )}

        {searched && (
          <>
            {/* Results header */}
            <div style={{ marginBottom: "1rem", color: "#64748b", fontSize: "0.85rem" }}>
              {loading ? "Searching…" : (
                <>
                  <span style={{ color: "#93c5fd", fontWeight: 700 }}>{summary?.total ?? tweets.length}</span> tweets matched
                  {activeQuery !== "all tweets" && <> for <span style={{ color: "#fcd34d" }}>"{activeQuery}"</span></>}
                  {filter !== "all" && <> · filtered to <span style={{ color: SENTIMENT_COLORS[filter]?.text }}>{filter}</span></>}
                </>
              )}
            </div>

            {/* Summary bar */}
            <SummaryBar summary={summary} loading={loading} />

            {/* Tweet list */}
            {!loading && tweets.length === 0 && (
              <div style={styles.emptyState}>
                <div style={{ fontSize: "2rem", marginBottom: "0.75rem" }}>🪹</div>
                <div style={{ color: "#94a3b8" }}>No tweets found. Try different hashtags or remove the sentiment filter.</div>
              </div>
            )}

            {!loading && tweets.map((t) => <TweetCard key={t.tweet_id} tweet={t} />)}
          </>
        )}
      </div>
    </>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const styles = {
  page: {
    minHeight: "100vh",
    background: "#0f172a",
    color: "#e2e8f0",
    fontFamily: "'Segoe UI', system-ui, -apple-system, sans-serif",
    padding: "2rem",
    maxWidth: "900px",
    margin: "0 auto",
  },
  header: {
    marginBottom: "2rem",
  },
  title: {
    fontSize: "1.75rem", fontWeight: 800, color: "#f1f5f9", margin: 0,
  },
  subtitle: {
    color: "#475569", fontSize: "0.8rem", margin: "0.25rem 0 0",
  },
  searchCard: {
    background: "#1e293b",
    borderRadius: "12px",
    padding: "1.5rem",
    marginBottom: "2rem",
    border: "1px solid #334155",
  },
  searchLabel: {
    display: "block",
    color: "#94a3b8", fontSize: "0.8rem", fontWeight: 600,
    textTransform: "uppercase", letterSpacing: "0.05em",
    marginBottom: "0.6rem",
  },
  searchRow: {
    display: "flex", gap: "0.75rem",
  },
  searchInput: {
    flex: 1,
    background: "#0f172a",
    border: "1px solid #334155",
    borderRadius: "8px",
    padding: "0.65rem 1rem",
    color: "#f1f5f9",
    fontSize: "0.95rem",
    outline: "none",
  },
  searchBtn: {
    background: "#6366f1",
    border: "none",
    borderRadius: "8px",
    padding: "0.65rem 1.5rem",
    color: "#fff",
    fontWeight: 700,
    fontSize: "0.9rem",
    cursor: "pointer",
    whiteSpace: "nowrap",
  },
  suggestBtn: {
    background: "#0f172a",
    border: "1px solid #334155",
    borderRadius: "6px",
    padding: "3px 10px",
    color: "#64748b",
    fontSize: "0.75rem",
    cursor: "pointer",
  },
  filterBtn: {
    borderRadius: "999px",
    padding: "5px 16px",
    cursor: "pointer",
    fontSize: "0.8rem",
  },
  errorBanner: {
    background: "#7f1d1d", border: "1px solid #ef4444",
    borderRadius: "6px", padding: "0.75rem 1rem",
    color: "#fca5a5", marginBottom: "1.5rem", fontSize: "0.85rem",
  },
  emptyState: {
    textAlign: "center",
    padding: "4rem 1rem",
    color: "#475569",
  },
};
