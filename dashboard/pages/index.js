/**
 * pages/index.js — Hashtag sentiment search dashboard with live streaming.
 *
 * User flow:
 *   1. Type one or more hashtags (e.g. "AIRevolution, Bitcoin")
 *   2. Optionally pick a sentiment pill (All / Positive / Negative / Neutral)
 *   3. Hit Enter or click Search
 *   4. /api/search fetches LIVE tweets from TwitterAPI.io (or topical mock),
 *      scores with VADER, saves to Neon, returns the full matching set.
 *   5. While the page is open, polls every 5 s for new tweets and updates
 *      the sentiment trend chart in real time.
 */

import { useState, useRef, useEffect, useCallback } from "react";
import Head from "next/head";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";

const POLL_INTERVAL = 5000; // ms between live-poll requests

const SENTIMENT_COLORS = {
  positive: { bg: "#14532d", border: "#22c55e", text: "#86efac", dot: "#22c55e", chart: "#22c55e" },
  negative: { bg: "#7f1d1d", border: "#ef4444", text: "#fca5a5", dot: "#ef4444", chart: "#ef4444" },
  neutral:  { bg: "#1c1917", border: "#f59e0b", text: "#fcd34d", dot: "#f59e0b", chart: "#f59e0b" },
};

const SUGGESTED_TAGS = [
  "AIRevolution", "Bitcoin", "BreakingNews", "python", "kafka",
  "crypto", "machinelearning", "spark", "devops", "tech",
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function bucketKey(isoDate) {
  // "HH:MM" bucket from ISO timestamp
  const d = new Date(isoDate);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function buildChartData(tweets) {
  // Aggregate tweet counts by minute-bucket and sentiment
  const map = {};
  for (const t of tweets) {
    const key = bucketKey(t.processed_at || new Date().toISOString());
    if (!map[key]) map[key] = { time: key, positive: 0, negative: 0, neutral: 0 };
    map[key][t.sentiment] = (map[key][t.sentiment] || 0) + 1;
  }
  return Object.values(map).sort((a, b) => a.time.localeCompare(b.time));
}

// ── Tweet card ────────────────────────────────────────────────────────────────
function TweetCard({ tweet, isNew }) {
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
      transition: "opacity 0.4s",
      animation: isNew ? "fadeIn 0.5s ease" : "none",
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

      <div style={{ color: "#475569", fontSize: "0.72rem", display: "flex", gap: "1rem", alignItems: "center" }}>
        <span>🔁 {tweet.retweet_count}</span>
        <span>❤️ {tweet.like_count}</span>
        <span>{time}</span>
        {tweet.source && (
          <span style={{
            marginLeft: "auto", fontSize: "0.65rem", padding: "1px 7px",
            borderRadius: "999px", background: "#0f172a", border: "1px solid #334155", color: "#475569",
          }}>
            {tweet.source === "twitterapi.io" ? "LIVE" : tweet.source.toUpperCase()}
          </span>
        )}
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

// ── Sentiment trend chart ─────────────────────────────────────────────────────
function SentimentChart({ chartData, isLive }) {
  if (!chartData || chartData.length === 0) return null;

  return (
    <div style={{
      background: "#1e293b",
      border: "1px solid #334155",
      borderRadius: "12px",
      padding: "1.25rem",
      marginBottom: "1.5rem",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "1rem" }}>
        <span style={{ color: "#94a3b8", fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
          Sentiment Trend
        </span>
        {isLive && (
          <span style={{
            display: "flex", alignItems: "center", gap: "0.3rem",
            fontSize: "0.65rem", fontWeight: 700, color: "#86efac",
            background: "#14532d", border: "1px solid #22c55e",
            borderRadius: "999px", padding: "1px 8px",
          }}>
            <span style={{
              width: "6px", height: "6px", borderRadius: "50%",
              background: "#22c55e", display: "inline-block",
              animation: "pulse 1.5s infinite",
            }} />
            LIVE
          </span>
        )}
        <span style={{ marginLeft: "auto", color: "#475569", fontSize: "0.7rem" }}>tweets per minute</span>
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={chartData} margin={{ top: 5, right: 10, left: -20, bottom: 0 }}>
          <defs>
            <linearGradient id="posGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="negGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="neuGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e3a5f" />
          <XAxis dataKey="time" tick={{ fill: "#475569", fontSize: 11 }} tickLine={false} />
          <YAxis tick={{ fill: "#475569", fontSize: 11 }} tickLine={false} axisLine={false} allowDecimals={false} />
          <Tooltip
            contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: "6px", fontSize: "0.8rem" }}
            labelStyle={{ color: "#94a3b8" }}
          />
          <Legend wrapperStyle={{ fontSize: "0.75rem", paddingTop: "0.5rem" }} />
          <Area type="monotone" dataKey="positive" stroke="#22c55e" fill="url(#posGrad)" strokeWidth={2} dot={false} name="Positive" />
          <Area type="monotone" dataKey="negative" stroke="#ef4444" fill="url(#negGrad)" strokeWidth={2} dot={false} name="Negative" />
          <Area type="monotone" dataKey="neutral"  stroke="#f59e0b" fill="url(#neuGrad)" strokeWidth={2} dot={false} name="Neutral" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [inputValue,  setInputValue]  = useState("");
  const [filter,      setFilter]      = useState("all");
  const [tweets,      setTweets]      = useState([]);
  const [newTweetIds, setNewTweetIds] = useState(new Set());
  const [seenIds,     setSeenIds]     = useState(new Set());
  const [summary,     setSummary]     = useState(null);
  const [source,      setSource]      = useState(null);
  const [loading,     setLoading]     = useState(false);
  const [error,       setError]       = useState(null);
  const [searched,    setSearched]    = useState(false);
  const [activeQuery, setActiveQuery] = useState("");
  const [isLive,      setIsLive]      = useState(false);
  const [chartData,   setChartData]   = useState([]);
  const [pollCount,   setPollCount]   = useState(0);

  const inputRef   = useRef(null);
  const pollRef    = useRef(null);    // interval handle
  const allTweets  = useRef([]);      // full accumulation across polls
  const activeSeenIds = useRef(new Set());

  // ── Stop polling ─────────────────────────────────────────────────────────
  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    setIsLive(false);
  }, []);

  // Cleanup on unmount
  useEffect(() => () => stopPolling(), [stopPolling]);

  // ── Single fetch ──────────────────────────────────────────────────────────
  const fetchTweets = useCallback(async (hashtagStr, sentimentFilter, isInitial = false) => {
    const tags = hashtagStr.trim();
    if (!tags) return;

    if (isInitial) {
      setLoading(true);
      setError(null);
    }

    const params = new URLSearchParams({ limit: "100" });
    params.set("hashtags", tags);
    if (sentimentFilter !== "all") params.set("sentiment", sentimentFilter);

    try {
      const res  = await fetch(`/api/search?${params}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const json = await res.json();

      const incoming = json.data ?? [];
      const freshOnes = incoming.filter((t) => !activeSeenIds.current.has(t.tweet_id));

      if (freshOnes.length > 0 || isInitial) {
        freshOnes.forEach((t) => activeSeenIds.current.add(t.tweet_id));

        // Merge: prepend fresh tweets, keep accumulation sorted newest-first
        allTweets.current = [
          ...freshOnes,
          ...allTweets.current.filter(
            (t) => !freshOnes.some((f) => f.tweet_id === t.tweet_id)
          ),
        ].slice(0, 200);

        setTweets([...allTweets.current]);
        setNewTweetIds(new Set(freshOnes.map((t) => t.tweet_id)));
        setSummary(json.summary ?? null);
        setSource(json.source ?? "db");
        setPollCount((c) => c + 1);

        // Rebuild chart from full accumulation
        setChartData(buildChartData(allTweets.current));
      }
    } catch (e) {
      if (isInitial) setError(e.message);
      console.error("[poll]", e.message);
    } finally {
      if (isInitial) setLoading(false);
    }
  }, []);

  // ── Start a search (resets everything) ───────────────────────────────────
  const doSearch = useCallback(async (hashtagStr, sentimentFilter) => {
    stopPolling();

    const tags = hashtagStr.trim();
    setSearched(true);
    setActiveQuery(tags || "all tweets");
    setTweets([]);
    setSummary(null);
    setChartData([]);
    setPollCount(0);
    allTweets.current = [];
    activeSeenIds.current = new Set();

    if (!tags) return;

    // Initial fetch (shows loading spinner)
    await fetchTweets(tags, sentimentFilter, true);
    setIsLive(true);

    // Start polling
    pollRef.current = setInterval(
      () => fetchTweets(tags, sentimentFilter, false),
      POLL_INTERVAL
    );
  }, [fetchTweets, stopPolling]);

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
      setInputValue([...current, tag].join(", "));
    }
  };

  const handleStop = () => stopPolling();

  return (
    <>
      <Head>
        <title>Twitter Sentiment — Live</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>{`
          @keyframes fadeIn { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; transform: translateY(0); } }
          @keyframes pulse  { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
        `}</style>
      </Head>

      <div style={styles.page}>
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <header style={styles.header}>
          <div>
            <h1 style={styles.title}>Twitter Sentiment Analysis</h1>
            <p style={styles.subtitle}>Search hashtags · Live tweets · VADER · Neon PostgreSQL</p>
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
            {isLive && (
              <button onClick={handleStop} style={styles.stopBtn} title="Stop live updates">
                ■ Stop
              </button>
            )}
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
        {error && <div style={styles.errorBanner}>⚠ {error}</div>}

        {/* ── Empty state ─────────────────────────────────────────────────── */}
        {!searched && !loading && (
          <div style={styles.emptyState}>
            <div style={{ fontSize: "3rem", marginBottom: "1rem" }}>🔍</div>
            <div style={{ color: "#94a3b8", fontSize: "1.1rem" }}>
              Enter a hashtag above and click <strong style={{ color: "#93c5fd" }}>Search</strong> to explore tweet sentiments live.
            </div>
            <div style={{ color: "#475569", fontSize: "0.85rem", marginTop: "0.5rem" }}>
              Tweets refresh every {POLL_INTERVAL / 1000}s while the page is open.
            </div>
          </div>
        )}

        {/* ── Results area ────────────────────────────────────────────────── */}
        {searched && (
          <>
            {/* Results header */}
            <div style={{ marginBottom: "1rem", display: "flex", alignItems: "center", gap: "0.75rem", flexWrap: "wrap" }}>
              <span style={{ color: "#64748b", fontSize: "0.85rem" }}>
                {loading ? "Fetching tweets…" : (
                  <>
                    <span style={{ color: "#93c5fd", fontWeight: 700 }}>{summary?.total ?? tweets.length}</span> tweets for{" "}
                    <span style={{ color: "#fcd34d" }}>"{activeQuery}"</span>
                    {filter !== "all" && <> · <span style={{ color: SENTIMENT_COLORS[filter]?.text }}>{filter} only</span></>}
                  </>
                )}
              </span>

              {/* Source badge */}
              {!loading && source && (
                <span style={{
                  fontSize: "0.7rem", fontWeight: 700, padding: "2px 10px",
                  borderRadius: "999px", textTransform: "uppercase",
                  ...(source === "live"
                    ? { background: "#14532d", border: "1px solid #22c55e", color: "#86efac" }
                    : source === "mock"
                    ? { background: "#1e3a5f", border: "1px solid #6366f1", color: "#a5b4fc" }
                    : { background: "#1c1917", border: "1px solid #78716c", color: "#a8a29e" }),
                }}>
                  {source === "live" ? "● LIVE" : source === "mock" ? "◎ MOCK" : "◉ DB"}
                </span>
              )}

              {/* Live indicator */}
              {isLive && (
                <span style={{
                  display: "flex", alignItems: "center", gap: "0.3rem",
                  fontSize: "0.7rem", fontWeight: 700, color: "#86efac",
                  background: "#14532d", border: "1px solid #22c55e",
                  borderRadius: "999px", padding: "2px 10px",
                }}>
                  <span style={{
                    width: "6px", height: "6px", borderRadius: "50%",
                    background: "#22c55e", display: "inline-block",
                    animation: "pulse 1.5s infinite",
                  }} />
                  Streaming · poll #{pollCount}
                </span>
              )}
            </div>

            {/* Summary bar */}
            <SummaryBar summary={summary} loading={loading} />

            {/* Live sentiment chart */}
            <SentimentChart chartData={chartData} isLive={isLive} />

            {/* Tweet list */}
            {!loading && tweets.length === 0 && (
              <div style={styles.emptyState}>
                <div style={{ fontSize: "2rem", marginBottom: "0.75rem" }}>🪹</div>
                <div style={{ color: "#94a3b8" }}>No tweets found. Try different hashtags or remove the sentiment filter.</div>
              </div>
            )}

            {tweets.map((t) => (
              <TweetCard key={t.tweet_id} tweet={t} isNew={newTweetIds.has(t.tweet_id)} />
            ))}
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
  header: { marginBottom: "2rem" },
  title: { fontSize: "1.75rem", fontWeight: 800, color: "#f1f5f9", margin: 0 },
  subtitle: { color: "#475569", fontSize: "0.8rem", margin: "0.25rem 0 0" },
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
  searchRow: { display: "flex", gap: "0.75rem" },
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
  stopBtn: {
    background: "#7f1d1d",
    border: "1px solid #ef4444",
    borderRadius: "8px",
    padding: "0.65rem 1rem",
    color: "#fca5a5",
    fontWeight: 700,
    fontSize: "0.85rem",
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
