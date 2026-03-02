/**
 * TweetFeed — Scrollable live card feed of the most recent tweets.
 */

const SENTIMENT_COLORS = {
  positive: { bg: "#14532d", border: "#22c55e", text: "#86efac" },
  negative: { bg: "#7f1d1d", border: "#ef4444", text: "#fca5a5" },
  neutral:  { bg: "#78350f", border: "#f59e0b", text: "#fde68a" },
};

function SentimentBadge({ sentiment }) {
  const colors = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.neutral;
  return (
    <span style={{
      background: colors.bg,
      border: `1px solid ${colors.border}`,
      color: colors.text,
      borderRadius: "999px",
      padding: "2px 10px",
      fontSize: "0.7rem",
      fontWeight: 700,
      textTransform: "uppercase",
      letterSpacing: "0.05em",
    }}>
      {sentiment}
    </span>
  );
}

function TweetCard({ tweet }) {
  const colors = SENTIMENT_COLORS[tweet.sentiment] || SENTIMENT_COLORS.neutral;
  return (
    <div style={{
      background: "#1e293b",
      borderLeft: `4px solid ${colors.border}`,
      borderRadius: "8px",
      padding: "1rem",
      marginBottom: "0.75rem",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.5rem" }}>
        <span style={{ color: "#60a5fa", fontWeight: 600 }}>@{tweet.author}</span>
        <SentimentBadge sentiment={tweet.sentiment} />
      </div>
      <p style={{ color: "#e2e8f0", fontSize: "0.9rem", margin: "0 0 0.5rem" }}>
        {tweet.text}
      </p>
      <div style={{ display: "flex", gap: "1rem", fontSize: "0.75rem", color: "#64748b" }}>
        <span>Score: {Number(tweet.compound_score).toFixed(3)}</span>
        <span>RT: {tweet.retweet_count}</span>
        <span>Likes: {tweet.like_count}</span>
        {tweet.hashtags?.length > 0 && (
          <span>{tweet.hashtags.map(h => `#${h}`).join(" ")}</span>
        )}
        <span style={{ marginLeft: "auto" }}>
          {new Date(tweet.processed_at).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}

export default function TweetFeed({ tweets }) {
  if (!tweets?.length) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: "3rem" }}>
        No tweets yet. Start the producer to begin streaming.
      </div>
    );
  }

  return (
    <div>
      <h2 style={{ color: "#e2e8f0", fontSize: "1rem", fontWeight: 600, marginBottom: "1rem" }}>
        Live Tweet Feed ({tweets.length} tweets)
      </h2>
      <div style={{ maxHeight: "600px", overflowY: "auto", paddingRight: "4px" }}>
        {tweets.map((tweet) => (
          <TweetCard key={tweet.tweet_id} tweet={tweet} />
        ))}
      </div>
    </div>
  );
}
