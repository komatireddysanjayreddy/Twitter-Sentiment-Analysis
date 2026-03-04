/**
 * GET /api/search
 *
 * Searches for tweets by hashtag.
 * Priority:
 *   1. Fetch LIVE tweets from TwitterAPI.io (if TWITTER_API_KEY is configured)
 *   2. Fall back to topic-relevant MOCK tweets if no key / API fails
 *   3. Merge both with existing DB rows for the same hashtags
 *   4. Score every new tweet with VADER, upsert into Neon
 *
 * Query params:
 *   hashtags  — comma-separated tag names, with or without #  (required)
 *   sentiment — positive | negative | neutral  (optional filter)
 *   limit     — max rows returned (default 50, max 200)
 *
 * Response:
 *   { data, count, summary, source }
 *   source = "live" | "mock" | "db"
 */

import { v4 as uuidv4 } from "uuid";
import Vader            from "vader-sentiment";
import { query as db }  from "../../lib/db";

const TWITTER_API_KEY   = process.env.TWITTER_API_KEY ?? "";
const TWITTERAPI_SEARCH = "https://api.twitterapi.io/twitter/tweet/advanced_search";

// ── VADER scoring ─────────────────────────────────────────────────────────────
function scoreText(text) {
  const r        = Vader.SentimentIntensityAnalyzer.polarity_scores(text);
  const compound = Math.round(r.compound * 10000) / 10000;
  const sentiment =
    compound >= 0.05 ? "positive" :
    compound <= -0.05 ? "negative" : "neutral";
  return { compound, sentiment };
}

// ── Live fetch from TwitterAPI.io ─────────────────────────────────────────────
async function fetchLive(hashtags) {
  const q   = hashtags.map((h) => `#${h}`).join(" OR ");
  const url = `${TWITTERAPI_SEARCH}?query=${encodeURIComponent(q)}&queryType=Latest&count=30`;

  const res = await fetch(url, {
    headers: { "X-API-Key": TWITTER_API_KEY },
    signal:  AbortSignal.timeout(8000),
  });

  if (!res.ok) throw new Error(`TwitterAPI.io returned ${res.status}`);
  const body = await res.json();

  // Handle both possible response shapes
  const raw = body.tweets ?? body.data ?? [];
  return raw.map((t) => {
    const { compound, sentiment } = scoreText(t.text ?? "");
    return {
      tweet_id:         t.id || t.id_str || uuidv4(),
      text:             t.text || t.full_text || "",
      author:           t.author?.userName || t.user?.screen_name || "unknown",
      author_followers: t.author?.followers || t.user?.followers_count || 0,
      lang:             t.lang || "en",
      tweet_created_at: t.createdAt || t.created_at || new Date().toISOString(),
      hashtags:         (t.entities?.hashtags ?? []).map((h) => h.text ?? h),
      hashtag_count:    (t.entities?.hashtags ?? []).length,
      retweet_count:    t.retweetCount  ?? t.retweet_count  ?? 0,
      like_count:       t.likeCount     ?? t.favorite_count ?? 0,
      compound_score:   compound,
      sentiment,
      source:           "twitterapi.io",
    };
  });
}

// ── Mock tweet generator (topic-aware) ────────────────────────────────────────
const MOCK_TEMPLATES = {
  bitcoin:      ["Bitcoin just hit a new high — crypto is unstoppable! 🚀", "Bitcoin crashing again. Bad day for crypto investors.", "Just watching Bitcoin charts. Market looks volatile today.", "Bitcoin adoption is growing fast across emerging markets.", "Sold my Bitcoin at the wrong time. Frustrating."],
  airevolution: ["The #AIRevolution is transforming every industry. Incredible times!", "AI is overhyped. Half these models can't reason properly.", "AI models are getting scarily good at creative tasks.", "The AI revolution is creating new jobs, not just destroying them.", "Worried about AI bias in hiring systems. Needs regulation."],
  breakingnews: ["Breaking news covered live with AI sentiment analysis. Game changer!", "Breaking news overload — too much noise, not enough signal.", "Real-time breaking news pipelines are the future of journalism.", "Hard to tell breaking news from misinformation these days.", "Breaking news alerts are destroying my focus. Need a filter."],
  crypto:       ["Crypto market is wild today. Up 20%, down 20%.", "Ethereum gas fees are insane. Need layer-2 solutions.", "DeFi is the future of finance, no doubt.", "Lost money on altcoins again. Sticking to Bitcoin.", "Crypto regulation uncertainty is hurting adoption."],
  python:       ["Python 3.12 performance improvements are incredible!", "Python is slowing down our ML pipeline. Need Rust.", "Just built a Kafka consumer in Python. Elegant and fast.", "Python type hints have saved me so many bugs.", "Python packaging is still a mess in 2025."],
  machinelearning: ["New transformer architecture beats GPT on benchmarks.", "ML models are impressive but still brittle at reasoning.", "Deployed my first ML model to production today!", "Training costs for large models are unsustainable.", "Transfer learning has changed everything in NLP."],
  default:      ["Really enjoying the conversation around this topic today.", "Mixed feelings about this — need more data to decide.", "This trend is fascinating to watch unfold in real time.", "Strong opinions on this. Hard to stay neutral.", "Interesting developments in this space lately."],
};

function getMockTemplates(hashtags) {
  const key = hashtags.map((h) => h.toLowerCase()).find((h) => MOCK_TEMPLATES[h]);
  return MOCK_TEMPLATES[key] ?? MOCK_TEMPLATES.default;
}

const MOCK_AUTHORS = [
  "dev_sanjay", "data_nerd_42", "kafka_queen", "spark_wizard",
  "ml_enthusiast", "cloud_architect", "crypto_watcher", "ai_tracker",
  "pipeline_pro", "realtime_data", "bytes_and_bits", "infra_ninja",
];

function generateMockTweets(hashtags, count = 20) {
  const templates = getMockTemplates(hashtags);
  const tweets    = [];

  for (let i = 0; i < count; i++) {
    const text   = templates[i % templates.length] + (i >= templates.length ? ` #${hashtags[0]}` : "");
    const { compound, sentiment } = scoreText(text);
    const jitterMs = Math.floor(Math.random() * 60_000);

    tweets.push({
      tweet_id:         uuidv4(),
      text,
      author:           MOCK_AUTHORS[Math.floor(Math.random() * MOCK_AUTHORS.length)],
      author_followers: Math.floor(Math.random() * 50_000) + 100,
      lang:             "en",
      tweet_created_at: new Date(Date.now() - jitterMs).toISOString(),
      hashtags:         [...hashtags, "trending"].slice(0, 3),
      hashtag_count:    Math.min(hashtags.length + 1, 3),
      retweet_count:    Math.floor(Math.random() * 500),
      like_count:       Math.floor(Math.random() * 2000),
      compound_score:   compound,
      sentiment,
      source:           "mock",
    });
  }

  return tweets;
}

// ── Upsert tweets into Neon ───────────────────────────────────────────────────
const UPSERT_SQL = `
  INSERT INTO tweet_sentiments
    (tweet_id, text, author, author_followers, lang, tweet_created_at,
     hashtags, hashtag_count, retweet_count, like_count,
     compound_score, sentiment, source)
  VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
  ON CONFLICT (tweet_id) DO NOTHING
`;

async function upsertTweets(tweets) {
  for (const t of tweets) {
    await db(UPSERT_SQL, [
      t.tweet_id, t.text, t.author, t.author_followers,
      t.lang, t.tweet_created_at, t.hashtags, t.hashtag_count,
      t.retweet_count, t.like_count, t.compound_score, t.sentiment, t.source,
    ]);
  }
}

// ── Summary helper ────────────────────────────────────────────────────────────
function buildSummary(rows) {
  const s = { total: rows.length, positive: 0, negative: 0, neutral: 0 };
  rows.forEach((r) => { s[r.sentiment] = (s[r.sentiment] ?? 0) + 1; });
  return s;
}

// ── Handler ───────────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  // Parse params
  const hashtagsRaw = req.query.hashtags ?? "";
  const hashtags = hashtagsRaw
    .split(",")
    .map((h) => h.trim().replace(/^#/, ""))
    .filter(Boolean);

  if (hashtags.length === 0) {
    return res.status(400).json({ error: "At least one hashtag is required" });
  }

  const sentiment = req.query.sentiment ?? null;
  const limit     = Math.min(parseInt(req.query.limit ?? "50", 10), 200);
  const validSentiments = ["positive", "negative", "neutral"];
  if (sentiment && !validSentiments.includes(sentiment)) {
    return res.status(400).json({ error: "Invalid sentiment filter" });
  }

  let freshTweets = [];
  let source      = "db";

  // ── Step 1: fetch fresh tweets (live or mock) ─────────────────────────────
  const hasLiveKey = TWITTER_API_KEY && !TWITTER_API_KEY.includes("your_") && TWITTER_API_KEY.length > 10;

  if (hasLiveKey) {
    try {
      freshTweets = await fetchLive(hashtags);
      source      = "live";
    } catch (err) {
      console.warn("[/api/search] Live fetch failed, falling back to mock:", err.message);
      freshTweets = generateMockTweets(hashtags);
      source      = "mock";
    }
  } else {
    freshTweets = generateMockTweets(hashtags);
    source      = "mock";
  }

  // ── Step 2: score + upsert fresh tweets into Neon ────────────────────────
  try {
    await upsertTweets(freshTweets);
  } catch (err) {
    console.error("[/api/search] DB upsert failed:", err.message);
    // Non-fatal — still return results
  }

  // ── Step 3: query full result set from Neon ───────────────────────────────
  try {
    const params     = [hashtags];
    const conditions = [`hashtags && $1::text[]`];

    if (sentiment) {
      params.push(sentiment);
      conditions.push(`sentiment = $${params.length}`);
    }

    params.push(limit);
    const rows = await db(
      `SELECT tweet_id, text, author, author_followers, hashtags,
              compound_score, sentiment, retweet_count, like_count,
              processed_at, source
       FROM tweet_sentiments
       WHERE ${conditions.join(" AND ")}
       ORDER BY processed_at DESC
       LIMIT $${params.length}`,
      params,
    );

    // Summary from the full matching set (ignoring limit)
    const countRows = await db(
      `SELECT sentiment, COUNT(*) AS cnt
       FROM tweet_sentiments
       WHERE hashtags && $1::text[]
       ${sentiment ? "AND sentiment = $2" : ""}
       GROUP BY sentiment`,
      sentiment ? [hashtags, sentiment] : [hashtags],
    );
    const summary = { total: 0, positive: 0, negative: 0, neutral: 0 };
    countRows.forEach((r) => {
      const n = parseInt(r.cnt, 10);
      summary[r.sentiment] = n;
      summary.total += n;
    });

    res.setHeader("Cache-Control", "no-store");
    return res.status(200).json({ data: rows, count: rows.length, summary, source });
  } catch (err) {
    console.error("[/api/search] DB query failed:", err.message);
    // Final fallback — return just the fresh tweets scored in memory
    const filtered = sentiment
      ? freshTweets.filter((t) => t.sentiment === sentiment)
      : freshTweets;
    return res.status(200).json({
      data:    filtered.slice(0, limit),
      count:   filtered.length,
      summary: buildSummary(freshTweets),
      source,
    });
  }
}
