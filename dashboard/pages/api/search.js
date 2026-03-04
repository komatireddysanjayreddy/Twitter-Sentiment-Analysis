/**
 * GET /api/search
 *
 * Source priority:
 *   1. TwitterAPI.io REST          — if TWITTER_API_KEY is set
 *   2. Cookie-based Twitter scrape — if TWITTER_AUTH_TOKEN + TWITTER_CT0 set
 *      (extract from browser: x.com → F12 → Application → Cookies)
 *   3. Topic-aware mock            — always-available fallback
 *
 * Query params:
 *   hashtags  — comma-separated tag names (required)
 *   sentiment — positive | negative | neutral  (optional filter)
 *   limit     — max rows returned (default 50, max 200)
 *
 * Response: { data, count, summary, source }
 *   source = "live" | "twitter" | "mock"
 */

import { v4 as uuidv4 } from "uuid";
import Vader            from "vader-sentiment";
import { query as db }  from "../../lib/db";

// ── Env ───────────────────────────────────────────────────────────────────────
const TWITTER_API_KEY   = process.env.TWITTER_API_KEY   ?? "";
const TWITTER_AUTH_TOKEN = process.env.TWITTER_AUTH_TOKEN ?? "";
const TWITTER_CT0        = process.env.TWITTER_CT0        ?? "";
const TWITTERAPI_SEARCH  = "https://api.twitterapi.io/twitter/tweet/advanced_search";

// ── VADER scoring ─────────────────────────────────────────────────────────────
function scoreText(text) {
  const r        = Vader.SentimentIntensityAnalyzer.polarity_scores(text);
  const compound = Math.round(r.compound * 10000) / 10000;
  const sentiment =
    compound >= 0.05  ? "positive" :
    compound <= -0.05 ? "negative" : "neutral";
  return { compound, sentiment };
}

// ── Source 1: TwitterAPI.io REST ──────────────────────────────────────────────
async function fetchViaTwitterApiIo(hashtags) {
  const q   = hashtags.map((h) => `#${h}`).join(" OR ");
  const url = `${TWITTERAPI_SEARCH}?query=${encodeURIComponent(q)}&queryType=Latest&count=30`;

  const res = await fetch(url, {
    headers: { "X-API-Key": TWITTER_API_KEY },
    signal:  AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`TwitterAPI.io returned ${res.status}`);
  const body = await res.json();

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

// ── Source 2: cookie-based scraping ──────────────────────────────────────────
// Module-level scraper cache (reused across warm Lambda invocations)
let _scraper       = null;
let _scraperExpiry = 0;
const SCRAPER_TTL  = 20 * 60 * 1000; // 20 min

async function getAuthScraper() {
  const { Scraper, SearchMode } = await import("@the-convocation/twitter-scraper");

  if (_scraper && Date.now() < _scraperExpiry) {
    try {
      if (await _scraper.isLoggedIn()) return { scraper: _scraper, SearchMode };
    } catch (_) { /* fall through */ }
  }

  const scraper = new Scraper();
  await scraper.setCookies([
    `auth_token=${TWITTER_AUTH_TOKEN}; Domain=.twitter.com; Path=/`,
    `ct0=${TWITTER_CT0}; Domain=.twitter.com; Path=/`,
  ]);

  const ok = await scraper.isLoggedIn();
  if (!ok) throw new Error("Twitter cookies are invalid or expired");

  _scraper       = scraper;
  _scraperExpiry = Date.now() + SCRAPER_TTL;
  return { scraper, SearchMode };
}

async function fetchViaCookies(hashtags) {
  const { scraper, SearchMode } = await getAuthScraper();

  // ANY hashtag works — no hardcoded list
  const q      = hashtags.map((h) => `#${h}`).join(" OR ") + " lang:en -is:retweet";
  const tweets = [];

  for await (const t of scraper.searchTweets(q, 25, SearchMode.Latest)) {
    if (tweets.length >= 25) break;
    const { compound, sentiment } = scoreText(t.text ?? "");
    tweets.push({
      tweet_id:         t.id     || uuidv4(),
      text:             t.text   || "",
      author:           t.username || "unknown",
      author_followers: t.followersCount ?? 0,
      lang:             t.lang   || "en",
      tweet_created_at: t.timeParsed?.toISOString() || new Date().toISOString(),
      hashtags:         (t.hashtags ?? []),
      hashtag_count:    (t.hashtags ?? []).length,
      retweet_count:    t.retweetCount  ?? 0,
      like_count:       t.likeCount     ?? 0,
      compound_score:   compound,
      sentiment,
      source:           "twitter",
    });
  }
  return tweets;
}

// ── Source 3: topic-aware mock ────────────────────────────────────────────────
const MOCK_TEMPLATES = {
  bitcoin:         ["Bitcoin just hit a new high — crypto is unstoppable! 🚀", "Bitcoin crashing again. Bad day for crypto investors.", "Just watching Bitcoin charts. Market looks volatile today.", "Bitcoin adoption is growing fast across emerging markets.", "Sold my Bitcoin at the wrong time. Frustrating.", "Bullish on Bitcoin long term. Short-term noise.", "Bitcoin lightning network transactions are super fast!", "Whales are accumulating Bitcoin quietly. Interesting.", "Bitcoin vs Gold — both are stores of value.", "BTC dominance rising again. Altcoins suffering."],
  airevolution:    ["The #AIRevolution is transforming every industry. Incredible times!", "AI is overhyped. Half these models can't reason properly.", "AI models are getting scarily good at creative tasks.", "The AI revolution is creating new jobs, not just destroying them.", "Worried about AI bias in hiring systems. Needs regulation.", "GPT-4 just helped me debug code in seconds. AI is wild.", "AI in healthcare is going to save millions of lives.", "These AI image generators are incredible!", "AI hallucinations are still a massive problem.", "The pace of AI development is both exciting and terrifying."],
  breakingnews:    ["Breaking news covered live with AI sentiment analysis. Game changer!", "Breaking news overload — too much noise, not enough signal.", "Real-time breaking news pipelines are the future of journalism.", "Hard to tell breaking news from misinformation these days.", "Breaking news alerts are destroying my focus. Need a filter.", "Live coverage is more accurate when multiple sources agree.", "Social media breaking news faster than traditional outlets now.", "Fact-checking needs to keep up with breaking news speed."],
  crypto:          ["Crypto market is wild today. Up 20%, down 20%.", "Ethereum gas fees are insane. Need layer-2 solutions.", "DeFi is the future of finance, no doubt.", "Lost money on altcoins again. Sticking to Bitcoin.", "Crypto regulation uncertainty is hurting adoption.", "NFTs making a comeback or still a dead market?", "Stablecoins are the real backbone of crypto.", "Layer-2 solutions are making Ethereum usable.", "Crypto taxes are a nightmare to calculate.", "HODL is the only strategy that consistently works."],
  python:          ["Python 3.12 performance improvements are incredible!", "Python is slowing down our ML pipeline. Need Rust.", "Just built a Kafka consumer in Python. Elegant and fast.", "Python type hints have saved me so many bugs.", "Python packaging is still a mess in 2025.", "FastAPI is the best web framework for Python.", "Python asyncio is confusing but incredibly powerful.", "Jupyter notebooks are Python's best feature for data science.", "Poetry vs pip vs uv — Python packaging wars.", "Python for data science is unbeatable. R can't compete."],
  machinelearning: ["New transformer architecture beats GPT on benchmarks.", "ML models are impressive but still brittle at reasoning.", "Deployed my first ML model to production today!", "Training costs for large models are unsustainable.", "Transfer learning has changed everything in NLP.", "MLOps is as important as the model itself.", "Explainable AI is not optional in regulated industries.", "Synthetic data generation is solving the data scarcity problem.", "Feature engineering still matters even with deep learning.", "AutoML is getting good but still needs human oversight."],
  kafka:           ["Kafka streams processing 1M events/sec in our pipeline. 🚀", "Kafka consumer lag is killing our real-time dashboard.", "Kafka Connect is underrated for ETL pipelines.", "ksqlDB makes stream processing so much easier.", "Kafka vs RabbitMQ — it depends entirely on the use case.", "Compacted Kafka topics are perfect for state storage.", "Schema Registry is essential for production Kafka.", "Kafka on Kubernetes is finally becoming manageable."],
  spark:           ["PySpark on Databricks is incredibly fast for large datasets.", "Spark streaming latency is too high for our use case.", "Delta Lake + Spark is the best data lakehouse combo.", "Spark SQL is underrated for complex aggregations.", "Moving from Spark to Flink for lower latency.", "Spark MLlib still holds up for classic ML tasks.", "Catalyst optimizer in Spark is brilliant engineering."],
  devops:          ["GitOps is the future of deployment. ArgoCD changed everything.", "Kubernetes is complex but worth it for scale.", "Terraform drift detection saved us from a major outage.", "CI/CD pipelines should be boring — that means they're working.", "Observability > monitoring. Logs + metrics + traces.", "Platform engineering is making developers more productive.", "Docker compose is underrated for local dev environments."],
  tech:            ["The pace of tech innovation is accelerating faster than ever.", "Open source is eating proprietary software.", "Remote work has made the tech talent market truly global.", "Low-code tools are surprisingly powerful now.", "Tech debt is the silent killer of startups.", "Developer experience is the most underinvested area in tech.", "Edge computing is the next frontier after cloud.", "Web3 promises didn't pan out but the tech is interesting."],
  default:         ["Really enjoying the conversation around this topic today.", "Mixed feelings about this — need more data to decide.", "This trend is fascinating to watch unfold in real time.", "Strong opinions on this. Hard to stay neutral.", "Interesting developments in this space lately.", "The community response to this has been massive.", "Worth keeping an eye on how this evolves.", "Data tells a different story than the headlines suggest."],
};

const MOCK_AUTHORS = ["dev_sanjay","data_nerd_42","kafka_queen","spark_wizard","ml_enthusiast","cloud_architect","crypto_watcher","ai_tracker","pipeline_pro","realtime_data","bytes_and_bits","infra_ninja","tech_observer","market_watcher","code_monkey_99","deep_learner"];

function getMockTemplates(hashtags) {
  const key = hashtags.map((h) => h.toLowerCase()).find((h) => MOCK_TEMPLATES[h]);
  return MOCK_TEMPLATES[key] ?? MOCK_TEMPLATES.default;
}

function generateMockTweets(hashtags, count = 20) {
  const templates = getMockTemplates(hashtags);
  const tweets    = [];
  for (let i = 0; i < count; i++) {
    const base   = templates[i % templates.length];
    const suffix = i >= templates.length ? ` #${hashtags[Math.floor(Math.random() * hashtags.length)]}` : "";
    const { compound, sentiment } = scoreText(base + suffix);
    tweets.push({
      tweet_id:         uuidv4(),
      text:             base + suffix,
      author:           MOCK_AUTHORS[Math.floor(Math.random() * MOCK_AUTHORS.length)],
      author_followers: Math.floor(Math.random() * 50_000) + 100,
      lang:             "en",
      tweet_created_at: new Date(Date.now() - Math.floor(Math.random() * 300_000)).toISOString(),
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

// ── DB upsert ─────────────────────────────────────────────────────────────────
const UPSERT_SQL = `
  INSERT INTO tweet_sentiments
    (tweet_id, text, author, author_followers, lang, tweet_created_at,
     hashtags, hashtag_count, retweet_count, like_count,
     compound_score, sentiment, source)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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

function buildSummary(rows) {
  const s = { total: rows.length, positive: 0, negative: 0, neutral: 0 };
  rows.forEach((r) => { s[r.sentiment] = (s[r.sentiment] ?? 0) + 1; });
  return s;
}

// ── Handler ───────────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  const hashtagsRaw = req.query.hashtags ?? "";
  const hashtags = hashtagsRaw.split(",").map((h) => h.trim().replace(/^#/, "")).filter(Boolean);
  if (hashtags.length === 0) return res.status(400).json({ error: "At least one hashtag is required" });

  const sentiment = req.query.sentiment ?? null;
  const limit     = Math.min(parseInt(req.query.limit ?? "50", 10), 200);
  const validSentiments = ["positive", "negative", "neutral"];
  if (sentiment && !validSentiments.includes(sentiment)) return res.status(400).json({ error: "Invalid sentiment filter" });

  let freshTweets = [];
  let source      = "db";

  // ── Step 1: fetch fresh tweets ────────────────────────────────────────────
  const hasIoKey     = TWITTER_API_KEY.length > 10 && !TWITTER_API_KEY.includes("your_");
  const hasCookies   = Boolean(TWITTER_AUTH_TOKEN && TWITTER_CT0);

  if (hasIoKey) {
    try { freshTweets = await fetchViaTwitterApiIo(hashtags); source = "live"; }
    catch (err) { console.warn("[search] TwitterAPI.io failed:", err.message); }
  }

  if (freshTweets.length === 0 && hasCookies) {
    try { freshTweets = await fetchViaCookies(hashtags); source = "twitter"; }
    catch (err) {
      console.warn("[search] Cookie scraping failed:", err.message);
      _scraper = null;
    }
  }

  if (freshTweets.length === 0) {
    freshTweets = generateMockTweets(hashtags);
    source      = "mock";
  }

  // ── Step 2: upsert ────────────────────────────────────────────────────────
  try { await upsertTweets(freshTweets); }
  catch (err) { console.error("[search] DB upsert failed:", err.message); }

  // ── Step 3: query DB ──────────────────────────────────────────────────────
  try {
    const params     = [hashtags];
    const conditions = [`hashtags && $1::text[]`];
    if (sentiment) { params.push(sentiment); conditions.push(`sentiment = $${params.length}`); }
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

    const countRows = await db(
      `SELECT sentiment, COUNT(*) AS cnt FROM tweet_sentiments
       WHERE hashtags && $1::text[]
       ${sentiment ? "AND sentiment = $2" : ""}
       GROUP BY sentiment`,
      sentiment ? [hashtags, sentiment] : [hashtags],
    );
    const summary = { total: 0, positive: 0, negative: 0, neutral: 0 };
    countRows.forEach((r) => { const n = parseInt(r.cnt, 10); summary[r.sentiment] = n; summary.total += n; });

    res.setHeader("Cache-Control", "no-store");
    return res.status(200).json({ data: rows, count: rows.length, summary, source });
  } catch (err) {
    console.error("[search] DB query failed:", err.message);
    const filtered = sentiment ? freshTweets.filter((t) => t.sentiment === sentiment) : freshTweets;
    return res.status(200).json({ data: filtered.slice(0, limit), count: filtered.length, summary: buildSummary(freshTweets), source });
  }
}
