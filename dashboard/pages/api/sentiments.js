/**
 * GET /api/sentiments
 *
 * Query params:
 *   hashtags  — comma-separated tag names, with or without #  (e.g. "AIRevolution,Bitcoin")
 *   sentiment — positive | negative | neutral
 *   limit     — max rows returned (default 50, max 200)
 *
 * Response:
 *   { data: [...tweets], count: N, summary: { total, positive, negative, neutral } }
 */

import { query } from "../../lib/db";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const limit = Math.min(parseInt(req.query.limit ?? "50", 10), 200);

  // Parse sentiment filter
  const sentiment = req.query.sentiment ?? null;
  const validSentiments = ["positive", "negative", "neutral"];
  if (sentiment && !validSentiments.includes(sentiment)) {
    return res.status(400).json({ error: "Invalid sentiment filter" });
  }

  // Parse hashtag filter — strip leading # from each tag
  const hashtagsRaw = req.query.hashtags ?? "";
  const hashtags = hashtagsRaw
    .split(",")
    .map((h) => h.trim().replace(/^#/, ""))
    .filter(Boolean);

  try {
    // ── Build WHERE clause ────────────────────────────────────────────────────
    const params = [];
    const conditions = [];

    if (hashtags.length > 0) {
      params.push(hashtags);
      conditions.push(`hashtags && $${params.length}::text[]`);
    }
    if (sentiment) {
      params.push(sentiment);
      conditions.push(`sentiment = $${params.length}`);
    }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    // ── Fetch tweets ──────────────────────────────────────────────────────────
    const tweetParams = [...params, limit];
    const tweetSql = `
      SELECT
        tweet_id, text, author, author_followers,
        hashtags, compound_score, sentiment,
        retweet_count, like_count, processed_at
      FROM tweet_sentiments
      ${where}
      ORDER BY processed_at DESC
      LIMIT $${tweetParams.length}
    `;
    const rows = await query(tweetSql, tweetParams);

    // ── Fetch summary counts (for the same filter, no LIMIT) ──────────────────
    const countSql = `
      SELECT sentiment, COUNT(*) AS cnt
      FROM tweet_sentiments
      ${where}
      GROUP BY sentiment
    `;
    const counts = await query(countSql, params);

    const summary = { total: 0, positive: 0, negative: 0, neutral: 0 };
    counts.forEach((r) => {
      const n = parseInt(r.cnt, 10);
      summary[r.sentiment] = n;
      summary.total += n;
    });

    res.setHeader("Cache-Control", "no-store");
    return res.status(200).json({ data: rows, count: rows.length, summary });
  } catch (err) {
    console.error("[/api/sentiments] error:", err.message);
    return res.status(500).json({ error: "Database query failed" });
  }
}
