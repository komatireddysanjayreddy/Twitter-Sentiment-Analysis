/**
 * GET /api/sentiments
 *
 * Returns the most recent N tweets with sentiment scores.
 *
 * Query params:
 *   limit    — number of tweets (default 50, max 200)
 *   sentiment — filter by label: positive | negative | neutral
 */

import { query } from "../../lib/db";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const limit = Math.min(parseInt(req.query.limit ?? "50", 10), 200);
  const sentiment = req.query.sentiment;

  const validSentiments = ["positive", "negative", "neutral"];
  if (sentiment && !validSentiments.includes(sentiment)) {
    return res.status(400).json({ error: "Invalid sentiment filter" });
  }

  try {
    let sql = `
      SELECT
        tweet_id,
        text,
        author,
        author_followers,
        hashtags,
        compound_score,
        sentiment,
        retweet_count,
        like_count,
        processed_at
      FROM tweet_sentiments
    `;
    const params = [];

    if (sentiment) {
      sql += ` WHERE sentiment = $1`;
      params.push(sentiment);
    }

    sql += ` ORDER BY processed_at DESC LIMIT $${params.length + 1}`;
    params.push(limit);

    const rows = await query(sql, params);

    // Cache for 5 seconds — fresh enough for near-real-time, avoids DB hammering
    res.setHeader("Cache-Control", "s-maxage=5, stale-while-revalidate=10");
    return res.status(200).json({ data: rows, count: rows.length });
  } catch (err) {
    console.error("[/api/sentiments] error:", err.message);
    return res.status(500).json({ error: "Database query failed" });
  }
}
