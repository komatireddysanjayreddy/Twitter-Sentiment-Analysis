/**
 * GET /api/stats
 *
 * Returns:
 *   summary     — overall counts, percentages, avg lag
 *   timeSeries  — per-minute buckets (last N minutes)
 *   rolling     — rolling 5-min sentiment averages (latest window per sentiment)
 *   hashtags    — top 20 hashtags (last 24h)
 */

import { query } from "../../lib/db";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const minutes = Math.min(parseInt(req.query.minutes ?? "60", 10), 1440);

  try {
    const [summary, timeSeries, rolling, hashtags] = await Promise.all([
      query(`SELECT * FROM v_sentiment_summary`),

      // Direct query — no materialized view, always current
      query(
        `SELECT
           date_trunc('minute', processed_at)            AS bucket,
           sentiment,
           COUNT(*)                                      AS tweet_count,
           ROUND(AVG(compound_score)::NUMERIC, 4)        AS avg_compound,
           SUM(retweet_count)                            AS total_retweets,
           SUM(like_count)                               AS total_likes
         FROM tweet_sentiments
         WHERE processed_at >= NOW() - ($1 * INTERVAL '1 minute')
         GROUP BY 1, 2
         ORDER BY 1 ASC, 2`,
        [minutes]
      ),

      // Rolling 5-min averages — latest snapshot per sentiment
      query(
        `SELECT sentiment, tweet_count,
                ROUND(avg_compound::NUMERIC, 4) AS avg_compound,
                window_start, window_end, computed_at
         FROM v_rolling_5min
         ORDER BY sentiment`
      ),

      // Direct query — no materialized view, always current
      query(
        `SELECT
           unnest(hashtags)                              AS hashtag,
           COUNT(*)                                      AS mention_count,
           ROUND(AVG(compound_score)::NUMERIC, 4)        AS avg_score
         FROM tweet_sentiments
         WHERE processed_at >= NOW() - INTERVAL '24 hours'
         GROUP BY 1
         ORDER BY 2 DESC
         LIMIT 20`
      ),
    ]);

    res.setHeader("Cache-Control", "s-maxage=5, stale-while-revalidate=5");
    return res.status(200).json({
      summary:    summary[0] ?? {},
      timeSeries,
      rolling,
      hashtags,
    });
  } catch (err) {
    console.error("[/api/stats] error:", err.message);
    return res.status(500).json({ error: "Database query failed" });
  }
}
