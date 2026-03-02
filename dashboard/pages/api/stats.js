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

      query(
        `SELECT
           bucket,
           sentiment,
           tweet_count,
           ROUND(avg_compound::NUMERIC, 4) AS avg_compound,
           total_retweets,
           total_likes
         FROM mv_sentiment_per_minute
         WHERE bucket >= NOW() - ($1 || ' minutes')::INTERVAL
         ORDER BY bucket ASC, sentiment`,
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

      query(
        `SELECT
           hashtag,
           SUM(mention_count)                              AS mention_count,
           ROUND(AVG(avg_sentiment_score)::NUMERIC, 4)    AS avg_score
         FROM mv_top_hashtags
         GROUP BY hashtag
         ORDER BY mention_count DESC
         LIMIT 20`
      ),
    ]);

    res.setHeader("Cache-Control", "s-maxage=10, stale-while-revalidate=20");
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
