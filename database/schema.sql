-- =============================================================================
-- Twitter Sentiment Analysis — PostgreSQL Schema
-- =============================================================================

-- Enable uuid extension for future use
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Core tweet sentiment table
-- =============================================================================
CREATE TABLE IF NOT EXISTS tweet_sentiments (
    id               BIGSERIAL       PRIMARY KEY,
    tweet_id         TEXT            NOT NULL UNIQUE,   -- Twitter/X tweet ID
    text             TEXT            NOT NULL,
    author           TEXT            NOT NULL,
    author_followers INTEGER         DEFAULT 0,
    lang             CHAR(5)         DEFAULT 'en',
    tweet_created_at TIMESTAMPTZ,                       -- When tweet was posted
    hashtags         TEXT[]          DEFAULT '{}',      -- Array of hashtag strings
    hashtag_count    SMALLINT        DEFAULT 0,
    retweet_count    INTEGER         DEFAULT 0,
    like_count       INTEGER         DEFAULT 0,
    compound_score   REAL            NOT NULL,           -- VADER compound: [-1, 1]
    sentiment        TEXT            NOT NULL            -- positive | negative | neutral
                     CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    source           TEXT            DEFAULT 'twitterapi.io',
    processed_at     TIMESTAMPTZ     DEFAULT NOW()
);

-- =============================================================================
-- Indexes for Power BI / dashboard query patterns
-- =============================================================================

-- Time-series queries (most common for real-time dashboards)
CREATE INDEX IF NOT EXISTS idx_ts_processed_at
    ON tweet_sentiments (processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_ts_tweet_created_at
    ON tweet_sentiments (tweet_created_at DESC);

-- Filter by sentiment label
CREATE INDEX IF NOT EXISTS idx_ts_sentiment
    ON tweet_sentiments (sentiment);

-- Filter by author
CREATE INDEX IF NOT EXISTS idx_ts_author
    ON tweet_sentiments (author);

-- GIN index for hashtag array searches
CREATE INDEX IF NOT EXISTS idx_ts_hashtags
    ON tweet_sentiments USING GIN (hashtags);

-- Compound index for time-bucketed sentiment aggregations
CREATE INDEX IF NOT EXISTS idx_ts_sentiment_processed
    ON tweet_sentiments (sentiment, processed_at DESC);

-- =============================================================================
-- Aggregation views (used by Power BI and the Next.js dashboard)
-- =============================================================================

-- Sentiment counts per 1-minute bucket
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sentiment_per_minute AS
SELECT
    date_trunc('minute', processed_at)  AS bucket,
    sentiment,
    COUNT(*)                            AS tweet_count,
    AVG(compound_score)                 AS avg_compound,
    SUM(retweet_count)                  AS total_retweets,
    SUM(like_count)                     AS total_likes
FROM tweet_sentiments
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_spm_bucket_sentiment
    ON mv_sentiment_per_minute (bucket, sentiment);

-- Top hashtags in the last 24 hours
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_hashtags AS
SELECT
    unnest(hashtags)    AS hashtag,
    sentiment,
    COUNT(*)            AS mention_count,
    AVG(compound_score) AS avg_sentiment_score
FROM tweet_sentiments
WHERE processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Overall summary stats
CREATE OR REPLACE VIEW v_sentiment_summary AS
SELECT
    COUNT(*)                                                AS total_tweets,
    COUNT(*) FILTER (WHERE sentiment = 'positive')          AS positive_count,
    COUNT(*) FILTER (WHERE sentiment = 'negative')          AS negative_count,
    COUNT(*) FILTER (WHERE sentiment = 'neutral')           AS neutral_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE sentiment = 'positive') / NULLIF(COUNT(*), 0), 2
    )                                                       AS positive_pct,
    ROUND(AVG(compound_score)::NUMERIC, 4)                  AS avg_compound_score,
    MIN(processed_at)                                       AS earliest_tweet,
    MAX(processed_at)                                       AS latest_tweet
FROM tweet_sentiments;

-- Last 100 tweets for live feed
CREATE OR REPLACE VIEW v_recent_tweets AS
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
ORDER BY processed_at DESC
LIMIT 100;

-- =============================================================================
-- Refresh helper function (call via pg_cron or manually)
-- =============================================================================
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sentiment_per_minute;
    REFRESH MATERIALIZED VIEW mv_top_hashtags;
END;
$$;

-- =============================================================================
-- Seed with a few rows so dashboard isn't empty on first load
-- =============================================================================
INSERT INTO tweet_sentiments
    (tweet_id, text, author, author_followers, lang, tweet_created_at,
     hashtags, hashtag_count, retweet_count, like_count,
     compound_score, sentiment, source)
VALUES
    ('seed_001', 'Just set up my Twitter Sentiment pipeline — this is amazing!',
     'sanjay_dev', 1200, 'en', NOW() - INTERVAL '5 minutes',
     ARRAY['kafka','spark','python'], 3, 10, 45, 0.82, 'positive', 'seed'),
    ('seed_002', 'Kafka consumer lag is driving me insane. Production issues again.',
     'angry_devops', 800, 'en', NOW() - INTERVAL '4 minutes',
     ARRAY['kafka','devops'], 2, 3, 8, -0.72, 'negative', 'seed'),
    ('seed_003', 'Reading about PySpark Structured Streaming. Interesting architecture.',
     'data_learner', 300, 'en', NOW() - INTERVAL '3 minutes',
     ARRAY['pyspark','bigdata'], 2, 1, 12, 0.0, 'neutral', 'seed')
ON CONFLICT (tweet_id) DO NOTHING;

-- Populate materialized views
REFRESH MATERIALIZED VIEW mv_sentiment_per_minute;
REFRESH MATERIALIZED VIEW mv_top_hashtags;
