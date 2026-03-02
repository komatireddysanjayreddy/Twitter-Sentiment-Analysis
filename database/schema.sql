-- =============================================================================
-- Twitter Sentiment Analysis — PostgreSQL Schema  (v2)
-- Changes from v1:
--   + rolling_sentiment table for 5-min windowed averages
--   + processed_lag column to measure end-to-end latency
--   + Indexes added for rolling_sentiment queries
--   + Seed rows use ON CONFLICT DO NOTHING (idempotent)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Core tweets table
-- =============================================================================
CREATE TABLE IF NOT EXISTS tweet_sentiments (
    id               BIGSERIAL    PRIMARY KEY,
    tweet_id         TEXT         NOT NULL UNIQUE,
    text             TEXT         NOT NULL,
    author           TEXT         NOT NULL,
    author_followers INTEGER      DEFAULT 0,
    lang             CHAR(5)      DEFAULT 'en',
    tweet_created_at TIMESTAMPTZ,
    hashtags         TEXT[]       DEFAULT '{}',
    hashtag_count    SMALLINT     DEFAULT 0,
    retweet_count    INTEGER      DEFAULT 0,
    like_count       INTEGER      DEFAULT 0,
    compound_score   REAL         NOT NULL,
    sentiment        TEXT         NOT NULL
                     CHECK (sentiment IN ('positive','negative','neutral')),
    source           TEXT         DEFAULT 'twitterapi.io',
    processed_at     TIMESTAMPTZ  DEFAULT NOW(),
    -- End-to-end latency in seconds (null when tweet_created_at missing)
    processed_lag    REAL         GENERATED ALWAYS AS (
        CASE WHEN tweet_created_at IS NOT NULL
             THEN EXTRACT(EPOCH FROM (processed_at - tweet_created_at))
             ELSE NULL
        END
    ) STORED
);

-- =============================================================================
-- Rolling 5-minute sentiment averages  (written by processor.py)
-- =============================================================================
CREATE TABLE IF NOT EXISTS rolling_sentiment (
    id           BIGSERIAL    PRIMARY KEY,
    window_start TIMESTAMPTZ  NOT NULL,
    window_end   TIMESTAMPTZ  NOT NULL,
    sentiment    TEXT         NOT NULL CHECK (sentiment IN ('positive','negative','neutral')),
    tweet_count  INTEGER      NOT NULL DEFAULT 0,
    avg_compound REAL         NOT NULL DEFAULT 0,
    computed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (window_end, sentiment)
);

-- =============================================================================
-- Indexes
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_ts_processed_at
    ON tweet_sentiments (processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_ts_tweet_created_at
    ON tweet_sentiments (tweet_created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ts_sentiment
    ON tweet_sentiments (sentiment);

CREATE INDEX IF NOT EXISTS idx_ts_author
    ON tweet_sentiments (author);

CREATE INDEX IF NOT EXISTS idx_ts_hashtags
    ON tweet_sentiments USING GIN (hashtags);

CREATE INDEX IF NOT EXISTS idx_ts_sentiment_processed
    ON tweet_sentiments (sentiment, processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_rolling_window_end
    ON rolling_sentiment (window_end DESC);

CREATE INDEX IF NOT EXISTS idx_rolling_sentiment_window
    ON rolling_sentiment (sentiment, window_end DESC);

-- =============================================================================
-- Materialized views
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sentiment_per_minute AS
SELECT
    date_trunc('minute', processed_at) AS bucket,
    sentiment,
    COUNT(*)                           AS tweet_count,
    AVG(compound_score)                AS avg_compound,
    SUM(retweet_count)                 AS total_retweets,
    SUM(like_count)                    AS total_likes
FROM tweet_sentiments
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_spm_bucket_sentiment
    ON mv_sentiment_per_minute (bucket, sentiment);

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

-- =============================================================================
-- Regular views
-- =============================================================================
CREATE OR REPLACE VIEW v_sentiment_summary AS
SELECT
    COUNT(*)                                                       AS total_tweets,
    COUNT(*) FILTER (WHERE sentiment = 'positive')                 AS positive_count,
    COUNT(*) FILTER (WHERE sentiment = 'negative')                 AS negative_count,
    COUNT(*) FILTER (WHERE sentiment = 'neutral')                  AS neutral_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE sentiment = 'positive')
        / NULLIF(COUNT(*), 0), 2
    )                                                              AS positive_pct,
    ROUND(AVG(compound_score)::NUMERIC, 4)                         AS avg_compound_score,
    ROUND(AVG(processed_lag)::NUMERIC, 2)                          AS avg_lag_seconds,
    MIN(processed_at)                                              AS earliest_tweet,
    MAX(processed_at)                                              AS latest_tweet
FROM tweet_sentiments;

CREATE OR REPLACE VIEW v_recent_tweets AS
SELECT
    tweet_id, text, author, author_followers,
    hashtags, compound_score, sentiment,
    retweet_count, like_count, processed_at, source
FROM tweet_sentiments
ORDER BY processed_at DESC
LIMIT 100;

-- Last 5-minute rolling window (latest snapshot per sentiment)
CREATE OR REPLACE VIEW v_rolling_5min AS
SELECT DISTINCT ON (sentiment)
    window_start, window_end, sentiment,
    tweet_count, avg_compound, computed_at
FROM rolling_sentiment
ORDER BY sentiment, window_end DESC;

-- =============================================================================
-- Refresh helper (called by processor.py after each batch)
-- =============================================================================
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sentiment_per_minute;
    REFRESH MATERIALIZED VIEW mv_top_hashtags;
END;
$$;

-- =============================================================================
-- Seed rows (idempotent — safe to re-run)
-- =============================================================================
INSERT INTO tweet_sentiments
    (tweet_id, text, author, author_followers, lang, tweet_created_at,
     hashtags, hashtag_count, retweet_count, like_count,
     compound_score, sentiment, source)
VALUES
    ('seed_001',
     'Just set up my Twitter Sentiment pipeline — this is amazing!',
     'sanjay_dev', 1200, 'en', NOW() - INTERVAL '5 minutes',
     ARRAY['kafka','spark','python'], 3, 10, 45, 0.82, 'positive', 'seed'),
    ('seed_002',
     'Kafka consumer lag is driving me insane. Production issues again.',
     'angry_devops', 800, 'en', NOW() - INTERVAL '4 minutes',
     ARRAY['kafka','devops'], 2, 3, 8, -0.72, 'negative', 'seed'),
    ('seed_003',
     'Reading about PySpark Structured Streaming. Interesting architecture.',
     'data_learner', 300, 'en', NOW() - INTERVAL '3 minutes',
     ARRAY['pyspark','bigdata'], 2, 1, 12, 0.0, 'neutral', 'seed')
ON CONFLICT (tweet_id) DO NOTHING;

-- Seed rolling averages so the dashboard chart is never empty
INSERT INTO rolling_sentiment (window_start, window_end, sentiment,
                                tweet_count, avg_compound, computed_at)
VALUES
    (NOW() - INTERVAL '5 minutes', NOW(), 'positive', 1, 0.82, NOW()),
    (NOW() - INTERVAL '5 minutes', NOW(), 'negative', 1, -0.72, NOW()),
    (NOW() - INTERVAL '5 minutes', NOW(), 'neutral',  1, 0.0,  NOW())
ON CONFLICT (window_end, sentiment) DO NOTHING;

-- Populate materialized views
REFRESH MATERIALIZED VIEW mv_sentiment_per_minute;
REFRESH MATERIALIZED VIEW mv_top_hashtags;
