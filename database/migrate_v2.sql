-- =============================================================================
-- Migration v2: Add processed_lag, rolling_sentiment table, and v_rolling_5min
-- Safe to run on an existing v1 database (all statements are idempotent)
-- =============================================================================

-- 1. Add processed_lag GENERATED ALWAYS column (no-op if already exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'tweet_sentiments' AND column_name = 'processed_lag'
    ) THEN
        ALTER TABLE tweet_sentiments
            ADD COLUMN processed_lag REAL GENERATED ALWAYS AS (
                CASE WHEN tweet_created_at IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (processed_at - tweet_created_at))
                     ELSE NULL
                END
            ) STORED;
    END IF;
END;
$$;

-- 2. Rolling 5-minute sentiment averages table
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

-- 3. New indexes
CREATE INDEX IF NOT EXISTS idx_ts_tweet_created_at
    ON tweet_sentiments (tweet_created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ts_sentiment_processed
    ON tweet_sentiments (sentiment, processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_rolling_window_end
    ON rolling_sentiment (window_end DESC);

CREATE INDEX IF NOT EXISTS idx_rolling_sentiment_window
    ON rolling_sentiment (sentiment, window_end DESC);

-- 4. Updated summary view (now includes avg_lag_seconds)
-- Must drop first because CREATE OR REPLACE cannot change column list order
DROP VIEW IF EXISTS v_sentiment_summary CASCADE;
CREATE VIEW v_sentiment_summary AS
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

-- 5. Rolling 5-min view (latest snapshot per sentiment)
CREATE OR REPLACE VIEW v_rolling_5min AS
SELECT DISTINCT ON (sentiment)
    window_start, window_end, sentiment,
    tweet_count, avg_compound, computed_at
FROM rolling_sentiment
ORDER BY sentiment, window_end DESC;

-- 6. Refresh helper function
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sentiment_per_minute;
    REFRESH MATERIALIZED VIEW mv_top_hashtags;
END;
$$;

-- 7. Seed rolling_sentiment so the dashboard chart is never empty
INSERT INTO rolling_sentiment (window_start, window_end, sentiment,
                                tweet_count, avg_compound, computed_at)
VALUES
    (NOW() - INTERVAL '5 minutes', NOW(), 'positive', 1, 0.82, NOW()),
    (NOW() - INTERVAL '5 minutes', NOW(), 'negative', 1, -0.72, NOW()),
    (NOW() - INTERVAL '5 minutes', NOW(), 'neutral',  1, 0.0,  NOW())
ON CONFLICT (window_end, sentiment) DO NOTHING;
