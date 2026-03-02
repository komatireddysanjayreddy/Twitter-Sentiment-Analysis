"""
mock_direct.py — Standalone mock producer that writes directly to Neon PostgreSQL.

Use this when Docker / Kafka are not available. It generates realistic mock tweets,
scores them with VADER, and inserts them straight into tweet_sentiments so the
Vercel dashboard stays live without any local infrastructure.

Includes the same rotation logic as producer.py: if a hashtag pool goes quiet
for ROTATION_IDLE_SECS the producer silently swaps it for a backup topic.

Usage:
  python producer/mock_direct.py

Environment variables (optional — defaults to Neon):
  DATABASE_URL         — full PostgreSQL connection string
  MOCK_RATE            — tweets per second (default: 1)
  ROTATION_IDLE_SECS   — silence threshold for rotation (default: 60)
  REFRESH_EVERY        — refresh materialized views every N inserts (default: 20)
"""

import os
import time
import random
import logging
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] mock_direct — %(message)s",
)
logger = logging.getLogger("mock_direct")

# ─── Config ───────────────────────────────────────────────────────────────────
DATABASE_URL       = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_c5wIWRQ6Olfd@ep-cool-heart-aepz1wsa-pooler"
    ".c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require",
)
MOCK_RATE          = float(os.getenv("MOCK_RATE", "1"))
ROTATION_IDLE_SECS = int(os.getenv("ROTATION_IDLE_SECS", "60"))
REFRESH_EVERY      = int(os.getenv("REFRESH_EVERY", "20"))

# ─── Sentiment ────────────────────────────────────────────────────────────────
_vader = SentimentIntensityAnalyzer()

def score(text: str) -> tuple[float, str]:
    compound = _vader.polarity_scores(text)["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return round(compound, 4), label

# ─── Mock data pools ──────────────────────────────────────────────────────────
_POSITIVE = [
    "I absolutely love the new features in Python 3.12! Incredible work 🚀",
    "Just deployed my first ML model to production. Feeling amazing!",
    "The AI revolution is creating so many opportunities. Super excited!",
    "Spark Streaming is blazingly fast. Genuinely impressed with performance!",
    "Cloud costs dropped 40% after optimization. Happy Friday!",
    "Real-time pipelines with Kafka are delightful to build.",
    "Data engineering is the backbone of AI. Love this field!",
    "Bitcoin just hit a new milestone — the future of finance is here! 🚀",
    "The #AIRevolution is transforming every industry. Incredible times!",
    "Breaking news covered live with AI analysis. Real journalism evolved!",
    "Open source communities are the best. So much collaboration ❤️",
    "Just got promoted to Senior Engineer. Hard work pays off!",
]
_NEGATIVE = [
    "This API documentation is absolutely terrible. Waste of hours.",
    "Production is down again. Third time this week. Beyond frustrated.",
    "Data breach reported — security needs to be taken more seriously.",
    "Layoffs hitting tech hard. Tough market out there.",
    "Deployment failed AGAIN. CI/CD pipeline is completely broken.",
    "Kafka consumer lag is out of control. Operations nightmare.",
    "Legacy code with zero documentation is killing my productivity.",
    "Bitcoin crashing again. Another rough day for crypto investors.",
    "AI hype is outpacing actual progress. Tired of the overblown claims.",
    "Breaking news overload — too much noise, not enough signal.",
]
_NEUTRAL = [
    "Just pushed a new commit. PR is open for review.",
    "Setting up a PostgreSQL cluster today. Standard setup.",
    "Attended a webinar on distributed systems. Covered CAP theorem.",
    "Updated dependencies in package.json. Minor version bumps.",
    "Pipeline processed 50k events with zero errors today.",
    "Reviewing schema migration script before applying to prod.",
    "Bitcoin trading volume is steady. Markets looking normal.",
    "New AI paper published on arxiv. Adding it to the reading list.",
    "BreakingNews feed running smoothly. No major incidents today.",
    "Docker containers are running. Moving to next task.",
]
_HASHTAG_POOLS = [
    ["python", "programming", "coding"],
    ["ai", "machinelearning", "datascience"],
    ["kafka", "streaming", "bigdata"],
    ["tech", "software", "engineering"],
    ["cloud", "aws", "devops"],
    ["spark", "pyspark", "dataengineering"],
    ["AIRevolution", "ai", "futuretech"],
    ["Bitcoin", "crypto", "blockchain"],
    ["BreakingNews", "news", "trending"],
]
_AUTHORS = [
    "dev_sanjay", "data_nerd_42", "kafka_queen", "spark_wizard",
    "ml_enthusiast", "cloud_architect", "pythonista", "stream_master",
    "bytes_and_bits", "code_craftsman", "realtime_data", "pipeline_pro",
    "data_wrangler", "infra_ninja", "kafka_hacker", "etl_guru",
]

BACKUP_KEYWORDS = [
    "ChatGPT", "MachineLearning", "Ethereum", "DataScience", "OpenAI",
    "CyberSecurity", "TechNews", "Crypto", "DevOps", "Startup",
    "ClimateChange", "SpaceX",
]

# ─── Rotation state ───────────────────────────────────────────────────────────
_active_pools: list = list(_HASHTAG_POOLS)   # mutable; rotated at runtime
_pool_last_seen: dict = {}                    # pool_index -> last insert time
_backup_cursor: int = 0

def _init_pool_timestamps() -> None:
    now = time.time()
    for i in range(len(_active_pools)):
        _pool_last_seen.setdefault(i, now)

def _rotate_silent_pools() -> bool:
    """Swap hashtag pools that have been unused for >= ROTATION_IDLE_SECS."""
    global _backup_cursor
    now = time.time()
    rotated = False
    active_flat = {tag for pool in _active_pools for tag in pool}

    for i, pool in enumerate(_active_pools):
        idle = now - _pool_last_seen.get(i, now)
        if idle < ROTATION_IDLE_SECS:
            continue
        # Build a backup pool around the next unused backup keyword
        tried = 0
        while tried < len(BACKUP_KEYWORDS):
            kw = BACKUP_KEYWORDS[_backup_cursor % len(BACKUP_KEYWORDS)]
            _backup_cursor += 1
            if kw not in active_flat:
                new_pool = [kw, "tech", "trending"]
                _active_pools[i] = new_pool
                _pool_last_seen[i] = now
                logger.warning(
                    "ROTATION: pool %d silent %.0fs → swapped in [%s]  |  active pools=%d",
                    i, idle, ", ".join(new_pool), len(_active_pools),
                )
                rotated = True
                break
            tried += 1
    return rotated

# ─── Tweet generator ──────────────────────────────────────────────────────────
def generate_tweet() -> dict:
    pool_idx = random.randrange(len(_active_pools))
    _pool_last_seen[pool_idx] = time.time()

    pool     = random.choices(
        [_POSITIVE, _NEGATIVE, _NEUTRAL],
        weights=[0.45, 0.35, 0.20],
    )[0]
    text     = random.choice(pool)
    hashtags = random.sample(
        _active_pools[pool_idx],
        k=random.randint(1, len(_active_pools[pool_idx])),
    )
    compound, sentiment = score(text)
    return {
        "tweet_id":         str(uuid.uuid4()),
        "text":             text,
        "author":           random.choice(_AUTHORS),
        "author_followers": random.randint(50, 50_000),
        "lang":             "en",
        "tweet_created_at": datetime.now(timezone.utc),
        "hashtags":         hashtags,
        "hashtag_count":    len(hashtags),
        "retweet_count":    random.randint(0, 500),
        "like_count":       random.randint(0, 2000),
        "compound_score":   compound,
        "sentiment":        sentiment,
        "source":           "mock_direct",
    }

# ─── DB helpers ───────────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO tweet_sentiments
    (tweet_id, text, author, author_followers, lang, tweet_created_at,
     hashtags, hashtag_count, retweet_count, like_count,
     compound_score, sentiment, source)
VALUES
    (%(tweet_id)s, %(text)s, %(author)s, %(author_followers)s, %(lang)s,
     %(tweet_created_at)s, %(hashtags)s, %(hashtag_count)s,
     %(retweet_count)s, %(like_count)s,
     %(compound_score)s, %(sentiment)s, %(source)s)
ON CONFLICT (tweet_id) DO NOTHING
"""

ROLLING_SQL = """
INSERT INTO rolling_sentiment (window_start, window_end, sentiment,
                                tweet_count, avg_compound, computed_at)
SELECT
    date_trunc('minute', NOW()) - INTERVAL '5 minutes',
    date_trunc('minute', NOW()),
    sentiment,
    COUNT(*)::INTEGER,
    ROUND(AVG(compound_score)::NUMERIC, 4)::REAL,
    NOW()
FROM tweet_sentiments
WHERE processed_at >= NOW() - INTERVAL '5 minutes'
GROUP BY sentiment
ON CONFLICT (window_end, sentiment) DO UPDATE
    SET tweet_count  = EXCLUDED.tweet_count,
        avg_compound = EXCLUDED.avg_compound,
        computed_at  = EXCLUDED.computed_at
"""

def connect() -> psycopg2.extensions.connection:
    # psycopg2 doesn't support channel_binding — strip it before connecting
    dsn = DATABASE_URL.replace("&channel_binding=require", "").replace(
        "channel_binding=require&", ""
    )
    logger.info("Connecting to database…")
    conn = psycopg2.connect(dsn, connect_timeout=15)
    logger.info("Database connection established.")
    return conn

# ─── Main loop ────────────────────────────────────────────────────────────────
def run() -> None:
    _init_pool_timestamps()
    interval   = 1.0 / max(MOCK_RATE, 0.1)
    inserted   = 0
    last_rotate_check = time.time()

    logger.info(
        "Starting mock_direct — rate=%.1f/s  rotation_idle=%ds  db=Neon",
        MOCK_RATE, ROTATION_IDLE_SECS,
    )
    logger.info("Active hashtag pools: %s", [p[0] for p in _active_pools])

    conn = connect()
    conn.autocommit = True   # each INSERT commits instantly — no round-trip overhead

    try:
        while True:
            tweet = generate_tweet()

            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, {**tweet, "hashtags": tweet["hashtags"]})
                inserted += 1

                # Refresh rolling averages and materialized views periodically
                if inserted % REFRESH_EVERY == 0:
                    cur.execute(ROLLING_SQL)
                    cur.execute("SELECT refresh_materialized_views()")
                    logger.info(
                        "Inserted %d tweets | sentiment=%s | hashtags=%s",
                        inserted, tweet["sentiment"], tweet["hashtags"],
                    )

            # Rotation check every 30 s
            if time.time() - last_rotate_check >= 30:
                last_rotate_check = time.time()
                if _rotate_silent_pools():
                    logger.info("Active pools after rotation: %s",
                                [p[0] for p in _active_pools])

            time.sleep(interval * random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        logger.info("Stopped. Total inserted: %d", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
