"""
producer.py — Robust Twitter ingestion with automatic mock fallback
              and per-hashtag rotation logic.

Strategy:
  1. Attempt live TwitterAPI.io WebSocket stream.
  2. After LIVE_FAIL_THRESHOLD consecutive failures → flip to MOCK mode so
     the dashboard never runs empty.
  3. Every RETRY_LIVE_INTERVAL seconds, silently probe the real API again.
     If it succeeds, flip back to LIVE automatically.
  4. A heartbeat file is touched every HEARTBEAT_INTERVAL tweets so the
     Docker healthcheck and monitor.py can detect a stalled producer.
  5. ROTATION LOGIC: every ~30 s the live stream checks which keywords have
     gone silent for >= ROTATION_IDLE_SECS.  Silent keywords are swapped out
     for the next candidate in BACKUP_KEYWORDS and the WebSocket reconnects
     with the updated subscription — keeping the dashboard live even when a
     topic falls off Twitter's trending list.

Environment variables:
  TWITTER_API_KEY           — TwitterAPI.io key (optional; triggers mock if absent)
  KAFKA_BOOTSTRAP_SERVERS   — default: localhost:9092
  KAFKA_TOPIC               — default: tweets
  SEARCH_KEYWORDS           — comma-separated starting keywords
                              default: python,ai,technology,AIRevolution,Bitcoin,BreakingNews
  BACKUP_KEYWORDS           — comma-separated rotation pool
  ROTATION_IDLE_SECS        — silence threshold before a keyword is rotated out (default: 60)
  MOCK_RATE                 — synthetic tweets/sec in fallback (default: 1)
  LIVE_FAIL_THRESHOLD       — consecutive failures before mock (default: 3)
  RETRY_LIVE_INTERVAL       — seconds between live-API retry attempts (default: 300)
  HEARTBEAT_FILE            — path written to signal health (default: /tmp/producer.heartbeat)
  HEARTBEAT_INTERVAL        — write heartbeat every N published tweets (default: 10)
"""

import os
import json
import time
import random
import logging
import asyncio
import uuid
from datetime import datetime, timezone
from collections import deque

import websockets
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer")

# ─── Config ───────────────────────────────────────────────────────────────────
TWITTER_API_KEY         = os.getenv("TWITTER_API_KEY", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "tweets")
SEARCH_KEYWORDS         = os.getenv(
    "SEARCH_KEYWORDS",
    "python,ai,technology,AIRevolution,Bitcoin,BreakingNews",
).split(",")
BACKUP_KEYWORDS         = [kw.strip() for kw in os.getenv(
    "BACKUP_KEYWORDS",
    "ChatGPT,MachineLearning,Ethereum,DataScience,OpenAI,"
    "CyberSecurity,TechNews,Crypto,DevOps,Startup,ClimateChange,SpaceX",
).split(",")]
ROTATION_IDLE_SECS      = int(os.getenv("ROTATION_IDLE_SECS", "60"))
MOCK_RATE               = float(os.getenv("MOCK_RATE", "1"))
LIVE_FAIL_THRESHOLD     = int(os.getenv("LIVE_FAIL_THRESHOLD", "3"))
RETRY_LIVE_INTERVAL     = int(os.getenv("RETRY_LIVE_INTERVAL", "300"))
HEARTBEAT_FILE          = os.getenv("HEARTBEAT_FILE", "/tmp/producer.heartbeat")
HEARTBEAT_INTERVAL      = int(os.getenv("HEARTBEAT_INTERVAL", "10"))

WS_URL = f"wss://api.twitterapi.io/twitter/tweet/websocket?apiKey={TWITTER_API_KEY}"

# ─── Session state ────────────────────────────────────────────────────────────
_published_count    = 0
_live_fail_count    = 0
_in_mock_mode       = False
_recent_ids: deque  = deque(maxlen=2000)   # sliding dedup ring-buffer

# Rotation state
_active_keywords: list  = []    # mutable copy of SEARCH_KEYWORDS; rotated at runtime
_keyword_last_seen: dict = {}   # keyword -> unix timestamp of last matching tweet
_backup_cursor: int     = 0    # cycling index into BACKUP_KEYWORDS
_rotation_triggered     = False # set by run_live(); read by orchestrate()

# ─── Heartbeat ────────────────────────────────────────────────────────────────
def _write_heartbeat() -> None:
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass   # non-fatal


# ─── Keyword rotation helpers ─────────────────────────────────────────────────
def _init_keyword_timestamps() -> None:
    """Seed last-seen timestamps to now so rotation isn't triggered on fresh connect."""
    now = time.time()
    for kw in _active_keywords:
        _keyword_last_seen.setdefault(kw, now)


def _kw_matches_tweet(kw: str, tweet: dict) -> bool:
    """True if the keyword appears in the tweet's hashtag list or body text."""
    needle = kw.lower().lstrip("#")
    if needle in (h.lower() for h in tweet.get("hashtags", [])):
        return True
    return needle in tweet.get("text", "").lower()


def _update_keyword_activity(tweet: dict) -> None:
    """Update last-seen timestamp for every active keyword matched by this tweet."""
    now = time.time()
    for kw in _active_keywords:
        if _kw_matches_tweet(kw, tweet):
            _keyword_last_seen[kw] = now


def _rotate_silent_keywords() -> bool:
    """
    Swap any keyword that has been silent for >= ROTATION_IDLE_SECS with the
    next unused candidate from BACKUP_KEYWORDS.
    Returns True if at least one rotation occurred (caller should reconnect).
    """
    global _backup_cursor
    now     = time.time()
    rotated = False

    for i, kw in enumerate(_active_keywords):
        idle_secs = now - _keyword_last_seen.get(kw, now)
        if idle_secs < ROTATION_IDLE_SECS:
            continue

        # Walk the backup pool until we find a candidate not already active
        tried = 0
        while tried < len(BACKUP_KEYWORDS):
            candidate = BACKUP_KEYWORDS[_backup_cursor % len(BACKUP_KEYWORDS)]
            _backup_cursor += 1
            if candidate not in _active_keywords:
                _active_keywords[i] = candidate
                _keyword_last_seen[candidate] = now
                logger.warning(
                    "ROTATION: '%s' silent %.0fs → swapped in '%s'  |  active=%s",
                    kw, idle_secs, candidate, _active_keywords,
                )
                rotated = True
                break
            tried += 1

    return rotated


# ─── Kafka ────────────────────────────────────────────────────────────────────
def build_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id":         "twitter-producer",
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  1000,
        "compression.type":  "snappy",
        "linger.ms":         20,
        "batch.size":        32768,
    })


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed topic=%s partition=%s: %s",
                     msg.topic(), msg.partition(), err)


def publish(producer: Producer, tweet: dict) -> None:
    global _published_count
    tid = tweet.get("id", "")

    # Sliding-window deduplication
    if tid and tid in _recent_ids:
        return
    if tid:
        _recent_ids.append(tid)

    # Track per-keyword activity for rotation logic
    _update_keyword_activity(tweet)

    try:
        payload = json.dumps(tweet, ensure_ascii=False).encode("utf-8")
        producer.produce(
            KAFKA_TOPIC,
            key=str(tid).encode("utf-8"),
            value=payload,
            callback=_delivery_report,
        )
        producer.poll(0)
        _published_count += 1
        if _published_count % HEARTBEAT_INTERVAL == 0:
            _write_heartbeat()
            logger.info("[%s] Published %d tweets total",
                        "MOCK" if _in_mock_mode else "LIVE", _published_count)
    except KafkaException as exc:
        logger.error("Kafka produce error: %s", exc)
    except BufferError:
        logger.warning("Kafka buffer full — flushing")
        producer.flush(5)
        producer.produce(KAFKA_TOPIC, key=str(tid).encode(),
                         value=payload, callback=_delivery_report)


# ─── Live tweet normalisation ─────────────────────────────────────────────────
def normalize_live(raw: dict) -> dict:
    return {
        "id":               raw.get("id") or raw.get("id_str") or str(uuid.uuid4()),
        "text":             raw.get("text") or raw.get("full_text", ""),
        "author":           raw.get("user", {}).get("screen_name", "unknown"),
        "author_followers": raw.get("user", {}).get("followers_count", 0),
        "lang":             raw.get("lang", "en"),
        "created_at":       raw.get("created_at",
                                    datetime.now(timezone.utc).isoformat()),
        "hashtags": [
            ht["text"]
            for ht in raw.get("entities", {}).get("hashtags", [])
        ],
        "retweet_count": raw.get("retweet_count", 0),
        "like_count":    raw.get("favorite_count", 0),
        "source":        "twitterapi.io",
    }


# ─── Mock data pools ──────────────────────────────────────────────────────────
_POSITIVE = [
    "I absolutely love the new features in Python 3.12! Incredible work 🚀",
    "Just deployed my first ML model to production. Feeling amazing!",
    "The AI revolution is creating so many opportunities. Super excited!",
    "Open source communities are the best. So much collaboration ❤️",
    "Spark Streaming is blazingly fast. Genuinely impressed with performance!",
    "Cloud costs dropped 40% after optimization. Happy Friday!",
    "Just got promoted to Senior Engineer. Hard work pays off!",
    "New PySpark feature makes windowed aggregations so elegant.",
    "Real-time pipelines with Kafka are delightful to build.",
    "Data engineering is the backbone of AI. Love this field!",
    "Bitcoin just hit a new milestone — the future of finance is here! 🚀",
    "The #AIRevolution is transforming every industry. Incredible times!",
    "Breaking news covered live with AI analysis. Real journalism evolved!",
]
_NEGATIVE = [
    "This API documentation is absolutely terrible. Waste of hours.",
    "Production is down again. Third time this week. Beyond frustrated.",
    "Why is Kubernetes so complex? Spent all day on config.",
    "Data breach reported — security needs to be taken more seriously.",
    "The new UI redesign is horrible. Usability went completely backwards.",
    "Stack overflow errors flooding the logs. Codebase is a nightmare.",
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
    "Reading the Kafka docs for the new consumer protocol.",
    "Setting up a PostgreSQL cluster today. Standard setup.",
    "Docker containers are running. Moving to next task.",
    "Attended a webinar on distributed systems. Covered CAP theorem.",
    "Updated dependencies in package.json. Minor version bumps.",
    "Code review scheduled for 3pm. Looking at the auth module.",
    "Migrating from Python 3.9 to 3.11. Updating type hints.",
    "Pipeline processed 50k events with zero errors today.",
    "Reviewing schema migration script before applying to prod.",
    "Bitcoin trading volume is steady. Markets looking normal.",
    "New AI paper published on arxiv. Adding it to the reading list.",
    "BreakingNews feed running smoothly. No major incidents today.",
]
_HASHTAG_POOLS = [
    ["python", "programming", "coding"],
    ["ai", "machinelearning", "datascience"],
    ["kafka", "streaming", "bigdata"],
    ["tech", "software", "engineering"],
    ["cloud", "aws", "devops"],
    ["spark", "pyspark", "dataengineering"],
    ["AIRevolution", "ai", "futuretech"],       # high-volume
    ["Bitcoin", "crypto", "blockchain"],        # high-volume
    ["BreakingNews", "news", "trending"],       # high-volume
]
_AUTHORS = [
    "dev_sanjay", "data_nerd_42", "kafka_queen", "spark_wizard",
    "ml_enthusiast", "cloud_architect", "pythonista", "stream_master",
    "bytes_and_bits", "code_craftsman", "realtime_data", "pipeline_pro",
    "data_wrangler", "infra_ninja", "kafka_hacker", "etl_guru",
]


def generate_mock_tweet() -> dict:
    pool     = random.choices(
        [_POSITIVE, _NEGATIVE, _NEUTRAL],
        weights=[0.45, 0.35, 0.20],
    )[0]
    hashtags = random.choice(_HASHTAG_POOLS)
    return {
        "id":               str(uuid.uuid4()),
        "text":             random.choice(pool),
        "author":           random.choice(_AUTHORS),
        "author_followers": random.randint(50, 50_000),
        "lang":             "en",
        "created_at":       datetime.now(timezone.utc).isoformat(),
        "hashtags":         random.sample(hashtags, k=random.randint(1, len(hashtags))),
        "retweet_count":    random.randint(0, 500),
        "like_count":       random.randint(0, 2000),
        "source":           "mock",
    }


# ─── Mock coroutine ───────────────────────────────────────────────────────────
async def run_mock(producer: Producer, stop_event: asyncio.Event) -> None:
    global _in_mock_mode
    _in_mock_mode = True
    interval = 1.0 / max(MOCK_RATE, 0.1)
    logger.warning("▶ MOCK MODE active — %.1f tweet(s)/sec", MOCK_RATE)
    _init_keyword_timestamps()
    while not stop_event.is_set():
        publish(producer, generate_mock_tweet())
        # Keep all keyword timestamps fresh in mock mode.
        # Rotation is a live-mode feature: mock always generates data for every topic.
        now = time.time()
        for kw in _active_keywords:
            _keyword_last_seen[kw] = now
        await asyncio.sleep(interval * random.uniform(0.5, 1.5))


# ─── Live WebSocket coroutine ─────────────────────────────────────────────────
async def run_live(producer: Producer) -> bool:
    """
    Open a live WebSocket stream for _active_keywords.

    Every ROTATION_CHECK_SECS (30 s) the connection checks which keywords have
    gone silent for >= ROTATION_IDLE_SECS.  If any are found:
      • They are swapped for the next backup candidate.
      • _rotation_triggered is set so orchestrate() reconnects cleanly.
      • This function returns False without incrementing the failure counter.

    Returns True  — at least one tweet received (healthy).
    Returns False — connection error, no data, or rotation triggered.
    """
    global _live_fail_count, _in_mock_mode, _rotation_triggered

    if not TWITTER_API_KEY:
        return False

    ROTATION_CHECK_SECS = 30.0

    keywords_query = " OR ".join(kw.strip() for kw in _active_keywords)
    subscribe_msg  = json.dumps({"type": "subscribe",
                                 "query": keywords_query, "lang": "en"})
    received_any   = False

    try:
        async with websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            open_timeout=10,
        ) as ws:
            await ws.send(subscribe_msg)
            logger.info("✔ LIVE MODE — tracking: %s", _active_keywords)
            _in_mock_mode    = False
            _live_fail_count = 0
            _init_keyword_timestamps()   # seed all timestamps on fresh connect

            last_rotation_check = time.time()

            while True:
                # Wait up to ROTATION_CHECK_SECS for the next message
                remaining = ROTATION_CHECK_SECS - (time.time() - last_rotation_check)
                try:
                    message = await asyncio.wait_for(
                        ws.recv(), timeout=max(0.5, remaining)
                    )
                except asyncio.TimeoutError:
                    message = None

                if message is not None:
                    try:
                        data = json.loads(message)
                        if data.get("type") == "tweet":
                            tweet = normalize_live(data.get("data", {}))
                            if tweet["text"]:
                                publish(producer, tweet)
                                received_any = True
                        elif data.get("type") == "error":
                            logger.error("API stream error: %s", data)
                            if "rate limit" in str(data).lower():
                                logger.warning("Rate limit hit — backing off 60 s")
                                await asyncio.sleep(60)
                                return False
                    except json.JSONDecodeError:
                        pass

                # Rotation check every ~ROTATION_CHECK_SECS seconds
                if time.time() - last_rotation_check >= ROTATION_CHECK_SECS:
                    last_rotation_check = time.time()
                    if _rotate_silent_keywords():
                        _rotation_triggered = True
                        logger.info(
                            "Reconnecting with updated subscription: %s",
                            _active_keywords,
                        )
                        return False

    except (websockets.ConnectionClosedError,
            websockets.InvalidHandshake,
            websockets.WebSocketException,
            OSError) as exc:
        logger.warning("Live stream ended: %s", exc)

    return received_any


# ─── Main orchestrator ────────────────────────────────────────────────────────
async def orchestrate(producer: Producer) -> None:
    global _live_fail_count, _in_mock_mode, _rotation_triggered

    stop_mock  = asyncio.Event()
    mock_task  = None
    backoff    = 2
    last_retry = 0.0

    while True:
        now = time.time()
        should_try_live = TWITTER_API_KEY and (
            not _in_mock_mode
            or (now - last_retry) >= RETRY_LIVE_INTERVAL
        )

        if should_try_live:
            last_retry = now
            # Pause mock so we don't double-publish during live probe
            if mock_task and not mock_task.done():
                stop_mock.set()
                await mock_task
                stop_mock.clear()
                mock_task = None

            success = await run_live(producer)

            # Rotation reconnect — not a failure, just re-subscribe with new keywords
            if _rotation_triggered:
                _rotation_triggered = False
                logger.info("Re-subscribing after rotation: %s", _active_keywords)
                continue   # reconnect immediately; don't touch failure counter

            if success:
                _live_fail_count = 0
                _in_mock_mode    = False
                backoff          = 2
                continue   # immediately reconnect

            _live_fail_count += 1
            logger.warning("Live failure %d/%d", _live_fail_count, LIVE_FAIL_THRESHOLD)

            if _live_fail_count < LIVE_FAIL_THRESHOLD:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            logger.warning("Threshold reached — engaging MOCK mode")
            _in_mock_mode = True

        # Keep mock running between live-retry windows
        if _in_mock_mode and (mock_task is None or mock_task.done()):
            stop_mock.clear()
            mock_task = asyncio.create_task(run_mock(producer, stop_mock))

        sleep_for = max(1, RETRY_LIVE_INTERVAL - (time.time() - last_retry))
        await asyncio.sleep(min(sleep_for, 30))


# ─── Entrypoint ───────────────────────────────────────────────────────────────
def main() -> None:
    global _active_keywords
    _active_keywords = list(SEARCH_KEYWORDS)   # mutable copy; rotated at runtime
    _init_keyword_timestamps()

    if not TWITTER_API_KEY:
        logger.warning(
            "TWITTER_API_KEY not set — starting in MOCK mode. "
            "Set the key in .env to enable live tweets."
        )

    logger.info(
        "Producer starting — topic=%s bootstrap=%s keywords=%s backup_pool=%d",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, _active_keywords, len(BACKUP_KEYWORDS),
    )
    producer = build_kafka_producer()
    _write_heartbeat()

    try:
        asyncio.run(orchestrate(producer))
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing Kafka queue…")
    finally:
        producer.flush(10)
        logger.info("Producer stopped. Total published: %d", _published_count)


if __name__ == "__main__":
    main()
