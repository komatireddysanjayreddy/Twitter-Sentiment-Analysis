"""
producer.py — Robust Twitter ingestion with automatic mock fallback.

Strategy:
  1. Attempt live TwitterAPI.io WebSocket stream.
  2. After LIVE_FAIL_THRESHOLD consecutive failures → flip to MOCK mode so
     the dashboard never runs empty.
  3. Every RETRY_LIVE_INTERVAL seconds, silently probe the real API again.
     If it succeeds, flip back to LIVE automatically.
  4. A heartbeat file is touched every HEARTBEAT_INTERVAL tweets so the
     Docker healthcheck and monitor.py can detect a stalled producer.

Environment variables:
  TWITTER_API_KEY           — TwitterAPI.io key (optional; triggers mock if absent)
  KAFKA_BOOTSTRAP_SERVERS   — default: localhost:9092
  KAFKA_TOPIC               — default: tweets
  SEARCH_KEYWORDS           — comma-separated, default: python,ai,technology
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
SEARCH_KEYWORDS         = os.getenv("SEARCH_KEYWORDS", "python,ai,technology").split(",")
MOCK_RATE               = float(os.getenv("MOCK_RATE", "1"))
LIVE_FAIL_THRESHOLD     = int(os.getenv("LIVE_FAIL_THRESHOLD", "3"))
RETRY_LIVE_INTERVAL     = int(os.getenv("RETRY_LIVE_INTERVAL", "300"))
HEARTBEAT_FILE          = os.getenv("HEARTBEAT_FILE", "/tmp/producer.heartbeat")
HEARTBEAT_INTERVAL      = int(os.getenv("HEARTBEAT_INTERVAL", "10"))

WS_URL = f"wss://api.twitterapi.io/twitter/tweet/websocket?apiKey={TWITTER_API_KEY}"

# ─── Session state ────────────────────────────────────────────────────────────
_published_count = 0
_live_fail_count = 0
_in_mock_mode    = False
_recent_ids: deque = deque(maxlen=2000)   # sliding dedup ring-buffer

# ─── Heartbeat ────────────────────────────────────────────────────────────────
def _write_heartbeat() -> None:
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass   # non-fatal


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
]
_HASHTAG_POOLS = [
    ["python", "programming", "coding"],
    ["ai", "machinelearning", "datascience"],
    ["kafka", "streaming", "bigdata"],
    ["tech", "software", "engineering"],
    ["cloud", "aws", "devops"],
    ["spark", "pyspark", "dataengineering"],
]
_AUTHORS = [
    "dev_sanjay", "data_nerd_42", "kafka_queen", "spark_wizard",
    "ml_enthusiast", "cloud_architect", "pythonista", "stream_master",
    "bytes_and_bits", "code_craftsman", "realtime_data", "pipeline_pro",
    "data_wrangler", "infra_ninja", "kafka_hacker", "etl_guru",
]


def generate_mock_tweet() -> dict:
    pool = random.choices(
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
    while not stop_event.is_set():
        publish(producer, generate_mock_tweet())
        # Add small jitter to simulate realistic burst patterns
        await asyncio.sleep(interval * random.uniform(0.5, 1.5))


# ─── Live WebSocket coroutine ─────────────────────────────────────────────────
async def run_live(producer: Producer) -> bool:
    """
    Try to open a live WebSocket stream.
    Returns True  if at least one tweet was received (healthy connection).
    Returns False if connection failed or no data received.
    """
    global _live_fail_count, _in_mock_mode

    if not TWITTER_API_KEY:
        return False

    keywords_query = " OR ".join(kw.strip() for kw in SEARCH_KEYWORDS)
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
            logger.info("✔ LIVE MODE — tracking: %s", SEARCH_KEYWORDS)
            _in_mock_mode    = False
            _live_fail_count = 0

            async for message in ws:
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
                            logger.warning("Rate limit hit — will fall back for 60s")
                            await asyncio.sleep(60)
                            return False
                except json.JSONDecodeError:
                    pass

    except (websockets.ConnectionClosedError,
            websockets.InvalidHandshake,
            websockets.WebSocketException,
            OSError) as exc:
        logger.warning("Live stream ended: %s", exc)

    return received_any


# ─── Main orchestrator ────────────────────────────────────────────────────────
async def orchestrate(producer: Producer) -> None:
    global _live_fail_count, _in_mock_mode

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
            # Pause mock so we don't double-publish during probe
            if mock_task and not mock_task.done():
                stop_mock.set()
                await mock_task
                stop_mock.clear()
                mock_task = None

            success = await run_live(producer)

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
    if not TWITTER_API_KEY:
        logger.warning(
            "TWITTER_API_KEY not set — starting in MOCK mode. "
            "Set the key in .env to enable live tweets."
        )

    logger.info("Producer starting — topic=%s bootstrap=%s",
                KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
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
