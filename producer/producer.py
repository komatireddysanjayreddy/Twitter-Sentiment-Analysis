"""
producer.py — Live Twitter/X data ingestion via TwitterAPI.io WebSocket
Publishes raw tweet JSON to Kafka topic 'tweets'.

Environment variables required:
  TWITTER_API_KEY          — TwitterAPI.io key
  KAFKA_BOOTSTRAP_SERVERS  — e.g. localhost:9092
  KAFKA_TOPIC              — default: tweets
  SEARCH_KEYWORDS          — comma-separated keywords to track
"""

import os
import json
import time
import logging
import asyncio
import websockets
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer")

# ─── Config ──────────────────────────────────────────────────────────────────
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]          # Required
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tweets")
SEARCH_KEYWORDS = os.getenv("SEARCH_KEYWORDS", "python,ai,technology").split(",")

# TwitterAPI.io WebSocket endpoint
WS_URL = f"wss://api.twitterapi.io/twitter/tweet/websocket?apiKey={TWITTER_API_KEY}"

# ─── Kafka Producer ──────────────────────────────────────────────────────────
def build_kafka_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "twitter-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
        "compression.type": "snappy",
        "linger.ms": 20,           # Small batching window
        "batch.size": 32768,
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for topic=%s partition=%s: %s",
                     msg.topic(), msg.partition(), err)
    else:
        logger.debug("Delivered to %s [%s] @ offset %s",
                     msg.topic(), msg.partition(), msg.offset())


def publish(producer: Producer, tweet: dict) -> None:
    """Serialize and publish one tweet dict to Kafka."""
    try:
        payload = json.dumps(tweet, ensure_ascii=False).encode("utf-8")
        # Use tweet id as key for partition ordering
        key = str(tweet.get("id", "")).encode("utf-8")
        producer.produce(
            KAFKA_TOPIC,
            key=key,
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)   # Non-blocking poll to trigger callbacks
    except KafkaException as exc:
        logger.error("Kafka produce error: %s", exc)
    except BufferError:
        logger.warning("Kafka internal queue full — flushing before retry")
        producer.flush(timeout=5)
        producer.produce(KAFKA_TOPIC, key=key, value=payload, callback=delivery_report)


def normalize_tweet(raw: dict) -> dict:
    """Extract only the fields we need downstream."""
    return {
        "id": raw.get("id") or raw.get("id_str"),
        "text": raw.get("text") or raw.get("full_text", ""),
        "author": raw.get("user", {}).get("screen_name", "unknown"),
        "author_followers": raw.get("user", {}).get("followers_count", 0),
        "lang": raw.get("lang", "en"),
        "created_at": raw.get("created_at", ""),
        "hashtags": [
            ht["text"]
            for ht in raw.get("entities", {}).get("hashtags", [])
        ],
        "retweet_count": raw.get("retweet_count", 0),
        "like_count": raw.get("favorite_count", 0),
        "source": "twitterapi.io",
    }


# ─── WebSocket Stream ────────────────────────────────────────────────────────
async def stream_tweets(producer: Producer) -> None:
    keywords_query = " OR ".join(SEARCH_KEYWORDS)
    subscribe_msg = json.dumps({
        "type": "subscribe",
        "query": keywords_query,
        "lang": "en",
    })

    backoff = 1  # seconds
    max_backoff = 60

    while True:
        try:
            logger.info("Connecting to TwitterAPI.io WebSocket…")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                await ws.send(subscribe_msg)
                logger.info("Subscribed — tracking: %s", SEARCH_KEYWORDS)
                backoff = 1  # Reset on successful connect

                async for message in ws:
                    try:
                        data = json.loads(message)
                        if data.get("type") == "tweet":
                            tweet = normalize_tweet(data.get("data", {}))
                            publish(producer, tweet)
                            logger.info("Published tweet id=%s author=@%s",
                                        tweet["id"], tweet["author"])
                        elif data.get("type") == "error":
                            logger.error("Stream error from API: %s", data)
                            if "rate limit" in str(data).lower():
                                logger.warning("Rate limit hit — sleeping 60s")
                                await asyncio.sleep(60)
                    except json.JSONDecodeError as e:
                        logger.warning("Failed to parse message: %s", e)

        except websockets.ConnectionClosedError as e:
            logger.warning("WebSocket closed (%s) — reconnecting in %ss", e, backoff)
        except OSError as e:
            logger.error("Network error: %s — reconnecting in %ss", e, backoff)
        except Exception as e:
            logger.exception("Unexpected error: %s", e)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


# ─── Entrypoint ──────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Starting Twitter producer — topic=%s", KAFKA_TOPIC)
    producer = build_kafka_producer()

    try:
        asyncio.run(stream_tweets(producer))
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing Kafka queue…")
    finally:
        producer.flush(timeout=10)
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
