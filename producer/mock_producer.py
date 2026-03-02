"""
mock_producer.py — Synthetic tweet stream for local testing.
No API key required. Generates realistic-looking tweet data
and publishes to Kafka at a configurable rate.

Usage:
  python mock_producer.py
  MOCK_RATE=2 python mock_producer.py   # 2 tweets/second
"""

import os
import json
import time
import random
import logging
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("mock-producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tweets")
MOCK_RATE = float(os.getenv("MOCK_RATE", "1"))  # tweets per second

# ─── Sample Data ─────────────────────────────────────────────────────────────
POSITIVE_TEXTS = [
    "I absolutely love the new features in Python 3.12! Incredible work 🚀",
    "Just deployed my first machine learning model to production. Feeling amazing!",
    "The AI revolution is creating so many opportunities. Super excited for the future!",
    "Open source communities are the best. So much collaboration and support ❤️",
    "Spark Streaming is blazingly fast. Genuinely impressed with the performance!",
    "Big data pipelines running flawlessly today. Engineering is beautiful.",
    "Cloud infrastructure costs dropped 40% after optimization. Happy Friday!",
    "Just got promoted to Senior Engineer. Hard work pays off!",
]

NEGATIVE_TEXTS = [
    "This API documentation is absolutely terrible. Waste of hours.",
    "Production is down again. Third time this week. Beyond frustrated.",
    "Why is Kubernetes so unnecessarily complex? Spent all day on config.",
    "Data breach reported — companies need to take security way more seriously.",
    "The new UI redesign is horrible. Usability went completely backwards.",
    "Stack overflow errors flooding the logs. This codebase is a nightmare.",
    "Layoffs hitting tech hard. It's a tough market out there.",
    "Deployment failed AGAIN. CI/CD pipeline is completely broken.",
]

NEUTRAL_TEXTS = [
    "Just pushed a new commit to my open source project. PR is open for review.",
    "Reading the Kafka documentation for the new consumer group protocol.",
    "Setting up a new PostgreSQL cluster today. Standard setup process.",
    "Docker containers are running. Moving to next sprint task.",
    "Attended a webinar on distributed systems. Covered CAP theorem basics.",
    "Updated dependencies in package.json. Minor version bumps across the board.",
    "Code review scheduled for 3pm. Will look at the authentication module.",
    "Migrating from Python 3.9 to 3.11. Updating type hints along the way.",
]

HASHTAG_POOLS = [
    ["python", "programming", "coding"],
    ["ai", "machinelearning", "datascience"],
    ["kafka", "streaming", "bigdata"],
    ["tech", "software", "engineering"],
    ["cloud", "aws", "devops"],
]

AUTHORS = [
    "dev_sanjay", "data_nerd_42", "kafka_queen", "spark_wizard",
    "ml_enthusiast", "cloud_architect", "pythonista", "stream_master",
    "bytes_and_bits", "code_craftsman", "realtime_data", "pipeline_pro",
]


def generate_tweet() -> dict:
    sentiment_pool = random.choices(
        [POSITIVE_TEXTS, NEGATIVE_TEXTS, NEUTRAL_TEXTS],
        weights=[0.45, 0.35, 0.20],
        k=1,
    )[0]
    hashtags = random.choice(HASHTAG_POOLS)
    return {
        "id": str(uuid.uuid4()),
        "text": random.choice(sentiment_pool),
        "author": random.choice(AUTHORS),
        "author_followers": random.randint(50, 50000),
        "lang": "en",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "hashtags": random.sample(hashtags, k=random.randint(1, len(hashtags))),
        "retweet_count": random.randint(0, 500),
        "like_count": random.randint(0, 2000),
        "source": "mock",
    }


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%s]", msg.topic(), msg.partition())


def main() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "mock-twitter-producer",
        "acks": "all",
        "retries": 3,
    }
    producer = Producer(conf)
    interval = 1.0 / MOCK_RATE

    logger.info("Mock producer started — %.1f tweet(s)/sec → topic=%s",
                MOCK_RATE, KAFKA_TOPIC)

    try:
        while True:
            tweet = generate_tweet()
            payload = json.dumps(tweet).encode("utf-8")
            try:
                producer.produce(
                    KAFKA_TOPIC,
                    key=tweet["id"].encode(),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)
                logger.info("Mock tweet: [%s] %.60s…",
                            tweet["author"], tweet["text"])
            except KafkaException as e:
                logger.error("Kafka error: %s", e)
            except BufferError:
                logger.warning("Buffer full — flushing")
                producer.flush(2)

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Stopping mock producer…")
    finally:
        producer.flush(10)
        logger.info("Done.")


if __name__ == "__main__":
    main()
