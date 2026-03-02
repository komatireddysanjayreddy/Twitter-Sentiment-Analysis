"""
monitor.py — Pipeline health monitor with auto-recovery.

Checks two signals every CHECK_INTERVAL seconds:
  1. Producer heartbeat file — stale if not updated in HEARTBEAT_STALE_SECS
  2. Kafka consumer lag      — high lag means processor is falling behind

Actions:
  • Stale producer  → restart the producer Docker container
  • High Kafka lag  → log warning (processor self-recovers via trigger)
  • DB connectivity → log warning (non-recoverable without manual intervention)

Run standalone:
  python producer/monitor.py

Or as a Docker service (see docker-compose.yml).

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS     — default: localhost:9092
  KAFKA_TOPIC                 — default: tweets
  KAFKA_CONSUMER_GROUP        — default: spark-sentiment-consumer
  HEARTBEAT_FILE              — default: /tmp/producer.heartbeat
  HEARTBEAT_STALE_SECS        — seconds before producer is considered dead (default: 120)
  MAX_LAG_THRESHOLD           — Kafka consumer lag warning level (default: 5000)
  CHECK_INTERVAL              — seconds between health checks (default: 30)
  PRODUCER_CONTAINER          — Docker container name to restart (default: twitter_producer)
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
"""

import os
import time
import logging

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] monitor — %(message)s",
)
logger = logging.getLogger("monitor")

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "tweets")
KAFKA_CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "spark-sentiment-consumer")
HEARTBEAT_FILE          = os.getenv("HEARTBEAT_FILE", "/tmp/producer.heartbeat")
HEARTBEAT_STALE_SECS    = int(os.getenv("HEARTBEAT_STALE_SECS", "120"))
MAX_LAG_THRESHOLD       = int(os.getenv("MAX_LAG_THRESHOLD", "5000"))
CHECK_INTERVAL          = int(os.getenv("CHECK_INTERVAL", "30"))
PRODUCER_CONTAINER      = os.getenv("PRODUCER_CONTAINER", "twitter_producer")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "sentiment_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "sentiment_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


# ─── Health checks ────────────────────────────────────────────────────────────
def check_producer_heartbeat() -> dict:
    """Return {'healthy': bool, 'age_secs': float}."""
    try:
        mtime    = os.path.getmtime(HEARTBEAT_FILE)
        age_secs = time.time() - mtime
        healthy  = age_secs < HEARTBEAT_STALE_SECS
        return {"healthy": healthy, "age_secs": round(age_secs, 1)}
    except FileNotFoundError:
        return {"healthy": False, "age_secs": None, "reason": "heartbeat file missing"}
    except OSError as exc:
        return {"healthy": False, "age_secs": None, "reason": str(exc)}


def check_kafka_lag() -> dict:
    """Return {'healthy': bool, 'total_lag': int}."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    try:
        metadata = admin.list_topics(topic=KAFKA_TOPIC, timeout=5)
        if KAFKA_TOPIC not in metadata.topics:
            return {"healthy": False, "total_lag": -1, "reason": "topic not found"}

        partitions    = metadata.topics[KAFKA_TOPIC].partitions
        num_parts     = len(partitions)
        total_lag     = 0

        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id":          KAFKA_CONSUMER_GROUP,
            "enable.auto.commit": False,
        })

        from confluent_kafka import TopicPartition
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]

        # Get committed offsets for the consumer group
        committed = consumer.committed(tps, timeout=5)

        # Get high-water marks (latest offsets)
        low_high = consumer.get_watermark_offsets
        for tp in tps:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(KAFKA_TOPIC, tp.partition), timeout=5
            )
            committed_offset = (
                tp.offset if tp.offset >= 0 else low
            )
            lag = max(0, high - committed_offset)
            total_lag += lag

        consumer.close()
        healthy = total_lag < MAX_LAG_THRESHOLD
        return {"healthy": healthy, "total_lag": total_lag, "partitions": num_parts}

    except KafkaException as exc:
        return {"healthy": False, "total_lag": -1, "reason": str(exc)}
    except Exception as exc:
        return {"healthy": False, "total_lag": -1, "reason": str(exc)}


def check_postgres() -> dict:
    """Return {'healthy': bool, 'row_count': int}."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD, connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tweet_sentiments")
            count = cur.fetchone()[0]
        conn.close()
        return {"healthy": True, "row_count": count}
    except Exception as exc:
        return {"healthy": False, "row_count": -1, "reason": str(exc)}


# ─── Recovery ─────────────────────────────────────────────────────────────────
def restart_producer_container() -> bool:
    """Restart the producer Docker container. Returns True on success."""
    try:
        import docker
        client    = docker.from_env()
        container = client.containers.get(PRODUCER_CONTAINER)
        container.restart(timeout=10)
        logger.info("Restarted container: %s", PRODUCER_CONTAINER)
        return True
    except ImportError:
        logger.warning("docker-py not installed — cannot restart container. "
                        "Install with: pip install docker")
        return False
    except Exception as exc:
        logger.error("Failed to restart %s: %s", PRODUCER_CONTAINER, exc)
        return False


# ─── Main loop ────────────────────────────────────────────────────────────────
def run_monitor() -> None:
    logger.info(
        "Monitor started — checking every %ds | heartbeat_stale=%ds | max_lag=%d",
        CHECK_INTERVAL, HEARTBEAT_STALE_SECS, MAX_LAG_THRESHOLD,
    )
    restart_count = 0

    while True:
        hb  = check_producer_heartbeat()
        db  = check_postgres()

        logger.info(
            "HEARTBEAT: %s (age=%.0fs) | DB: %s (rows=%s)",
            "OK" if hb["healthy"] else "STALE",
            hb.get("age_secs") or -1,
            "OK" if db["healthy"] else "ERR",
            db.get("row_count", "?"),
        )

        # Only check Kafka lag if Kafka is reachable
        try:
            lag = check_kafka_lag()
            status = "OK" if lag["healthy"] else "HIGH"
            logger.info("KAFKA LAG: %s (total=%s partitions=%s)",
                        status, lag.get("total_lag"), lag.get("partitions"))
            if not lag["healthy"]:
                logger.warning(
                    "Kafka lag %d exceeds threshold %d — processor may be slow",
                    lag.get("total_lag", 0), MAX_LAG_THRESHOLD,
                )
        except Exception as exc:
            logger.warning("Kafka lag check failed: %s", exc)

        # Auto-restart producer if heartbeat is stale
        if not hb["healthy"]:
            logger.warning(
                "Producer heartbeat stale (age=%s) — attempting restart #%d",
                hb.get("age_secs"), restart_count + 1,
            )
            if restart_producer_container():
                restart_count += 1
                # Give producer time to boot before next check
                time.sleep(15)

        if not db["healthy"]:
            logger.error("PostgreSQL unreachable: %s", db.get("reason"))

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_monitor()
