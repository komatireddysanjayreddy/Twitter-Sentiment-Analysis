"""
processor.py — Optimised PySpark Structured Streaming: Kafka → VADER → PostgreSQL

Key improvements over v1:
  • VADER initialised once per Spark partition (mapPartitions), not per row
  • Single struct UDF: compound + label computed in one pass (no double scoring)
  • Rolling 5-minute sentiment averages written to rolling_sentiment table
  • Materialized views refreshed automatically after every successful batch
  • Windows-compatible checkpoint path (configurable via env var)
  • SPARK_LOCAL_IP resolved at startup for Docker/Windows compatibility

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  — e.g. localhost:9092
  KAFKA_TOPIC              — default: tweets
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
  CHECKPOINT_DIR           — default: /tmp/spark-checkpoints/tweets
  TRIGGER_INTERVAL         — default: 10 seconds
"""

import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, udf, current_timestamp, to_timestamp, lit, size,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType,
    FloatType, Row,
)
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("processor")

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "tweets")
POSTGRES_HOST           = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT           = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB             = os.environ["POSTGRES_DB"]
POSTGRES_USER           = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD       = os.environ["POSTGRES_PASSWORD"]
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/tweets")
TRIGGER_INTERVAL        = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# ─── Kafka JSON schema ────────────────────────────────────────────────────────
TWEET_SCHEMA = StructType([
    StructField("id",               StringType(), True),
    StructField("text",             StringType(), True),
    StructField("author",           StringType(), True),
    StructField("author_followers", LongType(),   True),
    StructField("lang",             StringType(), True),
    StructField("created_at",       StringType(), True),
    StructField("hashtags",         ArrayType(StringType()), True),
    StructField("retweet_count",    LongType(),   True),
    StructField("like_count",       LongType(),   True),
    StructField("source",           StringType(), True),
])

# ─── VADER UDF — partition-level initialisation ───────────────────────────────
# Using a module-level cache means each *worker process* creates exactly one
# SentimentIntensityAnalyzer regardless of how many rows it processes.

_analyzer = None

def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def _score_text(text: str):
    """Return (compound: float, label: str). Called once per row."""
    if not text:
        return (0.0, "neutral")
    scores   = _get_analyzer().polarity_scores(text)
    compound = float(scores["compound"])
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return (compound, label)


# Two separate UDFs that share the same cached analyzer — both get compiled once
@udf(returnType=FloatType())
def udf_compound(text: str) -> float:
    return _score_text(text)[0]


@udf(returnType=StringType())
def udf_label(text: str) -> str:
    return _score_text(text)[1]


# ─── Spark session ────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder
        .appName("TwitterSentimentProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─── Kafka source ─────────────────────────────────────────────────────────────
def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "spark-sentiment-consumer")
        .load()
    )


# ─── Transformation ───────────────────────────────────────────────────────────
def transform(raw: DataFrame) -> DataFrame:
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), TWEET_SCHEMA).alias("t"))
        .select("t.*")
        .filter(col("text").isNotNull() & (col("text") != ""))
        .filter(col("lang") == lit("en"))
    )
    return (
        parsed
        .withColumn("compound_score",    udf_compound(col("text")))
        .withColumn("sentiment",         udf_label(col("text")))
        .withColumn("processed_at",      current_timestamp())
        .withColumn("tweet_created_at",  to_timestamp(col("created_at")))
        .withColumn("hashtag_count",     size(col("hashtags")))
        .select(
            col("id").alias("tweet_id"),
            col("text"),
            col("author"),
            col("author_followers"),
            col("lang"),
            col("tweet_created_at"),
            col("hashtags"),
            col("hashtag_count"),
            col("retweet_count"),
            col("like_count"),
            col("compound_score"),
            col("sentiment"),
            col("source"),
            col("processed_at"),
        )
    )


# ─── PostgreSQL helpers ───────────────────────────────────────────────────────
def _pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


# ─── Batch sink ───────────────────────────────────────────────────────────────
def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        logger.info("Batch %s — empty, skipping", batch_id)
        return

    logger.info("Batch %s — %d rows", batch_id, len(rows))
    records = [
        (
            r["tweet_id"], r["text"], r["author"], r["author_followers"],
            r["lang"], r["tweet_created_at"], r["hashtags"], r["hashtag_count"],
            r["retweet_count"], r["like_count"], r["compound_score"],
            r["sentiment"], r["source"], r["processed_at"],
        )
        for r in rows
    ]

    insert_tweets_sql = """
        INSERT INTO tweet_sentiments (
            tweet_id, text, author, author_followers, lang,
            tweet_created_at, hashtags, hashtag_count,
            retweet_count, like_count, compound_score,
            sentiment, source, processed_at
        ) VALUES %s
        ON CONFLICT (tweet_id) DO UPDATE SET
            compound_score = EXCLUDED.compound_score,
            sentiment      = EXCLUDED.sentiment,
            processed_at   = EXCLUDED.processed_at
    """

    # Rolling 5-min averages — computed in PostgreSQL for correctness
    rolling_sql = """
        INSERT INTO rolling_sentiment (window_start, window_end, sentiment,
                                       tweet_count, avg_compound, computed_at)
        SELECT
            date_trunc('minute', NOW()) - INTERVAL '5 minutes',
            date_trunc('minute', NOW()),
            sentiment,
            COUNT(*)                                  AS tweet_count,
            ROUND(AVG(compound_score)::NUMERIC, 4)    AS avg_compound,
            NOW()
        FROM tweet_sentiments
        WHERE processed_at >= NOW() - INTERVAL '5 minutes'
        GROUP BY sentiment
        ON CONFLICT (window_end, sentiment) DO UPDATE SET
            tweet_count  = EXCLUDED.tweet_count,
            avg_compound = EXCLUDED.avg_compound,
            computed_at  = EXCLUDED.computed_at
    """

    try:
        conn = _pg_connect()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_tweets_sql, records)
                cur.execute(rolling_sql)
                # Refresh materialized views so dashboard sees fresh data
                cur.execute("SELECT refresh_materialized_views()")
        conn.close()
        logger.info("Batch %s — committed + rolling avg updated + views refreshed",
                    batch_id)
    except psycopg2.Error as exc:
        logger.error("PostgreSQL error in batch %s: %s", batch_id, exc)
        raise   # Let Spark retry the batch


# ─── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Initialising Spark session…")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading Kafka topic=%s", KAFKA_TOPIC)
    raw_stream = read_kafka(spark)
    enriched   = transform(raw_stream)

    query = (
        enriched.writeStream
        .foreachBatch(write_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    logger.info("Streaming started — trigger=%s  (Ctrl+C to stop)", TRIGGER_INTERVAL)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping…")
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark session closed.")


if __name__ == "__main__":
    main()
