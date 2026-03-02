"""
processor.py — PySpark Structured Streaming: Kafka → VADER → PostgreSQL

Reads tweet JSON from Kafka topic 'tweets', scores sentiment with VADER,
writes enriched records to PostgreSQL in micro-batches.

Environment variables required:
  KAFKA_BOOTSTRAP_SERVERS  — e.g. localhost:9092
  KAFKA_TOPIC              — default: tweets
  POSTGRES_HOST            — PostgreSQL host
  POSTGRES_PORT            — PostgreSQL port (default: 5432)
  POSTGRES_DB              — database name
  POSTGRES_USER            — username
  POSTGRES_PASSWORD        — password
"""

import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, udf, current_timestamp, to_timestamp, lit, size
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType, FloatType
)
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# VADER must be importable in the driver; workers use broadcast-compatible import
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("processor")

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tweets")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/tweets")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# ─── Tweet Schema ─────────────────────────────────────────────────────────────
TWEET_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("author", StringType(), True),
    StructField("author_followers", LongType(), True),
    StructField("lang", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("retweet_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("source", StringType(), True),
])


# ─── Sentiment UDF ───────────────────────────────────────────────────────────
def _score_sentiment(text: str):
    """Return (compound_score, label) for a given text."""
    if not text:
        return (0.0, "neutral")
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    compound = float(scores["compound"])
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return (compound, label)


# Spark UDFs — keep them module-level so they serialize cleanly
@udf(returnType=FloatType())
def udf_compound_score(text: str) -> float:
    return _score_sentiment(text)[0]


@udf(returnType=StringType())
def udf_sentiment_label(text: str) -> str:
    return _score_sentiment(text)[1]


# ─── Spark Session ────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    # SPARK_LOCAL_IP avoids hostname-resolution failures on Windows/Docker
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


# ─── Kafka Source ─────────────────────────────────────────────────────────────
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
    """Parse JSON, score sentiment, produce enriched DataFrame."""
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), TWEET_SCHEMA).alias("t"))
        .select("t.*")
        .filter(col("text").isNotNull() & (col("text") != ""))
        .filter(col("lang") == lit("en"))
    )

    enriched = (
        parsed
        .withColumn("compound_score", udf_compound_score(col("text")))
        .withColumn("sentiment", udf_sentiment_label(col("text")))
        .withColumn("processed_at", current_timestamp())
        .withColumn(
            "tweet_created_at",
            to_timestamp(col("created_at"))
        )
        .withColumn(
            "hashtag_count",
            size(col("hashtags"))
        )
    )

    return enriched.select(
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


# ─── PostgreSQL Sink ──────────────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: write each micro-batch to PostgreSQL."""
    rows = batch_df.collect()
    if not rows:
        logger.info("Batch %s — empty, skipping", batch_id)
        return

    logger.info("Batch %s — writing %d rows to PostgreSQL", batch_id, len(rows))

    records = [
        (
            r["tweet_id"],
            r["text"],
            r["author"],
            r["author_followers"],
            r["lang"],
            r["tweet_created_at"],
            r["hashtags"],        # list → text[] in psycopg2
            r["hashtag_count"],
            r["retweet_count"],
            r["like_count"],
            r["compound_score"],
            r["sentiment"],
            r["source"],
            r["processed_at"],
        )
        for r in rows
    ]

    sql = """
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

    try:
        conn = get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, records)
        conn.close()
        logger.info("Batch %s — committed %d rows", batch_id, len(records))
    except psycopg2.Error as e:
        logger.error("PostgreSQL write error in batch %s: %s", batch_id, e)
        raise   # Let Spark retry the batch


# ─── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Initializing Spark session…")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka topic=%s", KAFKA_TOPIC)
    raw_stream = read_kafka(spark)
    enriched_stream = transform(raw_stream)

    query = (
        enriched_stream.writeStream
        .foreachBatch(write_batch_to_postgres)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    logger.info("Streaming query started — awaiting termination (Ctrl+C to stop)")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping query…")
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark session closed.")


if __name__ == "__main__":
    main()
