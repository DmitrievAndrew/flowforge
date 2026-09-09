"""
Spark Streaming Consumer for MOEX data.
Reads from Kafka, computes indicators, writes to MongoDB.
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    expr,
    from_json,
    max,
    min,
    stddev,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/spark_checkpoints")

# Схема сообщения (свечи)
SCHEMA = StructType(
    [
        StructField("open", DoubleType()),
        StructField("close", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("value", DoubleType()),
        StructField("volume", LongType()),
        StructField("begin", StringType()),
        StructField("end", StringType()),
        StructField("ticker", StringType()),
        StructField("fetched_at", DoubleType()),
    ]
)


def create_spark_session() -> SparkSession:
    """Создаёт и возвращает Spark сессию с настройками для MongoDB и Kafka."""
    return (
        SparkSession.builder.appName("FlowForgeStreaming")
        .config("spark.mongodb.output.uri", MONGODB_URI)
        .config("spark.mongodb.write.batch.size", "100")
        .getOrCreate()
    )


def read_stream(spark: SparkSession) -> DataFrame:
    """Читает поток из Kafka, десериализует JSON и возвращает DataFrame с колонками свечей."""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_IN)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), SCHEMA).alias("data"))
        .select("data.*")
    )
    return df


def compute_indicators(df: DataFrame) -> DataFrame:
    """
    Добавляет водяной знак, агрегирует по 1-минутным окнам,
    вычисляет скользящие средние (SMA 5 и 20).
    """
    # Преобразуем строку begin в timestamp
    df = df.withColumn("event_time", expr("to_timestamp(begin, 'yyyy-MM-dd HH:mm:ss')"))
    df = df.withWatermark("event_time", "1 minute")

    # Агрегация по окну
    aggregated = df.groupBy(
        col("ticker"), window(col("event_time"), "1 minute", "1 minute")
    ).agg(
        avg("close").alias("avg_close"),
        stddev("close").alias("volatility"),
        max("high").alias("high"),
        min("low").alias("low"),
        avg("volume").alias("avg_volume"),
    )

    aggregated = aggregated.select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_close"),
        col("volatility"),
        col("high"),
        col("low"),
        col("avg_volume"),
    )

    # Скользящие средние
    window_spec = (
        Window.partitionBy("ticker").orderBy("window_start").rowsBetween(-4, 0)
    )
    aggregated = aggregated.withColumn("sma_5", avg("avg_close").over(window_spec))

    window_spec20 = (
        Window.partitionBy("ticker").orderBy("window_start").rowsBetween(-19, 0)
    )
    aggregated = aggregated.withColumn("sma_20", avg("avg_close").over(window_spec20))

    aggregated = aggregated.withColumn("processed_at", current_timestamp())
    return aggregated


def write_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    """Записывает батч в MongoDB (коллекция processed_aggregates)."""
    if batch_df.count() > 0:
        batch_df.write.format("mongo").mode("append").option("uri", MONGODB_URI).option(
            "collection", "processed_aggregates"
        ).save()
        logger.info(f"Batch {batch_id} written to MongoDB ({batch_df.count()} rows)")
    else:
        logger.info(f"Batch {batch_id} empty, skipping")


def main() -> None:
    """Запускает Spark Streaming и обрабатывает данные до завершения."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = read_stream(spark)
    aggregated = compute_indicators(df)

    query = (
        aggregated.writeStream.foreachBatch(write_to_mongo)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info("Streaming started. Waiting for termination...")
    _ = query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception:
        logger.exception("Unhandled exception")
