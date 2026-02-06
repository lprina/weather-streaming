"""
Spark Structured Streaming job for aggregating precipitation forecasts.

This application consumes minutely weather forecast events from a Kafka topic,
applies event-time windowing, and continuously computes the total precipitation
forecast for the next hour per latitude/longitude coordinate pair.

Aggregated results are:
- Persisted to Postgres for analytical querying

The job is designed to run locally using Docker Compose and is structured
to scale to many coordinate pairs and persistent sinks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    sum as spark_sum,
    to_timestamp,
    from_unixtime,
)
from pyspark.sql.types import StructType, StructField, DoubleType, LongType


POSTGRES_URL = "jdbc:postgresql://postgres:5432/weather"
POSTGRES_TABLE = "hourly_precipitation"
POSTGRES_PROPS = {
    "user": "weather_user",
    "password": "weather_pass",
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(batch_df, batch_id):
    """
    Write each micro-batch into Postgres.
    """
    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=POSTGRES_URL,
            table=POSTGRES_TABLE,
            properties=POSTGRES_PROPS,
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("WeatherPrecipitationStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    weather_schema = StructType([
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),
        StructField("precipitation_mm", DoubleType(), nullable=False),
    ])

    # Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "minutely_forecasts")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON and add event-time column
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), weather_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("timestamp")))
        )
    )

    # Aggregate over 1-hour event-time window
    aggregated_df = (
        parsed_df
        .withWatermark("event_time", "1 hour")
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("lat"),
            col("lon"),
        )
        .agg(
            spark_sum("precipitation_mm").alias("total_precipitation_mm")
        )
    )

    # FLATTEN window struct for JDBC compatibility
    final_df = (
        aggregated_df
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("lat"),
            col("lon"),
            col("total_precipitation_mm"),
        )
    )

    # Write to Postgres using foreachBatch
    query = (
        final_df
        .writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/spark-checkpoints/weather")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
