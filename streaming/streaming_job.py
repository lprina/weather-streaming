"""
Spark Structured Streaming job for aggregating precipitation forecasts.

This application consumes minutely weather forecast events from a Kafka topic,
applies event-time windowing, and continuously computes the total precipitation
forecast for the next hour per latitude/longitude coordinate pair.

The job is designed to run in local Spark mode using Docker and can be extended
to support multiple coordinate pairs and persistent sinks.
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


def main() -> None:
    """
    Entry point for the Spark Structured Streaming application.

    - Creates a Spark session
    - Reads JSON-encoded weather events from Kafka
    - Parses and enriches events with event-time timestamps
    - Aggregates precipitation over a 1-hour tumbling window
    - Continuously outputs aggregated results to the console
    """
    spark = (
        SparkSession.builder
        .appName("WeatherPrecipitationStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Schema describing the Kafka message value
    weather_schema = StructType([
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),  # unix epoch (seconds)
        StructField("precipitation_mm", DoubleType(), nullable=False),
    ])

    # Read streaming data from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "minutely_forecasts")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON payload and convert unix timestamp to Spark event time
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), weather_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("timestamp")))
        )
    )

    # Aggregate precipitation over a 1-hour event-time window
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

    # Write aggregated results to the console
    query = (
        aggregated_df
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
