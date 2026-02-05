"""
Replay stored OpenWeatherMap minutely precipitation forecasts as a real-time stream.

This producer reads a static weather JSON file from disk and emits one Kafka
message per forecast minute. A sleep interval is used between messages to
simulate real-time data arrival without repeatedly calling the external API.
"""

import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "minutely_forecasts"

LAT = 52.084516
LON = 5.115539
DATA_PATH = "data/weather.json"

def load_weather_data(path: str) -> dict:
    """
    Load stored weather data from a JSON file.

    Parameters
    ----------
    path : str
        Path to the JSON file containing weather data.

    Returns
    -------
    dict
        Parsed weather data.
    """
    with open(path) as f:
        return json.load(f)

def create_producer() -> KafkaProducer:
    """
    Create and configure a Kafka producer.

    Returns
    -------
    KafkaProducer
        Configured Kafka producer instance.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def main() -> None:
    """
    Continuously replay minutely precipitation forecasts into Kafka.
    """
    data = load_weather_data(DATA_PATH)
    minutely = data.get("minutely", [])

    producer = create_producer()

    while True:
        for minute in minutely:
            event = {
                "lat": LAT,
                "lon": LON,
                "timestamp": minute["dt"],
                "precipitation_mm": minute.get("precipitation", 0.0),
            }

            producer.send(TOPIC, event)
            producer.flush()

            print(f"Sent event: {event}")
            time.sleep(60)  # simulate real-time streaming

if __name__ == "__main__":
    main()
