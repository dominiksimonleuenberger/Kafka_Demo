import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def create_producer(brokers: str) -> KafkaProducer:
    """Create a KafkaProducer 
    that serialises sensor event values written in a dictionary as JSON."""
    return KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_reading(sensor_id: int) -> dict:
    """Return a single sensor reading with a normally-distributed temperature."""
    temperature = round(random.gauss(mu=25.5, sigma=0.75), 1)
    event_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "sensor_id": sensor_id,
        "temperature_value": temperature,
        "event_time": event_time,
    }


def main() -> None:
    """Kafka producer – sends simulated temperature sensor readings."""
    parser = argparse.ArgumentParser(description="Kafka temperature sensor measurement producer")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic_temperature_sensor_values", help="Target topic")
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per second")
    args = parser.parse_args()

    producer = create_producer(args.brokers)
    interval = 1.0 / args.rate
    print(f"Producing to {args.topic} at ~{args.rate} msg/s  (Ctrl+C to stop)")

    try:
        while True:
            sensor_id = random.choice([1, 2])
            reading = generate_reading(sensor_id)
            producer.send(
                args.topic,
                # key by sensor_id: all readings for same sensor land in same partition (deterministic partitioning)
                key=str(sensor_id), 
                value=reading,
            )
            print(f"  > sent: {reading}")
            # interval between 2 messages in seconds
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nShutting down producer …")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()