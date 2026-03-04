import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


MAP_SENSOR = {
    1: {"mean":25, "sigma": 0.75},
    2: {"mean":18, "sigma": 1.25}
}


def create_producer(brokers: str) -> KafkaProducer:
    """Create a KafkaProducer 
    that serialises sensor event values written in a dictionary as JSON."""
    return KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_sensor_event(sensor_id: int) -> dict:
    """Return a single sensor reading with a normally-distributed temperature."""
    sensor = MAP_SENSOR.get(sensor_id, {"mean":0, "sigma": 0})
    temperature = round(random.gauss(mu=sensor["mean"], sigma=sensor["sigma"]), 1)
    event_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return {
        "sensor_id": sensor_id,
        "temperature_value": temperature,
        "event_time": event_time,
    }


def main() -> None:
    """Kafka producer – sends simulated temperature sensor readings."""
    parser = argparse.ArgumentParser(description="Kafka temperature sensor measurement producer")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic-sensor-values-temperature", help="Target topic")
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per second")
    args = parser.parse_args()

    producer = create_producer(args.brokers)
    interval = 1.0 / args.rate
    print(f"Producing to {args.topic} at ~{args.rate} msg/s  (Ctrl+C to stop)")

    try:
        while True:
            sensor_id = random.choice(list(MAP_SENSOR.keys()))
            dict_event = generate_sensor_event(sensor_id)
            dict_event["send_time   "] = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            producer.send(
                args.topic,
                # key by sensor_id: all readings for same sensor go to same partition (deterministic)
                key=str(sensor_id), 
                value=dict_event,
            )
            print(f"  > sent:     {dict_event}")
            # interval between two messages in seconds
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nShutting down producer …")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()