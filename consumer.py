import argparse
import json

from kafka import KafkaConsumer


def create_consumer(brokers: str, topic: str) -> KafkaConsumer:
    """Create a KafkaConsumer that deserialises JSON values."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        auto_offset_reset="earliest",
        group_id="temp-sensor-consumer-group",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def main() -> None:
    """Kafka consumer – prints temperature sensor readings from the topic."""
    parser = argparse.ArgumentParser(description="Kafka temperature sensor measurement consumer")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic_temperature_sensor_values", help="Source topic")
    args = parser.parse_args()

    consumer = create_consumer(args.brokers, args.topic)
    print(
        f"Consuming from {args.topic}  (Ctrl+C to stop)\n"
        f"{'partition':>9}  {'sensor_id':>9}  {'temperature':>11}  {'event_time'}"
    )

    try:
        for msg in consumer:
            v = msg.value
            print(
                f"  {msg.partition:>7}  "
                f"  {v['sensor_id']:>7}  "
                f"  {v['temperature_value']:>9.1f}  "
                f"  {v['event_time']}"
            )
    except KeyboardInterrupt:
        print("\nShutting down consumer …")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()