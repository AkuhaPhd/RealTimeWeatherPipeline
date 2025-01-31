### consumer_main.py
import json
from confluent_kafka import Consumer

# KafkaConsumerService: Handles Kafka message consumption
class KafkaConsumerService:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list):
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest"
        })
        self.consumer.subscribe(topics)

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Wait up to 1 second for a message
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                print(f"Received message from topic {msg.topic()}:")
                print(f"Key: {msg.key().decode('utf-8') if msg.key() else None}")
                print(f"Value: {json.loads(msg.value().decode('utf-8'))}\n")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

# Main function for the consumer pipeline
if __name__ == "__main__":
    # Configuration for consumer
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    CONSUMER_GROUP_ID = "test_group"
    TOPICS = ["air_quality_data", "weather_data"]

    # Initialize consumer
    consumer_service = KafkaConsumerService(KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID, TOPICS)

    # Start consuming messages
    consumer_service.consume_messages()