import os
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from schema import EarthquakeSchema
from repository import EarthquakeRepository

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "seismo_kafka:29092")
TOPIC_NAME = "seismo-earthquakes"


def create_kafka_consumer():
    """Kafka broker hazır olana kadar yeniden dengeleyen consumer bağlantısı."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='seismo-earthquake-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Successfully connected to Kafka Consumer!")
            return consumer
        except KafkaError as e:
            logging.warning(f"Kafka consumer connection failed: {e}. Retrying in 5 seconds...")
            import time
            time.sleep(5)


def main():
    logging.info("Seismo Consumer Daemon is starting...")
    consumer = create_kafka_consumer()
    repository = EarthquakeRepository()

    logging.info("Listening for incoming earthquake events...")
    for message in consumer:
        try:
            raw_data = message.value
            # Pydantic ile gelen veriyi doğrula
            earthquake = EarthquakeSchema(**raw_data) 

            # Veritabanına kaydet (Duplicate event_id'ler ON CONFLICT ile elenir)
            repository.save_earthquake(earthquake)

            logging.info(
                f"Processed and saved earthquake: [{earthquake.source}] {earthquake.event_id} - Magnitude: {earthquake.magnitude}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()