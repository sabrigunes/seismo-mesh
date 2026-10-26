import os
import json
import logging
import time
from typing import Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from schema import EarthquakeSchema
from repository import EarthquakeRepository

load_dotenv()

logger = logging.getLogger(__name__)


class SeismoConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "seismo_kafka:29092")
        self.topic_name = "seismo-earthquakes"
        self.group_id = "seismo-earthquake-group"
        self.repository = EarthquakeRepository()

    def _create_consumer(self) -> KafkaConsumer:
        """Kafka broker hazır olana kadar yeniden dengeleyen consumer bağlantısı."""
        while True:
            try:
                logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}...")
                consumer = KafkaConsumer(
                    self.topic_name,
                    bootstrap_servers=[self.bootstrap_servers],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=self.group_id,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("Successfully connected to Kafka Consumer!")
                return consumer
            except KafkaError as e:
                logger.warning(f"Kafka consumer connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def start(self):
        """Consumer daemon'ını başlatır ve mesajları dinler."""
        logger.info("Seismo Consumer Daemon is starting...")
        consumer = self._create_consumer()

        logger.info(f"Listening for incoming earthquake events on topic '{self.topic_name}'...")

        for message in consumer:
            try:
                raw_data = message.value

                # Pydantic ile gelen veriyi doğrula
                earthquake = EarthquakeSchema(**raw_data)

                # Veritabanına kaydet (Duplicate event_id'ler ON CONFLICT ile elenir)
                self.repository.save_earthquake(earthquake)

                logger.info(
                    f"Processed and saved earthquake: [{earthquake.source}] {earthquake.event_id} | Magnitude: {earthquake.magnitude}"
                )

            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    consumer_daemon = SeismoConsumer()
    consumer_daemon.start()