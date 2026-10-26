import os
import json
import time
import logging
from typing import List
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from afad_client import AFADClient
from afad_parser import AFADParser
from kandilli_client import KandilliClient
from kandilli_parser import KandilliParser

load_dotenv()

logger = logging.getLogger(__name__)


class SeismoProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "seismo_kafka:29092")
        self.topic_name = "seismo-earthquakes"

        # Client ve Parser bileşenlerinin başlatılması
        self.afad_client = AFADClient()
        self.afad_parser = AFADParser()
        self.kandilli_client = KandilliClient()
        self.kandilli_parser = KandilliParser()

        self.seen_events = set()

    def _create_producer(self) -> KafkaProducer:
        """Kafka broker ayağa kalkana kadar yeniden dengeleyen producer bağlantısı."""
        while True:
            try:
                logger.info(f"Connecting to Kafka Broker at {self.bootstrap_servers}...")
                producer = KafkaProducer(
                    bootstrap_servers=[self.bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    retries=5
                )
                logger.info("Successfully connected to Kafka Broker!")
                return producer
            except KafkaError as e:
                logger.warning(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def start(self, poll_interval: int = 30):
        """Producer daemon döngüsünü başlatır."""
        logger.info("Seismo Producer Daemon is starting...")
        producer = self._create_producer()

        while True:
            try:
                logger.info("Fetching earthquake records from sources...")
                all_earthquakes = []

                # 1. AFAD Verilerini Çek ve Parse Et
                raw_afad = self.afad_client.fetch_earthquakes()
                if raw_afad:
                    parsed_afad = self.afad_parser.parse(raw_afad)
                    all_earthquakes.extend(parsed_afad)

                # 2. Kandilli Verilerini Çek ve Parse Et
                raw_kandilli = self.kandilli_client.fetch_earthquake_text()
                if raw_kandilli:
                    parsed_kandilli = self.kandilli_parser.parse(raw_kandilli)
                    all_earthquakes.extend(parsed_kandilli)

                new_count = 0
                for eq in all_earthquakes:
                    event_id = eq.event_id
                    if event_id not in self.seen_events:
                        # Kafka'ya gönder (Pydantic modelini dict formatına çeviriyoruz)
                        producer.send(self.topic_name, key=event_id, value=eq.model_dump())
                        self.seen_events.add(event_id)
                        new_count += 1

                producer.flush()

                if new_count > 0:
                    logger.info(f"Published {new_count} new earthquake events to Kafka.")
                else:
                    logger.info("No new earthquake events found.")

                # Hafızanın şişmemesi için set çok büyürse temizlik yapalım
                if len(self.seen_events) > 10000:
                    logger.info("Clearing seen_events memory cache...")
                    self.seen_events.clear()

            except Exception as e:
                logger.error(f"Error in producer loop: {e}")

            # Belirtilen süre kadar bekle
            time.sleep(poll_interval)


if __name__ == "__main__":
    producer_daemon = SeismoProducer()
    producer_daemon.start()