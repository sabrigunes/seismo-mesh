import os
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from afad_client import AFADClient
from afad_parser import parse_afad_data
from kandilli_client import KandilliClient
from kandilli_parser import parse_kandilli_data


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "seismo_kafka:29092")
TOPIC_NAME = "seismo-earthquakes"


def create_kafka_producer():
    """Kafka broker ayağa kalkana kadar yeniden dengeleyen producer bağlantısı."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=5
            )
            logging.info("Successfully connected to Kafka Broker!")
            return producer
        except KafkaError as e:
            logging.warning(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def main():
    logging.info("Seismo Producer Daemon is starting...")
    producer = create_kafka_producer()

    afad_client = AFADClient()
    kandilli_client = KandilliClient()

    seen_events = set()

    while True:
        logging.info("Fetching earthquake records from sources...")
        all_earthquakes = []

        # 1. AFAD Verilerini Çek ve Parse Et
        raw_afad = afad_client.fetch_earthquakes()
        if raw_afad:
            parsed_afad = parse_afad_data(raw_afad)
            all_earthquakes.extend(parsed_afad)

        # 2. Kandilli Verilerini Çek ve Parse Et
        raw_kandilli = kandilli_client.fetch_earthquake_text()
        if raw_kandilli:
            parsed_kandilli = parse_kandilli_data(raw_kandilli)
            all_earthquakes.extend(parsed_kandilli)

        new_count = 0
        for eq in all_earthquakes:
            event_id = eq.event_id
            if event_id not in seen_events:
                # Kafka'ya gönder (Pydantic modelini dict formatına çeviriyoruz)
                producer.send(TOPIC_NAME, key=event_id, value=eq.model_dump())
                seen_events.add(event_id)
                new_count += 1

        producer.flush()
        if new_count > 0:
            logging.info(f"Published {new_count} new earthquake events to Kafka.")
        else:
            logging.info("No new earthquake events found.")

        # Hafızanın şişmemesi için set çok büyürse temizlik yapalım
        if len(seen_events) > 10000:
            seen_events.clear()

        # 30 saniyede bir döngüyü tekrarla
        time.sleep(30)


if __name__ == "__main__":
    main()