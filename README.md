# SeismoMesh 🌋

**SeismoMesh** is an event-driven data engineering pipeline designed to ingest, stream, and process real-time earthquake data from multiple official sources (Kandilli Observatory and AFAD) using Apache Kafka and PostgreSQL/PostGIS.

## 🚀 Architecture Overview
1. **Producer Daemons:** Concurrently poll and scrape real-time earthquake data from official Turkish monitoring agencies.
2. **Apache Kafka:** Acts as the central event broker to decouple data ingestion and storage pipelines.
3. **Consumer Worker:** Consumes raw events from Kafka, ensures idempotency and deduplication, and persists spatial data into a PostGIS-enabled database.

## 🛠️ Tech Stack
* **Language:** Python
* **Streaming Broker:** Apache Kafka
* **Storage:** PostgreSQL + PostGIS
* **Containerization:** Docker & Docker Compose
