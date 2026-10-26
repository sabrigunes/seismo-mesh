import os
import logging
import psycopg2
from dotenv import load_dotenv
from schema import EarthquakeSchema

load_dotenv()

logger = logging.getLogger(__name__)


class EarthquakeRepository:
    def __init__(self):
        # .env dosyasından gelen değerleri alıyoruz
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")
        db = os.getenv("POSTGRES_DB", "postgres")
        host = os.getenv("POSTGRES_HOST", "central_postgres")
        port = os.getenv("POSTGRES_PORT", "5432")

        self.conn_str = os.getenv(
            "DATABASE_URL",
            f"postgresql://{user}:{password}@{host}:{port}/{db}"
        )

        # Uygulama ayağa kalkarken tablo yoksa otomatik oluşturalım
        self.create_table_if_not_exists()

    def get_connection(self):
        return psycopg2.connect(self.conn_str)

    def create_table_if_not_exists(self):
        table_query = """
            CREATE EXTENSION IF NOT EXISTS postgis;

            CREATE TABLE IF NOT EXISTS earthquakes (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE NOT NULL,
                date VARCHAR(20) NOT NULL,
                time VARCHAR(20) NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                depth DOUBLE PRECISION NOT NULL,
                magnitude DOUBLE PRECISION NOT NULL,
                type VARCHAR(20),
                location VARCHAR(255) NOT NULL,
                city VARCHAR(100),
                district VARCHAR(100),
                neighborhood VARCHAR(100),
                solution_type VARCHAR(50),
                source VARCHAR(50) NOT NULL,
                geom GEOMETRY(Point, 4326),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_earthquakes_geom ON earthquakes USING GIST (geom);
            CREATE INDEX IF NOT EXISTS idx_earthquakes_event_id ON earthquakes (event_id);
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(table_query)
                    conn.commit()
            logger.info("Database tables and indexes verified/created successfully.")
        except Exception as e:
            logger.error(f"Error while creating tables: {e}")

    def save_earthquake(self, eq: EarthquakeSchema):

        query = """
            INSERT INTO earthquakes (
                event_id, date, time, latitude, longitude, depth, magnitude, 
                type, location, city, district, neighborhood, solution_type, source, geom
            ) 
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            ) 
            ON CONFLICT (event_id) DO NOTHING;
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        eq.event_id,
                        eq.date,
                        eq.time,
                        eq.latitude,
                        eq.longitude,
                        eq.depth,
                        eq.magnitude,
                        eq.type,
                        eq.location,
                        eq.city,
                        eq.district,
                        eq.neighborhood,
                        eq.solution_type,
                        eq.source,
                        eq.longitude,  # PostGIS ST_MakePoint(Boylam, Enlem) sırasını bekler!
                        eq.latitude
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Database save error for event_id {eq.event_id}: {e}")