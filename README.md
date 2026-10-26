# SeismoMesh 🌋🛰️

> Real-Time Earthquake Data Extraction, Normalization, & Streaming Pipeline

**SeismoMesh**, AFAD ve Kandilli Rasathanesi gibi farklı kaynaklardan gelen anlık deprem verilerini asenkron olarak toplayan, Pydantic ile doğrulayıp ortak bir şemada normalize eden, Apache Kafka üzerinden event-driven bir mimariyle ileten ve PostgreSQL üzerinde saklayan modern bir veri mühendisliği sistemidir.

## 🎯 Proje Mimarisi

Proje, gerçek zamanlı veri akışı ve katmanlı servis mimarisine dayanır:

```
+--------------------+        +---------------------+        +-----------------------+        +----------------------+
|    Data Sources    |        |   Seismo Producer   |        |     Apache Kafka      |        |   Seismo Consumer    |
| (AFAD / Kandilli)  | ---->  |   (Data Parsers)    | ---->  | (seismo-earthquakes)  | ---->  | (Validation & Store) |
+--------------------+        +---------------------+        +-----------------------+        +----------------------+
                                                                                                         |
                                                                                                         v
                                                                                               +----------------------+
                                                                                               | PostgreSQL / PostGIS |
                                                                                               +----------------------+
                                                                                                         |
                                                                                                         v
                                                                                               +----------------------+
                                                                                               |    FastAPI (Next)    |
                                                                                               +----------------------+
```

1. **Seismo Producer (Daemon):** AFAD (REST/JSON) ve Kandilli Rasathanesi (Web Scraping / Text Parsing) servislerinden deprem verilerini periyodik olarak çeker.

2. **Data Normalization & Validation:** Gelen ham ve kirli veriler Pydantic tabanlı `EarthquakeSchema` (`model_validator` desteğiyle) güvenli bir şekilde temizlenir ve doğrulanır.

3. **Apache Kafka (Event Streaming):** Normalize edilen deprem olayları `seismo-earthquakes` topic'ine event olarak iletilir.

4. **Seismo Consumer (Daemon):** Kafka'dan gelen mesajları tüketir ve `EarthquakeRepository` aracılığıyla PostgreSQL veritabanına kaydeder. Duplicate event'ler `ON CONFLICT (event_id) DO NOTHING` mekanizmasıyla engellenir.

## 📐 Veri Sözleşmesi (Schema Design)

Tüm kaynaklardan gelen veriler ortak Pydantic veri sözleşmesiyle modellenir. Dış kaynaklardaki bozuk veya eksik tipler (`-.-`, `None` vb.) model doğrulaması öncesinde güvenli varsayılan değerlere dönüştürülür.

```python
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, model_validator


class EarthquakeSchema(BaseModel):
    event_id: Optional[str] = Field(default="", description="Earthquake id")
    date: Optional[str] = Field(default="", description="Earthquake date")
    time: Optional[str] = Field(default="", description="Earthquake time")
    latitude: Optional[float] = Field(default=0.0, description="Latitude")
    longitude: Optional[float] = Field(default=0.0, description="Longitude")
    depth: Optional[float] = Field(default=0.0, description="Depth (km)")
    magnitude: Optional[float] = Field(default=0.0, description="Magnitude")
    type: Optional[str] = Field(default="", description="Type (MD,ML,Mw)")
    location: Optional[str] = Field(default="", description="Location")

    # TODO: These fields will be enriched/fetched later from spatial database tables
    city: Optional[str] = Field(default="", description="City")
    district: Optional[str] = Field(default="", description="District")
    neighborhood: Optional[str] = Field(default="", description="Neighborhood")

    solution_type: Optional[str] = Field(default="REVIZE", description="Solution type")
    source: Optional[str] = Field(default="", description="Source")

    @model_validator(mode="before")
    @classmethod
    def allow_anything_and_sanitize(cls, data: Any) -> Dict[str, Any]:
        """Cleans dirty data from external sources and safely converts None/broken types."""
        if not isinstance(data, dict):
            return {}

        str_fields = [
            "event_id", "date", "time", "type", "location",
            "city", "district", "neighborhood", "solution_type", "source"
        ]
        for field in str_fields:
            val = data.get(field)
            data[field] = "" if val is None else str(val).strip()

        float_fields = ["latitude", "longitude", "depth", "magnitude"]
        for field in float_fields:
            val = data.get(field)
            if val is None or val == "" or val == "-.-":
                data[field] = 0.0
            else:
                try:
                    data[field] = float(val)
                except (ValueError, TypeError):
                    data[field] = 0.0

        return data
```

## 🚀 Kurulum ve Çalıştırma

Proje, Docker Compose üzerinden interaktif servisler olarak orkestre edilir.

### 1. Gereksinimler

* Docker Engine 20.10+
* Docker Compose v2+
* Ortak Docker Ağı (`data-net`)

### 2. Ortak Ağı Oluşturma (Gerekliyse)

```bash
docker network create data-net
```

### 3. Servislerin Başlatılması

```bash
docker compose up -d seismo-kafka seismo-producer seismo-consumer
```

### 4. Yeniden Derleme (No-Cache)

```bash
docker compose build --no-cache seismo-producer seismo-consumer
docker compose up -d seismo-producer seismo-consumer
```

## 🔍 Log Takibi ve Hata Ayıklama

```bash
# Tüm servislerin logları
docker compose logs -f seismo-producer seismo-consumer

# Sadece Producer logları
docker compose logs -f seismo-producer
```

## 🛠️ Proje Dizini Yapısı

```text
seismo-mesh/
├── Dockerfile              # Python 3.10 tabanlı ortam tanımı
├── requirements.txt        # Bağımlılıklar (kafka-python, pydantic, psycopg2, vb.)
└── src/
    ├── producer.py         # Asenkron veri toplama ve Kafka event publisher servisi
    ├── consumer.py         # Kafka consumer daemon ve DB aktarım servisi
    ├── schema.py           # Pydantic EarthquakeSchema veri sözleşmesi ve sanitizer
    ├── repository.py       # PostgreSQL / PostGIS bağlantı ve kayıt yöneticisi
    ├── afad_client.py      # AFAD API client
    ├── afad_parser.py      # AFAD veri parser
    ├── kandilli_client.py  # Kandilli Rasathanesi web client
    └── kandilli_parser.py  # Kandilli metin parser
```

## 🔮 Roadmap & Gelecek Planlar (FastAPI & PostGIS Entegrasyonu)

Projenin bir sonraki aşamasında, toplanan verilerin dış dünyaya sunulması için bir **FastAPI** katmanı eklenecektir. Bu aşamada **PostGIS**'in güçlü coğrafi yetenekleri tam anlamıyla devreye girecektir:

* **Spatial Filtering:** Belirli bir merkez koordinata ve yarıçapa (`ST_DWithin`) göre deprem sorgulama.
* **Polygon / Region Query:** Belirli bir il, ilçe sınırları veya özel poligon alanlar (`ST_Contains`) içerisindeki sismik hareketleri filtreleme.
* **Bbox (Bounding Box) Arama:** Harita arayüzleri için min/max boylam ve enlem değerlerine göre anlık veri çekme.