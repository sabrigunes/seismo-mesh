# SeismoMesh 🌋🛰️
> Real-Time Earthquake Data Extraction, Normalization, & Enrichment Pipeline

**SeismoMesh**, AFAD ve Kandilli Rasathanesi gibi farklı kaynaklardan gelen anlık deprem verilerini toplayan, verileri ortak bir şema (`EarthquakeSchema`) altında normalize eden, Kafka üzerinden yayınlayan ve ilerleyen aşamalarda spatial/geographic zenginleştirme (enrichment) süreçleri sunan mikroservis tabanlı bir veri mühendisliği sistemidir.

---

## 🎯 Proje Mimarisi

Proje, gerçek zamanlı veri akışı ve asenkron veri işleme mimarisine dayanır:

```text
+--------------------+        +---------------------+        +-----------------------+        +----------------------+
|   Data Sources     |        |   Seismo Producer   |        |   Apache Kafka        |        |   Seismo Consumer    |
| (AFAD / Kandilli)  | ---->  |   (Data Parsers)    | ---->  | (seismo-earthquakes)  | ---->  | (Validation & Store) |
+--------------------+        +---------------------+        +-----------------------+        +----------------------+
                                                                                                         |
                                                                                                         v
                                                                                               +-------------------+
                                                                                               | PostgreSQL / PostGIS|
                                                                                               +-------------------+
```

1. **Seismo Producer (Daemon):** AFAD (REST/JSON) ve Kandilli Rasathanesi (Web Scraping / Text Parsing) servislerinden deprem verilerini periyodik olarak çeker.
2. **Data Normalization & Validation:** Gelen ham veriler Pydantic tabanlı `EarthquakeSchema` kullanılarak doğrulanır ve standart yapıya dönüştürülür.
3. **Apache Kafka (Event Streaming):** Normalize edilen deprem olayları `seismo-earthquakes` konusuna (topic) event olarak iletilir.
4. **Seismo Consumer (Daemon):** Kafka'dan gelen mesajları dinler, Pydantic doğrulamasından geçirir ve PostGIS destekli PostgreSQL veritabanına kaydeder.
5. **Spatial Enrichment (Planlanan / Gelecek Aşama):** Producer aşamasında lokasyon detayları (`city`, `district`, `neighborhood`) eksik/boş (`None`) gelen kayıtlar, tüketim ve saklama aşamalarında spatial veritabanı sorguları (PostGIS) ile zenginleştirilir.

---

## 📐 Veri Sözleşmesi (Schema Design)

Tüm kaynaklardan gelen veriler ortak Pydantic veri sözleşmesiyle modellenir. İl, ilçe ve mahalle gibi coğrafi veriler veritabanı aşamasında zenginleştirilene kadar opsiyonel (`Optional[str] = None`) kabul edilir.

```python
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class EarthquakeSchema(BaseModel):
    event_id: Optional[str] = Field(default=None, description="Benzersiz deprem kimliği")
    date: Optional[str] = Field(default=None, description="Deprem tarihi (YYYY-MM-DD)")
    time: Optional[str] = Field(default=None, description="Deprem saati (HH:MM:SS)")
    latitude: Optional[float] = Field(default=None, ge=-90.0, le=90.0, description="Enlem")
    longitude: Optional[float] = Field(default=None, ge=-180.0, le=180.0, description="Boylam")
    depth: Optional[float] = Field(default=None, ge=0.0, description="Derinlik (km)")
    magnitude: Optional[float] = Field(default=0.0, description="Deprem büyüklüğü")
    type: Optional[str] = Field(default=None, description="Büyüklük türü (MD, ML, Mw)")
    location: Optional[str] = Field(default=None, description="Ham lokasyon tanımı")
    
    # DB / Spatial sorgularla zenginleştirilecek opsiyonel alanlar
    city: Optional[str] = Field(default=None, description="İl")
    district: Optional[str] = Field(default=None, description="İlçe")
    neighborhood: Optional[str] = Field(default=None, description="Mahalle")
    
    solution_type: Optional[str] = Field(default="REVIZE", description="Çözüm türü")
    source: Optional[str] = Field(default=None, description="Veri kaynağı (AFAD / KANDILLI)")

    @field_validator('magnitude')
    @classmethod
    def validate_magnitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("Deprem büyüklüğü negatif olamaz")
        return v
```

---

## 🚀 Kurulum ve Çalıştırma

Proje, ana altyapı deposunun (`/opt/infra`) bir parçası olarak Docker Compose üzerinden orkestre edilir.

### 1. Gereksinimler
- Docker Engine 20.10+
- Docker Compose v2+
- Ortak Docker Ağı (`data-net`)

### 2. Ortak Ağı Oluşturma (Gerekliyse)
```bash
docker network create data-net
```

### 3. Servislerin Başlatılması

Proje dizinindeyken altyapıyı ve daemon servisleri ayağa kaldırın:

```bash
docker compose up -d seismo-kafka seismo-producer seismo-consumer
```

### 4. Yeniden Derleme (No-Cache)

Kod veya şema değişikliklerinden sonra konteynerleri temiz bir şekilde güncelleyin:

```bash
docker compose build --no-cache seismo-producer seismo-consumer
docker compose up -d seismo-producer seismo-consumer
```

---

## 🔍 Log Takibi ve Hata Ayıklama

Servislerin Kafka bağlantı durumunu ve veri akışını canlı takip etmek için:

```bash
# Tüm SeismoMesh servislerinin logları
docker compose logs -f seismo-producer seismo-consumer

# Sadece Producer logları
docker compose logs -f seismo-producer

# Sadece Consumer logları
docker compose logs -f seismo-consumer
```

---

## 🛠️ Proje Dizini Yapısı

```text
seismo-mesh/
├── Dockerfile              # Python 3.10 tabanlı ortam tanımı (PYTHONPATH=/app/src)
├── requirements.txt        # Bağımlılıklar (kafka-python, pydantic, psycopg2, vb.)
└── src/
    ├── producer.py         # Anlık veri toplama ve Kafka'ya yazma servisi
    ├── consumer.py         # Kafka dinleyici ve Postgres kaydetme servisi
    ├── schema.py           # Pydantic EarthquakeSchema veri sözleşmesi
    ├── afad_parser.py      # AFAD veri dönüştürücü
    └── kandilli_parser.py  # Kandilli Rasathanesi metin/html ayrıştırıcı
```

---

## 📝 Katkıda Bulunma ve Geliştirme

1. **Yeni Veri Kaynağı Ekleme:** `src/` dizini altına yeni parser modülü ekleyin ve `producer.py` döngüsüne dahil edin.
2. **Spatial Enrichment:** Lokasyon bilgileri boş gelen (`None`) olaylar için PostgreSQL PostGIS katmanında `ST_Contains` spatial query logic'i `consumer.py` seviyesinde geliştirilmektedir.
