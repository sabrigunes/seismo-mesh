import logging
from typing import List
from schema import EarthquakeSchema

logger = logging.getLogger(__name__)


class KandilliParser:
    def parse(self, raw_text: str) -> List[EarthquakeSchema]:
        """
        Kandilli Rasathanesi'nden gelen ham metin verisini satır satır ayrıştırır
        ve EarthquakeSchema nesnelerine dönüştürür.
        """
        if not raw_text:
            logger.warning("Raw text provided to KandilliParser is empty.")
            return []

        lines = raw_text.strip().split('\n')
        earthquakes = []

        # Kandilli formatında başlık satırlarını atlıyoruz (ilk 7 satır başlıktır)
        data_lines = lines[7:] if len(lines) > 7 else []

        for line in data_lines:
            parts = line.split()
            if len(parts) >= 9:
                try:
                    date_str = parts[0]
                    time_str = parts[1]
                    lat = float(parts[2])
                    lon = float(parts[3])
                    depth = float(parts[4])

                    # Büyüklük (ML veya Mw öncelikli)
                    mag_str = parts[6] if parts[6] != '-.-' else parts[5]
                    mag = float(mag_str) if mag_str != '-.-' else 0.0

                    # Çözüm tipi (MD, ML, Mw)
                    eq_type = "Mw" if parts[7] != '-.-' else ("ML" if parts[6] != '-.-' else "MD")

                    location = " ".join(parts[8:-1])
                    solution_type = parts[-1]

                    # Kandilli verisinde şehir bilgisi location içinde parantez içinde gelebilir (örn. KANDILLI (ISTANBUL))
                    city = None
                    if "(" in location and ")" in location:
                        city = location.split("(")[-1].replace(")", "").strip()

                    event_id = f"KANDILLI_{date_str}_{time_str}_{lat}_{lon}".replace(":", "").replace(".", "")

                    eq = EarthquakeSchema(
                        event_id=event_id,
                        date=date_str,
                        time=time_str,
                        latitude=lat,
                        longitude=lon,
                        depth=depth,
                        magnitude=mag,
                        type=eq_type,
                        location=location,
                        city=city,
                        district=None,
                        neighborhood=None,
                        solution_type=solution_type,
                        source="KANDILLI"
                    )
                    earthquakes.append(eq)

                except Exception as e:
                    logger.error(f"Kandilli line parse error: '{line.strip()}' -> {e}")

        logger.info(f"Successfully parsed {len(earthquakes)} earthquake records from Kandilli.")
        return earthquakes