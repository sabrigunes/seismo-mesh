import logging
from typing import List, Dict, Any
from datetime import datetime
from schema import EarthquakeSchema

logger = logging.getLogger(__name__)


class AFADParser:
    def parse(self, raw_json_list: List[Dict[str, Any]]) -> List[EarthquakeSchema]:
        """
        AFAD'dan gelen ham JSON listesini işler ve EarthquakeSchema nesnelerine dönüştürür.
        """
        earthquakes = []

        for item in raw_json_list:
            try:
                dt_str = item.get("date")
                if not dt_str:
                    logger.warning(f"AFAD item missing 'date' field: {item.get('eventID')}")
                    continue

                dt_obj = datetime.fromisoformat(dt_str)
                date_str = dt_obj.strftime("%Y.%m.%d")
                time_str = dt_obj.strftime("%H:%M:%S")

                eq = EarthquakeSchema(
                    event_id=f"AFAD_{item.get('eventID')}",
                    date=date_str,
                    time=time_str,
                    latitude=float(item.get("latitude")),
                    longitude=float(item.get("longitude")),
                    depth=float(item.get("depth")),
                    magnitude=float(item.get("magnitude", 0.0)),
                    type=item.get("type", "ML"),
                    location=item.get("location", ""),
                    city=item.get("province"),
                    district=item.get("district"),
                    neighborhood=item.get("neighborhood"),
                    solution_type="REVIZE" if item.get("isEventUpdate") else "İLKSEL",
                    source="AFAD"
                )
                earthquakes.append(eq)

            except Exception as e:
                logger.error(f"AFAD parse error for item {item.get('eventID')}: {e}")

        logger.info(f"Successfully parsed {len(earthquakes)} earthquake records from AFAD.")
        return earthquakes