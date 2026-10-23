from typing import List, Dict, Any
from datetime import datetime
from schema import EarthquakeSchema


def parse_afad_data(raw_json_list: List[Dict[str, Any]]) -> List[EarthquakeSchema]:
    earthquakes = []
    for item in raw_json_list:
        try:
            # AFAD tarih formatı: "2026-10-23T14:03:35" -> Tarih ve saati ayıralım
            dt_str = item.get("date")
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
                magnitude=float(item.get("magnitude")),
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
            print(f"AFAD parse error for item {item.get('eventID')}: {e}")

    return earthquakes