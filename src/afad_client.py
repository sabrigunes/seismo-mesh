import os
import logging
import requests
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class AFADClient:
    def __init__(self):
        self.api_url = "https://servisnet.afad.gov.tr/apigateway/deprem/apiv2/event/filter?limit=100"

    def fetch_earthquakes(self) -> List[Dict[str, Any]]:
        """
        AFAD API'sine GET isteği atarak son deprem listesini çeker.
        Bağlantı veya zaman aşımı hatalarını yakalayarak boş liste döner.
        """
        try:
            response = requests.get(self.api_url, timeout=10)

            if response.status_code == 200:
                data = response.json()

                if isinstance(data, list):
                    logging.info(f"Successfully fetched {len(data)} earthquake records from AFAD API.")
                    return data
                elif isinstance(data, dict):
                    for key in ["result", "data", "items", "events"]:
                        if key in data and isinstance(data[key], list):
                            return data[key]

                logging.warning("AFAD API returned data in an unexpected format.")
                return []
            else:
                logging.error(f"AFAD API request failed with status code: {response.status_code}")
                return []

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error while connecting to AFAD API: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error while fetching AFAD data: {e}")
            return []