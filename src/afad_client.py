import logging
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)


class AFADClient:
    def __init__(self, api_url: str = "https://servisnet.afad.gov.tr/apigateway/deprem/apiv2/event/filter?limit=100", timeout: int = 10):
        self.api_url = api_url
        self.timeout = timeout

    def fetch_earthquakes(self) -> List[Dict[str, Any]]:
        try:
            logger.info(f"Fetching earthquakes from AFAD API...")
            response = requests.get(self.api_url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()

                if isinstance(data, list):
                    logger.info(f"Successfully fetched {len(data)} earthquake records from AFAD API.")
                    return data
                elif isinstance(data, dict):
                    for key in ["result", "data", "items", "events"]:
                        if key in data and isinstance(data[key], list):
                            logger.info(f"Successfully fetched {len(data[key])} earthquake records from AFAD API (key: '{key}').")
                            return data[key]

                logger.warning("AFAD API returned data in an unexpected format.")
                return []
            else:
                logger.error(f"AFAD API request failed with status code: {response.status_code}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error while connecting to AFAD API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error while fetching AFAD data: {e}")
            return []