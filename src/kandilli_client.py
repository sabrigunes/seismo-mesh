import os
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional

logger = logging.getLogger(__name__)


class KandilliClient:
    def __init__(self, url: str = "http://www.koeri.boun.edu.tr/scripts/lst0.asp", timeout: int = 10):
        # Kandilli son depremler ana URL'si
        self.url = url
        self.timeout = timeout

    def fetch_earthquake_text(self) -> Optional[str]:
        """
        Kandilli Rasathanesi son depremler sayfasını çeker ve
        içerisindeki ham metin (pre tag) verisini döner.
        """
        try:
            logger.info("Fetching raw earthquake data from Kandilli...")
            response = requests.get(self.url, timeout=self.timeout)
            # Kandilli ISO-8859-9 (Türkçe) karakter kodlaması kullanır
            response.encoding = 'iso-8859-9'

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                pre_tag = soup.find('pre')

                if pre_tag:
                    logger.info("Successfully fetched raw earthquake data from Kandilli.")
                    return pre_tag.text
                else:
                    logger.warning("Kandilli page fetched, but <pre> tag not found.")
                    return None
            else:
                logger.error(f"Kandilli request failed with status code: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error while connecting to Kandilli: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching Kandilli data: {e}")
            return None