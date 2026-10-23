import os
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class KandilliClient:
    def __init__(self):
        # Kandilli son depremler ana URL'si (yönlendirmeleri takip eder)
        self.url = "http://www.koeri.boun.edu.tr/scripts/lst0.asp"

    def fetch_earthquake_text(self) -> Optional[str]:
        """
        Kandilli Rasathanesi son depremler sayfasını çeker ve
        içerisindeki ham metin (pre tag) verisini döner.
        """
        try:
            response = requests.get(self.url, timeout=10)
            # Kandilli ISO-8859-9 (Türkçe) karakter kodlaması kullanır
            response.encoding = 'iso-8859-9'

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                pre_tag = soup.find('pre')

                if pre_tag:
                    logging.info("Successfully fetched raw earthquake data from Kandilli.")
                    return pre_tag.text
                else:
                    logging.warning("Kandilli page fetched, but <pre> tag not found.")
                    return None
            else:
                logging.error(f"Kandilli request failed with status code: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error while connecting to Kandilli: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error while fetching Kandilli data: {e}")
            return None