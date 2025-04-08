import os
import logging
import pandas as pd
from Utilities.utilities import send_http_request

logger = logging.getLogger(__name__)

class Skinport:
    def __init__(self, download_folder='Sites_download'):
        self.download_folder = download_folder
        os.makedirs(self.download_folder, exist_ok=True)

    def download_data(self):
        """
        Download data from Skinport and save as CSV.
        """
        logger.info("Downloading data from Skinport")
        url = "https://api.skinport.com/v1/items"
        params = {"app_id": 730, "currency": "EUR", "tradable": 0}
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "br"  # Client must support Brotli compression otherwise error occurs
        }

        data = send_http_request(url, method='GET', params=params, headers=headers)
        if data:
            res = data  # Assuming data is a list of items
            df = pd.json_normalize(res)
            df['name'] = df['market_hash_name']
            df['price'] = df['min_price']
            df['price_after_sell'] = df['price'] * 0.88  # 12% commission on Skinport
            df = df[['name', 'price', 'price_after_sell']]
            file_path = os.path.join(self.download_folder, 'skinport.csv')
            df.to_csv(file_path, index=False, sep='@')
            logger.info("Data from Skinport saved to %s", file_path)
            return df
        else:
            logger.error("No data downloaded from Skinport")
        return None
