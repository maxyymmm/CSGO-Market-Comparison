import os
import logging
import pandas as pd
from Utilities.utilities import send_http_request

logger = logging.getLogger(__name__)

class ShadowPay:
    def __init__(self, download_folder='Sites_download'):
        self.download_folder = download_folder
        os.makedirs(self.download_folder, exist_ok=True)

    def download_data(self):
        """
        Download data from ShadowPay and save as CSV.
        """
        logger.info("Downloading data from ShadowPay")
        url = "https://api.shadowpay.com/api/v2/user/items/prices"
        headers = {'Token': ''}  # Replace with your ShadowPay token

        data = send_http_request(url, method="GET", headers=headers)
        if data:
            res = data['data']
            df = pd.json_normalize(res)
            df["name"] = df['steam_market_hash_name']
            df["price"] = df['price']
            df['price_after_sell'] = df["price"].astype(float) * 0.95  # 5% commission on ShadowPay
            df = df[['name', 'price', 'price_after_sell']]
            file_path = os.path.join(self.download_folder, 'shadowpay.csv')
            df.to_csv(file_path, index=False, sep='@')
            logger.info("Data from ShadowPay saved to %s", file_path)
            return df
        else:
            logger.error("No data downloaded from ShadowPay")
        return None
