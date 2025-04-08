import os
import logging
import pandas as pd
from Utilities.utilities import send_http_request

logger = logging.getLogger(__name__)

class CsDeals:
    def __init__(self, download_folder='sites_download'):
        self.download_folder = download_folder
        os.makedirs(self.download_folder, exist_ok=True)

    def download_data(self):
        """
        Download data from cs.deals and save as CSV.
        """
        logger.info("Downloading data from cs.deals")
        url = "https://cs.deals/API/IPricing/GetLowestPrices/v1"
        payload = "{\"appid\":730}"
        headers = {'content-type': 'application/json'}

        data = send_http_request(url, method="POST", payload=payload, headers=headers)
        if data:
            res = data['response']['items']
            df = pd.json_normalize(res)
            df["name"] = df['marketname']
            df["price"] = df['lowest_price']
            df['price_after_sell'] = df["price"].astype(float) * 0.98  # 2% commission on CS.DEALS
            df = df[['name', 'price', 'price_after_sell']]
            file_path = os.path.join(self.download_folder, 'csdeals.csv')
            df.to_csv(file_path, index=False, sep='@')
            logger.info("Data from cs.deals saved to %s", file_path)
            return df
        else:
            logger.error("No data downloaded from cs.deals")
        return None
