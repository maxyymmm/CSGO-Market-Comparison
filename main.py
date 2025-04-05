import os
import logging
import pandas as pd
import concurrent.futures
from itertools import permutations

from Sites.csdeals import CsDeals
from Sites.shadowpay import ShadowPay
from Sites.skinport import Skinport

# Set up global logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def compare_market_data(download_folder='sites_download', results_folder='sites_results', min_profit=0):
    logger.info("Comparing market data with minimum profit: %s", min_profit)
    results = []
    os.makedirs(results_folder, exist_ok=True)

    def compare_pair(site1, site2, threshold):
        try:
            df1 = pd.read_csv(os.path.join(download_folder, f"{site1}.csv"), delimiter='@')
            df2 = pd.read_csv(os.path.join(download_folder, f"{site2}.csv"), delimiter='@')
        except Exception as e:
            logger.error("Error reading CSV files for %s and %s: %s", site1, site2, e)
            return
        df = pd.merge(df1, df2, on='name')
        df['BUY X'] = site1
        df['SELL Y'] = site2
        df['profit'] = (df['price_after_sell_y'] - df['price_x']).round(2)
        df_filtered = df[df['profit'] > threshold]
        results.append(df_filtered)
        output_file_path = os.path.join(results_folder, f"{site1}_TO_{site2}.csv")
        df_filtered.to_csv(output_file_path, index=False, sep='@')
        logger.info("Comparison result for %s and %s saved to %s", site1, site2, output_file_path)

    # Get list of site CSV files from the download folder
    sites = [filename.split('.')[0] for filename in os.listdir(download_folder) if filename.endswith('.csv')]
    for pair in permutations(sites, 2):
        compare_pair(pair[0], pair[1], min_profit)

    if results:
        final_results = pd.concat(results)
        final_results = final_results.sort_values(['name', 'BUY X'], ascending=False)
        logger.info("Final comparison result:\n%s", final_results)
        final_results.to_csv('RESULTS.csv', index=False, sep='@')
    else:
        logger.info("No profitable trades found.")

def main():
    logger.info("Program started")
    logger.info("1 - Download data and compare \n2 - Compare without downloading")
    try:
        choice = int(input("Choose an option: "))
    except ValueError:
        logger.error("Invalid option input")
        return

    if choice == 1:
        download_folder = 'sites_download'
        # Create site objects
        csdeals = CsDeals(download_folder)
        shadowpay = ShadowPay(download_folder)
        skinport = Skinport(download_folder)
        # Download data concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            futures.append(executor.submit(csdeals.download_data))
            futures.append(executor.submit(shadowpay.download_data))
            futures.append(executor.submit(skinport.download_data))
            concurrent.futures.wait(futures)
        try:
            min_profit = float(input("Enter the minimum profit (USD): "))
        except ValueError:
            logger.error("Invalid profit value input")
            return
        compare_market_data(min_profit=min_profit)
    elif choice == 2:
        try:
            min_profit = float(input("Enter the minimum profit (USD): "))
        except ValueError:
            logger.error("Invalid profit value input")
            return
        compare_market_data(min_profit=min_profit)
    else:
        logger.error("Invalid option selected")

if __name__ == "__main__":
    main()
