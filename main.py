import requests
import pandas as pd
import os
from itertools import permutations

def send_http_request(url, method, payload=None, headers=None, params=None):
    try:
        response = requests.request(method, url, data=payload, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception in case of HTTP error
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while sending HTTP request: {e}")
        return None

def csdeals_down():
    url = "https://cs.deals/API/IPricing/GetLowestPrices/v1"
    payload = "{\"appid\":730}"
    headers = {'content-type': 'application/json'}

    data = send_http_request(url, method="POST", payload=payload, headers=headers)

    if data:
        res = [p for p in data['response']['items']]
        df = pd.json_normalize(res)
        df["name"] = df['marketname']
        df["price"] = df['lowest_price']
        df['price_after_sell'] = df["price"].astype(float) * 0.98  # 2% commission on CS.DEALS

        df = df[['name', 'price', 'price_after_sell']]
        df.to_csv('sites_download\\csdeals.csv', index=False, sep='@')

def shadowpay_down():
    url = "https://api.shadowpay.com/api/v2/user/items/prices"
    headers = {'Token': 'YOUR_SHADOWPAY_TOKEN'}  # Replace with your ShadowPay token

    data = send_http_request(url, headers=headers, method='GET')

    if data:
        res = [p for p in data['data']]
        df = pd.json_normalize(res)

        df["name"] = df['steam_market_hash_name']
        df["price"] = df['price']
        df['price_after_sell'] = df["price"].astype(float) * 0.95  # 5% commission on ShadowPay

        df = df[['name', 'price', 'price_after_sell']]
        df.to_csv('sites_download\\shadowpay.csv', index=False, sep='@')

def skinport_down():
    url = "https://api.skinport.com/v1/items"
    params = {"app_id": 730, "currency": "USD", "tradable": 0}

    data = send_http_request(url, params=params, method='GET')

    if data:
        res = [p for p in data]
        df = pd.json_normalize(res)

        df['name'] = df['market_hash_name']
        df['price'] = df['min_price']
        df['price_after_sell'] = df['price'] * 0.88  # 12% commission on SKINPORT

        df = df[['name', 'price', 'price_after_sell']]
        df.to_csv('sites_download\\skinport.csv', index=False, sep='@')

def skinwallet_down():
    url = "https://www.skinwallet.com/market/api/offers/overview"
    querystring = {"appId": "730"}

    headers = {
        'cookie': "YOUR_SKINWALLET_COOKIE",
        'accept': "application/json",
        'x-auth-token': "YOUR_SKINWALLET_TOKEN"
    }

    data = send_http_request(url, headers=headers, params=querystring, method='GET')

    if data:
        res = [p for p in data['result']]
        df = pd.json_normalize(res)

        df["name"] = df['marketHashName']
        df["price"] = df['cheapestOffer.price.amount']
        df['price_after_sell'] = df["price"].astype(float) * 0.95  # 5% commission on Skinwallet

        df = df[['name', 'price', 'price_after_sell']]
        df.to_csv('sites_download\\skinwallet.csv', index=False, sep='@')


def compare_market_data():
    results = []

    def con(to_pd1, to_pd2, entered_profit):
        df1 = pd.read_csv('sites_download\\' + to_pd1 + '.csv', delimiter='@')
        df2 = pd.read_csv('sites_download\\' + to_pd2 + '.csv', delimiter='@')
        df = pd.DataFrame()
        df = pd.merge(df1, df2, on='name')
        df['BUY X'] = to_pd1
        df['SELL Y'] = to_pd2
        df['profit'] = (df['price_after_sell_y'] - df['price_x']).round(2)
        df = df[df['profit'] > entered_profit]
        results.append(df)
        output_file_path = 'sites_results\\' + to_pd1 + '_TO_' + to_pd2 + '.csv'
        df.to_csv(output_file_path, index=False, sep='@')

    # Automatically read the names of bases from the 'sites_download' folder
    sites = [filename.split('.')[0] for filename in os.listdir('sites_download') if filename.endswith('.csv')]

    pairs = permutations(sites, 2)  # Generate permutations (without repetitions) for the "con" function

    entered_profit = int(input("Enter the minimum profit (USD): "))

    for pair in list(pairs):  # Send pairs of permutations to the "con" function
        to_pd1 = pair[0]
        to_pd2 = pair[1]
        con(to_pd1, to_pd2, entered_profit)

    results = pd.concat(results)
    results = results.sort_values(['name', 'BUY X'], ascending=False)
    print(results)
    results.to_csv('RESULTS.csv', index=False, sep='@')

if __name__ == "__main__":
    print("1 - Download data and compare \n 2 - Compare - WITHOUT downloading")
    case = int(input("Choose an option: "))

    if case == 1:
        csdeals_down()
        shadowpay_down()
        skinport_down()
        skinwallet_down()
        compare_market_data()

    elif case == 2:
        compare_market_data()
    else:
        print("Invalid option! Please try again.")
