import requests
import json
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('API_URL')
COLLECTION_SYMBOL = os.getenv('COLLECTION_SYMBOL')

import requests.exceptions

def fetch_transactions(limit=100, offset=0, start_time=None):
    url = f"{API_URL}/collections/{COLLECTION_SYMBOL}/activities"
    params = {
        "offset": offset,
        "limit": limit,
        "type": "buyNow"
    }

    if start_time:
        params["startTime"] = int(start_time.timestamp())

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://magiceden.io",
        "Origin": "https://magiceden.io"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def parse_transactions(data):
    transactions = []
    for tx in data:
        if tx.get('buyer') and tx.get('seller'):
            transaction = {
                'signature': tx.get('signature'),
                'type': tx.get('type'),
                'price': tx.get('price'),
                'buyer': tx.get('buyer'),
                'seller': tx.get('seller'),
                'timestamp': datetime.fromtimestamp(tx.get('blockTime')).isoformat()
        }
        transactions.append(transaction)
    return transactions

def main():
    all_transactions = []
    offset = 0
    limit = 100
    total_volume = 0
    total_sales = 0

    start_time = datetime.now() - timedelta(days=7)

    while True:
            data = fetch_transactions(limit, offset, start_time)
            if not data:
                break

            transactions = parse_transactions(data)
            
            transactions = [tx for tx in transactions if datetime.fromisoformat(tx['timestamp']) >= start_time]

            all_transactions.extend(transactions)

            batch_volume = sum(tx['price'] for tx in transactions)
            total_volume += batch_volume
            total_sales += len(transactions)

            print(f"fetched {len(transactions)} transactions. Total: {len(all_transactions)}")

            if len(transactions) < limit:
                break

            offset += limit
            time.sleep(1)

    with open(f'{COLLECTION_SYMBOL}_transactions.json', 'w') as f:
        json.dump(all_transactions, f, indent=2)

    average_price = total_volume / total_sales if total_sales > 0 else 0

    print(f"\nScraping complete. Total transactions: {len(all_transactions)}")
    print(f"Total sales: {total_sales}")
    print(f"Total sales volume: {total_volume:.2f} SOL")
    print(f"Average sale price: {average_price:.2f} SOL")

if __name__ == "__main__":
    main()