import logging
import asyncio
import ccxt
import pandas as pd
import sys
import os
import json
from git import Repo

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize the exchange object
exchange = ccxt.binance()

# List of symbols
symbols = ['BTC/USDT', 'ETH/USDT']  # Add all 50 symbols here

# Initialize the repo object
repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repo = Repo(repo_dir)


async def fetch_and_save_data(symbol):
    try:
        # Fetch OHLCV data
        ohlcv = await exchange.fetch_ohlcv(symbol, '1d')
        df = pd.DataFrame(
            ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Save to CSV
        csv_filename = f"{repo_dir}/data/{symbol.replace('/', '')}.csv"
        df.to_csv(csv_filename, index=False)

        logging.info(f"Data for {symbol} saved to {csv_filename}")
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")


async def main():
    # Fetch and save data for all symbols
    await asyncio.gather(*(fetch_and_save_data(symbol) for symbol in symbols))

    # Create JSON file
    json_data = {
        "symbol": symbols,
        "pricescale": [10] * len(symbols),  # Adjust as needed
        "description": [f"Description for {symbol}" for symbol in symbols]
    }
    json_filename = f"{repo_dir}/symbol_info/seed_ohlcv_data.json"
    with open(json_filename, 'w') as f:
        json.dump(json_data, f)

    # Commit and push to GitHub
    repo.git.add(A=True)
    repo.git.commit('-m', 'Updated data')
    repo.git.push()
