import logging
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import sys
import os
import json
from git import Repo

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize the exchange object in async mode
exchange = getattr(ccxt, 'bybit')({
    'enableRateLimit': True,
})


# Initialize the repo object
repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repo = Repo(repo_dir)


async def fetch_and_save_data(symbol):
    try:
        # Fetch OHLCV data
        ohlcv = await exchange.fetch_ohlcv(symbol, '1d')
        df = pd.DataFrame(
            ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Convert timestamp to YYYYMMDDT format
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], unit='ms').dt.strftime('%Y%m%dT%H%M%S')
        # Sort by timestamp in ascending order
        df = df.sort_values(by='timestamp', ascending=True)

        # Save to CSV
        csv_filename = f"{repo_dir}/data/{symbol.replace('/', '')}.csv"
        df.to_csv(csv_filename, index=False)

        logging.info(f"Data for {symbol} saved to {csv_filename}")
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")


async def get_symbols():
    # Fetch all symbols from the exchange
    await exchange.load_markets()
    all_symbols = exchange.symbols

    # Filter symbols traded against USDT and without special characters
    usdt_symbols = [symbol for symbol in all_symbols if symbol.endswith(
        '/USDT') and '/' in symbol and '.' not in symbol]

    # Select the first 50 symbols
    selected_symbols = usdt_symbols[:50]
    return selected_symbols


async def main():
    symbols = await get_symbols()
    # Create a list of coroutine objects
    coroutines = [fetch_and_save_data(symbol) for symbol in symbols]

    # Fetch and save data for all symbols
    await asyncio.gather(*coroutines)

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
    if repo.is_dirty():
        repo.git.commit('-m', 'Updated data')
        repo.git.push()
    else:
        logging.info("No changes to commit")

    # Close the exchange connection
    await exchange.close()


if __name__ == '__main__':
    asyncio.run(main())
