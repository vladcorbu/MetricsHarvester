import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests

# Setting up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_binance_data(symbol, interval, start, end=None):
    base_url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start.timestamp() * 1000),
        "limit": 1000,
    }
    if end:
        params["endTime"] = int(end.timestamp() * 1000)

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        logging.error(f"API error: {response.json()}")
        return pd.DataFrame()

    data = response.json()
    columns = [
        "Open time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "Ignore",
    ]
    df = pd.DataFrame(data, columns=columns)
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")
    return df


def fetch_data_for_period(symbol, interval, start_date, end_date):
    all_data = pd.DataFrame()
    aux_time = start_date
    while aux_time <= end_date:
        df = get_binance_data(symbol, interval, aux_time, end_date)
        if df.empty:
            break
        all_data = pd.concat([all_data, df], ignore_index=True)
        aux_time = df["Close time"].iloc[-1] + timedelta(milliseconds=1)
        time.sleep(0.5)
    return all_data


def parallel_fetch(symbol, interval, start_date, end_date, num_threads=10):
    # Calculate the time delta for each thread
    total_days = (end_date - start_date).days
    days_per_thread = total_days // num_threads

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            part_start_date = start_date + timedelta(days=i * days_per_thread)
            if i == num_threads - 1:
                part_end_date = end_date
            else:
                part_end_date = start_date + timedelta(days=(i + 1) * days_per_thread - 1)

            futures.append(
                executor.submit(
                    fetch_data_for_period, symbol, interval, part_start_date, part_end_date
                )
            )

        all_data = pd.DataFrame()
        for future in as_completed(futures):
            all_data = pd.concat([all_data, future.result()], ignore_index=True)

    return all_data


if __name__ == "__main__":
    start_date = datetime(2010, 7, 17)
    end_date = datetime.now()
    all_data = parallel_fetch("BTCUSDT", "1d", start_date, end_date)
    all_data = all_data.sort_values(by=["Open time"]).reset_index().drop("index", axis=1)
    all_data.to_csv("bitcoin_info.csv", index=False)
